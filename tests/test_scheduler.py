"""调度器核心行为测试。

使用可控时钟（FakeClock）精确驱动租约过期；
caplog 校验日志中打印作业标识、容量变化与判定依据。
"""

import logging

import pytest

from scheduler.errors import (
    CapacityExceeded,
    DuplicateSubmission,
    InvalidJobSubmission,
    InvalidStateTransition,
    LeaseViolation,
)
from scheduler.models import Resources
from scheduler.scheduler import Scheduler
from scheduler.store import InMemorySnapshotStore


class FakeClock:
    def __init__(self, t=100.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def make_scheduler(cpu=10, mem=10240, gpu=2, ttl=10.0, clock=None, store=None):
    return Scheduler(
        Resources(cpu=cpu, memory_mb=mem, gpu=gpu),
        store=store,
        lease_ttl_seconds=ttl,
        clock=clock or FakeClock(),
    )


JOB_A = {"job_id": "a", "resources": {"cpu": 2, "memory_mb": 1024, "gpu": 1}, "replicas": 2}
JOB_B = {"job_id": "b", "resources": {"cpu": 1, "memory_mb": 512}, "replicas": 1}


def test_submit_success_capacity_accounting(caplog):
    clock = FakeClock()
    s = make_scheduler(cpu=10, mem=4096, gpu=2, clock=clock)
    with caplog.at_level(logging.INFO, logger="scheduler"):
        resp = s.submit_gang([JOB_A, JOB_B], gang_id="g1", dedup_key="k1")
    assert resp["allocated"] is True
    assert [j["state"] for j in resp["jobs"]] == ["READY", "READY"]
    # a: 2cpu*2=4, 2048mem, 2gpu ; b: 1cpu 512mem -> used 5/2560/2
    cap = s.capacity_view()
    assert cap["used"] == {"cpu": 5, "memory_mb": 2560, "gpu": 2}
    assert cap["available"] == {"cpu": 5, "memory_mb": 1536, "gpu": 0}
    assert any("成组调度成功" in r.message and "容量变化" in r.message for r in caplog.records)


def test_invalid_submission_variants():
    s = make_scheduler()
    with pytest.raises(InvalidJobSubmission):
        s.submit_gang([], dedup_key="k")  # 空组
    with pytest.raises(InvalidJobSubmission):
        s.submit_gang([{"resources": {"cpu": -1}}], dedup_key="k")  # 负资源
    with pytest.raises(InvalidJobSubmission):
        s.submit_gang([{"resources": {"cpu": 1}, "replicas": 0}], dedup_key="k")
    with pytest.raises(InvalidJobSubmission):
        s.submit_gang([{"resources": {"cpu": 1}, "replicas": 2}], dedup_key="")  # 缺 dedup
    with pytest.raises(InvalidJobSubmission):
        s.submit_gang([{"resources": {"npu": 1}}], dedup_key="k")  # 未知维度
    # 非法提交不得落库、不得占容量
    assert s.capacity_view()["used"] == {"cpu": 0, "memory_mb": 0, "gpu": 0}


def test_capacity_exceeded_is_atomic_and_distinguishable():
    s = make_scheduler(cpu=3, mem=10000, gpu=10)
    # a 需要 4 cpu -> 超额
    with pytest.raises(CapacityExceeded) as ei:
        s.submit_gang([JOB_A, JOB_B], gang_id="g-bad", dedup_key="k-bad")
    assert ei.value.code == "capacity_exceeded"
    assert "cpu" in ei.value.details["violating_dims"]
    # 失败的组不残留任何作业/占位
    from scheduler.errors import JobNotFound

    assert s.capacity_view()["used"] == {"cpu": 0, "memory_mb": 0, "gpu": 0}
    with pytest.raises(JobNotFound):
        s.get_job("a")


def test_duplicate_dedup_key_rejected_without_extra_jobs():
    s = make_scheduler(cpu=100, mem=100000, gpu=10)
    first = s.submit_gang([JOB_A], gang_id="g1", dedup_key="same-key")
    with pytest.raises(DuplicateSubmission) as ei:
        s.submit_gang([JOB_B], gang_id="g2", dedup_key="same-key")
    assert ei.value.details["existing_gang_id"] == "g1"
    # 重复提交没有产生第二份可执行作业，容量只有第一组
    assert len(s.get_gang("g1")["jobs"]) == 1
    assert s.capacity_view()["used"]["cpu"] == 4
    assert first["jobs"][0]["job_id"] == "a"


def test_claim_fence_heartbeat_and_report():
    clock = FakeClock()
    s = make_scheduler(cpu=10, mem=10000, gpu=2, ttl=10, clock=clock)
    s.submit_gang([JOB_A], gang_id="g1", dedup_key="k1")
    c1 = s.claim("a", "w1")
    token = c1["lease"]["fence_token"]
    assert c1["state"] == "RUNNING"
    # 同 worker 重试幂等，token 不变
    assert s.claim("a", "w1")["lease"]["fence_token"] == token
    # 其他 worker 在租约有效期内不能抢占 -> 无并发重复执行
    with pytest.raises(LeaseViolation):
        s.claim("a", "w2")
    # 旧/错围栏心跳被拒
    with pytest.raises(LeaseViolation):
        s.heartbeat("a", token + 999)
    clock.advance(5)
    s.heartbeat("a", token)  # 续租
    clock.advance(8)  # 原 ttl 已过，但心跳后才过 8 < 10
    assert s.claim("a", "w1")["lease"]["fence_token"] == token


def test_lease_expiry_reassign_old_fence_rejected():
    clock = FakeClock()
    s = make_scheduler(cpu=10, mem=10000, gpu=2, ttl=10, clock=clock)
    s.submit_gang([JOB_A], gang_id="g1", dedup_key="k1")
    c1 = s.claim("a", "w1")
    old = c1["lease"]["fence_token"]
    clock.advance(10)  # 过期
    # 旧持有者过期后上报被拒
    with pytest.raises(LeaseViolation):
        s.report_result("a", old, "SUCCEEDED")
    # 新领取获得更大围栏
    c2 = s.claim("a", "w2")
    new = c2["lease"]["fence_token"]
    assert new > old and c2["reassigned"] is True
    # 旧围栏之后的心跳/上报全部拒绝
    with pytest.raises(LeaseViolation):
        s.heartbeat("a", old)
    # 新围栏成功上报 -> 终态释放容量
    resp = s.report_result("a", new, "SUCCEEDED")
    assert resp["state"] == "SUCCEEDED"
    assert s.capacity_view()["used"] == {"cpu": 0, "memory_mb": 0, "gpu": 0}
    # 终态不可再领取
    with pytest.raises(InvalidStateTransition):
        s.claim("a", "w3")
    # 旧围栏再上报拒绝；同围栏重试幂等且不重复计入
    with pytest.raises(LeaseViolation):
        s.report_result("a", old, "SUCCEEDED")
    again = s.report_result("a", new, "SUCCEEDED")
    assert again["result_reported"] is True


def test_cancel_and_preempt_deterministic():
    clock = FakeClock()
    s = make_scheduler(cpu=10, mem=10000, gpu=2, ttl=10, clock=clock)
    s.submit_gang([JOB_A, JOB_B], gang_id="g1", dedup_key="k1")
    c = s.claim("a", "w1")
    token = c["lease"]["fence_token"]
    # 取消整组：运行中的 a 租约被撤销，围栏立即失效，容量释放
    resp = s.cancel_gang("g1")
    assert {m["state"] for m in resp["members"]} == {"CANCELLED"}
    with pytest.raises(LeaseViolation):
        s.heartbeat("a", token)
    assert s.capacity_view()["used"] == {"cpu": 0, "memory_mb": 0, "gpu": 0}
    # 对已终态组再次取消，结论确定（幂等，成员仍 CANCELLED）
    again = s.cancel_gang("g1")
    assert all(m["state"] == "CANCELLED" for m in again["members"])


def test_preempt_releases_capacity_for_new_gang():
    clock = FakeClock()
    s = make_scheduler(cpu=6, mem=10000, gpu=2, ttl=10, clock=clock)
    s.submit_gang([JOB_A], gang_id="g1", dedup_key="k1")  # 用 4cpu/2gpu
    assert s.capacity_view()["used"]["gpu"] == 2
    # 新组需要 1 gpu，必然超额
    with pytest.raises(CapacityExceeded):
        s.submit_gang(
            [{"job_id": "c", "resources": {"cpu": 1, "gpu": 1}}],
            gang_id="g2", dedup_key="k2",
        )
    s.preempt_gang("g1")
    # 抢占后容量回到可用，新组可整体进入 READY
    resp = s.submit_gang(
        [{"job_id": "c", "resources": {"cpu": 1, "gpu": 1}}],
        gang_id="g2", dedup_key="k2",
    )
    assert resp["allocated"] is True


def test_restart_recovers_jobs_and_capacity(tmp_path):
    import os
    from scheduler.store import JsonSnapshotStore

    path = str(tmp_path / "state.json")
    clock = FakeClock()
    store = JsonSnapshotStore(path)
    s1 = make_scheduler(cpu=10, mem=10000, gpu=2, ttl=10, clock=clock, store=store)
    s1.submit_gang([JOB_A, JOB_B], gang_id="g1", dedup_key="k1")
    c = s1.claim("a", "w1")
    assert os.path.exists(path)

    # 全新进程：重新加载快照
    s2 = make_scheduler(cpu=99, mem=99, gpu=99, ttl=10, clock=clock, store=store)
    view = s2.capacity_view()
    # 容量以快照为准，而不是新构造参数
    assert view["capacity"] == {"cpu": 10, "memory_mb": 10000, "gpu": 2}
    assert view["used"] == {"cpu": 5, "memory_mb": 2560, "gpu": 2}
    job = s2.get_job("a")
    assert job["state"] == "RUNNING"
    assert job["lease"]["fence_token"] == c["lease"]["fence_token"]
    # 重启后围栏依旧有效：可继续上报
    s2.report_result("a", c["lease"]["fence_token"], "SUCCEEDED")
    assert s2.get_job("a")["state"] == "SUCCEEDED"
