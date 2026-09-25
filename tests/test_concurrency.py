"""并发场景测试：并发领取、容量竞争、并发重复提交。

验证在多线程竞争下：
- 同一作业只有一个执行者领取成功，不出现并发重复执行；
- 超额成组提交在容量竞争下只有能整体容纳的组成功，失败组不残留占位；
- 相同 dedup_key 的并发提交只有一份可执行作业。
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from scheduler.errors import CapacityExceeded, DuplicateSubmission, LeaseViolation
from scheduler.models import Resources
from scheduler.scheduler import Scheduler
from scheduler.store import InMemorySnapshotStore


class FakeClock:
    def __init__(self, t=100.0):
        self.t = t
        self._lock = threading.Lock()

    def __call__(self):
        with self._lock:
            return self.t

    def advance(self, dt):
        with self._lock:
            self.t += dt


def _scheduler(cpu=10, mem=10000, gpu=2, ttl=100.0):
    return Scheduler(
        Resources(cpu=cpu, memory_mb=mem, gpu=gpu),
        InMemorySnapshotStore(),
        lease_ttl_seconds=ttl,
        clock=FakeClock(),
    )


def test_concurrent_claim_only_one_winner():
    s = _scheduler()
    s.submit_gang(
        [{"job_id": "j", "resources": {"cpu": 1}}], gang_id="g", dedup_key="k"
    )
    winners: list[str] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(16)

    def attempt(worker_id: str):
        barrier.wait()
        try:
            resp = s.claim("j", worker_id)
            winners.append(f"{worker_id}:{resp['lease']['fence_token']}")
        except LeaseViolation as exc:
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(attempt, [f"w{i}" for i in range(16)]))

    # 恰好一个领取成功，其余全部围栏冲突；作业只有一份、一个 RUNNING 租约
    assert len(winners) == 1, winners
    assert len(errors) == 15
    job = s.get_job("j")
    assert job["state"] == "RUNNING"
    assert job["lease"]["fence_token"] == 1
    assert job["lease"]["worker_id"] == winners[0].split(":")[0]


def test_concurrent_capacity_contention():
    # 容量 6 cpu：两组各需要 4 cpu，只有一组能整体成功
    s = _scheduler(cpu=6)
    results: dict[str, object] = {}
    barrier = threading.Barrier(2)

    def submit(gang_id: str):
        barrier.wait()
        try:
            s.submit_gang(
                [{"resources": {"cpu": 4}}], gang_id=gang_id, dedup_key=f"key-{gang_id}"
            )
            results[gang_id] = "ok"
        except CapacityExceeded as exc:
            results[gang_id] = exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(submit, ["g1", "g2"]))

    oks = [g for g, v in results.items() if v == "ok"]
    rejected = [g for g, v in results.items() if v == "capacity_exceeded"]
    assert len(oks) == 1, results
    assert len(rejected) == 1
    # 容量恰好被成功的那组占用 4 cpu，失败组没有残留
    view = s.capacity_view()
    assert view["used"]["cpu"] == 4
    assert len(view["consumers"]) == 1


def test_concurrent_same_dedup_key_single_gang():
    s = _scheduler(cpu=100)
    outcomes: list[str] = []
    barrier = threading.Barrier(12)

    def attempt(i: int):
        barrier.wait()
        try:
            s.submit_gang(
                [{"resources": {"cpu": 1}}], gang_id=f"g-{i}", dedup_key="same"
            )
            outcomes.append("ok")
        except DuplicateSubmission:
            outcomes.append("dup")

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(attempt, range(12)))

    assert outcomes.count("ok") == 1
    assert outcomes.count("dup") == 11
    # 只有一份可执行作业、只占 1 cpu
    assert s.capacity_view()["used"]["cpu"] == 1
    assert len(s.capacity_view()["consumers"]) == 1


def test_concurrent_claims_then_single_result_counted(caplog):
    s = _scheduler()
    s.submit_gang(
        [{"job_id": "j", "resources": {"cpu": 1}}], gang_id="g", dedup_key="k"
    )
    # w1 先合法领取
    token = s.claim("j", "w1")["lease"]["fence_token"]

    done: list[str] = []
    caplog.set_level(logging.INFO, logger="scheduler")

    def report(i: int):
        try:
            s.report_result("j", token + i, "SUCCEEDED" if i == 0 else "FAILED")
            done.append(f"ok-{i}")
        except LeaseViolation:
            done.append(f"deny-{i}")

    # i=0 用正确 token 报 SUCCEEDED；其余用错误 token，全部必须拒绝
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(report, range(8)))

    assert done.count("ok-0") == 1
    assert sum(1 for d in done if d.startswith("deny")) == 7
    assert s.get_job("j")["state"] == "SUCCEEDED"
    # 日志里必须出现作业标识、容量变化、判定依据
    logs = [r.message for r in caplog.records]
    assert any("结果计入 job=j" in m and "容量变化" in m for m in logs)
