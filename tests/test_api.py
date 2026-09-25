"""HTTP 端到端测试：状态码区分拒绝原因、租约围栏流程、重启恢复。"""

import pytest
from fastapi.testclient import TestClient

from scheduler.api import create_app
from scheduler.models import Resources
from scheduler.scheduler import Scheduler
from scheduler.store import InMemorySnapshotStore, JsonSnapshotStore


class FakeClock:
    def __init__(self, t=100.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


@pytest.fixture
def client():
    clock = FakeClock()
    sched = Scheduler(
        Resources(cpu=8, memory_mb=8192, gpu=2),
        InMemorySnapshotStore(),
        lease_ttl_seconds=10,
        clock=clock,
    )
    app = create_app(scheduler=sched)
    app.state.clock = clock
    return TestClient(app), clock


def submit(client, jobs, gang_id, dedup_key, expected=201):
    resp = client.post(
        "/gangs",
        json={"jobs": jobs, "gang_id": gang_id, "dedup_key": dedup_key},
    )
    assert resp.status_code == expected, resp.text
    return resp


def test_submit_and_distinguishable_rejections(client):
    c, _ = client
    # 非法提交 -> 422
    r = c.post("/gangs", json={"jobs": [{"resources": {"cpu": -1}}], "dedup_key": "k"})
    assert r.status_code == 422 and r.json()["error"] == "invalid_job_submission"
    # 未知字段 -> 422（pydantic extra forbid）
    r = c.post("/gangs", json={"jobs": [{"resources": {"cpu": 1, "npu": 1}}], "dedup_key": "k"})
    assert r.status_code == 422

    jobs = [
        {"job_id": "a", "resources": {"cpu": 4, "memory_mb": 2048, "gpu": 1}, "replicas": 2}
    ]
    r = submit(c, jobs, "g1", "k1")
    assert r.json()["allocated"] is True
    # 重复提交 -> 409 duplicate_submission
    r = c.post("/gangs", json={"jobs": jobs, "gang_id": "g2", "dedup_key": "k1"})
    assert r.status_code == 409 and r.json()["error"] == "duplicate_submission"
    assert r.json()["details"]["existing_gang_id"] == "g1"
    # 超额 -> 409 capacity_exceeded（已用 8 cpu）
    r = c.post(
        "/gangs",
        json={"jobs": [{"resources": {"cpu": 1}}], "dedup_key": "k2"},
    )
    assert r.status_code == 409 and r.json()["error"] == "capacity_exceeded"
    assert "cpu" in r.json()["details"]["violating_dims"]


def test_claim_heartbeat_result_fencing_flow(client):
    c, clock = client
    submit(c, [{"job_id": "a", "resources": {"cpu": 1, "gpu": 1}}], "g1", "k1")
    r = c.post("/jobs/a/claim", json={"worker_id": "w1"})
    assert r.status_code == 200
    token = r.json()["lease"]["fence_token"]
    # 并发的另一执行者领取 -> 409
    r = c.post("/jobs/a/claim", json={"worker_id": "w2"})
    assert r.status_code == 409 and r.json()["error"] == "lease_violation"
    # 错围栏心跳
    assert c.post("/jobs/a/heartbeat", json={"fence_token": token + 1}).status_code == 409
    # 正确围栏心跳
    assert c.post("/jobs/a/heartbeat", json={"fence_token": token}).status_code == 200
    # 非法 result -> 422
    r = c.post("/jobs/a/result", json={"fence_token": token, "result": "BOGUS"})
    assert r.status_code == 422
    # 正确上报
    r = c.post("/jobs/a/result", json={"fence_token": token, "result": "SUCCEEDED"})
    assert r.status_code == 200 and r.json()["state"] == "SUCCEEDED"
    # 重复上报 -> 409，结果不重复计入
    r = c.post("/jobs/a/result", json={"fence_token": token, "result": "SUCCEEDED"})
    assert r.status_code == 200  # 同围栏幂等
    r = c.post("/jobs/a/result", json={"fence_token": token - 1, "result": "SUCCEEDED"})
    assert r.status_code == 409
    # 容量已释放
    cap = c.get("/capacity").json()
    assert cap["used"] == {"cpu": 0, "memory_mb": 0, "gpu": 0}


def test_lease_expiry_over_http(client):
    c, clock = client
    submit(c, [{"job_id": "a", "resources": {"cpu": 1}}], "g1", "k1")
    old = c.post("/jobs/a/claim", json={"worker_id": "w1"}).json()["lease"]["fence_token"]
    clock.advance(10)
    # 旧围栏上报过期被拒
    r = c.post("/jobs/a/result", json={"fence_token": old, "result": "SUCCEEDED"})
    assert r.status_code == 409
    # w2 过期接管
    r = c.post("/jobs/a/claim", json={"worker_id": "w2"})
    new = r.json()["lease"]["fence_token"]
    assert new > old
    assert c.post("/jobs/a/result", json={"fence_token": new, "result": "FAILED"}).status_code == 200


def test_cancel_preempt_and_queries(client):
    c, _ = client
    submit(
        c,
        [
            {"job_id": "a", "resources": {"cpu": 4, "gpu": 2}},
            {"job_id": "b", "resources": {"cpu": 1}},
        ],
        "g1", "k1",
    )
    token = c.post("/jobs/a/claim", json={"worker_id": "w1"}).json()["lease"]["fence_token"]
    r = c.post("/gangs/g1/cancel")
    assert r.status_code == 200
    assert {m["state"] for m in r.json()["members"]} == {"CANCELLED"}
    # 租约被撤销
    assert c.post("/jobs/a/heartbeat", json={"fence_token": token}).status_code == 409
    assert c.get("/jobs/a").json()["state"] == "CANCELLED"
    assert c.get("/gangs/g1").status_code == 200
    assert c.get("/gangs/missing").status_code == 404
    assert c.get("/jobs/missing").status_code == 404


def test_restart_recovery_over_http(tmp_path):
    path = str(tmp_path / "sched.json")
    clock = FakeClock()
    app1 = create_app(
        scheduler=Scheduler(
            Resources(cpu=8, memory_mb=8192, gpu=2),
            JsonSnapshotStore(path),
            lease_ttl_seconds=10,
            clock=clock,
        )
    )
    with TestClient(app1) as c1:
        r = c1.post(
            "/gangs",
            json={
                "jobs": [{"job_id": "a", "resources": {"cpu": 2, "gpu": 1}, "replicas": 2}],
                "gang_id": "g1",
                "dedup_key": "k1",
            },
        )
        assert r.status_code == 201
        token = c1.post("/jobs/a/claim", json={"worker_id": "w1"}).json()["lease"]["fence_token"]

    # 模拟进程重启：新 app + 同一文件
    app2 = create_app(
        capacity=Resources(cpu=99, memory_mb=99, gpu=99),  # 应被快照覆盖
        store_path=path,
        lease_ttl_seconds=10,
    )
    # 重启后需要时钟保持单调：注入同一 clock
    app2.state.scheduler._clock = clock
    with TestClient(app2) as c2:
        cap = c2.get("/capacity").json()
        assert cap["capacity"] == {"cpu": 8, "memory_mb": 8192, "gpu": 2}
        assert cap["used"] == {"cpu": 4, "memory_mb": 0, "gpu": 2}
        job = c2.get("/jobs/a").json()
        assert job["state"] == "RUNNING"
        r = c2.post("/jobs/a/result", json={"fence_token": token, "result": "SUCCEEDED"})
        assert r.status_code == 200
        # 重启后 dedup 仍生效
        r = c2.post(
            "/gangs",
            json={"jobs": [{"resources": {"cpu": 1}}], "gang_id": "g2", "dedup_key": "k1"},
        )
        assert r.status_code == 409 and r.json()["error"] == "duplicate_submission"
