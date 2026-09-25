"""快照持久化：写入 -> 重新加载 -> 状态一致。"""

from scheduler.models import Gang, Job, JobState, Lease, Resources, Snapshot
from scheduler.store import JsonSnapshotStore


def test_json_snapshot_roundtrip(tmp_path):
    path = str(tmp_path / "state" / "sched.json")
    store = JsonSnapshotStore(path)
    assert store.load() is None

    snap = Snapshot(capacity=Resources(cpu=16, memory_mb=8192, gpu=2))
    job = Job(
        job_id="j1",
        gang_id="g1",
        dedup_key="d1",
        per_replica=Resources(cpu=2, memory_mb=512),
        replicas=3,
        state=JobState.RUNNING,
        lease=Lease(fence_token=7, worker_id="w1", issued_at=1.0, expires_at=99.0),
        submitted_at=1.0,
    )
    snap.jobs["j1"] = job
    snap.gangs["g1"] = Gang(gang_id="g1", member_ids=["j1"], submitted_at=1.0)
    snap.dedup_index["d1"] = "g1"
    store.save(snap)

    restored = store.load()
    assert restored is not None
    assert restored.capacity == Resources(cpu=16, memory_mb=8192, gpu=2)
    rj = restored.jobs["j1"]
    assert rj.state is JobState.RUNNING
    assert rj.demand == Resources(cpu=6, memory_mb=1536, gpu=0)
    assert rj.lease is not None and rj.lease.fence_token == 7
    assert restored.gangs["g1"].member_ids == ["j1"]
    assert restored.dedup_index == {"d1": "g1"}
