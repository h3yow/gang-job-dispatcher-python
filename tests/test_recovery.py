"""Restart recovery: jobs, capacity accounting and fencing tokens survive."""
import logging

from dispatcher.models import JobState
from tests.helpers import job, make_scheduler


def test_restart_recovers_jobs_and_capacity(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, cpu=4.0)
    s.submit([job("a", cpu=1.0), job("b", cpu=1.0, replicas=2)])
    _, tok = s.claim("w1")           # lease job "a"
    s.report("a", tok, success=True)  # finish "a"
    s.close()

    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        s2 = make_scheduler(tmp_path, clock, cpu=4.0)  # simulate restart
    states = {j.job_id: j.state for j in s2.jobs()}
    assert states == {"a": JobState.SUCCEEDED, "b": JobState.READY}
    # capacity accounting recovered: only "b" (2 cpu) still reserved
    assert s2.available().cpu == 2.0
    # over-capacity submission must still be rejected after restart
    from dispatcher.scheduler import RejectReason
    ok, reason = s2.submit([job("c", cpu=3.0)])
    assert (ok, reason) == (False, RejectReason.INSUFFICIENT_CAPACITY)
    assert any("recovered 2 jobs" in r.getMessage() for r in caplog.records)


def test_restart_recovers_lease_and_fencing_monotonicity(tmp_path, clock):
    s = make_scheduler(tmp_path, clock, lease=10.0)
    s.submit([job("j1")])
    _, tok1 = s.claim("w1")
    s.close()

    s2 = make_scheduler(tmp_path, clock, lease=10.0)
    # lease still valid (clock not advanced): job not claimable yet
    j, _ = s2.claim("w2")
    assert j is None
    # stale token from before restart is still the *current* one
    ok, _ = s2.report("j1", tok1, success=True)
    assert ok

    # fencing tokens keep increasing across restarts
    s2.submit([job("j2")])
    _, tok2 = s2.claim("w9")
    assert tok2 > tok1


def test_restart_with_expired_lease_requeues_job(tmp_path, clock):
    s = make_scheduler(tmp_path, clock, lease=10.0)
    s.submit([job("j1")])
    s.claim("w1")
    s.close()

    clock.advance(11.0)  # lease expires while "down"
    s2 = make_scheduler(tmp_path, clock, lease=10.0)
    j, tok = s2.claim("w2")
    assert j is not None and j.job_id == "j1"
    assert s2.report("j1", tok, success=True) == (True, None)


def test_capacity_contention_between_gangs(tmp_path, clock, caplog):
    """Two gangs race for remaining capacity; exactly one wins, no oversell."""
    import threading
    s = make_scheduler(tmp_path, clock, cpu=4.0)
    s.submit([job("base", cpu=1.0)])  # 3 cpu left

    outcomes = []
    def race(gid):
        outcomes.append(s.submit([job(f"{gid}-1", group=gid, cpu=2.0),
                                  job(f"{gid}-2", group=gid, cpu=1.0)]))
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        threads = [threading.Thread(target=race, args=(g,)) for g in ("ga", "gb")]
        for t in threads: t.start()
        for t in threads: t.join()

    wins = [o for o in outcomes if o[0]]
    losses = [o for o in outcomes if not o[0]]
    assert len(wins) == 1 and len(losses) == 1
    assert losses[0][1] == "INSUFFICIENT_CAPACITY"
    # no oversell: exactly 4 cpu reserved, 0 available
    assert s.available().cpu == 0.0
    # losing gang left no residue
    loser = "gb" if any(j.job_id.startswith("ga") for j in s.jobs()) else "ga"
    assert all(not j.job_id.startswith(loser) for j in s.jobs())
