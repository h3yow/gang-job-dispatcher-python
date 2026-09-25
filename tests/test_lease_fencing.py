"""Concurrent claims, lease expiry, fencing tokens, cancel & preempt."""
import logging
import threading

from dispatcher.scheduler import RejectReason
from tests.helpers import job, make_scheduler


def test_concurrent_claims_single_winner(tmp_path, clock, caplog):
    """Many workers race for one job; exactly one lease is granted."""
    s = make_scheduler(tmp_path, clock)
    s.submit([job("hot")])
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        results = []
        def race(w):
            results.append(s.claim(w))
        threads = [threading.Thread(target=race, args=(f"w{i}",)) for i in range(16)]
        for t in threads: t.start()
        for t in threads: t.join()
    winners = [r for r in results if r[0] is not None]
    assert len(winners) == 1
    assert winners[0][1] is not None
    grants = [r.getMessage() for r in caplog.records if "lease granted" in r.getMessage()]
    assert len(grants) == 1 and "hot" in grants[0]


def test_concurrent_claims_no_double_execution(tmp_path, clock):
    """N jobs, M>N workers: each job leased to exactly one worker."""
    s = make_scheduler(tmp_path, clock, cpu=8.0)
    s.submit([job(f"j{i}") for i in range(5)])
    leased = []
    lock = threading.Lock()
    def race(w):
        j, _ = s.claim(w)
        if j:
            with lock: leased.append(j.job_id)
    threads = [threading.Thread(target=race, args=(f"w{i}",)) for i in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert sorted(leased) == [f"j{i}" for i in range(5)]


def test_lease_expiry_allows_reclaim_and_fences_old_token(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, lease=10.0)
    s.submit([job("j1")])
    _, tok1 = s.claim("w1")
    clock.advance(11.0)  # lease expires
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        j2, tok2 = s.claim("w2")
    assert j2 is not None and j2.job_id == "j1"
    assert tok2 > tok1  # fencing tokens strictly increase
    # old token is fenced off
    ok, reason = s.report("j1", tok1, success=True)
    assert (ok, reason) == (False, RejectReason.STALE_FENCING_TOKEN)
    # new token works
    ok, _ = s.report("j1", tok2, success=True)
    assert ok
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "lease expired" in msg and "j1" in msg and str(tok1) in msg


def test_report_with_wrong_token_rejected(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1")])
    s.claim("w1")
    ok, reason = s.report("j1", 999, success=True)
    assert (ok, reason) == (False, RejectReason.STALE_FENCING_TOKEN)


def test_result_not_counted_twice(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1")])
    _, tok = s.claim("w1")
    assert s.report("j1", tok, success=True) == (True, None)
    ok, reason = s.report("j1", tok, success=True)
    assert (ok, reason) == (False, RejectReason.ILLEGAL_STATE)
    # capacity released exactly once
    assert s.available().cpu == 4.0


def test_cancel_leased_job_fences_token_and_releases(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, cpu=2.0)
    s.submit([job("j1", cpu=2.0)])
    _, tok = s.claim("w1")
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        ok, _ = s.cancel("j1")
    assert ok
    assert s.available().cpu == 2.0
    ok, reason = s.report("j1", tok, success=True)
    assert (ok, reason) == (False, RejectReason.ILLEGAL_STATE)
    # cancel is idempotent / deterministic
    assert s.cancel("j1") == (True, None)
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "j1" in msg and "cancelled" in msg


def test_cancel_unknown_job(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    ok, reason = s.cancel("nope")
    assert (ok, reason) == (False, RejectReason.UNKNOWN_JOB)


def test_preempt_requeues_and_fences_old_lease(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1")])
    _, tok1 = s.claim("w1")
    ok, _ = s.preempt("j1")
    assert ok
    # capacity still reserved (job is READY again, not terminal)
    assert s.available().cpu == 3.0
    j2, tok2 = s.claim("w2")
    assert j2.job_id == "j1" and tok2 > tok1
    ok, reason = s.report("j1", tok1, success=True)
    assert (ok, reason) == (False, RejectReason.STALE_FENCING_TOKEN)
    assert s.report("j1", tok2, success=False) == (True, None)


def test_preempt_non_leased_rejected(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1")])
    ok, reason = s.preempt("j1")
    assert (ok, reason) == (False, RejectReason.ILLEGAL_STATE)
