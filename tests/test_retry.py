"""Bounded retry: requeue on failure, fresh identity per attempt, gang fate."""
import logging

from dispatcher.models import JobState, DEFAULT_MAX_ATTEMPTS
from dispatcher.scheduler import RejectReason
from tests.helpers import job, make_scheduler


def _states(s):
    return {j.job_id: j.state for j in s.jobs()}


def test_failure_requeues_with_fresh_identity(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, lease=10.0)
    s.submit([job("j1", max_attempts=2)])
    _, tok1 = s.claim("w1")
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        ok, _ = s.report("j1", tok1, success=False)
    assert ok
    # not terminal: back to READY, still claimable
    assert _states(s) == {"j1": JobState.READY}
    # re-claim is a brand-new attempt: bigger token, attempt counted
    j2, tok2 = s.claim("w2")
    assert j2.job_id == "j1" and tok2 > tok1 and j2.attempt == 2
    # the old identity is fenced off for good
    ok, reason = s.report("j1", tok1, success=True)
    assert (ok, reason) == (False, RejectReason.STALE_FENCING_TOKEN)
    ok, reason = s.renew("j1", tok1)
    assert (ok, reason) == (False, RejectReason.LEASE_LOST)
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "attempt=1/2" in msg and "re-queued" in msg and "j1" in msg


def test_retry_exhaustion_marks_failed_and_releases(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, cpu=2.0)
    s.submit([job("j1", cpu=2.0, max_attempts=2)])
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        for _ in range(2):
            _, tok = s.claim("w")
            ok, _ = s.report("j1", tok, success=False)
            assert ok
    assert _states(s) == {"j1": JobState.FAILED}
    # capacity released exactly once, at the terminal transition
    assert s.available().cpu == 2.0
    # nothing left to claim; further reports rejected (no double counting)
    assert s.claim("w")[0] is None
    ok, reason = s.report("j1", tok, success=False)
    assert (ok, reason) == (False, RejectReason.ILLEGAL_STATE)
    assert s.available().cpu == 2.0
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "attempt=2/2" in msg and "FAILED" in msg


def test_capacity_held_during_retry_no_double_release(tmp_path, clock):
    """Reservation stays for the whole retry loop; released once at the end."""
    s = make_scheduler(tmp_path, clock, cpu=4.0)
    s.submit([job("j1", cpu=3.0, max_attempts=3)])
    assert s.available().cpu == 1.0
    for expected_attempt in (1, 2):
        _, tok = s.claim("w")
        assert s.report("j1", tok, success=False) == (True, None)
        # retry never frees and never double-reserves
        assert s.available().cpu == 1.0
    _, tok = s.claim("w")
    assert s.report("j1", tok, success=False) == (True, None)
    assert _states(s) == {"j1": JobState.FAILED}
    assert s.available().cpu == 4.0  # released exactly once


def test_default_max_attempts_applied(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1")])  # no explicit max_attempts
    for _ in range(DEFAULT_MAX_ATTEMPTS - 1):
        _, tok = s.claim("w")
        assert s.report("j1", tok, success=False) == (True, None)
        assert _states(s) == {"j1": JobState.READY}
    _, tok = s.claim("w")
    s.report("j1", tok, success=False)
    assert _states(s) == {"j1": JobState.FAILED}


def test_success_on_retry_counts_result_once(tmp_path, clock):
    s = make_scheduler(tmp_path, clock, cpu=2.0)
    s.submit([job("j1", cpu=2.0, max_attempts=2)])
    _, tok1 = s.claim("w1")
    s.report("j1", tok1, success=False)
    _, tok2 = s.claim("w2")
    assert s.report("j1", tok2, success=True, result="done") == (True, None)
    assert _states(s) == {"j1": JobState.SUCCEEDED}
    assert s.available().cpu == 2.0
    # duplicate report of the same result is refused, capacity untouched
    ok, reason = s.report("j1", tok2, success=True, result="done")
    assert (ok, reason) == (False, RejectReason.ILLEGAL_STATE)
    assert s.available().cpu == 2.0


def test_gang_retry_is_atomic(tmp_path, clock, caplog):
    """One member fails -> the whole gang (incl. a SUCCEEDED member) re-runs."""
    s = make_scheduler(tmp_path, clock, cpu=4.0)
    s.submit([job("a", group="g", cpu=1.0, max_attempts=2),
              job("b", group="g", cpu=1.0, max_attempts=2)])
    _, tok_a = s.claim("w1")   # claims a
    _, tok_b = s.claim("w2")   # claims b
    assert s.report("a", tok_a, success=True) == (True, None)
    # gang still open -> reservation NOT released by a's success
    assert s.available().cpu == 2.0
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        assert s.report("b", tok_b, success=False) == (True, None)
    # the whole gang is claimable again: no member left behind in a terminal
    # state while siblings retry
    assert _states(s) == {"a": JobState.READY, "b": JobState.READY}
    # b's live lease was revoked by the gang reset
    ok, reason = s.report("b", tok_b, success=True)
    assert (ok, reason) == (False, RejectReason.STALE_FENCING_TOKEN)
    # capacity held throughout the retry (no oversell window)
    assert s.available().cpu == 2.0
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "gang" in msg and "['a', 'b']" in msg and "re-queued" in msg


def test_gang_exhaustion_ends_whole_gang(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, cpu=4.0)
    s.submit([job("a", group="g", cpu=1.0, max_attempts=1),
              job("b", group="g", cpu=1.0, max_attempts=5)])
    _, tok_a = s.claim("w1")
    _, tok_b = s.claim("w2")
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        # a is out of budget -> the entire gang ends, b's lease revoked
        assert s.report("a", tok_a, success=False) == (True, None)
    assert _states(s) == {"a": JobState.FAILED, "b": JobState.FAILED}
    ok, reason = s.report("b", tok_b, success=True)
    assert (ok, reason) == (False, RejectReason.ILLEGAL_STATE)
    # gang reservation released as a whole, exactly once
    assert s.available().cpu == 4.0
    assert s.claim("w9")[0] is None
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "gang" in msg and "FAILED" in msg and "stale_token" in msg


def test_retry_state_survives_restart(tmp_path, clock):
    s = make_scheduler(tmp_path, clock, cpu=2.0)
    s.submit([job("j1", cpu=2.0, max_attempts=2)])
    _, tok1 = s.claim("w1")
    s.report("j1", tok1, success=False)  # attempt 1 spent, back to READY
    s.close()

    s2 = make_scheduler(tmp_path, clock, cpu=2.0)
    j = s2.jobs()[0]
    assert (j.state, j.attempt, j.max_attempts) == (JobState.READY, 1, 2)
    assert s2.available().cpu == 0.0  # reservation recovered
    _, tok2 = s2.claim("w2")
    assert tok2 > tok1
    s2.report("j1", tok2, success=False)  # budget exhausted
    assert _states(s2) == {"j1": JobState.FAILED}
    assert s2.available().cpu == 2.0
