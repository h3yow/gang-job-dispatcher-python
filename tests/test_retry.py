"""Retries: fresh attempt identity, capacity accounting, gang atomicity."""
import logging

from dispatcher.models import DEFAULT_MAX_ATTEMPTS, JobState
from dispatcher.scheduler import RejectReason
from tests.helpers import job, make_scheduler


def states(s):
    return {j.job_id: j.state for j in s.jobs()}


def test_failure_below_cap_requeues_with_fresh_identity(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1", max_attempts=3)])
    _, tok1 = s.claim("w1")
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        assert s.report("j1", tok1, success=False, result="boom") == (True, None)
    # back to READY, attempt counter kept
    j1 = [j for j in s.jobs()][0]
    assert j1.state == JobState.READY and j1.attempts == 1
    # old identity is fenced off immediately
    assert s.report("j1", tok1, success=True) == \
        (False, RejectReason.STALE_FENCING_TOKEN)
    # re-claim is a brand-new attempt with a distinguishable identity
    j2, tok2 = s.claim("w2")
    assert j2.job_id == "j1" and tok2 > tok1 and j2.attempts == 2
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "retry" in msg and "j1" in msg and "attempt=1/3" in msg


def test_retry_until_cap_then_terminal(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, cpu=4.0)
    s.submit([job("j1", cpu=2.0, max_attempts=2)])
    assert s.available().cpu == 2.0

    _, tok1 = s.claim("w1")
    assert s.report("j1", tok1, success=False) == (True, None)
    assert states(s)["j1"] == JobState.READY
    assert s.available().cpu == 2.0  # still reserved during retry

    _, tok2 = s.claim("w2")
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        assert s.report("j1", tok2, success=False) == (True, None)
    # cap reached: terminally FAILED, capacity released exactly once
    assert states(s)["j1"] == JobState.FAILED
    assert s.available().cpu == 4.0
    # result cannot be counted twice
    assert s.report("j1", tok2, success=False) == \
        (False, RejectReason.ILLEGAL_STATE)
    assert s.available().cpu == 4.0
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "cap reached" in msg and "attempt=2/2" in msg


def test_default_max_attempts_when_unspecified(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1")])  # no explicit max_attempts
    j1 = [j for j in s.jobs()][0]
    assert j1.max_attempts == DEFAULT_MAX_ATTEMPTS
    for i in range(DEFAULT_MAX_ATTEMPTS - 1):
        _, tok = s.claim("w")
        assert s.report("j1", tok, success=False) == (True, None)
        assert states(s)["j1"] == JobState.READY
    _, tok = s.claim("w")
    s.report("j1", tok, success=False)
    assert states(s)["j1"] == JobState.FAILED


def test_capacity_not_oversold_during_retry(tmp_path, clock):
    s = make_scheduler(tmp_path, clock, cpu=4.0)
    s.submit([job("j1", cpu=3.0, max_attempts=2)])
    _, tok1 = s.claim("w1")
    assert s.report("j1", tok1, success=False) == (True, None)
    # retry keeps the 3-cpu reservation: a 2-cpu job must not fit
    ok, reason = s.submit([job("j2", cpu=2.0)])
    assert (ok, reason) == (False, RejectReason.INSUFFICIENT_CAPACITY)
    # after the final failure the reservation is released and j2 fits
    _, tok2 = s.claim("w2")
    s.report("j1", tok2, success=False)
    assert states(s)["j1"] == JobState.FAILED
    assert s.submit([job("j2", cpu=2.0)]) == (True, None)


def test_invalid_max_attempts_rejected(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    ok, reason = s.submit([job("j1", max_attempts=0)])
    assert (ok, reason) == (False, RejectReason.INVALID_RESOURCES)


def test_gang_retry_is_atomic(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, cpu=8.0)
    s.submit([job("a", group="g", max_attempts=2),
              job("b", group="g", max_attempts=2)])
    _, tok_a = s.claim("w1")
    jb, tok_b = s.claim("w2")
    assert jb.job_id == "b"

    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        assert s.report("a", tok_a, success=False) == (True, None)
    # the whole group moved back to READY together — no member left LEASED
    assert states(s) == {"a": JobState.READY, "b": JobState.READY}
    # b's outstanding lease is fenced as part of the same decision
    assert s.report("b", tok_b, success=True) == \
        (False, RejectReason.STALE_FENCING_TOKEN)
    # capacity for the whole gang stayed reserved throughout
    assert s.available().cpu == 6.0
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "requeued=['a', 'b']" in msg


def test_gang_terminates_together_at_cap(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, cpu=8.0)
    s.submit([job("a", group="g", cpu=2.0, max_attempts=1),
              job("b", group="g", cpu=2.0, max_attempts=1)])
    _, tok_a = s.claim("w1")
    _, tok_b = s.claim("w2")
    assert s.available().cpu == 4.0

    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        assert s.report("a", tok_a, success=False) == (True, None)
    # one member hit the cap -> the whole gang ends together
    assert states(s) == {"a": JobState.FAILED, "b": JobState.FAILED}
    # both members' capacity released exactly once
    assert s.available().cpu == 8.0
    # b's lease fenced; neither member accepts results any more
    assert s.report("b", tok_b, success=True) == \
        (False, RejectReason.ILLEGAL_STATE)
    assert s.report("a", tok_a, success=False) == \
        (False, RejectReason.ILLEGAL_STATE)
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "terminated=['a', 'b']" in msg


def test_restart_preserves_attempts_and_retry_budget(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1", max_attempts=2)])
    _, tok1 = s.claim("w1")
    s.report("j1", tok1, success=False)  # attempt 1 used
    s.close()

    s2 = make_scheduler(tmp_path, clock)  # restart
    j1 = [j for j in s2.jobs()][0]
    assert j1.state == JobState.READY
    assert j1.attempts == 1 and j1.max_attempts == 2
    # only one attempt left: the next failure must be terminal
    _, tok2 = s2.claim("w2")
    s2.report("j1", tok2, success=False)
    assert states(s2)["j1"] == JobState.FAILED
