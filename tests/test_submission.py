"""Submission validation, capacity accounting, gang atomicity."""
import logging

from dispatcher.scheduler import RejectReason
from tests.helpers import job, make_scheduler


def test_invalid_declarations_re_zero_cpu(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock)
    with caplog.at_level(logging.WARNING, "dispatcher.scheduler"):
        ok, reason = s.submit([job("bad", cpu=0.0)])
    assert (ok, reason) == (False, RejectReason.INVALID_RESOURCES)
    assert any("bad" in r.getMessage() for r in caplog.records)


def test_invalid_declarations_zero_replicas(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    ok, reason = s.submit([job("bad2", replicas=0)])
    assert (ok, reason) == (False, RejectReason.INVALID_RESOURCES)


def test_over_capacity_rejected_with_reason(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, cpu=2.0)
    with caplog.at_level(logging.WARNING, "dispatcher.scheduler"):
        ok, reason = s.submit([job("big", cpu=3.0)])
    assert (ok, reason) == (False, RejectReason.INSUFFICIENT_CAPACITY)
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "big" in msg and "insufficient capacity" in msg


def test_duplicate_submission_rejected_no_second_copy(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    assert s.submit([job("dup")]) == (True, None)
    ok, reason = s.submit([job("dup")])
    assert (ok, reason) == (False, RejectReason.DUPLICATE_JOB)
    # still exactly one executable job
    claimed = [s.claim(f"w{i}")[0] for i in range(3)]
    assert [j.job_id if j else None for j in claimed] == ["dup", None, None]


def test_duplicate_id_within_batch_rejected(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    ok, reason = s.submit([job("x"), job("x")])
    assert (ok, reason) == (False, RejectReason.DUPLICATE_JOB)
    assert s.jobs() == []


def test_gang_atomicity_partial_fit_rejected_wholly(tmp_path, clock, caplog):
    # capacity fits only one of the two jobs; the gang must fail as a whole
    s = make_scheduler(tmp_path, clock, cpu=3.0)
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        ok, reason = s.submit([job("g1", group="gang", cpu=2.0),
                               job("g2", group="gang", cpu=2.0)])
    assert (ok, reason) == (False, RejectReason.INSUFFICIENT_CAPACITY)
    # no residual placeholders: nothing claimable, full capacity remains
    assert s.claim("w")[0] is None
    assert s.available().cpu == 3.0
    assert s.jobs() == []


def test_gang_admitted_atomically(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, cpu=4.0)
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        ok, _ = s.submit([job("g1", group="gang", cpu=1.0, replicas=2),
                          job("g2", group="gang", cpu=1.0)])
    assert ok
    assert s.available().cpu == 1.0  # 2*1 + 1 reserved
    # capacity-change log line names both jobs and the delta
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "g1" in msg and "g2" in msg and "available" in msg


def test_capacity_released_on_terminal_report(tmp_path, clock):
    s = make_scheduler(tmp_path, clock, cpu=2.0)
    s.submit([job("j", cpu=2.0)])
    assert s.available().cpu == 0.0
    _, token = s.claim("w")
    ok, _ = s.report("j", token, success=True)
    assert ok
    assert s.available().cpu == 2.0
