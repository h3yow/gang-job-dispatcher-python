"""Lease renewal: extends expiry, keeps identity, never touches capacity."""
import logging

from dispatcher.scheduler import RejectReason
from tests.helpers import job, make_scheduler


def test_renew_success_extends_expiry_keeps_identity_and_capacity(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, cpu=4.0, lease=10.0)
    s.submit([job("j1")])
    j, tok = s.claim("w1")
    first_expiry = j.lease.expires_at
    avail_before = s.available()

    clock.advance(6.0)  # still inside the lease
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        ok, reason = s.renew("j1", tok, "w1")
    assert (ok, reason) == (True, None)

    renewed = [j for j in s.jobs() if j.job_id == "j1"][0]
    assert renewed.lease.fencing_token == tok          # identity unchanged
    assert renewed.lease.worker_id == "w1"
    assert renewed.lease.expires_at > first_expiry     # expiry pushed forward
    assert renewed.lease.expires_at == clock.now + 10.0
    assert s.available() == avail_before               # capacity untouched

    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "lease renewed" in msg and "j1" in msg and f"token={tok}" in msg


def test_renew_keeps_lease_alive_past_original_expiry(tmp_path, clock):
    s = make_scheduler(tmp_path, clock, lease=10.0)
    s.submit([job("j1")])
    _, tok = s.claim("w1")
    clock.advance(6.0)
    assert s.renew("j1", tok, "w1") == (True, None)
    clock.advance(6.0)  # past the original expiry, inside the renewed one
    j2, _ = s.claim("w2")
    assert j2 is None  # lease still held by w1, not reclaimable
    # and the original token still reports successfully
    assert s.report("j1", tok, success=True) == (True, None)


def test_renew_rejected_after_lease_reclaimed_by_other_worker(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, lease=10.0)
    s.submit([job("j1")])
    _, tok1 = s.claim("w1")
    clock.advance(11.0)          # lease expires
    _, tok2 = s.claim("w2")      # reclaimed by someone else
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        ok, reason = s.renew("j1", tok1, "w1")
    assert (ok, reason) == (False, RejectReason.LEASE_LOST)
    # distinguishable from other rejection reasons
    assert reason != RejectReason.STALE_FENCING_TOKEN
    assert reason != RejectReason.ILLEGAL_STATE
    # the new holder can renew
    assert s.renew("j1", tok2, "w2") == (True, None)
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "lease lost" in msg and "j1" in msg


def test_renew_rejected_on_terminal_job(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1")])
    _, tok = s.claim("w1")
    assert s.report("j1", tok, success=True) == (True, None)
    ok, reason = s.renew("j1", tok, "w1")
    assert (ok, reason) == (False, RejectReason.LEASE_LOST)


def test_renew_rejected_on_expired_lease_before_reclaim(tmp_path, clock):
    s = make_scheduler(tmp_path, clock, lease=10.0)
    s.submit([job("j1")])
    _, tok = s.claim("w1")
    clock.advance(11.0)  # expired, nobody reclaimed yet
    ok, reason = s.renew("j1", tok, "w1")
    assert (ok, reason) == (False, RejectReason.LEASE_LOST)


def test_renew_rejected_wrong_token_or_worker(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1")])
    _, tok = s.claim("w1")
    assert s.renew("j1", tok + 999, "w1") == (False, RejectReason.LEASE_LOST)
    assert s.renew("j1", tok, "w2") == (False, RejectReason.LEASE_LOST)
    # the real holder is unaffected by the failed attempts
    assert s.renew("j1", tok, "w1") == (True, None)


def test_renew_unknown_job(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    ok, reason = s.renew("nope", 1, "w1")
    assert (ok, reason) == (False, RejectReason.UNKNOWN_JOB)
