"""Lease renewal: extends expiry for the live holder, rejects everyone else."""
import logging

from dispatcher.models import JobState
from dispatcher.scheduler import RejectReason
from tests.helpers import job, make_scheduler


def test_renew_extends_expiry_and_keeps_identity(tmp_path, clock, caplog):
    s = make_scheduler(tmp_path, clock, cpu=2.0, lease=10.0)
    s.submit([job("j1", cpu=2.0)])
    leased, tok = s.claim("w1")
    first_expiry = leased.lease.expires_at

    clock.advance(6.0)
    with caplog.at_level(logging.INFO, "dispatcher.scheduler"):
        ok, reason = s.renew("j1", tok)
    assert (ok, reason) == (True, None)

    # expiry pushed out from *now*, identity (token/worker) unchanged
    j = {j.job_id: j for j in s.jobs()}["j1"]
    assert j.lease.expires_at == clock.now + 10.0 > first_expiry
    assert j.lease.fencing_token == tok and j.lease.worker_id == "w1"
    # renewal must not touch capacity accounting
    assert s.available().cpu == 0.0
    # old expiry would have passed by now; lease survives because of renewal
    clock.advance(9.0)
    assert s.claim("w2")[0] is None
    assert s.report("j1", tok, success=True) == (True, None)
    msg = "\n".join(r.getMessage() for r in caplog.records)
    assert "lease renewed" in msg and "j1" in msg and str(tok) in msg


def test_renew_rejected_after_lease_reassigned(tmp_path, clock):
    """Lease expired and handed to another worker: old holder gets LEASE_LOST."""
    s = make_scheduler(tmp_path, clock, lease=10.0)
    s.submit([job("j1")])
    _, tok1 = s.claim("w1")
    clock.advance(11.0)  # expire
    _, tok2 = s.claim("w2")
    assert tok2 > tok1
    ok, reason = s.renew("j1", tok1)
    assert (ok, reason) == (False, RejectReason.LEASE_LOST)
    # the current holder can renew
    assert s.renew("j1", tok2) == (True, None)


def test_renew_rejected_on_terminal_job(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1")])
    _, tok = s.claim("w1")
    assert s.report("j1", tok, success=True) == (True, None)
    ok, reason = s.renew("j1", tok)
    assert (ok, reason) == (False, RejectReason.LEASE_LOST)


def test_renew_rejected_after_preempt(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    s.submit([job("j1")])
    _, tok = s.claim("w1")
    assert s.preempt("j1") == (True, None)
    ok, reason = s.renew("j1", tok)
    assert (ok, reason) == (False, RejectReason.LEASE_LOST)


def test_renew_unknown_job(tmp_path, clock):
    s = make_scheduler(tmp_path, clock)
    ok, reason = s.renew("ghost", 1)
    assert (ok, reason) == (False, RejectReason.UNKNOWN_JOB)


def test_renew_via_api(tmp_path, clock):
    from fastapi.testclient import TestClient
    from dispatcher.api import create_app
    s = make_scheduler(tmp_path, clock, lease=10.0)
    client = TestClient(create_app(s))
    client.post("/jobs", json={"jobs": [
        {"job_id": "j1", "cpu": 1, "memory_mb": 128, "replicas": 1}]})
    tok = client.post("/claim", json={"worker_id": "w1"}).json()["fencing_token"]
    r = client.post("/jobs/j1/renew", json={"fencing_token": tok}).json()
    assert r == {"accepted": True, "reason": None}
    r = client.post("/jobs/j1/renew", json={"fencing_token": tok + 99}).json()
    assert r == {"accepted": False, "reason": "LEASE_LOST"}
