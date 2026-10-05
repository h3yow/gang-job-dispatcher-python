"""End-to-end HTTP API tests."""
import pytest
from fastapi.testclient import TestClient

from dispatcher.api import create_app
from tests.helpers import make_scheduler


@pytest.fixture
def client(tmp_path, clock):
    s = make_scheduler(tmp_path, clock, cpu=2.0, mem=1024)
    return TestClient(create_app(s))


def test_submit_claim_report_flow(client):
    r = client.post("/jobs", json={"jobs": [
        {"job_id": "j1", "group_id": "g", "cpu": 1.0, "memory_mb": 256, "replicas": 1}]})
    assert r.json() == {"accepted": True, "reason": None}

    r = client.post("/claim", json={"worker_id": "w1"}).json()
    assert r["job"]["job_id"] == "j1" and r["fencing_token"] >= 1

    r = client.post("/jobs/j1/report",
                    json={"fencing_token": r["fencing_token"], "success": True,
                          "result": "ok"}).json()
    assert r == {"accepted": True, "reason": None}
    assert client.get("/capacity").json()["available"]["cpu"] == 2.0


def test_reject_reasons_distinguishable(client):
    # invalid declaration (schema-level: cpu must be > 0)
    r = client.post("/jobs", json={"jobs": [
        {"job_id": "bad", "cpu": -1, "memory_mb": 1, "replicas": 1}]})
    assert r.status_code == 422

    # over capacity
    r = client.post("/jobs", json={"jobs": [
        {"job_id": "big", "cpu": 99, "memory_mb": 1, "replicas": 1}]}).json()
    assert r == {"accepted": False, "reason": "INSUFFICIENT_CAPACITY"}

    # duplicate
    spec = {"job_id": "d", "cpu": 1, "memory_mb": 1, "replicas": 1}
    assert client.post("/jobs", json={"jobs": [spec]}).json()["accepted"]
    r = client.post("/jobs", json={"jobs": [spec]}).json()
    assert r == {"accepted": False, "reason": "DUPLICATE_JOB"}

    # stale fencing token
    tok = client.post("/claim", json={"worker_id": "w"}).json()["fencing_token"]
    r = client.post("/jobs/d/report",
                    json={"fencing_token": tok + 100, "success": True}).json()
    assert r == {"accepted": False, "reason": "STALE_FENCING_TOKEN"}

    # unknown job
    r = client.post("/jobs/ghost/cancel").json()
    assert r == {"accepted": False, "reason": "UNKNOWN_JOB"}


def test_cancel_via_api(client):
    client.post("/jobs", json={"jobs": [
        {"job_id": "c1", "cpu": 2.0, "memory_mb": 512, "replicas": 1}]})
    assert client.get("/capacity").json()["available"]["cpu"] == 0.0
    r = client.post("/jobs/c1/cancel").json()
    assert r == {"accepted": True, "reason": None}
    assert client.get("/capacity").json()["available"]["cpu"] == 2.0


def test_renew_and_retry_via_api(client, clock):
    r = client.post("/jobs", json={"jobs": [
        {"job_id": "j1", "cpu": 1.0, "memory_mb": 256, "replicas": 1,
         "max_attempts": 2}]}).json()
    assert r == {"accepted": True, "reason": None}

    claimed = client.post("/claim", json={"worker_id": "w1"}).json()
    tok = claimed["fencing_token"]
    assert claimed["job"]["max_attempts"] == 2
    assert claimed["job"]["attempts"] == 1

    # renew with the right identity succeeds
    r = client.post("/jobs/j1/renew",
                    json={"worker_id": "w1", "fencing_token": tok}).json()
    assert r == {"accepted": True, "reason": None}
    # renew with a foreign identity is rejected as LEASE_LOST
    r = client.post("/jobs/j1/renew",
                    json={"worker_id": "w2", "fencing_token": tok}).json()
    assert r == {"accepted": False, "reason": "LEASE_LOST"}

    # first attempt fails -> job is requeued, not terminal
    r = client.post("/jobs/j1/report",
                    json={"fencing_token": tok, "success": False}).json()
    assert r == {"accepted": True, "reason": None}
    view = client.get("/jobs").json()["jobs"][0]
    assert view["state"] == "READY" and view["attempts"] == 1

    # second attempt fails -> cap reached, terminal
    tok2 = client.post("/claim", json={"worker_id": "w2"}).json()["fencing_token"]
    client.post("/jobs/j1/report",
                json={"fencing_token": tok2, "success": False})
    view = client.get("/jobs").json()["jobs"][0]
    assert view["state"] == "FAILED"
    # renew on a terminal job is LEASE_LOST
    r = client.post("/jobs/j1/renew",
                    json={"worker_id": "w2", "fencing_token": tok2}).json()
    assert r == {"accepted": False, "reason": "LEASE_LOST"}
