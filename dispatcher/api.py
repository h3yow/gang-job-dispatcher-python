"""HTTP API wiring for the dispatcher."""
from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel, Field

from .models import Job, JobState, Resources
from .scheduler import Scheduler


class JobSpec(BaseModel):
    job_id: str
    group_id: str | None = None
    cpu: float = Field(gt=0)
    memory_mb: int = Field(gt=0)
    replicas: int = Field(ge=1)


class SubmitRequest(BaseModel):
    jobs: list[JobSpec]


class ClaimRequest(BaseModel):
    worker_id: str


class ReportRequest(BaseModel):
    fencing_token: int
    success: bool
    result: str | None = None


def _job_view(job: Job) -> dict:
    return {
        "job_id": job.job_id,
        "group_id": job.group_id,
        "state": job.state.value,
        "cpu": job.per_replica.cpu,
        "memory_mb": job.per_replica.memory_mb,
        "replicas": job.replicas,
        "result": job.result,
    }


def create_app(scheduler: Scheduler) -> FastAPI:
    app = FastAPI(title="gang-job-dispatcher")

    @app.post("/jobs")
    def submit(req: SubmitRequest):
        jobs = [Job(job_id=s.job_id, group_id=s.group_id, state=JobState.PENDING,
                    per_replica=Resources(cpu=s.cpu, memory_mb=s.memory_mb),
                    replicas=s.replicas) for s in req.jobs]
        ok, reason = scheduler.submit(jobs)
        return {"accepted": ok, "reason": reason}

    @app.post("/claim")
    def claim(req: ClaimRequest):
        job, token = scheduler.claim(req.worker_id)
        if job is None:
            return {"job": None, "fencing_token": None}
        return {"job": _job_view(job), "fencing_token": token}

    @app.post("/jobs/{job_id}/report")
    def report(job_id: str, req: ReportRequest):
        ok, reason = scheduler.report(job_id, req.fencing_token, req.success, req.result)
        return {"accepted": ok, "reason": reason}

    @app.post("/jobs/{job_id}/cancel")
    def cancel(job_id: str):
        ok, reason = scheduler.cancel(job_id)
        return {"accepted": ok, "reason": reason}

    @app.post("/jobs/{job_id}/preempt")
    def preempt(job_id: str):
        ok, reason = scheduler.preempt(job_id)
        return {"accepted": ok, "reason": reason}

    @app.get("/jobs")
    def list_jobs():
        return {"jobs": [_job_view(j) for j in scheduler.jobs()]}

    @app.get("/capacity")
    def capacity():
        avail = scheduler.available()
        return {"available": {"cpu": avail.cpu, "memory_mb": avail.memory_mb}}

    return app
