"""FastAPI HTTP 层：把调度器能力暴露为 REST 接口。

路由
----
POST   /gangs                 提交成组作业（原子调度/容量核算/去重）
POST   /jobs/{job_id}/claim   执行方领取作业，获得租约围栏
POST   /jobs/{job_id}/heartbeat   持围栏续租
POST   /jobs/{job_id}/result  持围栏上报执行结果
POST   /gangs/{gang_id}/cancel    整组取消（任意状态确定结论）
POST   /gangs/{gang_id}/preempt   整组抢占
GET    /jobs/{job_id}         查询作业
GET    /gangs/{gang_id}       查询成组作业
GET    /capacity              容量口径与占用明细

调度器以单例形式挂在 app.state 上，所有请求共享同一把锁与同一份持久化状态。
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import SchedulerError
from .models import Resources
from .scheduler import Scheduler
from .store import JsonSnapshotStore

logger = logging.getLogger("scheduler.api")


class ResourceSpec(BaseModel):
    cpu: int = Field(default=0, ge=0)
    memory_mb: int = Field(default=0, ge=0)
    gpu: int = Field(default=0, ge=0)

    model_config = {"extra": "forbid"}


class JobSpec(BaseModel):
    job_id: str | None = None
    resources: ResourceSpec
    replicas: int = Field(default=1, ge=1)

    model_config = {"extra": "forbid"}


class SubmitGangRequest(BaseModel):
    jobs: list[JobSpec] = Field(min_length=1)
    gang_id: str | None = None
    dedup_key: str

    model_config = {"extra": "forbid"}


class ClaimRequest(BaseModel):
    worker_id: str

    model_config = {"extra": "forbid"}


class HeartbeatRequest(BaseModel):
    fence_token: int

    model_config = {"extra": "forbid"}


class ResultRequest(BaseModel):
    fence_token: int
    result: str

    model_config = {"extra": "forbid"}


def create_app(
    *,
    capacity: Resources | None = None,
    store_path: str = "scheduler_state.json",
    lease_ttl_seconds: float = 30.0,
    scheduler: Scheduler | None = None,
) -> FastAPI:
    """构造 FastAPI 应用。

    生产路径默认使用 :class:`JsonSnapshotStore`，重启自动恢复；
    测试可直接注入已构造好的 ``scheduler``。
    """
    app = FastAPI(title="gang-job-dispatcher")

    if scheduler is None:
        store = JsonSnapshotStore(store_path)
        scheduler = Scheduler(
            capacity or Resources(cpu=64, memory_mb=131072, gpu=8),
            store,
            lease_ttl_seconds=lease_ttl_seconds,
        )
    app.state.scheduler = scheduler

    @app.exception_handler(SchedulerError)
    async def _handle_scheduler_error(_request, exc: SchedulerError):
        # 4xx 拒绝同样记录判定原因，便于本地排查
        logger.info("请求被拒绝 code=%s details=%s", exc.code, exc.details)
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def _handle_validation_error(_request, exc: RequestValidationError):
        # 请求体/参数非法统一映射为可区分的 invalid_job_submission 信封
        logger.info("请求体非法 details=%s", exc.errors())
        return JSONResponse(
            status_code=422,
            content={
                "error": "invalid_job_submission",
                "message": "请求体不符合作业提交规范",
                "details": {"validation": jsonable_encoder(exc.errors())},
            },
        )

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/gangs", status_code=201)
    def submit_gang(req: SubmitGangRequest):
        payload = [_job_spec_to_dict(spec) for spec in req.jobs]
        return scheduler.submit_gang(
            payload, gang_id=req.gang_id, dedup_key=req.dedup_key
        )

    @app.post("/jobs/{job_id}/claim")
    def claim(job_id: str, req: ClaimRequest):
        return scheduler.claim(job_id, req.worker_id)

    @app.post("/jobs/{job_id}/heartbeat")
    def heartbeat(job_id: str, req: HeartbeatRequest):
        return scheduler.heartbeat(job_id, req.fence_token)

    @app.post("/jobs/{job_id}/result")
    def report_result(job_id: str, req: ResultRequest):
        return scheduler.report_result(job_id, req.fence_token, req.result)

    @app.post("/gangs/{gang_id}/cancel")
    def cancel_gang(gang_id: str):
        return scheduler.cancel_gang(gang_id)

    @app.post("/gangs/{gang_id}/preempt")
    def preempt_gang(gang_id: str):
        return scheduler.preempt_gang(gang_id)

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str):
        return scheduler.get_job(job_id)

    @app.get("/gangs/{gang_id}")
    def get_gang(gang_id: str):
        return scheduler.get_gang(gang_id)

    @app.get("/capacity")
    def capacity_view():
        return scheduler.capacity_view()

    return app


def _job_spec_to_dict(spec: JobSpec) -> dict[str, Any]:
    return {
        "job_id": spec.job_id,
        "resources": spec.resources.model_dump(),
        "replicas": spec.replicas,
    }
