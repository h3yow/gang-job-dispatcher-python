"""核心调度逻辑：容量核算、成组原子调度、租约与围栏。

所有公共方法都在同一把可重入锁内完成"检查 + 状态迁移 + 持久化"，
因此并发竞争下结论唯一；租约 fence_token 在作业维度单调递增，
过期租约可被新领取取代，旧围栏的心跳/上报一律拒绝。
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable

from .errors import (
    CapacityExceeded,
    DuplicateSubmission,
    GangNotFound,
    InvalidJobSubmission,
    InvalidStateTransition,
    JobNotFound,
    LeaseViolation,
)
from .models import Gang, Job, JobState, Lease, Resources, Snapshot
from .store import InMemorySnapshotStore, SnapshotStore

logger = logging.getLogger("scheduler")

# 上报结果字符串 -> 终态
_RESULT_STATES = {
    "SUCCEEDED": JobState.SUCCEEDED,
    "FAILED": JobState.FAILED,
    "KILLED": JobState.KILLED,
}


class Scheduler:
    """线程安全的本地集群作业调度器。

    容量口径：集群容量在构造时固定；任一时刻"已用容量"=
    所有非终态作业（READY + RUNNING）的 demand（单份资源 × 副本数）之和。
    终态在迁移的瞬间释放容量。
    """

    def __init__(
        self,
        capacity: Resources,
        store: SnapshotStore | None = None,
        *,
        lease_ttl_seconds: float = 30.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds 必须为正数")
        self._ttl = float(lease_ttl_seconds)
        self._clock = clock
        self._store = store or InMemorySnapshotStore()
        self._lock = threading.RLock()

        restored = self._store.load()
        if restored is not None:
            self._snap = restored
            logger.info(
                "重启恢复完成 capacity=%s jobs=%d gangs=%d 判定依据=快照加载",
                self._snap.capacity,
                len(self._snap.jobs),
                len(self._snap.gangs),
            )
        else:
            self._snap = Snapshot(capacity=capacity)
            logger.info("初始化空调度器 capacity=%s", capacity)
        self._persist()

    # ================================================================
    # 作业提交（成组原子调度 + 容量核算 + 去重）
    # ================================================================
    def submit_gang(
        self,
        jobs: list[dict],
        *,
        gang_id: str | None = None,
        dedup_key: str | None = None,
    ) -> dict:
        if not isinstance(dedup_key, str) or not dedup_key.strip():
            raise InvalidJobSubmission(
                "dedup_key 必须为非空字符串，用于重复提交判定",
                details={"field": "dedup_key"},
            )
        dedup_key = dedup_key.strip()

        # 第一步：构造候选作业之前先查重（重复提交 -> 拒绝，绝不产生第二份）
        with self._lock:
            existing = self._snap.dedup_index.get(dedup_key)
            if existing is not None:
                logger.info(
                    "拒绝重复提交 dedup_key=%s 判定依据=幂等键已存在 existing_gang=%s",
                    dedup_key,
                    existing,
                )
                raise DuplicateSubmission(
                    "相同 dedup_key 的作业组已提交，重复提交不会产生多份可执行作业",
                    details={"dedup_key": dedup_key, "existing_gang_id": existing},
                )

            # 第二步：校验全部成员的资源声明（任一非法则整组拒绝，无任何落库）
            candidates = self._validate_specs(jobs)
            gid = gang_id or f"gang-{uuid.uuid4().hex[:12]}"
            if gid in self._snap.gangs:
                raise DuplicateSubmission(
                    "gang_id 已存在", details={"gang_id": gid}
                )
            now = self._clock()
            for spec in candidates:
                spec["job"].gang_id = gid
                spec["job"].dedup_key = dedup_key
                spec["job"].submitted_at = now

            # 第三步：整组需求与可用容量核算（all-or-nothing）
            requested = Resources()
            for c in candidates:
                requested = Resources(
                    cpu=requested.cpu + c["job"].demand.cpu,
                    memory_mb=requested.memory_mb + c["job"].demand.memory_mb,
                    gpu=requested.gpu + c["job"].demand.gpu,
                )
            used_before = self._used_locked()
            free = self._snap.capacity.subtract(used_before)
            violating = self._violating_dims(requested, free)
            if violating:
                logger.info(
                    "拒绝成组调度 gang=%s member_count=%d requested=%s used=%s "
                    "capacity=%s 判定依据=容量不足(%s)",
                    gid, len(candidates), requested, used_before,
                    self._snap.capacity, ",".join(violating),
                )
                raise CapacityExceeded(
                    "集群剩余容量无法满足整组需求，整组不分配、不残留占位",
                    details={
                        "gang_id": gid,
                        "capacity": _res_dict(self._snap.capacity),
                        "used": _res_dict(used_before),
                        "available": _res_dict(free),
                        "requested": _res_dict(requested),
                        "violating_dims": violating,
                    },
                )

            # 第四步：全部检查通过 -> 一次性原子落库
            gang = Gang(
                gang_id=gid,
                member_ids=[c["job"].job_id for c in candidates],
                submitted_at=now,
            )
            self._snap.gangs[gid] = gang
            for c in candidates:
                self._snap.jobs[c["job"].job_id] = c["job"]
            self._snap.dedup_index[dedup_key] = gid
            self._persist()
            used_after = self._used_locked()
            logger.info(
                "成组调度成功 gang=%s jobs=%s 容量变化 %s -> %s (请求 %s) "
                "判定依据=整组需求 %s 未超剩余容量 %s，原子整体进入 READY",
                gid, gang.member_ids, used_before, used_after, requested,
                requested, free,
            )
            return {
                "gang_id": gid,
                "dedup_key": dedup_key,
                "allocated": True,
                "state": JobState.READY.value,
                "jobs": [self._snap.jobs[jid].to_view() for jid in gang.member_ids],
                "capacity": self._capacity_view_locked(),
            }

    # ================================================================
    # 执行方领取 / 心跳 / 上报（租约 + 围栏）
    # ================================================================
    def claim(self, job_id: str, worker_id: str) -> dict:
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise InvalidJobSubmission(
                "worker_id 必须为非空字符串", details={"field": "worker_id"}
            )
        worker_id = worker_id.strip()
        with self._lock:
            job = self._get_job_locked(job_id)
            now = self._clock()

            if job.state.is_terminal:
                raise InvalidStateTransition(
                    f"作业 {job_id} 已处于终态 {job.state.value}，不可再被领取",
                    details={"job_id": job_id, "state": job.state.value},
                )

            reassigned = False
            if job.state is JobState.READY:
                token = (job.lease.fence_token if job.lease else 0) + 1
                job.lease = self._new_lease(token, worker_id, now)
                job.state = JobState.RUNNING
                logger.info(
                    "作业首次领取 job=%s gang=%s worker=%s fence=%d expires_at=%.3f "
                    "判定依据=作业处于 READY，授予新租约",
                    job_id, job.gang_id, worker_id, token, job.lease.expires_at,
                )
            elif job.state is JobState.RUNNING:
                assert job.lease is not None
                if now < job.lease.expires_at:
                    if job.lease.worker_id == worker_id:
                        # 同一执行方的重试：幂等返回同一租约，不发新围栏
                        logger.info(
                            "幂等领取 job=%s worker=%s fence=%d 判定依据=租约仍有效且持有者相同",
                            job_id, worker_id, job.lease.fence_token,
                        )
                        return self._claim_view(job, reassigned=False)
                    raise LeaseViolation(
                        "作业已被其他执行方持有且租约未过期，禁止并发重复执行",
                        details={
                            "job_id": job_id,
                            "current_worker": job.lease.worker_id,
                            "expires_at": job.lease.expires_at,
                        },
                    )
                # 旧租约已过期 -> 可重新分配，发放更大的围栏，旧围栏立即失效
                token = job.lease.fence_token + 1
                old_worker = job.lease.worker_id
                job.lease = self._new_lease(token, worker_id, now)
                reassigned = True
                logger.info(
                    "租约过期重新分配 job=%s old_worker=%s new_worker=%s "
                    "old_fence=%d new_fence=%d 判定依据=now=%.3f >= expires_at=%.3f，"
                    "新围栏单调递增，旧持有者之后的心跳/上报将被拒绝",
                    job_id, old_worker, worker_id, token - 1, token,
                    now, job.lease.expires_at,
                )

            self._persist()
            return self._claim_view(job, reassigned=reassigned)

    def heartbeat(self, job_id: str, fence_token: int) -> dict:
        with self._lock:
            job = self._get_job_locked(job_id)
            now = self._clock()
            self._require_valid_fence(job, fence_token, now, action="heartbeat")
            assert job.lease is not None
            job.lease = Lease(
                fence_token=job.lease.fence_token,
                worker_id=job.lease.worker_id,
                issued_at=job.lease.issued_at,
                expires_at=now + self._ttl,
            )
            self._persist()
            logger.info(
                "心跳续租 job=%s worker=%s fence=%d new_expires_at=%.3f "
                "判定依据=围栏匹配且租约未过期",
                job_id, job.lease.worker_id, fence_token, job.lease.expires_at,
            )
            return {"job_id": job_id, "lease": job.lease.to_view()}

    def report_result(self, job_id: str, fence_token: int, result: str) -> dict:
        if result not in _RESULT_STATES:
            raise InvalidJobSubmission(
                f"result 必须是 {sorted(_RESULT_STATES)} 之一",
                details={"field": "result", "actual": result},
            )
        target = _RESULT_STATES[result]
        with self._lock:
            job = self._get_job_locked(job_id)

            # 结果已计入：只有"消费了结果的那个围栏"的重试可幂等返回；
            # 更旧/更新的其他围栏一律拒绝，结果绝不会被重复计入。
            if job.result_reported:
                if fence_token == job.consumed_fence_token:
                    logger.info(
                        "同围栏重复上报幂等返回 job=%s fence=%d state=%s "
                        "判定依据=该围栏已消费此结果，不重复计入",
                        job_id, fence_token, job.state.value,
                    )
                    return {
                        "job_id": job_id,
                        "state": job.state.value,
                        "result_reported": True,
                        "capacity": self._capacity_view_locked(),
                    }
                raise LeaseViolation(
                    "围栏与已计入结果的租约不一致，拒绝重复计入",
                    details={
                        "job_id": job_id,
                        "submitted_token": fence_token,
                        "consumed_token": job.consumed_fence_token,
                        "state": job.state.value,
                    },
                )

            now = self._clock()
            self._require_valid_fence(job, fence_token, now, action="report_result")
            assert job.lease is not None
            worker = job.lease.worker_id
            used_before = self._used_locked()
            job.state = target
            job.result_reported = True
            job.consumed_fence_token = fence_token
            job.lease = None  # 回收租约：任何旧 token 之后都无法通过围栏校验
            self._persist()
            logger.info(
                "结果计入 job=%s worker=%s fence=%d result=%s 容量变化 %s -> %s "
                "判定依据=围栏有效且结果首次上报，迁移终态并释放容量",
                job_id, worker, fence_token, target.value,
                used_before, self._used_locked(),
            )
            return {
                "job_id": job_id,
                "state": target.value,
                "result_reported": True,
                "capacity": self._capacity_view_locked(),
            }

    # ================================================================
    # 取消 / 抢占（任意状态下确定结论，整组原子）
    # ================================================================
    def cancel_gang(self, gang_id: str) -> dict:
        return self._terminate_gang(
            gang_id, JobState.CANCELLED, action="cancel"
        )

    def preempt_gang(self, gang_id: str) -> dict:
        return self._terminate_gang(
            gang_id, JobState.PREEMPTED, action="preempt"
        )

    # ================================================================
    # 查询
    # ================================================================
    def get_job(self, job_id: str) -> dict:
        with self._lock:
            return self._get_job_locked(job_id).to_view()

    def get_gang(self, gang_id: str) -> dict:
        with self._lock:
            gang = self._snap.gangs.get(gang_id)
            if gang is None:
                raise GangNotFound(
                    f"成组作业 {gang_id} 不存在", details={"gang_id": gang_id}
                )
            return {
                "gang_id": gang_id,
                "member_ids": list(gang.member_ids),
                "jobs": [self._snap.jobs[jid].to_view() for jid in gang.member_ids],
            }

    def capacity_view(self) -> dict:
        with self._lock:
            return self._capacity_view_locked()

    # ================================================================
    # 内部辅助（调用方须持锁，除非名字注明）
    # ================================================================
    def _validate_specs(self, jobs: list[dict]) -> list[dict]:
        if not isinstance(jobs, list) or not jobs:
            raise InvalidJobSubmission(
                "jobs 必须为非空列表", details={"field": "jobs"}
            )
        candidates: list[dict] = []
        for index, spec in enumerate(jobs):
            where = f"jobs[{index}]"
            if not isinstance(spec, dict):
                raise InvalidJobSubmission(
                    f"{where} 必须是对象", details={"index": index}
                )
            res = spec.get("resources")
            replicas = spec.get("replicas", 1)
            if not isinstance(res, dict):
                raise InvalidJobSubmission(
                    f"{where}.resources 必须是包含 cpu/memory_mb/gpu 的对象",
                    details={"index": index, "field": "resources"},
                )
            bad_dim = [k for k in res if k not in ("cpu", "memory_mb", "gpu")]
            if bad_dim:
                raise InvalidJobSubmission(
                    f"{where}.resources 含未知资源维度: {bad_dim}",
                    details={"index": index, "unknown": bad_dim},
                )
            try:
                per_replica = Resources(
                    cpu=res.get("cpu", 0),
                    memory_mb=res.get("memory_mb", 0),
                    gpu=res.get("gpu", 0),
                )
            except (TypeError, ValueError) as exc:
                raise InvalidJobSubmission(
                    f"{where}.resources 非法：{exc}", details={"index": index}
                ) from exc
            if isinstance(replicas, bool) or not isinstance(replicas, int) or replicas < 1:
                raise InvalidJobSubmission(
                    f"{where}.replicas 必须是 >=1 的整数",
                    details={"index": index, "actual": replicas},
                )
            job_id = spec.get("job_id") or f"job-{uuid.uuid4().hex[:12]}"
            if not isinstance(job_id, str) or not job_id.strip():
                raise InvalidJobSubmission(
                    f"{where}.job_id 非法", details={"index": index}
                )
            if job_id in self._snap.jobs:
                raise DuplicateSubmission(
                    f"job_id {job_id} 已存在", details={"job_id": job_id}
                )
            candidates.append(
                {
                    "job": Job(
                        job_id=job_id,
                        gang_id="",
                        dedup_key="",
                        per_replica=per_replica,
                        replicas=replicas,
                    )
                }
            )
        return candidates

    def _terminate_gang(self, gang_id: str, target: JobState, *, action: str) -> dict:
        with self._lock:
            gang = self._snap.gangs.get(gang_id)
            if gang is None:
                raise GangNotFound(
                    f"成组作业 {gang_id} 不存在", details={"gang_id": gang_id}
                )
            used_before = self._used_locked()
            members: list[dict] = []
            changed = 0
            for jid in gang.member_ids:
                job = self._snap.jobs[jid]
                if not job.state.is_terminal:
                    job.state = target
                    job.lease = None  # 撤销租约，围栏立即失效
                    changed += 1
                members.append({"job_id": jid, "state": job.state.value})
            if changed:
                self._persist()
            logger.info(
                "%s 完成 gang=%s 终态目标=%s 迁移成员=%d 容量变化 %s -> %s "
                "判定依据=非终态成员整组原子迁移并撤销租约，已终态成员保持不变",
                action, gang_id, target.value, changed,
                used_before, self._used_locked(),
            )
            return {"gang_id": gang_id, "action": action, "members": members}

    def _new_lease(self, token: int, worker_id: str, now: float) -> Lease:
        return Lease(
            fence_token=token,
            worker_id=worker_id,
            issued_at=now,
            expires_at=now + self._ttl,
        )

    def _require_valid_fence(
        self, job: Job, fence_token: int, now: float, *, action: str
    ) -> None:
        """围栏 + 有效期校验。任何不满足都抛 LeaseViolation。"""
        if job.lease is None:
            raise LeaseViolation(
                f"作业 {job.job_id} 不存在有效租约（可能已终态或被抢占）",
                details={"job_id": job.job_id, "state": job.state.value},
            )
        if not isinstance(fence_token, int) or isinstance(fence_token, bool):
            raise LeaseViolation(
                "fence_token 必须是整数", details={"actual": repr(fence_token)}
            )
        if fence_token != job.lease.fence_token:
            raise LeaseViolation(
                "围栏失效：提交的 fence_token 不是当前租约（持旧租约操作被拒绝）",
                details={
                    "job_id": job.job_id,
                    "submitted_token": fence_token,
                    "current_token": job.lease.fence_token,
                },
            )
        if now >= job.lease.expires_at:
            raise LeaseViolation(
                "租约已过期，围栏不再有效，作业可被重新分配",
                details={
                    "job_id": job.job_id,
                    "expires_at": job.lease.expires_at,
                    "now": now,
                },
            )

    def _get_job_locked(self, job_id: str) -> Job:
        job = self._snap.jobs.get(job_id)
        if job is None:
            raise JobNotFound(
                f"作业 {job_id} 不存在", details={"job_id": job_id}
            )
        return job

    def _used_locked(self) -> Resources:
        cpu = mem = gpu = 0
        for job in self._snap.jobs.values():
            if job.state.consumes_capacity:
                d = job.demand
                cpu += d.cpu
                mem += d.memory_mb
                gpu += d.gpu
        return Resources(cpu=cpu, memory_mb=mem, gpu=gpu)

    @staticmethod
    def _violating_dims(requested: Resources, free: Resources) -> list[str]:
        dims = []
        if requested.cpu > free.cpu:
            dims.append("cpu")
        if requested.memory_mb > free.memory_mb:
            dims.append("memory_mb")
        if requested.gpu > free.gpu:
            dims.append("gpu")
        return dims

    def _capacity_view_locked(self) -> dict:
        used = self._used_locked()
        return {
            "capacity": _res_dict(self._snap.capacity),
            "used": _res_dict(used),
            "available": _res_dict(self._snap.capacity.subtract(used)),
            "consumers": [
                {
                    "job_id": j.job_id,
                    "gang_id": j.gang_id,
                    "state": j.state.value,
                    "demand": _res_dict(j.demand),
                }
                for j in self._snap.jobs.values()
                if j.state.consumes_capacity
            ],
        }

    def _claim_view(self, job: Job, *, reassigned: bool) -> dict:
        return {
            "job_id": job.job_id,
            "gang_id": job.gang_id,
            "state": job.state.value,
            "reassigned": reassigned,
            "lease": job.lease.to_view() if job.lease else None,
        }

    def _persist(self) -> None:
        self._store.save(self._snap)


def _res_dict(r: Resources) -> dict:
    return {"cpu": r.cpu, "memory_mb": r.memory_mb, "gpu": r.gpu}
