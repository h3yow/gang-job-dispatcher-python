"""调度领域的数据模型。

状态机
------
作业在提交成功的瞬间即按"整组原子"完成容量核算并获得资源：

    READY ──claim──▶ RUNNING ──report(SUCCEEDED/FAILED/KILLED)──▶ 终态
      │                 │
      │              lease 过期（被新领取取代）──▶ READY
      │
    cancel / preempt ──▶ CANCELLED / PREEMPTED（任意非终态）

终态：SUCCEEDED / FAILED / KILLED / CANCELLED / PREEMPTED。
终态作业的容量在迁移时即释放，不会再被领取或重复计入结果。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class JobState(str, Enum):
    READY = "READY"            # 已获得资源，等待执行方领取
    RUNNING = "RUNNING"        # 已被领取，存在有效租约
    SUCCEEDED = "SUCCEEDED"    # 终态：执行成功
    FAILED = "FAILED"          # 终态：执行失败
    KILLED = "KILLED"          # 终态：被抢占后杀死
    CANCELLED = "CANCELLED"    # 终态：被取消
    PREEMPTED = "PREEMPTED"    # 终态：被抢占（驱逐信号已下发）

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL_STATES

    @property
    def consumes_capacity(self) -> bool:
        """处于该状态的作业是否仍占用容量。"""
        return not self.is_terminal


_TERMINAL_STATES = frozenset(
    {
        JobState.SUCCEEDED,
        JobState.FAILED,
        JobState.KILLED,
        JobState.CANCELLED,
        JobState.PREEMPTED,
    }
)


@dataclass(frozen=True)
class Resources:
    """单份资源需求（非负整数，按离散槽位核算）。"""

    cpu: int = 0
    memory_mb: int = 0
    gpu: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.cpu, int) or not isinstance(self.memory_mb, int) or not isinstance(self.gpu, int):
            object.__setattr__(self, "cpu", int(self.cpu))
            object.__setattr__(self, "memory_mb", int(self.memory_mb))
            object.__setattr__(self, "gpu", int(self.gpu))
        if self.cpu < 0 or self.memory_mb < 0 or self.gpu < 0:
            raise ValueError("资源需求不能为负数")

    def scaled(self, factor: int) -> "Resources":
        return Resources(
            cpu=self.cpu * factor,
            memory_mb=self.memory_mb * factor,
            gpu=self.gpu * factor,
        )

    def fits_in(self, capacity: "Resources") -> bool:
        return (
            self.cpu <= capacity.cpu
            and self.memory_mb <= capacity.memory_mb
            and self.gpu <= capacity.gpu
        )

    def subtract(self, other: "Resources") -> "Resources":
        return Resources(
            cpu=self.cpu - other.cpu,
            memory_mb=self.memory_mb - other.memory_mb,
            gpu=self.gpu - other.gpu,
        )


@dataclass
class Lease:
    """执行方领取作业后获得的租约。

    ``fence_token`` 单调递增（作业维度），每次重新领取都会产生更大的 token；
    执行方必须在心跳/上报时携带该 token，旧 token 一律拒绝（围栏语义）。
    """

    fence_token: int
    worker_id: str
    issued_at: float
    expires_at: float

    def is_valid(self, now: float, *, token: int | None = None) -> bool:
        if token is not None and token != self.fence_token:
            return False
        return now < self.expires_at

    def to_view(self) -> dict:
        return {
            "fence_token": self.fence_token,
            "worker_id": self.worker_id,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
        }


@dataclass
class Job:
    job_id: str
    gang_id: str
    dedup_key: str
    per_replica: Resources
    replicas: int
    state: JobState = JobState.READY
    lease: Lease | None = None
    result_reported: bool = False
    # 终态迁移时记录被消费的围栏号，用于同一围栏重试的幂等判定
    consumed_fence_token: int | None = None
    submitted_at: float = 0.0

    @property
    def demand(self) -> Resources:
        """该作业的总资源需求 = 单份需求 × 规模。"""
        return self.per_replica.scaled(self.replicas)

    def to_view(self) -> dict:
        return {
            "job_id": self.job_id,
            "gang_id": self.gang_id,
            "dedup_key": self.dedup_key,
            "per_replica": {
                "cpu": self.per_replica.cpu,
                "memory_mb": self.per_replica.memory_mb,
                "gpu": self.per_replica.gpu,
            },
            "replicas": self.replicas,
            "state": self.state.value,
            "lease": self.lease.to_view() if self.lease else None,
            "result_reported": self.result_reported,
        }


@dataclass
class Gang:
    """成组作业：成员要么整体 READY，要么提交时整体被拒绝。"""

    gang_id: str
    member_ids: list[str] = field(default_factory=list)
    submitted_at: float = 0.0


@dataclass
class Snapshot:
    """可持久化 / 重启恢复的完整调度状态。"""

    capacity: Resources
    jobs: dict[str, Job] = field(default_factory=dict)
    gangs: dict[str, Gang] = field(default_factory=dict)
    dedup_index: dict[str, str] = field(default_factory=dict)  # dedup_key -> gang_id

    def to_dict(self) -> dict:
        return {
            "capacity": {
                "cpu": self.capacity.cpu,
                "memory_mb": self.capacity.memory_mb,
                "gpu": self.capacity.gpu,
            },
            "jobs": {
                jid: {
                    "job_id": j.job_id,
                    "gang_id": j.gang_id,
                    "dedup_key": j.dedup_key,
                    "per_replica": {
                        "cpu": j.per_replica.cpu,
                        "memory_mb": j.per_replica.memory_mb,
                        "gpu": j.per_replica.gpu,
                    },
                    "replicas": j.replicas,
                    "state": j.state.value,
                    "lease": (
                        {
                            "fence_token": j.lease.fence_token,
                            "worker_id": j.lease.worker_id,
                            "issued_at": j.lease.issued_at,
                            "expires_at": j.lease.expires_at,
                        }
                        if j.lease
                        else None
                    ),
                    "result_reported": j.result_reported,
                    "consumed_fence_token": j.consumed_fence_token,
                    "submitted_at": j.submitted_at,
                }
                for jid, j in self.jobs.items()
            },
            "gangs": {
                gid: {
                    "gang_id": g.gang_id,
                    "member_ids": list(g.member_ids),
                    "submitted_at": g.submitted_at,
                }
                for gid, g in self.gangs.items()
            },
            "dedup_index": dict(self.dedup_index),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Snapshot":
        cap = data["capacity"]
        snap = cls(capacity=Resources(**cap))
        for jid, jd in data.get("jobs", {}).items():
            pr = jd["per_replica"]
            lease = None
            if jd.get("lease"):
                ld = jd["lease"]
                lease = Lease(
                    fence_token=ld["fence_token"],
                    worker_id=ld["worker_id"],
                    issued_at=ld["issued_at"],
                    expires_at=ld["expires_at"],
                )
            snap.jobs[jid] = Job(
                job_id=jd["job_id"],
                gang_id=jd["gang_id"],
                dedup_key=jd["dedup_key"],
                per_replica=Resources(**pr),
                replicas=jd["replicas"],
                state=JobState(jd["state"]),
                lease=lease,
                result_reported=jd.get("result_reported", False),
                consumed_fence_token=jd.get("consumed_fence_token"),
                submitted_at=jd.get("submitted_at", 0.0),
            )
        for gid, gd in data.get("gangs", {}).items():
            snap.gangs[gid] = Gang(
                gang_id=gd["gang_id"],
                member_ids=list(gd["member_ids"]),
                submitted_at=gd.get("submitted_at", 0.0),
            )
        snap.dedup_index = dict(data.get("dedup_index", {}))
        return snap
