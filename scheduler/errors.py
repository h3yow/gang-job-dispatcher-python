"""可区分的调度拒绝原因。

每个错误都带有稳定的 ``code``，HTTP 层据此映射为不同状态码，
调用方可以依据 code 区分"非法提交 / 超额 / 重复 / 围栏失效"等场景。
"""


class SchedulerError(Exception):
    """所有调度错误的基类。"""

    code = "scheduler_error"
    http_status = 400

    def __init__(self, message: str, *, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        return {
            "error": self.code,
            "message": self.message,
            "details": self.details,
        }


class InvalidJobSubmission(SchedulerError):
    """资源声明非法（字段缺失、为负数、规模非法等）。"""

    code = "invalid_job_submission"
    http_status = 422


class CapacityExceeded(SchedulerError):
    """调度会超出集群可用容量，整组原子拒绝。"""

    code = "capacity_exceeded"
    http_status = 409


class DuplicateSubmission(SchedulerError):
    """幂等键（dedup_key）重复提交，不会产生多份可执行作业。"""

    code = "duplicate_submission"
    http_status = 409


class JobNotFound(SchedulerError):
    """作业不存在。"""

    code = "job_not_found"
    http_status = 404


class GangNotFound(SchedulerError):
    """成组作业不存在。"""

    code = "gang_not_found"
    http_status = 404


class LeaseViolation(SchedulerError):
    """围栏失效：租约不存在、已过期或持有旧围栏上报/心跳。"""

    code = "lease_violation"
    http_status = 409


class InvalidStateTransition(SchedulerError):
    """对已处于终态的作业执行取消/抢占等非法状态迁移。"""

    code = "invalid_state_transition"
    http_status = 409
