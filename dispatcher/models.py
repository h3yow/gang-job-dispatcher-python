"""Domain models: resources, jobs, leases. Types only for now."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class JobState(str, enum.Enum):
    PENDING = "PENDING"        # accepted, waiting for capacity
    READY = "READY"            # capacity allocated, claimable
    LEASED = "LEASED"          # claimed by a worker under a lease
    SUCCEEDED = "SUCCEEDED"    # terminal
    FAILED = "FAILED"          # terminal
    CANCELLED = "CANCELLED"    # terminal


TERMINAL_STATES = {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED}

#: Default upper bound on execution attempts when the submitter does not
#: specify one. Attempt N is created by the Nth claim of the job.
DEFAULT_MAX_ATTEMPTS = 3


@dataclass(frozen=True)
class Resources:
    cpu: float
    memory_mb: int

    def fits_in(self, other: "Resources") -> bool:
        return self.cpu <= other.cpu and self.memory_mb <= other.memory_mb

    def scale(self, n: int) -> "Resources":
        return Resources(cpu=self.cpu * n, memory_mb=self.memory_mb * n)

    def plus(self, other: "Resources") -> "Resources":
        return Resources(cpu=self.cpu + other.cpu, memory_mb=self.memory_mb + other.memory_mb)

    def minus(self, other: "Resources") -> "Resources":
        return Resources(cpu=self.cpu - other.cpu, memory_mb=self.memory_mb - other.memory_mb)


@dataclass
class Job:
    job_id: str
    group_id: str | None
    state: JobState
    per_replica: Resources
    replicas: int
    result: str | None = None
    lease: "Lease | None" = None
    attempts: int = 0                      # how many times the job was claimed
    max_attempts: int = DEFAULT_MAX_ATTEMPTS

    @property
    def demand(self) -> Resources:
        return self.per_replica.scale(self.replicas)


@dataclass
class Lease:
    job_id: str
    worker_id: str
    fencing_token: int
    expires_at: float
