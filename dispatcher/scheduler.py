"""Scheduler: capacity accounting, gang (all-or-nothing) scheduling, leases & fencing.

Concurrency model: a single in-process lock serializes every decision. Each
decision is persisted *before* it becomes visible, so a process restart can
rebuild an identical view from the store (leases expire by wall-clock time;
fencing tokens only ever increase).
"""
from __future__ import annotations

import logging
import threading
import time

from .models import Job, JobState, Lease, Resources, TERMINAL_STATES
from .store import Store

log = logging.getLogger("dispatcher.scheduler")


class RejectReason:
    INVALID_RESOURCES = "INVALID_RESOURCES"
    DUPLICATE_JOB = "DUPLICATE_JOB"
    INSUFFICIENT_CAPACITY = "INSUFFICIENT_CAPACITY"
    UNKNOWN_JOB = "UNKNOWN_JOB"
    STALE_FENCING_TOKEN = "STALE_FENCING_TOKEN"
    ILLEGAL_STATE = "ILLEGAL_STATE"
    # The lease the caller holds is gone: reclaimed & handed to another
    # worker, expired, or the job already reached a terminal state. The
    # caller must stop working on the job. Distinct from
    # STALE_FENCING_TOKEN so executors can tell "wrong token" apart from
    # "your lease is dead, do not continue".
    LEASE_LOST = "LEASE_LOST"


class Scheduler:
    def __init__(self, store: Store, capacity: Resources,
                 lease_seconds: float = 30.0, clock=time.monotonic) -> None:
        self._store = store
        self._capacity = capacity
        self._lease_seconds = lease_seconds
        self._clock = clock
        self._lock = threading.RLock()
        # Rebuild cache from persistence (restart recovery).
        self._jobs: dict[str, Job] = {j.job_id: j for j in store.all_jobs()}
        log.info("recovered %d jobs; cluster_capacity=%s",
                 len(self._jobs), self._cap_repr(capacity))

    # -- introspection ----------------------------------------------------
    def available(self) -> Resources:
        with self._lock:
            return self._available_locked()

    @staticmethod
    def _cap_repr(r: Resources) -> str:
        return f"cpu={r.cpu},mem_mb={r.memory_mb}"

    def _used_locked(self) -> Resources:
        used = Resources(0.0, 0)
        for job in self._jobs.values():
            if job.state not in TERMINAL_STATES:
                used = used.plus(job.demand)
        return used

    def _available_locked(self) -> Resources:
        return self._capacity.minus(self._used_locked())

    def jobs(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    # -- submission / gang scheduling -------------------------------------
    def submit(self, jobs: list[Job]) -> tuple[bool, str | None]:
        """Atomically admit a gang: all jobs become READY or none do."""
        if not jobs:
            return False, RejectReason.INVALID_RESOURCES

        # 1) validate every declaration
        seen: set[str] = set()
        for j in jobs:
            if (not j.job_id or not isinstance(j.job_id, str)
                    or not isinstance(j.replicas, int)
                    or j.replicas < 1
                    or j.per_replica.cpu <= 0
                    or j.per_replica.memory_mb <= 0
                    or not isinstance(j.max_attempts, int)
                    or j.max_attempts < 1):
                log.warning("reject submit: invalid declaration job=%s replicas=%s res=%s",
                            getattr(j, "job_id", None), getattr(j, "replicas", None),
                            getattr(j, "per_replica", None))
                return False, RejectReason.INVALID_RESOURCES
            if j.job_id in seen:
                log.warning("reject submit: duplicated id within batch job=%s", j.job_id)
                return False, RejectReason.DUPLICATE_JOB
            seen.add(j.job_id)

        gang_demand = Resources(0.0, 0)
        ids = sorted(j.job_id for j in jobs)
        with self._lock:
            # 2) duplicate against committed state (whole batch rejected,
            #    so retries never create a second executable copy)
            for j in jobs:
                if j.job_id in self._jobs:
                    log.warning("reject submit: duplicate job=%s (already %s)",
                                j.job_id, self._jobs[j.job_id].state.value)
                    return False, RejectReason.DUPLICATE_JOB

            # 3) capacity check for the whole gang (all-or-nothing)
            for j in jobs:
                gang_demand = gang_demand.plus(j.demand)
            avail = self._available_locked()
            if not gang_demand.fits_in(avail):
                log.warning(
                    "reject submit: insufficient capacity gang=%s demand={%s} available={%s}",
                    ids, self._cap_repr(gang_demand), self._cap_repr(avail))
                return False, RejectReason.INSUFFICIENT_CAPACITY

            # 4) commit: the gang as a whole enters READY
            before = avail
            for j in jobs:
                j.state = JobState.READY
                j.lease = None
                self._store.save_job(j)
                self._jobs[j.job_id] = j
            log.info("gang admitted jobs=%s demand={%s} available: {%s} -> {%s}",
                     ids, self._cap_repr(gang_demand),
                     self._cap_repr(before),
                     self._cap_repr(self._available_locked()))
            return True, None

    # -- lease expiry -----------------------------------------------------
    def _expire_locked(self) -> list[str]:
        now = self._clock()
        expired: list[str] = []
        for job in self._jobs.values():
            if (job.state == JobState.LEASED and job.lease is not None
                    and job.lease.expires_at <= now):
                old = job.lease
                job.state = JobState.READY
                job.lease = None
                self._store.save_job(job)
                expired.append(job.job_id)
                log.info("lease expired job=%s worker=%s stale_token=%d "
                         "-> READY (capacity stays reserved)",
                         job.job_id, old.worker_id, old.fencing_token)
        return expired

    # -- claim / lease ----------------------------------------------------
    def claim(self, worker_id: str) -> tuple[Job | None, int | None]:
        with self._lock:
            self._expire_locked()
            # deterministic FIFO by insertion (store rowid order)
            for job in self._store.all_jobs():
                if job.state != JobState.READY:
                    continue
                cached = self._jobs[job.job_id]
                token = self._store.next_fencing_token()
                lease = Lease(job_id=job.job_id, worker_id=worker_id,
                              fencing_token=token,
                              expires_at=self._clock() + self._lease_seconds)
                cached.state = JobState.LEASED
                cached.lease = lease
                cached.attempts += 1  # every claim starts a fresh attempt
                self._store.save_job(cached)
                log.info("lease granted job=%s worker=%s token=%d attempt=%d/%d "
                         "expires_in=%.1fs available={%s} (reserved, unchanged)",
                         job.job_id, worker_id, token, cached.attempts,
                         cached.max_attempts, self._lease_seconds,
                         self._cap_repr(self._available_locked()))
                return cached, token
            return None, None

    # -- lease renewal: extend the current lease, never change its identity --
    def renew(self, job_id: str, fencing_token: int,
              worker_id: str) -> tuple[bool, str | None]:
        """Extend the lease held by (worker_id, fencing_token).

        Renewal only pushes expires_at forward: the fencing token (lease
        identity) is untouched and capacity accounting is unaffected.
        If the lease is no longer the caller's — expired, reclaimed by
        another worker, or the job is already terminal — the renewal fails
        with LEASE_LOST so the executor knows it must stop.
        """
        with self._lock:
            self._expire_locked()
            job = self._jobs.get(job_id)
            if job is None:
                log.warning("reject renew: unknown job=%s token=%s worker=%s",
                            job_id, fencing_token, worker_id)
                return False, RejectReason.UNKNOWN_JOB

            current = job.lease
            if (job.state != JobState.LEASED or current is None
                    or current.fencing_token != fencing_token
                    or current.worker_id != worker_id):
                held = (f"worker={current.worker_id} token={current.fencing_token}"
                        if current else "none")
                log.warning("reject renew: lease lost job=%s state=%s "
                            "caller=(worker=%s token=%s) current_lease=%s",
                            job_id, job.state.value, worker_id, fencing_token, held)
                return False, RejectReason.LEASE_LOST

            old_expiry = current.expires_at
            current.expires_at = self._clock() + self._lease_seconds
            self._store.save_job(job)
            log.info("lease renewed job=%s worker=%s token=%d (identity kept) "
                     "expires_at: %.1f -> %.1f available={%s} (unchanged)",
                     job_id, worker_id, fencing_token, old_expiry,
                     current.expires_at, self._cap_repr(self._available_locked()))
            return True, None

    # -- result reporting with fencing -----------------------------------
    def report(self, job_id: str, fencing_token: int, success: bool,
               result: str | None = None) -> tuple[bool, str | None]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                log.warning("reject report: unknown job=%s token=%s",
                            job_id, fencing_token)
                return False, RejectReason.UNKNOWN_JOB

            if job.state in TERMINAL_STATES:
                # duplicate result must never be counted twice
                log.warning("reject report: job=%s already terminal=%s token=%s",
                            job_id, job.state.value, fencing_token)
                return False, RejectReason.ILLEGAL_STATE

            current = job.lease
            if current is None or fencing_token != current.fencing_token:
                held = current.fencing_token if current else None
                log.warning("reject report: stale fence job=%s got_token=%s current_token=%s",
                            job_id, fencing_token, held)
                return False, RejectReason.STALE_FENCING_TOKEN

            if not success:
                return self._handle_failure_locked(job, fencing_token, result)

            before = self._available_locked()
            job.state = JobState.SUCCEEDED
            job.result = result
            released = job.demand
            job.lease = None
            self._store.save_job(job)
            log.info("job=%s reported %s token=%d released={%s} available: {%s} -> {%s}",
                     job_id, job.state.value, fencing_token,
                     self._cap_repr(released), self._cap_repr(before),
                     self._cap_repr(self._available_locked()))
            return True, None

    # -- failure handling: retry below the attempt cap, else terminate ------
    def _handle_failure_locked(self, job: Job, fencing_token: int,
                               result: str | None) -> tuple[bool, str | None]:
        """A failure never ends a job outright: below max_attempts the job
        (and, for gang submissions, its whole group) goes back to READY with
        capacity still reserved; at the cap the job/group goes terminally
        FAILED and releases capacity exactly once."""
        if job.group_id is not None:
            members = [j for j in self._jobs.values()
                       if j.group_id == job.group_id
                       and j.state not in TERMINAL_STATES]
        else:
            members = [job]

        retry = job.attempts < job.max_attempts
        ids = sorted(j.job_id for j in members)
        if retry:
            # Capacity stays reserved throughout: READY jobs still occupy
            # their demand, so retries can never oversubscribe the cluster.
            for m in members:
                m.state = JobState.READY
                m.lease = None  # fences every outstanding lease in the group
                self._store.save_job(m)
            log.info("job=%s failed attempt=%d/%d token=%d -> retry; "
                     "requeued=%s available={%s} (reserved, unchanged)",
                     job.job_id, job.attempts, job.max_attempts, fencing_token,
                     ids, self._cap_repr(self._available_locked()))
            return True, None

        before = self._available_locked()
        released = Resources(0.0, 0)
        for m in members:
            m.state = JobState.FAILED
            m.result = result if m is job else (
                f"group {job.group_id} failed with member {job.job_id}")
            m.lease = None
            released = released.plus(m.demand)
            self._store.save_job(m)
        log.info("job=%s failed attempt=%d/%d (cap reached) token=%d -> FAILED; "
                 "terminated=%s released={%s} available: {%s} -> {%s}",
                 job.job_id, job.attempts, job.max_attempts, fencing_token,
                 ids, self._cap_repr(released), self._cap_repr(before),
                 self._cap_repr(self._available_locked()))
        return True, None

    # -- cancellation (any state, deterministic) --------------------------
    def cancel(self, job_id: str) -> tuple[bool, str | None]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                log.warning("reject cancel: unknown job=%s", job_id)
                return False, RejectReason.UNKNOWN_JOB
            if job.state in TERMINAL_STATES:
                # idempotent, definite outcome
                log.info("cancel no-op job=%s already terminal=%s",
                         job_id, job.state.value)
                return True, None

            before = self._available_locked()
            old = job.lease
            job.state = JobState.CANCELLED
            job.lease = None
            self._store.save_job(job)
            log.info("job=%s cancelled (was %s, token=%s) released={%s} "
                     "available: {%s} -> {%s}; old lease now fenced",
                     job_id, JobState.LEASED.value if old else job.state.value,
                     old.fencing_token if old else None,
                     self._cap_repr(job.demand), self._cap_repr(before),
                     self._cap_repr(self._available_locked()))
            return True, None

    # -- preemption: invalidate active lease, job re-queued ---------------
    def preempt(self, job_id: str) -> tuple[bool, str | None]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                log.warning("reject preempt: unknown job=%s", job_id)
                return False, RejectReason.UNKNOWN_JOB
            if job.state != JobState.LEASED:
                log.warning("reject preempt: job=%s not LEASED (state=%s)",
                            job_id, job.state.value)
                return False, RejectReason.ILLEGAL_STATE
            old = job.lease
            assert old is not None
            job.state = JobState.READY
            job.lease = None
            self._store.save_job(job)
            log.info("job=%s preempted from worker=%s stale_token=%d -> READY; "
                     "capacity stays reserved",
                     job_id, old.worker_id, old.fencing_token)
            return True, None

    def close(self) -> None:
        self._store.close()
