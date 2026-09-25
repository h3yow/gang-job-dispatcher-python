"""SQLite-backed persistence so state survives process restarts.

Single connection guarded by a lock; all writes commit immediately so a
process crash/restart loses nothing that was already acknowledged.
"""
from __future__ import annotations

import sqlite3
import threading

from .models import Job, JobState, Lease, Resources

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id     TEXT PRIMARY KEY,
    group_id   TEXT,
    state      TEXT NOT NULL,
    cpu        REAL NOT NULL,
    memory_mb  INTEGER NOT NULL,
    replicas   INTEGER NOT NULL,
    result     TEXT
);
CREATE TABLE IF NOT EXISTS leases (
    job_id        TEXT PRIMARY KEY,
    worker_id     TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    expires_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


class Store:
    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # -- jobs -------------------------------------------------------------
    def save_job(self, job: Job) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO jobs VALUES (?,?,?,?,?,?,?)",
                (job.job_id, job.group_id, job.state.value,
                 job.per_replica.cpu, job.per_replica.memory_mb,
                 job.replicas, job.result),
            )
            if job.lease is not None:
                self._conn.execute(
                    "INSERT OR REPLACE INTO leases VALUES (?,?,?,?)",
                    (job.lease.job_id, job.lease.worker_id,
                     job.lease.fencing_token, job.lease.expires_at),
                )
            else:
                self._conn.execute("DELETE FROM leases WHERE job_id=?", (job.job_id,))
            self._conn.commit()

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                return None
            return self._row_to_job(row)

    def all_jobs(self) -> list[Job]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM jobs").fetchall()
            return [self._row_to_job(r) for r in rows]

    def _row_to_job(self, row: tuple) -> Job:
        job_id, group_id, state, cpu, memory_mb, replicas, result = row
        lease_row = self._conn.execute(
            "SELECT worker_id, fencing_token, expires_at FROM leases WHERE job_id=?",
            (job_id,)).fetchone()
        lease = None
        if lease_row is not None:
            lease = Lease(job_id=job_id, worker_id=lease_row[0],
                          fencing_token=lease_row[1], expires_at=lease_row[2])
        return Job(job_id=job_id, group_id=group_id, state=JobState(state),
                   per_replica=Resources(cpu=cpu, memory_mb=memory_mb),
                   replicas=replicas, result=result, lease=lease)

    # -- fencing token counter (monotonic across restarts) -----------------
    def next_fencing_token(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key='fencing_token'").fetchone()
            token = (row[0] if row else 0) + 1
            self._conn.execute(
                "INSERT OR REPLACE INTO meta VALUES ('fencing_token', ?)", (token,))
            self._conn.commit()
            return token

    def close(self) -> None:
        with self._lock:
            self._conn.close()
