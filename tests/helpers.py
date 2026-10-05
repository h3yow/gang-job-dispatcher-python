from dispatcher.models import Job, JobState, Resources
from dispatcher.scheduler import Scheduler
from dispatcher.store import Store


def make_scheduler(tmp_path, clock, cpu=4.0, mem=4096, lease=10.0, name="t.db"):
    store = Store(str(tmp_path / name))
    return Scheduler(store, Resources(cpu=cpu, memory_mb=mem),
                     lease_seconds=lease, clock=clock)


def job(job_id, group=None, cpu=1.0, mem=256, replicas=1, max_attempts=3):
    return Job(job_id=job_id, group_id=group, state=JobState.PENDING,
               per_replica=Resources(cpu=cpu, memory_mb=mem), replicas=replicas,
               max_attempts=max_attempts)
