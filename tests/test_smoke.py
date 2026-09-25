"""骨架冒烟测试：确认包可导入、应用可构造。"""


def test_imports():
    from scheduler.api import create_app
    from scheduler.models import JobState, Resources, Snapshot  # noqa: F401
    from scheduler.scheduler import Scheduler
    from scheduler.store import InMemorySnapshotStore

    app = create_app(scheduler=Scheduler(Resources(cpu=1), InMemorySnapshotStore()))
    assert app is not None


def test_resources_basic():
    from scheduler.models import Resources

    r = Resources(cpu=2, memory_mb=1024, gpu=1)
    assert r.scaled(3).cpu == 6
    assert r.scaled(3).gpu == 3
