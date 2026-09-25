"""调度状态的持久化：JSON 文件快照 + 临时文件原子替换。

重启后通过 :meth:`JsonSnapshotStore.load` 恢复作业与容量状态。
"""

from __future__ import annotations

import json
import os
import tempfile

from .models import Snapshot


class SnapshotStore:
    """快照存储接口。"""

    def load(self) -> Snapshot | None:
        raise NotImplementedError

    def save(self, snapshot: Snapshot) -> None:
        raise NotImplementedError


class JsonSnapshotStore(SnapshotStore):
    """基于本地 JSON 文件的快照存储，写入采用临时文件 + os.replace 原子替换。"""

    def __init__(self, path: str):
        self.path = path

    def load(self) -> Snapshot | None:
        if not os.path.exists(self.path):
            return None
        with open(self.path, encoding="utf-8") as f:
            data = json.load(f)
        return Snapshot.from_dict(data)

    def save(self, snapshot: Snapshot) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        # 先写临时文件再原子替换，避免崩溃/并发读到半截文件
        fd, tmp_path = tempfile.mkstemp(
            prefix=".scheduler_state-", suffix=".tmp", dir=directory
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(snapshot.to_dict(), f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise


class InMemorySnapshotStore(SnapshotStore):
    """不落盘的存储，供单元测试使用。"""

    def __init__(self) -> None:
        self._snapshot: Snapshot | None = None

    def load(self) -> Snapshot | None:
        return self._snapshot

    def save(self, snapshot: Snapshot) -> None:
        self._snapshot = snapshot
