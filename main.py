"""本地启动入口：uvicorn main:app / python main.py"""

from __future__ import annotations

import os

import uvicorn

from scheduler.api import create_app
from scheduler.models import Resources

capacity = Resources(
    cpu=int(os.environ.get("CLUSTER_CPU", "64")),
    memory_mb=int(os.environ.get("CLUSTER_MEMORY_MB", "131072")),
    gpu=int(os.environ.get("CLUSTER_GPU", "8")),
)

app = create_app(
    capacity=capacity,
    store_path=os.environ.get("SCHEDULER_STORE", "scheduler_state.json"),
    lease_ttl_seconds=float(os.environ.get("LEASE_TTL_SECONDS", "30")),
)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
