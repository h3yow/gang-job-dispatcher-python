import uvicorn

from dispatcher.api import create_app
from dispatcher.models import Resources
from dispatcher.scheduler import Scheduler
from dispatcher.store import Store

store = Store("dispatcher.db")
scheduler = Scheduler(store, capacity=Resources(cpu=8.0, memory_mb=16384),
                      lease_seconds=30.0)
app = create_app(scheduler)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
