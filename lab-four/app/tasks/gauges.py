import asyncio
import logging

import asyncpg
from prometheus_client import Gauge

from app.tasks.repository import TaskRepository

REFRESH_SECONDS = 5.0
STATUSES = ("queued", "running", "done", "failed", "cancelled")

QUEUED = Gauge("tasks_queued", "Tasks in status queued, refreshed from the database every 5 s")
RUNNING = Gauge("tasks_running", "Tasks in status running, refreshed from the database every 5 s")
BY_STATUS = Gauge("tasks_by_status", "Tasks by status, refreshed from the database every 5 s", ["status"])

log = logging.getLogger("tasks.gauges")


async def refresh_forever(pool: asyncpg.Pool):
    repo = TaskRepository(pool)
    while True:
        try:
            async with asyncio.timeout(REFRESH_SECONDS):
                counts = await repo.counts()
            QUEUED.set(counts.get("queued", 0))
            RUNNING.set(counts.get("running", 0))
            for status in STATUSES:
                BY_STATUS.labels(status).set(counts.get(status, 0))
        except Exception as exc:
            log.warning("tasks gauge refresh failed: %r", exc)
        await asyncio.sleep(REFRESH_SECONDS)
