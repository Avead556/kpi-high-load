import json
import random

import asyncpg

COLUMNS = ("id, record_id, status, seed, attempts, max_attempts, token, lease_until, worker_id, "
           "cancel_requested_at, created_at, started_at, finished_at")
SEED_LIMIT = 2**31


class TaskRepository:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def create(self, record_id: int) -> asyncpg.Record | None:
        seed = random.randrange(1, SEED_LIMIT)
        return await self.pool.fetchrow(
            f"INSERT INTO tasks (record_id, seed) SELECT id, $2 FROM vaccine_lots WHERE id = $1 RETURNING {COLUMNS}",
            record_id, seed,
        )

    async def get(self, task_id: int) -> tuple[asyncpg.Record | None, asyncpg.Record | None]:
        async with self.pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
            task = await conn.fetchrow(f"SELECT {COLUMNS} FROM tasks WHERE id = $1", task_id)
            result = await conn.fetchrow(
                "SELECT token, worker_id, result, created_at FROM task_results WHERE task_id = $1 "
                "ORDER BY created_at LIMIT 1", task_id,
            ) if task is not None else None
        return task, result

    async def page(self, status: str | None, limit: int, offset: int) -> tuple[list[asyncpg.Record], int]:
        where, params = ("WHERE status = $1", [status]) if status is not None else ("", [])
        n = len(params)
        async with self.pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
            total = await conn.fetchval(f"SELECT count(*) FROM tasks {where}", *params)
            rows = await conn.fetch(
                f"SELECT {COLUMNS} FROM tasks {where} ORDER BY id LIMIT ${n + 1} OFFSET ${n + 2}",
                *params, limit, offset,
            )
        return rows, total

    async def cancel(self, task_id: int) -> tuple[asyncpg.Record | None, str | None]:
        """queued -> cancelled at once; running -> cancel_requested_at, the worker stops between passes."""
        row = await self.pool.fetchrow(
            f"UPDATE tasks SET "
            f"  status = CASE WHEN status = 'queued' THEN 'cancelled' ELSE status END, "
            f"  finished_at = CASE WHEN status = 'queued' THEN clock_timestamp() ELSE finished_at END, "
            f"  worker_id = CASE WHEN status = 'queued' THEN NULL ELSE worker_id END, "
            f"  cancel_requested_at = coalesce(cancel_requested_at, clock_timestamp()) "
            f"WHERE id = $1 AND status IN ('queued', 'running') RETURNING {COLUMNS}",
            task_id,
        )
        if row is not None:
            return row, None
        return None, await self.pool.fetchval("SELECT status FROM tasks WHERE id = $1", task_id)

    async def counts(self) -> dict[str, int]:
        rows = await self.pool.fetch("SELECT status, count(*) AS n FROM tasks GROUP BY status")
        return {r["status"]: r["n"] for r in rows}


def decode_result(record: asyncpg.Record | None) -> dict | None:
    if record is None:
        return None
    out = dict(record)
    if isinstance(out["result"], str):
        out["result"] = json.loads(out["result"])
    return out
