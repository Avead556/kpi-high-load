"""SQL of the task queue. Every statement that changes a claimed task carries `AND token = %(token)s`."""
from dataclasses import dataclass
from datetime import timedelta

import psycopg
from psycopg.types.json import Jsonb

RETURNING = "RETURNING id, record_id, seed, token, attempts, max_attempts"
CLAIM_SET = ("status = 'running', worker_id = %(worker)s, token = token + 1, "
             "lease_until = clock_timestamp() + %(lease)s, attempts = attempts + 1, started_at = clock_timestamp()")

CLAIM = {
    "skip_locked": f"""
        UPDATE tasks SET {CLAIM_SET}
        WHERE id = (SELECT id FROM tasks WHERE status = 'queued'
                    ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED)
        {RETURNING}""",
    "for_update": f"""
        UPDATE tasks SET {CLAIM_SET}
        WHERE id = (SELECT id FROM tasks WHERE status = 'queued'
                    ORDER BY created_at LIMIT 1 FOR UPDATE)
        {RETURNING}""",
}
NAIVE_SELECT = "SELECT id FROM tasks WHERE status = 'queued' ORDER BY created_at LIMIT 1"
NAIVE_UPDATE = f"UPDATE tasks SET {CLAIM_SET} WHERE id = %(id)s {RETURNING}"

# expired lease: back to the queue, or failed after max_attempts, or cancelled if a cancel was requested
REAP = """
    UPDATE tasks SET
        status = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled'
                      WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
        finished_at = CASE WHEN cancel_requested_at IS NOT NULL OR attempts >= max_attempts
                           THEN clock_timestamp() END,
        worker_id = NULL, lease_until = NULL
    WHERE id IN (SELECT id FROM tasks WHERE status = 'running' AND lease_until < clock_timestamp()
                 FOR UPDATE SKIP LOCKED)
    RETURNING id, status, token, attempts"""

HEARTBEAT = """UPDATE tasks SET lease_until = clock_timestamp() + %(lease)s
               WHERE id = %(id)s AND token = %(token)s AND status = 'running'"""
CHECK = "SELECT cancel_requested_at IS NOT NULL FROM tasks WHERE id = %(id)s AND token = %(token)s"
FINISH = """
    UPDATE tasks SET status = CASE WHEN cancel_requested_at IS NULL THEN 'done' ELSE 'cancelled' END,
                     finished_at = clock_timestamp(), lease_until = NULL
    WHERE id = %(id)s AND token = %(token)s RETURNING status"""
INSERT_RESULT = """
    INSERT INTO task_results (task_id, token, worker_id, result) VALUES (%(id)s, %(token)s, %(worker)s, %(result)s)
    ON CONFLICT (task_id) DO NOTHING"""
NAIVE_INSERT = ("INSERT INTO task_results (task_id, token, worker_id, result) "
                "VALUES (%(id)s, %(token)s, %(worker)s, %(result)s)")
NAIVE_FINISH = "UPDATE tasks SET status = 'done', finished_at = clock_timestamp() WHERE id = %(id)s"
CANCEL = """
    UPDATE tasks SET status = 'cancelled', finished_at = clock_timestamp(), lease_until = NULL
    WHERE id = %(id)s AND token = %(token)s"""
FAIL = """
    UPDATE tasks SET status = CASE WHEN %(final)s OR attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
                     finished_at = CASE WHEN %(final)s OR attempts >= max_attempts THEN clock_timestamp() END,
                     worker_id = NULL, lease_until = NULL
    WHERE id = %(id)s AND token = %(token)s RETURNING status"""
RETURN = "UPDATE tasks SET status = 'queued', worker_id = NULL, lease_until = NULL WHERE id = %(id)s AND token = %(token)s"


@dataclass(frozen=True)
class Task:
    id: int
    record_id: int
    seed: int
    token: int
    attempts: int
    max_attempts: int


class Queue:
    def __init__(self, conn: psycopg.Connection, worker_id: str, lease_seconds: float):
        self.conn = conn
        self.worker = worker_id
        self.lease = timedelta(seconds=lease_seconds)

    def _params(self, task: Task | None = None, **extra):
        p = {"worker": self.worker, "lease": self.lease, **extra}
        if task is not None:
            p |= {"id": task.id, "token": task.token}
        return p

    def claim(self, mode: str) -> Task | None:
        if mode == "naive":
            row = self.conn.execute(NAIVE_SELECT).fetchone()
            if row is None:
                return None
            row = self.conn.execute(NAIVE_UPDATE, self._params(id=row[0])).fetchone()
        else:
            row = self.conn.execute(CLAIM[mode], self._params()).fetchone()
        return Task(*row) if row else None

    def reap(self) -> list[tuple]:
        return self.conn.execute(REAP).fetchall()

    def heartbeat(self, task: Task) -> int:
        return self.conn.execute(HEARTBEAT, self._params(task)).rowcount

    def check(self, task: Task) -> bool | None:
        """True if a cancel was requested, None if the task is no longer ours."""
        row = self.conn.execute(CHECK, self._params(task)).fetchone()
        return None if row is None else row[0]

    def finish(self, task: Task, result: dict, mode: str) -> str | None:
        """Returns the final status, or None when the token is stale (zero rows updated)."""
        params = self._params(task, result=Jsonb(result))
        if mode == "naive":
            self.conn.execute(NAIVE_INSERT, params)
            self.conn.execute(NAIVE_FINISH, params)
            return "done"
        with self.conn.transaction():
            row = self.conn.execute(FINISH, params).fetchone()
            if row is None:
                return None
            if row[0] == "done":
                self.conn.execute(INSERT_RESULT, params)
            return row[0]

    def cancel(self, task: Task) -> int:
        return self.conn.execute(CANCEL, self._params(task)).rowcount

    def fail(self, task: Task, final: bool) -> str | None:
        row = self.conn.execute(FAIL, self._params(task, final=final)).fetchone()
        return row[0] if row else None

    def give_back(self, task: Task) -> int:
        return self.conn.execute(RETURN, self._params(task)).rowcount
