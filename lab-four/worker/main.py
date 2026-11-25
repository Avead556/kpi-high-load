"""Queue worker: claim -> fetch record over HTTP -> process the slice -> store the result.

One task at a time in the main thread; a supervisor thread with its own connection extends the lease
(heartbeat every L / 3) and, after SIGTERM, gives the task back once the grace deadline passes.
"""
import http.client
import json
import os
import random
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit

import psycopg
from prometheus_client import start_http_server

from worker import metrics as m
from worker.config import Settings
from worker.queue import Queue, Task
from worker.slice import process


def log(event: str, **fields):
    ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    print(" ".join([f"ts={ts}", f"event={event}", *(f"{k}={v}" for k, v in fields.items())]), flush=True)


class Abort(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class Records:
    """GET /vaccine-lots/{id} from the web service over one keep-alive connection."""

    def __init__(self, url: str):
        parts = urlsplit(url)
        self.host, self.port = parts.hostname, parts.port or 80
        self.conn = None

    def get(self, record_id: int) -> dict | None:
        for attempt in (1, 2):
            if self.conn is None:
                self.conn = http.client.HTTPConnection(self.host, self.port, timeout=5)
            try:
                self.conn.request("GET", f"/vaccine-lots/{record_id}")
                response = self.conn.getresponse()
                body = response.read()
            except (OSError, http.client.HTTPException):
                self.conn.close()
                self.conn = None
                if attempt == 2:
                    raise
                continue
            if response.status == 404:
                return None
            if response.status != 200:
                raise RuntimeError(f"web answered {response.status}")
            return json.loads(body)


class Worker:
    def __init__(self, settings: Settings):
        self.s = settings
        self.stop = threading.Event()
        self.deadline = None
        self.lock = threading.Lock()
        self.current: Task | None = None
        self.claimed_at = 0.0
        self.lost = False
        self.records = Records(settings.web_url)
        self.fencing = settings.store_mode == "idempotent"

    # connections -----------------------------------------------------------------------------------------------
    def connect(self, role: str) -> psycopg.Connection:
        delay = 0.2
        while True:
            try:
                return psycopg.connect(self.s.dsn, autocommit=True, application_name=f"{self.s.worker_id}:{role}")
            except psycopg.OperationalError as exc:
                log("db_connect_failed", worker=self.s.worker_id, role=role, error=repr(str(exc).strip()[:120]))
                time.sleep(delay)
                delay = min(delay * 2, 2.0)

    def reconnect(self, exc: Exception):
        log("db_reconnect", worker=self.s.worker_id, role="main", error=repr(str(exc).strip()[:120]))
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = self.connect("main")
        self.queue = Queue(self.conn, self.s.worker_id, self.s.lease_seconds)

    # main loop -------------------------------------------------------------------------------------------------
    def on_sigterm(self, signum, frame):
        if not self.stop.is_set():
            self.deadline = time.monotonic() + self.s.grace_seconds
            self.stop.set()
            log("sigterm", worker=self.s.worker_id, in_flight=self.current.id if self.current else None)

    def run(self):
        start_http_server(self.s.metrics_port)
        signal.signal(signal.SIGTERM, self.on_sigterm)
        signal.signal(signal.SIGINT, self.on_sigterm)
        self.conn = self.connect("main")
        self.queue = Queue(self.conn, self.s.worker_id, self.s.lease_seconds)
        threading.Thread(target=self.supervise, name="supervisor", daemon=True).start()
        log("start", worker=self.s.worker_id, claim=self.s.claim_mode, store=self.s.store_mode,
            lease_s=self.s.lease_seconds, heartbeat=int(self.s.heartbeat), reap=int(self.s.reap),
            poll_ms=round(self.s.poll_seconds * 1000, 3), fail_rate=self.s.fail_rate, q=f"{self.s.q_lo}/{self.s.q_hi}")
        while not self.stop.is_set():
            try:
                if self.s.reap:
                    self.reap()
                started = time.perf_counter()
                task = self.queue.claim(self.s.claim_mode)
                m.DURATION.labels("claim").observe(time.perf_counter() - started)
            except psycopg.OperationalError as exc:
                self.reconnect(exc)
                continue
            if task is None:
                m.CLAIMS.labels("empty").inc()
                self.stop.wait(self.s.poll_seconds)
                continue
            m.CLAIMS.labels("task").inc()
            self.execute(task)
        self.conn.close()
        log("exit", worker=self.s.worker_id, code=0)
        sys.exit(0)

    def reap(self):
        for task_id, status, token, attempts in self.queue.reap():
            m.REAPED.labels(status).inc()
            log("reaped", worker=self.s.worker_id, task=task_id, to=status, token=token, attempts=attempts)

    def execute(self, task: Task):
        with self.lock:
            self.current, self.claimed_at, self.lost = task, time.monotonic(), False
        m.IN_FLIGHT.inc()
        log("claimed", worker=self.s.worker_id, task=task.id, token=task.token, attempts=task.attempts,
            record=task.record_id)
        try:
            self.steps(task)
        except Abort as exc:
            m.TASKS.labels(exc.reason).inc()
        except psycopg.OperationalError as exc:
            m.TASKS.labels("lost").inc()
            log("lost", worker=self.s.worker_id, task=task.id, token=task.token, reason="db connection")
            self.reconnect(exc)
        except Exception as exc:
            log("error", worker=self.s.worker_id, task=task.id, token=task.token, error=repr(exc)[:200])
            try:
                status = self.queue.fail(task, final=False)
            except psycopg.OperationalError as db_exc:
                self.reconnect(db_exc)
                status = None
            m.TASKS.labels("failed" if status == "failed" else "lost").inc()
        finally:
            m.IN_FLIGHT.dec()
            with self.lock:
                self.current = None

    def steps(self, task: Task):
        t0 = time.perf_counter()
        record = self.records.get(task.record_id)
        t1 = time.perf_counter()
        if record is None:
            status = self.queue.fail(task, final=True)
            log("failed", worker=self.s.worker_id, task=task.id, token=task.token, reason="record not found")
            raise Abort("failed" if status else "lost")
        self.between(task)
        result = process(task.seed, self.s.slice_size, self.s.q_lo, self.s.q_hi, lambda: self.between(task))
        t2 = time.perf_counter()
        if self.s.fail_rate and random.random() < self.s.fail_rate:
            log("crash", worker=self.s.worker_id, task=task.id, token=task.token, attempts=task.attempts)
            os._exit(1)
        status = self.queue.finish(task, result, self.s.store_mode)
        t3 = time.perf_counter()
        if status is None:
            m.STALE.labels("store").inc()
            log("stale", worker=self.s.worker_id, op="store", rows=0, task=task.id, token=task.token)
            raise Abort("lost")
        if status == "cancelled":
            log("cancelled", worker=self.s.worker_id, task=task.id, token=task.token, at="store")
            raise Abort("cancelled")
        m.DURATION.labels("fetch").observe(t1 - t0)
        m.DURATION.labels("process").observe(t2 - t1)
        m.DURATION.labels("store").observe(t3 - t2)
        m.TASKS.labels("done").inc()
        log("done", worker=self.s.worker_id, task=task.id, token=task.token, attempts=task.attempts,
            fetch_ms=round((t1 - t0) * 1000, 2), process_ms=round((t2 - t1) * 1000, 2),
            store_ms=round((t3 - t2) * 1000, 2), p_lo=result["p_lo"], p_hi=result["p_hi"])

    def between(self, task: Task):
        """Pass boundary: stop if the heartbeat lost the task, the token went stale or a cancel was requested."""
        if not self.fencing:
            return
        if self.lost:
            raise Abort("lost")
        cancel = self.queue.check(task)
        if cancel is None:
            m.STALE.labels("check").inc()
            log("stale", worker=self.s.worker_id, op="check", rows=0, task=task.id, token=task.token)
            raise Abort("lost")
        if cancel:
            rows = self.queue.cancel(task)
            log("cancelled", worker=self.s.worker_id, task=task.id, token=task.token, at="pass", rows=rows)
            raise Abort("cancelled" if rows else "lost")

    # supervisor thread -----------------------------------------------------------------------------------------
    def supervise(self):
        conn = self.connect("heartbeat")
        queue = Queue(conn, self.s.worker_id, self.s.lease_seconds)
        interval = self.s.lease_seconds / 3
        tick = min(interval / 4, 0.05)
        key, next_beat = None, 0.0
        while True:
            time.sleep(tick)
            with self.lock:
                task, claimed_at = self.current, self.claimed_at
            now = time.monotonic()
            try:
                if self.deadline is not None and now >= self.deadline:
                    if task is not None:
                        rows = queue.give_back(task)
                        m.TASKS.labels("returned").inc()
                        log("returned", worker=self.s.worker_id, task=task.id, token=task.token, rows=rows)
                    log("exit", worker=self.s.worker_id, code=0, reason="grace deadline")
                    os._exit(0)
                if task is None or not self.s.heartbeat:
                    continue
                if key != (task.id, task.token):
                    key, next_beat = (task.id, task.token), claimed_at + interval
                if now < next_beat:
                    continue
                next_beat = now + interval
                if queue.heartbeat(task) == 0:
                    m.STALE.labels("heartbeat").inc()
                    log("stale", worker=self.s.worker_id, op="heartbeat", rows=0, task=task.id, token=task.token)
                    with self.lock:
                        if self.current is task:
                            self.lost = True
            except psycopg.OperationalError as exc:
                log("db_reconnect", worker=self.s.worker_id, role="heartbeat", error=repr(str(exc).strip()[:120]))
                conn.close()
                conn = self.connect("heartbeat")
                queue = Queue(conn, self.s.worker_id, self.s.lease_seconds)


def main():
    Worker(Settings.from_env()).run()


if __name__ == "__main__":
    main()
