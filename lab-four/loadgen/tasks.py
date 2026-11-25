"""Task generator: POST /vaccine-lots/{id}/tasks with a fixed rate (open loop) for the 100 seeded lots."""
import argparse
import csv
import http.client
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

from common import ROOT, WEB


def load_ids(path=ROOT / "results" / "ids.txt") -> list[int]:
    return [int(x) for x in Path(path).read_text().split(",")]


class Generator:
    def __init__(self, url=WEB, ids=None, threads=16, seed=18):
        parts = urlsplit(url)
        self.host, self.port = parts.hostname, parts.port or 80
        self.ids = ids or load_ids()
        self.rng = random.Random(seed)
        self.local = threading.local()
        self.pool = ThreadPoolExecutor(threads)

    def post(self, index, planned, record_id):
        sent = time.time()
        status, task_id, error = None, None, ""
        for attempt in (1, 2):
            conn = getattr(self.local, "conn", None)
            if conn is None:
                conn = self.local.conn = http.client.HTTPConnection(self.host, self.port, timeout=10)
            try:
                conn.request("POST", f"/vaccine-lots/{record_id}/tasks")
                response = conn.getresponse()
                body = response.read()
                status = response.status
                if status == 202:
                    task_id = json.loads(body)["id"]
                break
            except (OSError, http.client.HTTPException) as exc:
                conn.close()
                self.local.conn = None
                error = type(exc).__name__
        return {"i": index, "planned": round(planned, 4), "sent": round(sent, 4), "done": round(time.time(), 4),
                "record_id": record_id, "status": status, "task_id": task_id, "error": error}

    def run(self, rate: float, count: int | None = None, duration: float | None = None, records=None):
        """Sends `count` tasks, or rate * duration tasks, spaced 1 / rate apart."""
        n = count if count is not None else int(round(rate * duration))
        records = records or self.ids
        t0 = time.time() + 0.2
        futures = []
        for i in range(n):
            planned = t0 + i / rate
            delay = planned - time.time()
            if delay > 0:
                time.sleep(delay)
            futures.append(self.pool.submit(self.post, i, planned, self.rng.choice(records)))
        rows = [f.result() for f in futures]
        return {"t0": t0, "rate": rate, "count": n, "rows": rows,
                "accepted": sum(r["status"] == 202 for r in rows),
                "non_2xx": sum(r["status"] is None or not 200 <= r["status"] < 300 for r in rows)}

    def start(self, rate, count=None, duration=None, records=None):
        """Runs in the background; .join() returns the run summary."""
        box = {}
        thread = threading.Thread(target=lambda: box.update(self.run(rate, count, duration, records)))
        thread.start()

        class Handle:
            def join(self_inner):
                thread.join()
                return box

        return Handle()


def save(run: dict, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(run["rows"][0]))
        w.writeheader()
        w.writerows(run["rows"])


def main():
    parser = argparse.ArgumentParser(description="create processing tasks at a fixed rate")
    parser.add_argument("--url", default=WEB)
    parser.add_argument("--rate", type=float, required=True)
    parser.add_argument("--count", type=int)
    parser.add_argument("--duration", type=float)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    run = Generator(args.url).run(args.rate, args.count, args.duration)
    save(run, Path(args.out))
    print(f"sent {run['count']} at {args.rate}/s: accepted {run['accepted']}, non-2xx {run['non_2xx']}")


if __name__ == "__main__":
    main()
