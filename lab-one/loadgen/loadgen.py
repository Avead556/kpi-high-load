import argparse
import gzip
import http.client
import json
import multiprocessing as mp
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

TIMEOUT = 10.0
SAMPLE_INTERVAL = 0.005


class Client:
    def __init__(self, base):
        parts = urlsplit(base)
        self.host = parts.hostname
        self.port = parts.port or 80
        self.local = threading.local()

    def get(self, path):
        conn = getattr(self.local, "conn", None)
        if conn is None:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=TIMEOUT)
            self.local.conn = conn
        try:
            conn.request("GET", path)
            response = conn.getresponse()
            response.read()
            return response.status, response.getheader("X-Instance", ""), ""
        except Exception as exc:
            conn.close()
            self.local.conn = None
            return 0, "", type(exc).__name__


def percentile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def open_worker(args, index, start_at, queue):
    client = Client(args.url)
    rng = random.Random(index)
    interval = args.procs / args.rate
    total = int((args.warmup + args.duration) * args.rate / args.procs)
    window = (start_at + args.warmup, start_at + args.warmup + args.duration)
    records = []
    lock = threading.Lock()
    state = {"done": 0, "wire": 0}
    phase = index / args.procs

    def fire(scheduled, path):
        with lock:
            state["wire"] += 1
        status, instance, error = client.get(path)
        end = time.perf_counter()
        with lock:
            state["wire"] -= 1
            state["done"] += 1
            records.append((scheduled, end, status, instance, error))

    samples = []
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            with lock:
                now = time.perf_counter()
                if window[0] <= now < window[1]:
                    due = min(total, int((now - start_at) / interval - phase) + 1)
                    samples.append((due - state["done"], state["wire"]))
            time.sleep(SAMPLE_INTERVAL)

    watcher = threading.Thread(target=sampler, daemon=True)
    watcher.start()
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        for i in range(total):
            scheduled = start_at + (i + phase) * interval
            delay = scheduled - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            pool.submit(fire, scheduled, args.path.format(id=rng.choice(args.ids)))
    stop.set()
    watcher.join()
    queue.put({"records": records, "samples": samples})


def closed_worker(args, index, start_at, queue, concurrency):
    client = Client(args.url)
    rng = random.Random(index)
    finish = start_at + args.warmup + args.duration
    window = (start_at + args.warmup, finish)
    records = []
    lock = threading.Lock()
    state = {"busy": 0}
    samples = []

    def loop():
        while True:
            begin = time.perf_counter()
            if begin >= finish:
                return
            with lock:
                state["busy"] += 1
            status, instance, error = client.get(args.path.format(id=rng.choice(args.ids)))
            end = time.perf_counter()
            with lock:
                state["busy"] -= 1
                records.append((begin, end, status, instance, error))

    def sampler():
        while time.perf_counter() < finish:
            with lock:
                if time.perf_counter() >= window[0]:
                    samples.append((state["busy"], state["busy"]))
            time.sleep(SAMPLE_INTERVAL)

    while time.perf_counter() < start_at:
        time.sleep(0.001)
    threads = [threading.Thread(target=loop) for _ in range(concurrency)]
    threads.append(threading.Thread(target=sampler))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    queue.put({"records": records, "samples": samples})


def run(args):
    start_at = time.perf_counter() + 0.5
    queue = mp.get_context("fork").SimpleQueue()
    procs = []
    if args.mode == "open":
        for i in range(args.procs):
            procs.append(mp.get_context("fork").Process(target=open_worker, args=(args, i, start_at, queue)))
    else:
        share = [args.concurrency // args.procs + (1 if i < args.concurrency % args.procs else 0) for i in range(args.procs)]
        for i, c in enumerate(share):
            if c:
                procs.append(mp.get_context("fork").Process(target=closed_worker, args=(args, i, start_at, queue, c)))
    for p in procs:
        p.start()
    parts = [queue.get() for _ in procs]
    for p in procs:
        p.join()
    return start_at, parts


def summarize(args, start_at, parts):
    lo = start_at + args.warmup
    hi = lo + args.duration
    records = [r for part in parts for r in part["records"]]
    measured = [r for r in records if lo <= r[0] < hi]
    finished_in_window = [r for r in records if lo <= r[1] < hi]
    latencies = [(r[1] - r[0]) * 1000 for r in measured]
    non_2xx = sum(1 for r in measured if not 200 <= r[2] < 300)
    instances, errors = {}, {}
    for r in measured:
        instances[r[3] or "-"] = instances.get(r[3] or "-", 0) + 1
        if r[4]:
            errors[r[4]] = errors.get(r[4], 0) + 1
    sample_count = min((len(p["samples"]) for p in parts), default=0)
    measured_l = sum(statistics.fmean(x[0] for x in p["samples"][:sample_count]) for p in parts) if sample_count else None
    on_wire = sum(statistics.fmean(x[1] for x in p["samples"][:sample_count]) for p in parts) if sample_count else None
    mean_w = statistics.fmean(latencies) / 1000 if latencies else None
    summary = {
        "mode": args.mode,
        "path": args.path,
        "target_rps": args.rate if args.mode == "open" else None,
        "concurrency": args.concurrency if args.mode == "closed" else None,
        "duration_s": args.duration,
        "warmup_s": args.warmup,
        "procs": args.procs,
        "sent": len(measured),
        "achieved_rps": round(sum(1 for r in finished_in_window if 200 <= r[2] < 300) / args.duration, 2),
        "offered_rps": round(len(measured) / args.duration, 2),
        "p50_ms": round(percentile(latencies, 0.50), 2) if latencies else None,
        "p95_ms": round(percentile(latencies, 0.95), 2) if latencies else None,
        "p99_ms": round(percentile(latencies, 0.99), 2) if latencies else None,
        "mean_ms": round(mean_w * 1000, 2) if mean_w else None,
        "non_2xx": non_2xx,
        "non_2xx_share": round(non_2xx / len(measured), 4) if measured else None,
        "measured_L": round(measured_l, 3) if measured_l is not None else None,
        "on_wire_mean": round(on_wire, 3) if on_wire is not None else None,
        "instances": instances,
        "errors": errors,
    }
    return summary, measured


def main():
    parser = argparse.ArgumentParser(description="open and closed loop HTTP load generator")
    parser.add_argument("--url", default="http://127.0.0.1:8018")
    parser.add_argument("--path", default="/vaccine-lots/{id}")
    parser.add_argument("--ids", default="")
    parser.add_argument("--mode", choices=("open", "closed"), default="open")
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--procs", type=int, default=0)
    parser.add_argument("--threads", type=int, default=128)
    parser.add_argument("--out", default="")
    parser.add_argument("--raw", default="")
    args = parser.parse_args()
    if not args.procs:
        args.procs = max(1, min(os.cpu_count(), int(args.rate // 25) or 1)) if args.mode == "open" else max(1, min(os.cpu_count(), args.concurrency))
    args.ids = [int(x) for x in args.ids.split(",")] if args.ids else [0]
    start_at, parts = run(args)
    summary, measured = summarize(args, start_at, parts)
    text = json.dumps(summary, ensure_ascii=False, indent=2)
    print(text)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    if args.raw:
        with gzip.open(args.raw, "wt", encoding="utf-8") as f:
            f.write("scheduled_s,latency_ms,status,instance,error\n")
            for r in sorted(measured):
                f.write(f"{r[0] - start_at:.6f},{(r[1] - r[0]) * 1000:.3f},{r[2]},{r[3]},{r[4]}\n")


if __name__ == "__main__":
    main()
