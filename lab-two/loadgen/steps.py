import argparse
import asyncio
import csv
import json
import math
import multiprocessing as mp
import os
import random
import socket
import statistics
import struct
import time
from array import array
from collections import deque
from pathlib import Path
from urllib.parse import urlsplit

TIMEOUT = 5.0
MAX_SECONDS = 3600
WINDOW = 15


def rate_at(rel, args):
    if rel < args.warmup:
        return args.warmup_rate
    if args.constant:
        return args.constant
    return args.start + int((rel - args.warmup) // args.step_seconds) * args.step


def percentile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


async def read_response(reader):
    line = await reader.readline()
    if not line:
        raise ConnectionResetError("connection closed by peer")
    status = int(line[9:12])
    length, pod, close = 0, "", False
    while True:
        header = await reader.readline()
        if header in (b"\r\n", b""):
            break
        name, _, value = header.partition(b":")
        name = name.strip().lower()
        if name == b"content-length":
            length = int(value)
        elif name == b"x-pod":
            pod = value.strip().decode()
        elif name == b"connection" and value.strip().lower() == b"close":
            close = True
    if length:
        await reader.readexactly(length)
    return status, pod, close


def worker(index, args, t0, stop_at, achieved, queue):
    seconds = MAX_SECONDS
    scheduled = array("i", [0]) * seconds
    sent = array("i", [0]) * seconds
    ok = array("i", [0]) * seconds
    non_2xx = array("i", [0]) * seconds
    errors = array("i", [0]) * seconds
    latencies = {}
    pods = {}
    error_kinds = {}

    async def main():
        loop = asyncio.get_running_loop()
        parts = urlsplit(args.url)
        host, port = parts.hostname, parts.port or 80
        rng = random.Random(index)
        requests = [f"GET {args.path.format(id=i)} HTTP/1.1\r\nHost: {host}\r\n\r\n".encode() for i in args.ids]
        idle, waiters = [], deque()
        state = {"open": 0}

        async def acquire():
            while True:
                if idle:
                    return idle.pop()
                if state["open"] < args.conn_limit:
                    state["open"] += 1
                    try:
                        return await asyncio.open_connection(host, port)
                    except BaseException:
                        state["open"] -= 1
                        wake(None)
                        raise
                waiter = loop.create_future()
                waiters.append(waiter)
                conn = await waiter
                if conn is not None:
                    return conn

        def wake(conn):
            while waiters:
                waiter = waiters.popleft()
                if not waiter.done():
                    waiter.set_result(conn)
                    return True
            return False

        def release(conn):
            if not wake(conn):
                idle.append(conn)

        def discard(conn, reset):
            writer = conn[1]
            if reset:
                sock = writer.get_extra_info("socket")
                if sock is not None:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            writer.close()
            state["open"] -= 1
            wake(None)

        def finish(planned, outcome, pod="", kind=""):
            end = time.time()
            sec = min(int(end - t0), seconds - 1)
            if outcome == "ok":
                ok[sec] += 1
                with achieved.get_lock():
                    achieved[sec] += 1
            elif outcome == "error":
                errors[sec] += 1
                error_kinds[kind] = error_kinds.get(kind, 0) + 1
            else:
                non_2xx[sec] += 1
                if kind:
                    error_kinds[kind] = error_kinds.get(kind, 0) + 1
            latencies.setdefault(sec, array("f")).append((end - planned) * 1000)
            if pod:
                pods[pod] = pods.get(pod, 0) + 1

        async def fire(planned, request):
            try:
                conn = await acquire()
            except OSError as exc:
                finish(planned, "error", kind=f"{type(exc).__name__}:{exc.errno}")
                return
            sent[min(int(time.time() - t0), seconds - 1)] += 1
            reader, writer = conn
            try:
                writer.write(request)
                status, pod, close = await asyncio.wait_for(read_response(reader), TIMEOUT)
            except asyncio.TimeoutError:
                discard(conn, True)
                finish(planned, "timeout", kind="Timeout")
                return
            except (OSError, asyncio.IncompleteReadError, ValueError) as exc:
                discard(conn, True)
                finish(planned, "error", kind=type(exc).__name__)
                return
            if close:
                discard(conn, False)
            else:
                release(conn)
            finish(planned, "ok" if 200 <= status < 300 else "non_2xx", pod, "" if 200 <= status < 300 else f"HTTP {status}")

        tasks = set()
        planned = t0 + index / args.procs * args.procs / rate_at(0, args)
        while True:
            rel = planned - t0
            if rel >= stop_at.value or rel >= seconds - 10:
                break
            delay = planned - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
            scheduled[int(rel)] += 1
            task = loop.create_task(fire(planned, requests[rng.randrange(len(requests))]))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            planned += args.procs / rate_at(rel, args)
        if tasks:
            await asyncio.wait(tasks)
        for conn in idle:
            discard(conn, True)

    asyncio.run(main())
    queue.put({
        "scheduled": scheduled.tolist(), "sent": sent.tolist(), "ok": ok.tolist(), "non_2xx": non_2xx.tolist(),
        "errors": errors.tolist(), "latencies": {k: v.tobytes() for k, v in latencies.items()},
        "pods": pods, "error_kinds": error_kinds,
    })


def step_bounds(args, k):
    begin = args.warmup + k * args.step_seconds
    return begin, begin + args.step_seconds


def run(args):
    ctx = mp.get_context("fork")
    stop_at = ctx.Value("d", math.inf)
    achieved = ctx.Array("i", MAX_SECONDS)
    queue = ctx.SimpleQueue()
    t0 = time.time() + 1.0
    procs = [ctx.Process(target=worker, args=(i, args, t0, stop_at, achieved, queue)) for i in range(args.procs)]
    for p in procs:
        p.start()
    decisions = []
    if args.constant:
        stop_at.value = args.warmup + args.duration
    else:
        below_in_row, k = 0, 0
        while True:
            begin, end = step_bounds(args, k)
            target = args.start + k * args.step
            while time.time() < t0 + end + 1.0:
                time.sleep(0.2)
            with achieved.get_lock():
                window = list(achieved[int(end) - WINDOW:int(end)])
            mean = statistics.fmean(window)
            below_in_row = below_in_row + 1 if mean < 0.9 * target else 0
            decisions.append({"step": k, "target": target, "achieved_mean": round(mean, 1), "below_in_row": below_in_row})
            print(f"step {k:>2} target {target:>6} achieved {mean:9.1f}", flush=True)
            reached_g = args.g and target >= args.g
            if below_in_row >= 2 or reached_g or target >= args.max_rate:
                stop_at.value = time.time() - t0
                break
            k += 1
    parts = [queue.get() for _ in procs]
    for p in procs:
        p.join()
    return t0, parts, decisions


def merge(args, t0, parts):
    last = max(i for part in parts for i, v in enumerate(part["scheduled"]) if v) + int(TIMEOUT) + 2
    latencies = {}
    for part in parts:
        for sec, raw in part["latencies"].items():
            values = array("f")
            values.frombytes(raw)
            latencies.setdefault(sec, []).extend(values)
    rows = []
    for sec in range(last):
        lat = latencies.get(sec, [])
        row = {
            "second": sec, "epoch": round(t0 + sec, 3), "target_rps": rate_at(sec, args),
            "scheduled": sum(p["scheduled"][sec] for p in parts), "sent": sum(p["sent"][sec] for p in parts),
            "achieved": sum(p["ok"][sec] for p in parts), "non_2xx": sum(p["non_2xx"][sec] for p in parts),
            "conn_errors": sum(p["errors"][sec] for p in parts),
            "p50_ms": round(percentile(lat, 0.50), 2) if lat else None,
            "p95_ms": round(percentile(lat, 0.95), 2) if lat else None,
            "p99_ms": round(percentile(lat, 0.99), 2) if lat else None,
        }
        rows.append(row)
    pods, kinds = {}, {}
    for part in parts:
        for k, v in part["pods"].items():
            pods[k] = pods.get(k, 0) + v
        for k, v in part["error_kinds"].items():
            kinds[k] = kinds.get(k, 0) + v
    return rows, latencies, pods, kinds


def summarize_steps(args, rows, latencies, stopped_at):
    steps = []
    k = 0
    while not args.constant:
        begin, end = step_bounds(args, k)
        if end > stopped_at + 1e-6:
            break
        window = range(int(end) - WINDOW, int(end))
        achieved = [rows[s]["achieved"] for s in window]
        lat = [v for s in window for v in latencies.get(s, [])]
        done = sum(rows[s]["achieved"] + rows[s]["non_2xx"] + rows[s]["conn_errors"] for s in window)
        bad = sum(rows[s]["non_2xx"] + rows[s]["conn_errors"] for s in window)
        steps.append({
            "step": k, "target_rps": args.start + k * args.step,
            "begin_s": begin, "end_s": end,
            "window_epoch": [rows[0]["epoch"] + end - WINDOW, rows[0]["epoch"] + end],
            "sent_mean": round(statistics.fmean(rows[s]["sent"] for s in window), 1),
            "achieved_mean": round(statistics.fmean(achieved), 1),
            "achieved_std": round(statistics.stdev(achieved), 1),
            "p50_ms": round(percentile(lat, 0.50), 2) if lat else None,
            "p95_ms": round(percentile(lat, 0.95), 2) if lat else None,
            "p99_ms": round(percentile(lat, 0.99), 2) if lat else None,
            "non_2xx_share": round(bad / done, 5) if done else None,
            "conn_errors": sum(rows[s]["conn_errors"] for s in window),
        })
        k += 1
    return steps


def main():
    parser = argparse.ArgumentParser(description="open loop step load generator with per second records")
    parser.add_argument("--url", default="http://127.0.0.1:8018")
    parser.add_argument("--path", default="/vaccine-lots/{id}")
    parser.add_argument("--ids", default="results/ids.txt")
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--warmup-rate", type=float, default=200)
    parser.add_argument("--start", type=float, default=200)
    parser.add_argument("--step", type=float, default=200)
    parser.add_argument("--step-seconds", type=float, default=20)
    parser.add_argument("--g", type=float, default=0)
    parser.add_argument("--max-rate", type=float, default=20000)
    parser.add_argument("--constant", type=float, default=0)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--procs", type=int, default=os.cpu_count())
    parser.add_argument("--conn-limit", type=int, default=0)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    ids_file = Path(args.ids)
    args.ids = [int(x) for x in ids_file.read_text().split(",")] if "{id}" in args.path else [0]
    if not args.conn_limit:
        args.conn_limit = 15000 // args.procs
    t0, parts, decisions = run(args)
    stopped = max(i for part in parts for i, v in enumerate(part["scheduled"]) if v) + 1
    rows, latencies, pods, kinds = merge(args, t0, parts)
    steps = summarize_steps(args, rows, latencies, stopped)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "seconds.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    best = max(steps, key=lambda s: s["achieved_mean"]) if steps else None
    summary = {
        "path": args.path, "procs": args.procs, "conn_limit_total": args.conn_limit * args.procs,
        "timeout_s": TIMEOUT, "warmup_s": args.warmup, "warmup_rate": args.warmup_rate,
        "step_rps": args.step, "step_seconds": args.step_seconds, "g": args.g or None,
        "start_epoch": round(t0, 3), "stopped_at_s": stopped, "decisions": decisions,
        "steps": steps, "X": best, "pods": pods, "error_kinds": kinds,
    }
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    if best:
        print(f"X = {best['achieved_mean']} +- {best['achieved_std']} rps at step target {best['target_rps']}, "
              f"p99 client {best['p99_ms']} ms, non-2xx share {best['non_2xx_share']}")
    print(f"pods {pods} errors {kinds}")


if __name__ == "__main__":
    main()
