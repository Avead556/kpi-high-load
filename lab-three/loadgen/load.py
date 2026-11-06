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


def parse_profile(text):
    segments = []
    for part in text.split(","):
        duration, rate = part.split(":")
        segments.append((float(duration), float(rate)))
    return segments


def rate_at(rel, args):
    if args.profile:
        t = 0.0
        for duration, rate in args.profile:
            if rel < t + duration:
                return rate
            t += duration
        return args.profile[-1][1]
    if rel < args.warmup:
        return args.warmup_rate
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


def worker(index, conns, share, args, t0, stop_at, achieved, queue):
    seconds = MAX_SECONDS
    scheduled = array("i", [0]) * seconds
    sent = array("i", [0]) * seconds
    ok = array("i", [0]) * seconds
    non_2xx = array("i", [0]) * seconds
    errors = array("i", [0]) * seconds
    opened = array("i", [0]) * seconds
    latencies = {}
    pods = {}
    error_kinds = {}

    async def main():
        loop = asyncio.get_running_loop()
        parts = urlsplit(args.url)
        host, port = parts.hostname, parts.port or 80
        rng = random.Random(index)
        requests = [f"GET {args.path.format(id=i)} HTTP/1.1\r\nHost: {host}\r\n\r\n".encode() for i in args.ids]
        slots = [None] * conns
        used = [0] * conns
        idle = list(range(conns))
        waiters = deque()

        def release(i):
            while waiters:
                fut = waiters.popleft()
                if not fut.done():
                    fut.set_result(i)
                    return
            idle.append(i)

        async def acquire(deadline):
            if idle:
                return idle.pop()
            fut = loop.create_future()
            waiters.append(fut)
            try:
                return await asyncio.wait_for(fut, max(deadline - time.time(), 0.001))
            except asyncio.TimeoutError:
                if fut.done() and not fut.cancelled():
                    release(fut.result())
                raise

        def drop(i, reset):
            conn = slots[i]
            slots[i], used[i] = None, 0
            if conn is None:
                return
            writer = conn[1]
            if reset:
                sock = writer.get_extra_info("socket")
                if sock is not None:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            writer.close()

        def sec_of(t):
            return min(max(int(t - t0), 0), seconds - 1)

        def finish(planned, outcome, pod="", kind=""):
            end = time.time()
            sec = sec_of(end)
            if outcome == "ok":
                ok[sec] += 1
                with achieved.get_lock():
                    achieved[sec] += 1
                if pod:
                    per = pods.setdefault(pod, {})
                    per[sec] = per.get(sec, 0) + 1
            elif outcome == "error":
                errors[sec] += 1
                error_kinds[kind] = error_kinds.get(kind, 0) + 1
            else:
                non_2xx[sec] += 1
                error_kinds[kind] = error_kinds.get(kind, 0) + 1
            latencies.setdefault(sec, array("f")).append((end - planned) * 1000)

        async def exchange(i, request):
            try:
                if slots[i] is None:
                    slots[i] = await asyncio.wait_for(asyncio.open_connection(host, port), TIMEOUT)
                    opened[sec_of(time.time())] += 1
                reader, writer = slots[i]
                sent[sec_of(time.time())] += 1
                writer.write(request)
                used[i] += 1
                status, pod, close = await asyncio.wait_for(read_response(reader), args.read_limit)
            except BaseException:
                drop(i, True)
                release(i)
                raise
            if close or used[i] >= args.per_conn:
                drop(i, False)
            release(i)
            return status, pod

        async def fire(planned, request):
            deadline = planned + TIMEOUT
            try:
                i = await acquire(deadline)
            except asyncio.TimeoutError:
                finish(planned, "timeout", kind="Timeout waiting for connection")
                return
            if time.time() >= deadline:
                release(i)
                finish(planned, "timeout", kind="Timeout waiting for connection")
                return
            task = asyncio.ensure_future(exchange(i, request))
            done, _ = await asyncio.wait({task}, timeout=deadline - time.time())
            if not done:
                finish(planned, "timeout", kind="Timeout")
                task.add_done_callback(lambda t: t.cancelled() or t.exception())
                return
            exc = task.exception()
            if exc is not None:
                errno = getattr(exc, "errno", None)
                finish(planned, "error", kind=type(exc).__name__ + (f":{errno}" if errno else ""))
                return
            status, pod = task.result()
            if 200 <= status < 300:
                finish(planned, "ok", pod)
            else:
                finish(planned, "non_2xx", pod, f"HTTP {status}")

        tasks = set()
        planned = t0 + rng.random() / max(rate_at(0, args) * share, 1e-9)
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
            planned += 1.0 / (rate_at(rel, args) * share)
        if tasks:
            await asyncio.wait(tasks)
        for i in range(conns):
            drop(i, False)

    asyncio.run(main())
    queue.put({
        "scheduled": scheduled.tolist(), "sent": sent.tolist(), "ok": ok.tolist(), "non_2xx": non_2xx.tolist(),
        "errors": errors.tolist(), "opened": opened.tolist(),
        "latencies": {k: v.tobytes() for k, v in latencies.items()}, "pods": pods, "error_kinds": error_kinds,
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
    split = [args.conns // args.procs + (1 if i < args.conns % args.procs else 0) for i in range(args.procs)]
    procs = [ctx.Process(target=worker, args=(i, c, c / args.conns, args, t0, stop_at, achieved, queue))
             for i, c in enumerate(split) if c]
    for p in procs:
        p.start()
    decisions = []
    if args.profile:
        stop_at.value = sum(d for d, _ in args.profile)
    else:
        below_in_row, k = 0, 0
        while True:
            begin, end = step_bounds(args, k)
            target = args.start + k * args.step
            while time.time() < t0 + end + 1.0:
                time.sleep(0.2)
            with achieved.get_lock():
                window = list(achieved[int(end) - args.window:int(end)])
            mean = statistics.fmean(window)
            below_in_row = below_in_row + 1 if mean < 0.9 * target else 0
            decisions.append({"step": k, "target": target, "achieved_mean": round(mean, 1), "below_in_row": below_in_row})
            print(f"step {k:>2} target {target:>6.0f} achieved {mean:9.1f}", flush=True)
            if below_in_row >= 2 or target >= args.max_rate:
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
        rows.append({
            "second": sec, "epoch": round(t0 + sec, 3), "target_rps": rate_at(sec, args),
            "scheduled": sum(p["scheduled"][sec] for p in parts), "sent": sum(p["sent"][sec] for p in parts),
            "achieved": sum(p["ok"][sec] for p in parts), "non_2xx": sum(p["non_2xx"][sec] for p in parts),
            "conn_errors": sum(p["errors"][sec] for p in parts),
            "conns_opened": sum(p["opened"][sec] for p in parts),
            "p50_ms": round(percentile(lat, 0.50), 2) if lat else None,
            "p95_ms": round(percentile(lat, 0.95), 2) if lat else None,
            "p99_ms": round(percentile(lat, 0.99), 2) if lat else None,
        })
    pods, kinds = {}, {}
    for part in parts:
        for pod, per in part["pods"].items():
            dst = pods.setdefault(pod, {})
            for sec, v in per.items():
                dst[sec] = dst.get(sec, 0) + v
        for k, v in part["error_kinds"].items():
            kinds[k] = kinds.get(k, 0) + v
    return rows, latencies, pods, kinds


def summarize_steps(args, rows, latencies, pods, stopped_at):
    steps = []
    k = 0
    while not args.profile:
        begin, end = step_bounds(args, k)
        if end > stopped_at + 1e-6:
            break
        window = range(int(end) - args.window, int(end))
        achieved = [rows[s]["achieved"] for s in window]
        lat = [v for s in window for v in latencies.get(s, [])]
        done = sum(rows[s]["achieved"] + rows[s]["non_2xx"] + rows[s]["conn_errors"] for s in window)
        bad = sum(rows[s]["non_2xx"] + rows[s]["conn_errors"] for s in window)
        per_pod = {pod: round(sum(per.get(s, 0) for s in window) / args.window, 1) for pod, per in pods.items()}
        steps.append({
            "step": k, "target_rps": args.start + k * args.step, "begin_s": begin, "end_s": end,
            "window_epoch": [rows[0]["epoch"] + end - args.window, rows[0]["epoch"] + end],
            "sent_mean": round(statistics.fmean(rows[s]["sent"] for s in window), 1),
            "achieved_mean": round(statistics.fmean(achieved), 1),
            "achieved_std": round(statistics.stdev(achieved), 1),
            "p50_ms": round(percentile(lat, 0.50), 2) if lat else None,
            "p95_ms": round(percentile(lat, 0.95), 2) if lat else None,
            "p99_ms": round(percentile(lat, 0.99), 2) if lat else None,
            "non_2xx_share": round(bad / done, 5) if done else None,
            "pods_rps": {p: v for p, v in sorted(per_pod.items()) if v > 0},
        })
        k += 1
    return steps


def main():
    parser = argparse.ArgumentParser(description="open loop load generator with a fixed connection pool")
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--path", default="/vaccine-lots/{id}")
    parser.add_argument("--ids", default="results/ids.txt")
    parser.add_argument("--conns", type=int, default=24, help="persistent connections, 8 * R")
    parser.add_argument("--per-conn", type=int, default=100, help="requests per connection before reconnect")
    parser.add_argument("--procs", type=int, default=8)
    parser.add_argument("--read-limit", type=float, default=30.0,
                        help="hard limit for reading a response after the caller timed out; then reset")
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--warmup-rate", type=float, default=200)
    parser.add_argument("--start", type=float, default=200)
    parser.add_argument("--step", type=float, default=200)
    parser.add_argument("--step-seconds", type=int, default=30)
    parser.add_argument("--window", type=int, default=20, help="last seconds of a step used for averages")
    parser.add_argument("--max-rate", type=float, default=20000)
    parser.add_argument("--profile", type=parse_profile, default=None, help="duration:rate,duration:rate,...")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if args.profile:
        args.warmup = 0
    args.ids = [int(x) for x in Path(args.ids).read_text().split(",")] if "{id}" in args.path else [0]
    t0, parts, decisions = run(args)
    stopped = max(i for part in parts for i, v in enumerate(part["scheduled"]) if v) + 1
    rows, latencies, pods, kinds = merge(args, t0, parts)
    steps = summarize_steps(args, rows, latencies, pods, stopped)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "seconds.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    names = sorted(pods)
    with open(out / "pods.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["second", *names])
        for sec in range(len(rows)):
            writer.writerow([sec, *(pods[n].get(sec, 0) for n in names)])
    best = max(steps, key=lambda s: s["achieved_mean"]) if steps else None
    segments = []
    if args.profile:
        begin = 0
        for duration, rate in args.profile:
            window = range(int(begin), min(int(begin + duration), len(rows)))
            lat = [v for s in window for v in latencies.get(s, [])]
            segments.append({
                "begin_s": begin, "end_s": begin + duration, "target_rps": rate,
                "achieved_mean": round(statistics.fmean(rows[s]["achieved"] for s in window), 1),
                "p50_ms": round(percentile(lat, 0.50), 2) if lat else None,
                "p99_ms": round(percentile(lat, 0.99), 2) if lat else None,
                "non_2xx": sum(rows[s]["non_2xx"] + rows[s]["conn_errors"] for s in window),
            })
            begin += duration
    summary = {
        "path": args.path, "url": args.url, "procs": args.procs, "conns": args.conns, "per_conn": args.per_conn,
        "timeout_s": TIMEOUT, "warmup_s": args.warmup, "warmup_rate": args.warmup_rate,
        "start_rps": args.start, "step_rps": args.step, "step_seconds": args.step_seconds, "window_s": args.window,
        "profile": args.profile, "start_epoch": round(t0, 3), "stopped_at_s": stopped, "decisions": decisions,
        "steps": steps, "segments": segments, "X": best, "pods_total": {n: sum(pods[n].values()) for n in names}, "error_kinds": kinds,
        "totals": {k: sum(r[k] for r in rows) for k in ("scheduled", "sent", "achieved", "non_2xx", "conn_errors",
                                                         "conns_opened")},
    }
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    if best:
        print(f"X = {best['achieved_mean']} +- {best['achieved_std']} rps at step {best['target_rps']:.0f}, "
              f"p99 client {best['p99_ms']} ms, non-2xx share {best['non_2xx_share']}, pods {best['pods_rps']}")
    for seg in segments:
        print(f"segment {seg['begin_s']:.0f}-{seg['end_s']:.0f}s target {seg['target_rps']:.0f}: "
              f"achieved {seg['achieved_mean']}, p99 {seg['p99_ms']} ms, non-2xx {seg['non_2xx']}")
    print(f"totals {summary['totals']} errors {kinds}")


if __name__ == "__main__":
    main()
