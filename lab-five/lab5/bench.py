"""Test bench for the thread-safe tree (variant 18: C8, n = 3, keys 0..99 999).

  python -m lab5.bench run   --lock global|rw     n threads: odd numbers write, even numbers read
  python -m lab5.bench x1    --lock global|rw     one thread, writes and reads in the proportion n_w : n_r
  python -m lab5.bench check-readonly             reads under RWLock leave every field of the tree untouched
  python -m lab5.bench series                     everything above, each run in a fresh process, 3 repeats

Operations follow one another without pauses; arguments are drawn in the thread before the call.
t_call is taken before the lock is requested, t_acq right after it is acquired, t_ret after it is released:
tau = t_ret - t_call, t_wait = t_acq - t_call. Only operations whose t_call falls into the D seconds after the warm-up
are counted.
"""
import argparse
import json
import math
import os
import platform
import random
import subprocess
import sys
import threading
from array import array
from pathlib import Path
from time import perf_counter_ns as now

from lab5.locks import LOCKS
from lab5.structure import RANGE_WIDTH, ThreadSafeBST

ROOT = Path(__file__).resolve().parents[1]
THREADS, KEYS = 3, 100_000
DURATION, WARMUP, RUNS = 10.0, 1.0, 3
NS = 1_000_000_000


def build(lock_name, keys, seed):
    """Fresh tree filled with half of the key space, inserted in random order (a random BST, depth ~ 2 ln n)."""
    ts = ThreadSafeBST(LOCKS[lock_name]())
    rng = random.Random(seed)
    st = [0, 0]
    for k in rng.sample(range(keys), keys // 2):
        ts.insert(k, st)
    return ts


def percentile(sorted_values, q):
    """p_q = tau_(ceil(q N)), 1-based index into the ascending list."""
    if not sorted_values:
        return None
    return sorted_values[max(math.ceil(q * len(sorted_values)), 1) - 1]


def role_stats(threads, duration):
    """threads: dicts of one role with counts, tau, wait, hold arrays (ns)."""
    k = len(threads)
    counts = [t["count"] for t in threads]
    taus = sorted(v for t in threads for v in t["tau"])
    wait_sum = sum(t["wait_sum"] for t in threads)
    n = len(taus)
    mu = sum(counts) / k
    sigma = math.sqrt(sum((c - mu) ** 2 for c in counts) / k)
    p50, p99 = percentile(taus, 0.5), percentile(taus, 0.99)
    holds = sum(t["hold_sum"] for t in threads)
    curve = [taus[min(int(i / 400 * (n - 1)), n - 1)] / 1000 for i in range(401)] if n else []
    return {"threads": k, "ops": n, "X": n / duration, "x_per_thread": n / duration / k,
            "p50_us": p50 / 1000, "p99_us": p99 / 1000, "h": p99 / p50 if p50 else None,
            "mean_us": sum(taus) / n / 1000 if n else None,
            "wait_max_ms": max(t["wait_max"] for t in threads) / 1e6, "w": wait_sum / NS / (k * duration),
            "cv": sigma / mu if mu else None, "mu": mu, "sigma": sigma,
            "t_cs_us": holds / n / 1000 if n else None, "by_kind": merge_kinds(threads), "tau_curve_us": curve}


def merge_kinds(threads):
    out = {}
    for t in threads:
        for kind, c in t["kinds"].items():
            out[kind] = out.get(kind, 0) + c
    return out


def worker(ts, pattern, seed, keys, times, barrier, result):
    """pattern: sequence of roles 'w'/'r' repeated; writes alternate insert/remove, reads contains/range_count."""
    rng = random.Random(seed)
    randrange = rng.randrange
    st = [0, 0]
    stats = {r: {"tau": array("q"), "count": 0, "wait_sum": 0, "wait_max": 0, "hold_sum": 0,
                 "kinds": {}} for r in set(pattern)}
    write_flip = read_flip = False
    plen, i = len(pattern), 0
    barrier.wait()
    t_start, t_end = times
    insert, remove, contains, range_count = ts.insert, ts.remove, ts.contains, ts.range_count
    while True:
        role = pattern[i]
        i = i + 1 if i + 1 < plen else 0
        if role == "w":
            write_flip = not write_flip
            key = randrange(keys)
            if write_flip:
                kind = "insert"
                t_call = now()
                insert(key, st)
            else:
                kind = "remove"
                t_call = now()
                remove(key, st)
        else:
            read_flip = not read_flip
            if read_flip:
                key = randrange(keys)
                kind = "contains"
                t_call = now()
                contains(key, st)
            else:
                lo = randrange(keys - RANGE_WIDTH + 1)
                kind = "range_count"
                t_call = now()
                range_count(lo, lo + RANGE_WIDTH - 1, st)
        t_ret = now()
        if t_call >= t_end:
            break
        if t_call >= t_start:
            s = stats[role]
            s["tau"].append(t_ret - t_call)
            wait = st[0] - t_call
            s["wait_sum"] += wait
            if wait > s["wait_max"]:
                s["wait_max"] = wait
            s["hold_sum"] += st[1] - st[0]
            s["count"] += 1
            s["kinds"][kind] = s["kinds"].get(kind, 0) + 1
    result.update(stats)


def run(lock_name, threads, keys, duration, warmup, seed, single=False):
    ts = build(lock_name, keys, seed)
    before = ts.tree.check_invariants()
    n_w, n_r = math.ceil(threads / 2), threads // 2
    if single:
        # one thread, writes and reads interleaved in the proportion n_w : n_r (for n = 3: w r w)
        pattern = "".join("w" if j % 2 == 0 else "r" for j in range(threads))
        specs = [(0, pattern)]
    else:
        specs = [(j, "w" if j % 2 == 1 else "r") for j in range(1, threads + 1)]
    barrier = threading.Barrier(len(specs) + 1)
    times = [0, 0]
    results = [dict() for _ in specs]
    pool = [threading.Thread(target=worker, name=f"t{j}",
                             args=(ts, p, seed * 1000 + j, keys, times, barrier, results[idx]))
            for idx, (j, p) in enumerate(specs)]
    for t in pool:
        t.start()
    t0 = now() + 20_000_000
    times[0], times[1] = t0 + int(warmup * NS), t0 + int((warmup + duration) * NS)
    load_before = os.getloadavg()
    barrier.wait()
    for t in pool:
        t.join()
    after = ts.tree.check_invariants()
    out = {"lock": lock_name, "mode": "x1" if single else "run", "threads": threads, "n_w": n_w, "n_r": n_r,
           "keys": keys, "duration": duration, "warmup": warmup, "seed": seed, "range_width": RANGE_WIDTH,
           "python": platform.python_version(), "gil": getattr(sys, "_is_gil_enabled", lambda: True)(),
           "switch_interval_s": sys.getswitchinterval(), "logical_cpus": os.cpu_count(),
           "loadavg_before": load_before, "invariants_before": before, "invariants_after": after}
    if single:
        res = results[0]
        out["X_1"] = sum(res[r]["count"] for r in res) / duration
        out["roles"] = {("write" if r == "w" else "read"): role_stats([res[r]], duration) for r in res}
    else:
        per_thread = [{"thread": j, "role": "write" if p == "w" else "read", "ops": results[idx][p]["count"]}
                      for idx, (j, p) in enumerate(specs)]
        out["per_thread"] = per_thread
        out["roles"] = {"write": role_stats([results[i]["w"] for i, (_, p) in enumerate(specs) if p == "w"], duration),
                        "read": role_stats([results[i]["r"] for i, (_, p) in enumerate(specs) if p == "r"], duration)}
        out["X"] = out["roles"]["write"]["X"] + out["roles"]["read"]["X"]
        for r in ("write", "read"):
            out["roles"][r]["phi"] = out["roles"][r]["X"] / out["X"]
    return out


def check_readonly(keys, seed, seconds=3.0, readers=4):
    """Readers only, under RWLock and in parallel: the fingerprint of every node must stay the same,
    and the version never moves while a read holds the lock."""
    ts = build("rw", keys, seed)
    fp_before = ts.tree.fingerprint()
    stop = threading.Event()
    bad, ops = [0] * readers, [0] * readers

    def reader(i):
        rng = random.Random(seed + i)
        while not stop.is_set():
            if not ts.contains_checked(rng.randrange(keys - RANGE_WIDTH)):
                bad[i] += 1
            ops[i] += 1

    pool = [threading.Thread(target=reader, args=(i,)) for i in range(readers)]
    for t in pool:
        t.start()
    threading.Event().wait(seconds)
    stop.set()
    for t in pool:
        t.join()
    fp_after = ts.tree.fingerprint()
    # the same check while writers run: a read must never see the version move under its read lock
    ts2 = build("rw", keys, seed)
    stop2 = threading.Event()
    torn = [0]
    reads = [0]

    def writer(i):
        rng = random.Random(seed + 100 + i)
        st = [0, 0]
        flip = False
        while not stop2.is_set():
            flip = not flip
            (ts2.insert if flip else ts2.remove)(rng.randrange(keys), st)

    def reader2():
        rng = random.Random(seed + 200)
        while not stop2.is_set():
            if not ts2.contains_checked(rng.randrange(keys - RANGE_WIDTH)):
                torn[0] += 1
            reads[0] += 1

    pool = [threading.Thread(target=writer, args=(i,)) for i in range(2)] + [threading.Thread(target=reader2)]
    for t in pool:
        t.start()
    threading.Event().wait(seconds)
    stop2.set()
    for t in pool:
        t.join()
    return {"readers": readers, "seconds": seconds, "read_ops": sum(ops), "fingerprint_equal": fp_before == fp_after,
            "nodes_fingerprinted": fp_before[1], "version_moved_under_read_lock": sum(bad),
            "with_writers": {"read_ops": reads[0], "version_moved_under_read_lock": torn[0],
                             "invariants": ts2.tree.check_invariants()}}


def series(args):
    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    env = {**os.environ, "PYTHONHASHSEED": "0"}
    for lock in ("global", "rw"):
        for mode in ("x1", "run"):
            for rep in range(1, args.runs + 1):
                path = out / f"{mode}_{lock}_{rep}.json"
                cmd = [sys.executable, "-m", "lab5.bench", mode, "--lock", lock, "--seed", str(rep),
                       "--out", str(path), "--duration", str(args.duration)]
                subprocess.run(cmd, cwd=ROOT, env=env, check=True)
    path = out / "check_readonly.json"
    subprocess.run([sys.executable, "-m", "lab5.bench", "check-readonly", "--out", str(path)], cwd=ROOT, env=env,
                   check=True)


def report(res):
    if res["mode"] == "x1":
        r = res["roles"]
        print(f"[{res['lock']} x1] X_1 = {res['X_1']:.0f} op/s, t_cs write {r['write']['t_cs_us']:.2f} us, "
              f"read {r['read']['t_cs_us']:.2f} us, invariants {res['invariants_after']['ok']}")
        return
    print(f"[{res['lock']} n={res['threads']}] per thread: " +
          ", ".join(f"{t['thread']}({t['role'][0]})={t['ops']}" for t in res["per_thread"]))
    for role in ("write", "read"):
        s = res["roles"][role]
        print(f"  {role:<5} X = {s['X']:9.0f} op/s, p50 {s['p50_us']:7.2f} us, p99 {s['p99_us']:8.2f} us, "
              f"max t_wait {s['wait_max_ms']:7.3f} ms, w = {s['w']:.3f}, cv = {s['cv']:.4f}")
    print(f"  X = {res['X']:.0f} op/s, invariants after: {res['invariants_after']['ok']} "
          f"({res['invariants_after']['present']} keys, {res['invariants_after']['nodes']} nodes, "
          f"depth {res['invariants_after']['depth']})")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["run", "x1", "check-readonly", "series"])
    p.add_argument("--lock", choices=list(LOCKS), default="global")
    p.add_argument("--threads", type=int, default=THREADS)
    p.add_argument("--keys", type=int, default=KEYS)
    p.add_argument("--duration", type=float, default=DURATION)
    p.add_argument("--warmup", type=float, default=WARMUP)
    p.add_argument("--runs", type=int, default=RUNS)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--out")
    a = p.parse_args()
    if a.mode == "series":
        series(a)
        return
    if a.mode == "check-readonly":
        res = check_readonly(a.keys, 18)
        print(json.dumps({k: v for k, v in res.items()}, indent=1))
    else:
        res = run(a.lock, a.threads, a.keys, a.duration, a.warmup, a.seed, single=a.mode == "x1")
        report(res)
    if a.out:
        Path(a.out).write_text(json.dumps(res, indent=1) + "\n")


if __name__ == "__main__":
    main()
