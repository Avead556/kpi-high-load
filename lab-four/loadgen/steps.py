"""Lab 4 experiments, one sub-command per step of the assignment. Raw data goes to results/<step>/."""
import argparse
import csv
import json
import math
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

from common import (ROOT, WEB, ExitWatcher, LockSampler, LogCollector, bucket_counts, bucket_quantile, counter_delta,
                    claim_from_metrics, metrics_delta, prom_rate, worker_metrics,
                    db_now, dump_tables, kubectl, parse_log, prom_range, psql, psql_file, queue_counts, reset_queue,
                    scale_workers, set_worker_env, wait_queue_empty, wait_workers, worker_pods, write_json)
from tasks import Generator, save

sys.path.insert(0, str(ROOT))
from worker.slice import process  # noqa: E402

R = 3
POLL_MS = 10
LEASE = 10.0
Q_LO, Q_HI = 0.01, 0.99
RES = ROOT / "results"
BASE_ENV = {"CLAIM_MODE": "skip_locked", "STORE_MODE": "idempotent", "POLL_MS": str(POLL_MS),
            "LEASE_SECONDS": str(int(LEASE)), "HEARTBEAT": "1", "REAP": "1", "FAIL_RATE": "0",
            "SLICE_SIZE": "256", "GRACE_SECONDS": "25"}
NS_SEL = 'namespace="vaccines"'


def say(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def configure(n: int, **env):
    rolled = set_worker_env(**{**BASE_ENV, **{k: str(v) for k, v in env.items()}})
    scale_workers(n)
    if rolled:
        time.sleep(2)
    return worker_pods()


def read_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def fnum(v):
    return float(v) if v not in (None, "") else None


def tasks_table(out):
    rows = read_csv(out / "tasks.csv")
    for r in rows:
        for k in ("created", "started", "finished", "cancel_at"):
            r[k] = fnum(r[k])
        for k in ("id", "attempts", "token", "record_id"):
            r[k] = int(r[k])
    return rows


def results_table(out):
    return read_csv(out / "results.csv") if (out / "results.csv").exists() and (out / "results.csv").stat().st_size else []


def percentile(values, q):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def duplicates(out, log_path):
    tasks = tasks_table(out)
    res = Counter(r["task_id"] for r in results_table(out))
    done = Counter(e["task"] for e in parse_log(log_path) if e["event"] == "done")
    n = len(tasks)
    by_results = sum(1 for c in res.values() if c > 1)
    by_logs = sum(1 for c in done.values() if c > 1)
    return {"n": n, "dup_by_results": by_results, "D_results": by_results / n if n else None,
            "dup_by_logs": by_logs, "D_logs": by_logs / n if n else None,
            "result_rows": sum(res.values()), "done_events": sum(done.values()),
            "max_results_per_task": max(res.values(), default=0)}


def claim_stats(lo, hi):
    buckets = bucket_counts("worker_task_duration_seconds", f'{NS_SEL},step="claim"', lo, hi)
    q = bucket_quantile(buckets, 0.99)
    return {"claim_p99_ms": round(q * 1000, 3) if q is not None else None,
            "claims_observed": buckets[-1][1] if buckets else 0,
            "claims_empty": counter_delta(f'sum by (pod) (worker_claims_total{{{NS_SEL},result="empty"}})', lo, hi),
            "claims_task": counter_delta(f'sum by (pod) (worker_claims_total{{{NS_SEL},result="task"}})', lo, hi)}


def batch(out: Path, rate: float, count: int, locks=False, snapshot=False):
    """Clean queue, `count` tasks at `rate`, wait until nothing is queued or running."""
    reset_queue()
    samplers = [LockSampler(1.0), LockSampler(0.01)] if locks else []
    with LogCollector(out / "worker.log"):
        for sm in samplers:
            sm.__enter__()
        time.sleep(1)
        before = worker_metrics() if snapshot else None
        t_start = db_now()
        run = Generator(threads=32).run(rate, count)
        wait_queue_empty(poll=0.2)
        t_end = db_now()
        after = worker_metrics() if snapshot else None
        for sm in samplers:
            sm.__exit__(None, None, None)
    save(run, out / "generator.csv")
    dump_tables(out)
    meta = {"rate": rate, "count": count, "accepted": run["accepted"], "non_2xx": run["non_2xx"],
            "t_start": t_start, "t_end": t_end, "pods": [p["name"] for p in worker_pods()]}
    if snapshot:
        meta.update(claim_from_metrics(metrics_delta(before, after)))
    if samplers:
        every_s, dense = samplers
        meta.update({"lock_waiting_max_1s": every_s.max_waiting, "lock_samples_1s": len(every_s.samples),
                     "lock_waiting_max_10ms": dense.max_waiting, "lock_samples_10ms": len(dense.samples),
                     "lock_waiting_share_10ms": dense.share_waiting()})
        write_json(out / "locks.json", {"1s": every_s.samples, "10ms": dense.samples})
    return meta


# step 2 ---------------------------------------------------------------------------------------------------------
def step2(args):
    out = RES / "step2"
    configure(1)
    meta = batch(out, rate=2, count=20)
    time.sleep(7)
    done = [e for e in parse_log(out / "worker.log") if e["event"] == "done"]
    fetch = [float(e["fetch_ms"]) for e in done]
    proc = [float(e["process_ms"]) for e in done]
    store = [float(e["store_ms"]) for e in done]
    t_io = statistics.fmean(f + s for f, s in zip(fetch, store))
    t_cpu = statistics.fmean(proc)
    # the result is deterministic: recompute a few seeds on the host
    tasks = {r["id"]: r for r in read_csv(out / "tasks.csv")}
    check = []
    for r in results_table(out)[:5]:
        expected = process(int(tasks[r["task_id"]]["seed"]), 256, Q_LO, Q_HI)
        check.append({"task": r["task_id"], "db": [int(r["p_lo"]), int(r["p_hi"]), float(r["mean"])],
                      "host": [expected["p_lo"], expected["p_hi"], expected["mean"]]})
    summary = {**meta, "tasks_done": len(done), "fetch_ms": statistics.fmean(fetch), "store_ms": statistics.fmean(store),
               "t_io_ms": t_io, "t_cpu_ms": t_cpu, "t_cpu_p50_ms": percentile(proc, 0.5),
               "t_cpu_max_ms": max(proc), "X1_estimate": 1000 / (t_io + t_cpu + POLL_MS / 2),
               "determinism": check}
    write_json(out / "summary.json", summary)
    say(f"t_io = {t_io:.2f} ms, t_cpu = {t_cpu:.2f} ms, X(1) estimate {summary['X1_estimate']:.1f}/s")


# steps 3 and 4 --------------------------------------------------------------------------------------------------
def step3(args):
    psql_file(ROOT / "db" / "modes" / "naive.sql")
    summary = {}
    for poll in (POLL_MS, 1):
        configure(R, CLAIM_MODE="naive", STORE_MODE="naive", REAP=0, HEARTBEAT=0, POLL_MS=poll)
        runs = []
        for rep in range(1, args.repeat + 1):
            out = RES / "step3" / f"poll{poll}" / f"run{rep}"
            meta = batch(out, rate=100, count=300)
            runs.append({**meta, **duplicates(out, out / "worker.log")})
            say(f"naive T_poll={poll} ms run {rep}: D by results {runs[-1]['D_results']:.3f}, "
                f"by logs {runs[-1]['D_logs']:.3f}")
        summary[f"poll_{poll}ms"] = {
            "runs": runs, "D_results_mean": statistics.fmean(r["D_results"] for r in runs),
            "D_logs_mean": statistics.fmean(r["D_logs"] for r in runs),
            "dup_total": sum(r["dup_by_results"] for r in runs), "n_total": sum(r["n"] for r in runs)}
    write_json(RES / "step3" / "summary.json", summary)


def step4(args):
    """Series of step 3 (300 tasks at 100/s, T_poll = 1 ms) plus the same 300 tasks as an instant backlog."""
    psql_file(ROOT / "db" / "modes" / "strict.sql")
    summary = {}
    for mode in ("skip_locked", "for_update"):
        configure(R, CLAIM_MODE=mode, POLL_MS=1, REAP=0, HEARTBEAT=0)
        for series, rate in (("rate100", 100), ("backlog", 1500)):
            out = RES / "step4" / mode / series
            meta = batch(out, rate=rate, count=300, locks=True, snapshot=True)
            s = {**meta, **duplicates(out, out / "worker.log"), "duration_s": meta["t_end"] - meta["t_start"]}
            summary.setdefault(mode, {})[series] = s
            say(f"{mode}/{series}: D {s['D_results']:.3f}, claim p50 {s['claim_p50_ms']} p99 {s['claim_p99_ms']} ms, "
                f"empty {s['claims_empty']:.0f}, task {s['claims_task']:.0f}, lock waits max 1s/10ms "
                f"{s['lock_waiting_max_1s']}/{s['lock_waiting_max_10ms']}, share {s['lock_waiting_share_10ms']:.3f}, "
                f"{s['duration_s']:.2f} s")
    write_json(RES / "step4" / "summary.json", summary)


def step4x(args):
    """Extra: the same comparison with more workers than R, where claims of different workers overlap."""
    summary = {}
    for mode in ("skip_locked", "for_update"):
        configure(args.workers, CLAIM_MODE=mode, POLL_MS=1, REAP=0, HEARTBEAT=0)
        out = RES / "step4x" / f"{mode}_w{args.workers}"
        meta = batch(out, rate=3000, count=args.count, locks=True, snapshot=True)
        s = {**meta, **duplicates(out, out / "worker.log"), "duration_s": meta["t_end"] - meta["t_start"],
             "W": args.workers}
        summary[mode] = s
        say(f"W={args.workers} {mode}: D {s['D_results']:.3f}, claim p50 {s['claim_p50_ms']} p99 {s['claim_p99_ms']} ms, "
            f"empty {s['claims_empty']:.0f}, lock waits max 1s/10ms {s['lock_waiting_max_1s']}/"
            f"{s['lock_waiting_max_10ms']}, share {s['lock_waiting_share_10ms']:.3f}, {s['duration_s']:.2f} s")
    write_json(RES / "step4x" / "summary.json", summary)
    configure(R)


# step 5 ---------------------------------------------------------------------------------------------------------
def task_row(task_id):
    return psql(f"SELECT id, status, attempts, token, worker_id, lease_until, started_at, finished_at "
                f"FROM tasks WHERE id = {task_id}")[0]


def host_pid(pod):
    """PID of the worker process as seen by the kind node, an ancestor PID namespace of the container."""
    info = json.loads(kubectl("get", "pod", pod, "-o", "json"))
    node = info["spec"]["nodeName"]
    cid = info["status"]["containerStatuses"][0]["containerID"].split("://")[1]
    pid = subprocess.run(["docker", "exec", node, "crictl", "inspect", "-o", "go-template", "--template",
                          "{{.info.pid}}", cid], capture_output=True, text=True, check=True).stdout.strip()
    return node, int(pid)


def exec_kill(pod, sig, target=None):
    """`kubectl exec <pod> -- kill -STOP 1` is dropped by the kernel: from inside its PID namespace the init process
    receives only signals it handles, never SIGSTOP. The same signal is sent to the same process from the node."""
    node, pid = target or host_pid(pod)
    subprocess.run(["docker", "exec", node, "kill", f"-{sig}", str(pid)], check=True)


def step5(args):
    out = RES / "step5"
    pods = configure(2)
    reset_queue()
    victim = pods[0]["name"]
    other = pods[1]["name"]
    with LogCollector(out / "worker.log"):
        gen = Generator().start(rate=20, count=100)
        caught, tries = None, 0
        target = host_pid(victim)
        while caught is None:
            tries += 1
            exec_kill(victim, "STOP", target)
            t_stop = time.time()
            # caught in the processing step, not inside the store transaction (that row would be locked)
            rows = psql(f"SELECT id FROM tasks WHERE status = 'running' AND worker_id = '{victim}' "
                        f"FOR UPDATE SKIP LOCKED")
            if rows:
                caught = int(rows[0]["id"])
                break
            exec_kill(victim, "CONT", target)
            time.sleep(0.15)
        before = task_row(caught)
        say(f"stopped {victim} holding task {caught} after {tries} tries: {before}")
        time.sleep(2 * LEASE - (time.time() - t_stop))
        mid = task_row(caught)
        state = subprocess.run(["docker", "exec", target[0], "grep", "State", f"/proc/{target[1]}/status"],
                               capture_output=True, text=True).stdout.strip()
        exec_kill(victim, "CONT", target)
        t_cont = time.time()
        say(f"continued {victim} after {t_cont - t_stop:.1f} s; task now {mid}")
        run = gen.join()
        time.sleep(3)
        wait_queue_empty()
        time.sleep(2)
    save(run, out / "generator.csv")
    dump_tables(out)
    after = task_row(caught)
    results = psql(f"SELECT task_id, token, worker_id, created_at FROM task_results WHERE task_id = {caught}")
    events = [e for e in parse_log(out / "worker.log") if e.get("task") == str(caught)]
    summary = {"victim": victim, "other": other, "task": caught, "tries": tries, "stop_seconds": t_cont - t_stop,
               "node_pid": target, "state_while_stopped": state,
               "before": before, "at_continue": mid, "after": after, "results": results, "task_log": events,
               "victim_stale_lines": [l for l in (out / "worker.log").read_text().splitlines()
                                      if f"pod={victim}" in l and "event=stale" in l],
               "all_tasks": queue_counts(), "accepted": run["accepted"]}
    write_json(out / "summary.json", summary)
    say(json.dumps({k: summary[k] for k in ("task", "after", "results", "victim_stale_lines")}, default=str))


# step 6 ---------------------------------------------------------------------------------------------------------
def throughput_window(out, window=30.0):
    tasks = tasks_table(out)
    t_empty = max(t["started"] for t in tasks if t["started"])
    lo = t_empty - window
    per_sec = [0] * int(window)
    for t in tasks:
        if t["finished"] and lo <= t["finished"] < t_empty and t["status"] == "done":
            per_sec[int(t["finished"] - lo)] += 1
    return lo, t_empty, per_sec


RESOURCE_WINDOW = 60.0  # cAdvisor refreshes container counters every ~15 s, a 30 s window holds 1-2 samples


def window_resources(lo, hi):
    lo = min(lo, hi - RESOURCE_WINDOW)
    sel = lambda c: f'{{{NS_SEL},container="{c}"}}'
    throttled = prom_rate(f"container_cpu_cfs_throttled_periods_total{sel('worker')}", lo, hi)
    periods = prom_rate(f"container_cpu_cfs_periods_total{sel('worker')}", lo, hi)
    proc_buckets = bucket_counts("worker_task_duration_seconds", f'{NS_SEL},step="process"', lo, hi)
    return {"db_tps": prom_rate('pg_stat_database_xact_commit{datname="vaccines"}', lo, hi)
                      + prom_rate('pg_stat_database_xact_rollback{datname="vaccines"}', lo, hi),
            "db_cpu_cores": prom_rate(f"container_cpu_usage_seconds_total{sel('postgres')}", lo, hi),
            "worker_cpu_cores": prom_rate(f"container_cpu_usage_seconds_total{sel('worker')}", lo, hi),
            "web_cpu_cores": prom_rate(f"container_cpu_usage_seconds_total{sel('web')}", lo, hi),
            "worker_throttled_share": throttled / periods if periods else None,
            "process_p50_ms": round(bucket_quantile(proc_buckets, 0.5) * 1000, 2) if proc_buckets else None,
            "process_p99_ms": round(bucket_quantile(proc_buckets, 0.99) * 1000, 2) if proc_buckets else None}


def step6_recompute(args):
    path = RES / "step6" / "summary.json"
    data = json.loads(path.read_text())
    for w, s in data.items():
        s.update(claim_stats(*s["window"]))
        s.update(window_resources(*s["window"]))
        say(f"W={w}: X = {s['X']:.1f} ± {s['std']:.1f}, claim p99 {s['claim_p99_ms']} ms, TPS {s['db_tps']:.0f}, "
            f"db cpu {s['db_cpu_cores']:.3f}, worker cpu {s['worker_cpu_cores']:.2f}, web cpu {s['web_cpu_cores']:.2f}, "
            f"throttled {s['worker_throttled_share']}, process p50 {s['process_p50_ms']} ms")
    write_json(path, data)


def step6(args):
    s2 = json.loads((RES / "step2" / "summary.json").read_text())
    x1 = s2["X1_estimate"]
    summary = {}
    for w in args.workers:
        out = RES / "step6" / f"w{w}"
        count = max(400, math.ceil(args.seconds * w * x1))
        configure(w)
        scale_workers(0)
        reset_queue()
        say(f"W={w}: filling the queue with {count} tasks")
        run = Generator(threads=32).run(rate=400, count=count)
        save(run, out / "generator.csv")
        t_fill = db_now()
        scale_workers(w)
        wait_queue_empty(timeout=1800, poll=1)
        time.sleep(8)
        dump_tables(out)
        lo, hi, per_sec = throughput_window(out)
        x = statistics.fmean(per_sec)
        sd = statistics.stdev(per_sec)
        summary[w] = {"W": w, "count": count, "accepted": run["accepted"], "t_fill": t_fill, "window": [lo, hi],
                      "X": x, "std": sd, "per_second": per_sec, "drain_seconds": hi - t_fill,
                      **claim_stats(lo, hi), **window_resources(lo, hi)}
        s = summary[w]
        say(f"W={w}: X = {x:.1f} ± {sd:.1f}/s, claim p99 {s['claim_p99_ms']} ms, TPS {s['db_tps']:.0f}, "
            f"db cpu {s['db_cpu_cores']:.3f}, worker cpu {s['worker_cpu_cores']:.2f}, "
            f"throttled {s['worker_throttled_share']}, process p50 {s['process_p50_ms']} ms")
    path = RES / "step6" / "summary.json"
    old = json.loads(path.read_text()) if path.exists() else {}
    old.update({str(k): v for k, v in summary.items()})
    write_json(path, old)


def x_r():
    return json.loads((RES / "step6" / "summary.json").read_text())[str(R)]["X"]


# step 7 ---------------------------------------------------------------------------------------------------------
def gauge_mean(metric, lo, hi, step=5):
    vals = []
    for s in prom_range(f"max({metric}{{{NS_SEL}}})", lo, hi, step):
        vals += [float(v) for _, v in s["values"]]
    return statistics.fmean(vals) if vals else None, vals


def step7(args):
    xr = x_r()
    configure(R)
    summary = {"X_R": xr}
    # steady state
    out = RES / "step7" / "steady"
    reset_queue()
    rate = 0.7 * xr
    t0 = db_now()
    run = Generator().run(rate, duration=180)
    t1 = db_now()
    wait_queue_empty()
    time.sleep(8)
    save(run, out / "generator.csv")
    dump_tables(out)
    tasks = tasks_table(out)
    lo, hi = t1 - 120, t1
    win = [t for t in tasks if lo <= t["created"] < hi]
    lam = len(win) / (hi - lo)
    wq = statistics.fmean(t["started"] - t["created"] for t in win)
    lq, samples = gauge_mean("tasks_queued", lo, hi)
    # time-average of the queue length straight from the timestamps, for reference
    lq_exact = sum(max(0.0, min(t["started"], hi) - max(t["created"], lo)) for t in tasks if t["started"]) / (hi - lo)
    summary["steady"] = {"rate": rate, "t0": t0, "t1": t1, "window": [lo, hi], "lambda": lam, "W_q_s": wq,
                         "lambda_Wq": lam * wq, "L_q_gauge": lq, "gauge_samples": samples, "L_q_timestamps": lq_exact,
                         "diff_pct": (lq - lam * wq) / (lam * wq) * 100 if lq is not None else None,
                         "W_q_p50_ms": percentile([(t["started"] - t["created"]) * 1000 for t in win], 0.5),
                         "W_q_p99_ms": percentile([(t["started"] - t["created"]) * 1000 for t in win], 0.99),
                         "accepted": run["accepted"], "non_2xx": run["non_2xx"]}
    st = summary["steady"]
    say(f"steady: λ {lam:.2f}/s, W_q {wq * 1000:.2f} ms, λW_q {lam * wq:.4f}, L_q gauge {lq}, exact {lq_exact:.4f}")
    # overload
    out = RES / "step7" / "overload"
    reset_queue()
    rate = 1.3 * xr
    t0 = db_now()
    run = Generator(threads=32).run(rate, duration=120)
    t1 = db_now()
    queue_at_end = queue_counts().get("queued", 0)
    wait_queue_empty(timeout=1800)
    time.sleep(8)
    save(run, out / "generator.csv")
    dump_tables(out)
    tasks = sorted(tasks_table(out), key=lambda t: t["created"])
    first, last = tasks[:100], tasks[-100:]
    gauge = prom_range(f"max(tasks_queued{{{NS_SEL}}})", t0 - 10, t1 + 60, 5)
    summary["overload"] = {"rate": rate, "t0": t0, "t1": t1, "queue_at_end": queue_at_end, "created": len(tasks),
                           "W_q_first100_s": statistics.fmean(t["started"] - t["created"] for t in first),
                           "W_q_last100_s": statistics.fmean(t["started"] - t["created"] for t in last),
                           "accepted": run["accepted"], "non_2xx": run["non_2xx"],
                           "gauge": gauge[0]["values"] if gauge else []}
    ov = summary["overload"]
    say(f"overload: queue at end {queue_at_end}, W_q first {ov['W_q_first100_s']:.3f} s, last {ov['W_q_last100_s']:.1f} s")
    write_json(RES / "step7" / "summary.json", summary)


# step 8 ---------------------------------------------------------------------------------------------------------
def step8(args):
    xr = x_r()
    out = RES / ("step8" if args.grace is None else f"step8_grace{args.grace}_slice{args.slice}")
    extra = {} if args.grace is None else {"GRACE_SECONDS": args.grace, "SLICE_SIZE": args.slice}
    configure(R, **extra)
    reset_queue()
    with ExitWatcher() as exits, LogCollector(out / "worker.log"):
        old = [p["name"] for p in worker_pods()]
        rate = args.rate or 0.7 * xr
        gen = Generator().start(rate=rate, duration=args.duration)
        time.sleep(args.restart_at)
        t_restart = db_now()
        kubectl("rollout", "restart", "deployment/worker")
        kubectl("rollout", "status", "deployment/worker", "--timeout=300s")
        t_rolled = db_now()
        run = gen.join()
        wait_queue_empty()
        time.sleep(3)
    save(run, out / "generator.csv")
    dump_tables(out)
    events = parse_log(out / "worker.log")
    tasks = tasks_table(out)
    res = Counter(r["task_id"] for r in results_table(out))
    old_exits = [e for e in exits.exits.values() if e["pod"] in old]
    summary = {"rate": rate, "env": extra, "t_restart": t_restart, "t_rolled": t_rolled, "old_pods": old,
               "exits": old_exits, "exit_codes": Counter(e["exit_code"] for e in old_exits),
               "sigterm": [e for e in events if e["event"] == "sigterm"],
               "exit_lines": [e for e in events if e["event"] == "exit"],
               "returned": [e for e in events if e["event"] == "returned"],
               "status": Counter(t["status"] for t in tasks), "failed": sum(t["status"] == "failed" for t in tasks),
               "multi_result": sum(1 for c in res.values() if c > 1),
               "attempts_gt1": sum(t["attempts"] > 1 for t in tasks), "tasks": len(tasks),
               "accepted": run["accepted"], "non_2xx": run["non_2xx"]}
    write_json(out / "summary.json", summary)
    if args.grace is not None:
        returned = [int(e["task"]) for e in summary["returned"]]
        by_id = {t["id"]: t for t in tasks}
        summary["returned_tasks"] = [{"task": i, "status": by_id[i]["status"], "attempts": by_id[i]["attempts"],
                                      "results": res.get(str(i), 0), "worker": by_id[i]["worker_id"]} for i in returned]
        write_json(out / "summary.json", summary)
        configure(R)
    say(f"exit codes {dict(summary['exit_codes'])}, returned {len(summary['returned'])}, failed {summary['failed']}, "
        f"multi-result {summary['multi_result']}, attempts>1 {summary['attempts_gt1']} of {len(tasks)}")


# step 9 + scenario F5 -------------------------------------------------------------------------------------------
def wait_rolled(n, timeout=300):
    """All non-terminating worker pods belong to the newest ReplicaSet; crashing pods need not be Ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        dep = json.loads(kubectl("get", "deployment/worker", "-o", "json"))
        rev = dep["metadata"]["annotations"].get("deployment.kubernetes.io/revision")
        items = json.loads(kubectl("get", "pods", "-l", "app=worker", "-o", "json"))["items"]
        rs = json.loads(kubectl("get", "rs", "-l", "app=worker", "-o", "json"))["items"]
        newest = {r["metadata"]["name"] for r in rs
                  if r["metadata"]["annotations"].get("deployment.kubernetes.io/revision") == rev}
        owners = [i["metadata"]["ownerReferences"][0]["name"] for i in items]
        if len(items) == n and all(o in newest for o in owners):
            return db_now()
        time.sleep(1)
    raise TimeoutError("worker rollout did not finish")


def step9_probe(args):
    """FAIL_RATE with the default kubelet crash-loop backoff (10 s doubling up to 5 min)."""
    xr = x_r()
    out = RES / args.out
    reset_queue()
    configure(R, FAIL_RATE=args.fail_rate)
    time.sleep(5)
    snaps = []
    with ExitWatcher() as exits:
        t0 = time.time()
        gen = Generator().start(rate=0.7 * xr, duration=args.duration)
        while time.time() - t0 < args.duration:
            snaps.append({"t": round(time.time() - t0), "counts": queue_counts(),
                          "pods": [" ".join(l.split()) for l in kubectl("get", "pods", "-l", "app=worker",
                                                                         "--no-headers").splitlines()]})
            say(snaps[-1])
            time.sleep(10)
        run = gen.join()
    done = queue_counts().get("done", 0)
    write_json(out / "summary.json", {"rate": 0.7 * xr, "duration": args.duration, "snapshots": snaps,
                                      "final": queue_counts(), "X": done / args.duration,
                                      "exits": list(exits.exits.values()),
                                      "kubelet": json.loads(kubectl("get", "--raw",
                                          "/api/v1/nodes/lab4-worker/proxy/configz", ns=False))
                                          ["kubeletconfig"].get("crashLoopBackOff")})
    say(f"done {done} in {args.duration} s, X = {done / args.duration:.1f}/s vs X(R) = {xr:.1f}")
    reset_queue()
    configure(R)
def step9(args):
    xr = x_r()
    t_cpu = json.loads((RES / "step2" / "summary.json").read_text())["t_cpu_ms"] / 1000
    l_half = round(t_cpu / 2, 4)
    x_f = json.loads((RES / "step9_capacity" / "summary.json").read_text())["X"]
    if args.literal:
        # as the assignment says: 0.7 * X(R); the pool cannot keep up, so the queue is not drained afterwards
        out, rate = RES / "step9_literal", 0.7 * xr
    else:
        out, rate = RES / "step9", args.rate or 0.7 * x_f
    reset_queue()  # an empty queue first: crashing pods never become Ready and would hang the rollout
    configure(R, FAIL_RATE=args.fail_rate)
    time.sleep(5)
    phases, pods_tables = [], []
    with ExitWatcher() as exits, LogCollector(out / "worker.log"):
        t0 = db_now()
        gen = Generator().start(rate=rate, duration=args.duration)
        third = args.duration / 3
        for at, env in ((third, {"LEASE_SECONDS": l_half, "HEARTBEAT": 0}),
                        (2 * third, {"LEASE_SECONDS": l_half, "HEARTBEAT": 1})):
            time.sleep(max(0.0, t0 + at - db_now()))
            pods_tables.append(kubectl("get", "pods", "-l", "app=worker"))
            p = {"at": db_now(), "env": env}
            say(f"scenario F5: set {env}")
            kubectl("set", "env", "deployment/worker", *(f"{k}={v}" for k, v in env.items()))
            p["rolled"] = wait_rolled(R)
            say(f"rolled in {p['rolled'] - p['at']:.1f} s")
            phases.append(p)
        run = gen.join()
        t1 = db_now()
        restarts_end = kubectl("get", "pods", "-l", "app=worker")
        pods_tables.append(restarts_end)
        counts_end = queue_counts()
        if not args.literal:
            wait_queue_empty(timeout=1800)
        time.sleep(8)
    save(run, out / "generator.csv")
    dump_tables(out)
    (out / "pods.txt").write_text("\n".join(pods_tables))
    tasks = tasks_table(out)
    res = Counter(r["task_id"] for r in results_table(out))
    events = parse_log(out / "worker.log")
    bounds = [("baseline L=10s, heartbeat", t0, phases[0]["at"]),
              (f"F5 L={l_half}s, no heartbeat", phases[0]["rolled"], phases[1]["at"]),
              (f"F5 L={l_half}s, heartbeat", phases[1]["rolled"], t1)]
    per_phase = []
    for name, lo, hi in bounds:
        if args.literal:
            # backlog: a phase processes tasks created earlier, so take the tasks that ended inside the phase
            sel = [t for t in tasks if t["finished"] and lo <= t["finished"] < hi and t["status"] in ("done", "failed")]
        else:
            sel = [t for t in tasks if lo <= t["created"] < hi]
        attempts = Counter(t["attempts"] for t in sel)
        ev = [e for e in events if lo <= iso_epoch(e["ts"]) < hi]
        done_sel = [t for t in sel if t["status"] == "done"]
        per_phase.append({
            "name": name, "window": [lo, hi], "tasks": len(sel), "attempts": dict(sorted(attempts.items())),
            "share_attempts_gt1": sum(t["attempts"] > 1 for t in sel) / len(sel) if sel else None,
            "status": Counter(t["status"] for t in sel),
            "share_failed": sum(t["status"] == "failed" for t in sel) / len(sel) if sel else None,
            "multi_result": sum(1 for t in sel if res.get(str(t["id"]), 0) > 1),
            "zero_row_lines": Counter(e["op"] for e in ev if e["event"] == "stale"),
            "crashes": sum(e["event"] == "crash" for e in ev),
            "reaped": Counter(e["to"] for e in ev if e["event"] == "reaped"),
            "X_done": len([t for t in tasks if t["finished"] and lo <= t["finished"] < hi and t["status"] == "done"])
                      / (hi - lo),
            "W_q_p50_ms": percentile([(t["started"] - t["created"]) * 1000 for t in done_sel], 0.5),
        })
    crashes = [e for e in exits.exits.values() if e["exit_code"] == 1]
    summary = {"fail_rate": args.fail_rate, "rate": rate, "X_R": xr, "X_f": x_f, "t_cpu_s": t_cpu, "L_half_s": l_half,
               "t0": t0, "t1": t1, "phases": phases, "per_phase": per_phase,
               "attempts_all": dict(sorted(Counter(t["attempts"] for t in tasks).items())),
               "status_all": Counter(t["status"] for t in tasks), "multi_result": sum(1 for c in res.values() if c > 1),
               "container_exits": Counter(e["exit_code"] for e in exits.exits.values()),
               "crash_exits": len(crashes), "crash_events_logged": sum(e["event"] == "crash" for e in events),
               "accepted": run["accepted"], "non_2xx": run["non_2xx"], "counts_end": counts_end,
               "literal": args.literal}
    write_json(out / "summary.json", summary)
    if args.literal:
        reset_queue()
    for p in per_phase:
        say(json.dumps(p, default=str))
    say(f"exits {dict(summary['container_exits'])}, statuses {dict(summary['status_all'])}")
    configure(R, FAIL_RATE=0)


def iso_epoch(ts: str) -> float:
    from datetime import datetime
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


# step 10: requirement D6, cancellation --------------------------------------------------------------------------
def http(method, path):
    req = urllib.request.Request(WEB + path, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


def step10(args):
    xr = x_r()
    out = RES / "step10"
    configure(R)
    # queued tasks: cancelled at once and never processed
    scale_workers(0)
    reset_queue()
    queued = Generator().run(rate=50, count=20)
    queued_ids = [r["task_id"] for r in queued["rows"]]
    queued_codes = Counter(http("DELETE", f"/tasks/{i}")[0] for i in queued_ids)
    scale_workers(R)
    # running tasks: the flag is checked by the worker at the next pass boundary
    with LogCollector(out / "worker.log"):
        gen = Generator().start(rate=0.7 * xr, duration=args.duration)
        time.sleep(3)
        accepted, codes, attempts = [], Counter(), 0
        while len(accepted) < 20:
            status, page = http("GET", "/tasks?status=running&limit=10")
            for t in page["items"]:
                if t["id"] in accepted:
                    continue  # already flagged, the worker has not reached a pass boundary yet
                attempts += 1
                code, body = http("DELETE", f"/tasks/{t['id']}")
                codes[code] += 1
                if code == 202:
                    accepted.append(t["id"])
                    if len(accepted) == 20:
                        break
        run = gen.join()
        wait_queue_empty()
        time.sleep(2)
    save(run, out / "generator.csv")
    dump_tables(out)
    tasks = {t["id"]: t for t in tasks_table(out)}
    res = Counter(int(r["task_id"]) for r in results_table(out))
    stop_ms = [(tasks[i]["finished"] - tasks[i]["cancel_at"]) * 1000 for i in accepted]
    events = parse_log(out / "worker.log")
    cancel_events = Counter(e.get("at") for e in events if e["event"] == "cancelled")
    summary = {"queued": {"ids": queued_ids, "delete_codes": queued_codes,
                          "status": Counter(tasks[i]["status"] for i in queued_ids),
                          "results": sum(res.get(i, 0) for i in queued_ids),
                          "attempts": Counter(tasks[i]["attempts"] for i in queued_ids)},
               "running": {"ids": accepted, "delete_codes": codes, "delete_requests": attempts,
                           "status": Counter(tasks[i]["status"] for i in accepted),
                           "results": sum(res.get(i, 0) for i in accepted),
                           "stop_ms": stop_ms, "stop_p50_ms": percentile(stop_ms, 0.5),
                           "stop_p99_ms": percentile(stop_ms, 0.99), "stop_max_ms": max(stop_ms),
                           "worker_cancel_events": cancel_events},
               "rate": 0.7 * xr, "all_status": Counter(t["status"] for t in tasks.values())}
    write_json(out / "summary.json", summary)
    say(f"queued: {dict(queued_codes)} -> {dict(summary['queued']['status'])}; running: codes {dict(codes)}, "
        f"status {dict(summary['running']['status'])}, results {summary['running']['results']}, "
        f"stop p50 {summary['running']['stop_p50_ms']:.1f} ms, p99 {summary['running']['stop_p99_ms']:.1f} ms")


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="step", required=True)
    for name in ("step2", "step4", "step5", "step6_recompute", "step7"):
        sub.add_parser(name)
    sub.add_parser("step3").add_argument("--repeat", type=int, default=3)
    p4x = sub.add_parser("step4x")
    p4x.add_argument("--workers", type=int, default=12)
    p4x.add_argument("--count", type=int, default=1500)
    p6 = sub.add_parser("step6")
    p6.add_argument("--workers", type=int, nargs="+", default=[1, 2, R])
    p6.add_argument("--seconds", type=float, default=60, help="planned drain time at ideal scaling")
    p8 = sub.add_parser("step8")
    p8.add_argument("--grace", type=float, default=None, help="worker GRACE_SECONDS for the return-to-queue check")
    p8.add_argument("--slice", type=int, default=256)
    p8.add_argument("--rate", type=float, default=None)
    p8.add_argument("--duration", type=float, default=90)
    p8.add_argument("--restart-at", type=float, default=30)
    p9 = sub.add_parser("step9")
    p9.add_argument("--fail-rate", type=float, default=0.15)
    p9.add_argument("--duration", type=float, default=300)
    p9.add_argument("--rate", type=float, default=None, help="default 0.7 * X_f from step9_capacity")
    p9.add_argument("--literal", action="store_true", help="0.7 * X(R) as in the assignment, no drain")
    p9p = sub.add_parser("step9_probe")
    p9p.add_argument("--fail-rate", type=float, default=0.15)
    p9p.add_argument("--duration", type=float, default=90)
    p9p.add_argument("--out", default="rejected/fail_rate_default_backoff")
    p10 = sub.add_parser("step10")
    p10.add_argument("--duration", type=float, default=60)
    args = parser.parse_args()
    globals()[args.step](args)


if __name__ == "__main__":
    main()
