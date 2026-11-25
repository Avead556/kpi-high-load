"""Helpers shared by the lab 4 experiments: kubectl, psql inside postgres-0, Prometheus, worker logs and exits."""
import bisect
import csv
import io
import json
import math
import subprocess
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

NS = "vaccines"
ROOT = Path(__file__).resolve().parents[1]
WEB = "http://127.0.0.1:8084"
PROM = "http://127.0.0.1:9094"


# kubectl / psql -------------------------------------------------------------------------------------------------
def kubectl(*args, check=True, ns=True, timeout=300):
    cmd = ["kubectl", *(["-n", NS] if ns else []), *args]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and res.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}: {res.stderr.strip()}")
    return res.stdout


def psql(sql: str) -> list[dict]:
    """Runs SQL in postgres-0 over the local socket, returns the rows of the last statement."""
    res = subprocess.run(
        ["kubectl", "-n", NS, "exec", "-i", "postgres-0", "--", "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1",
         "-U", "vaccines", "-d", "vaccines", "--csv", "-f", "-"],
        input=sql, capture_output=True, text=True, timeout=300)
    if res.returncode != 0:
        raise RuntimeError(f"psql: {res.stderr.strip()}")
    return list(csv.DictReader(io.StringIO(res.stdout))) if res.stdout.strip() else []


def psql_file(path: Path) -> str:
    return subprocess.run(
        ["kubectl", "-n", NS, "exec", "-i", "postgres-0", "--", "psql", "-X", "-v", "ON_ERROR_STOP=1",
         "-U", "vaccines", "-d", "vaccines", "-f", "-"],
        input=Path(path).read_text(), capture_output=True, text=True, check=True).stdout


def db_now() -> float:
    return float(psql("SELECT extract(epoch FROM clock_timestamp()) AS t")[0]["t"])


def queue_counts() -> dict[str, int]:
    return {r["status"]: int(r["n"]) for r in psql("SELECT status, count(*) AS n FROM tasks GROUP BY status")}


def wait_queue_empty(timeout=900, poll=1.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        c = queue_counts()
        if c.get("queued", 0) == 0 and c.get("running", 0) == 0:
            return c
        time.sleep(poll)
    raise TimeoutError(f"queue not empty after {timeout}s: {queue_counts()}")


TASK_COLUMNS = ("id, record_id, status, seed, attempts, token, worker_id, "
                "extract(epoch FROM created_at) AS created, extract(epoch FROM started_at) AS started, "
                "extract(epoch FROM finished_at) AS finished, extract(epoch FROM cancel_requested_at) AS cancel_at")


def dump_tables(out: Path):
    out.mkdir(parents=True, exist_ok=True)
    for name, sql in (("tasks", f"SELECT {TASK_COLUMNS} FROM tasks ORDER BY id"),
                      ("results", "SELECT task_id, token, worker_id, extract(epoch FROM created_at) AS created, "
                                  "result->>'p_lo' AS p_lo, result->>'p_hi' AS p_hi, result->>'mean' AS mean "
                                  "FROM task_results ORDER BY task_id, created_at")):
        rows = psql(sql)
        with open(out / f"{name}.csv", "w", newline="") as f:
            if rows:
                w = csv.DictWriter(f, fieldnames=list(rows[0]))
                w.writeheader()
                w.writerows(rows)


def reset_queue():
    psql_file(ROOT / "db" / "modes" / "reset.sql")


# worker deployment ----------------------------------------------------------------------------------------------
def worker_pods(include_terminating=False) -> list[dict]:
    items = json.loads(kubectl("get", "pods", "-l", "app=worker", "-o", "json"))["items"]
    pods = []
    for p in items:
        terminating = "deletionTimestamp" in p["metadata"]
        if terminating and not include_terminating:
            continue
        ready = any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))
        restarts = sum(c.get("restartCount", 0) for c in p["status"].get("containerStatuses", []))
        pods.append({"name": p["metadata"]["name"], "node": p["spec"].get("nodeName"), "ready": ready,
                     "terminating": terminating, "restarts": restarts})
    return pods


def wait_workers(n: int, timeout=300):
    deadline = time.time() + timeout
    while time.time() < deadline:
        pods = worker_pods(include_terminating=True)
        if len(pods) == n and all(p["ready"] and not p["terminating"] for p in pods):
            return pods
        time.sleep(1)
    raise TimeoutError(f"workers not ready: {worker_pods(True)}")


def scale_workers(n: int):
    kubectl("scale", "deployment/worker", f"--replicas={n}")
    return wait_workers(n)


def set_worker_env(**env):
    """kubectl set env overrides the ConfigMap; changing it rolls the Deployment."""
    current = worker_env()
    if all(current.get(k) == str(v) for k, v in env.items()):
        return False
    kubectl("set", "env", "deployment/worker", *(f"{k}={v}" for k, v in env.items()))
    kubectl("rollout", "status", "deployment/worker", "--timeout=300s")
    return True


def worker_env() -> dict:
    spec = json.loads(kubectl("get", "deployment/worker", "-o", "json"))["spec"]["template"]["spec"]
    return {e["name"]: e.get("value") for e in spec["containers"][0].get("env", []) if "value" in e}


def replicas() -> int:
    return int(json.loads(kubectl("get", "deployment/worker", "-o", "json"))["spec"].get("replicas", 1))


# worker logs and container exits --------------------------------------------------------------------------------
class LogCollector:
    """Follows the log of every worker container, including restarted and terminating ones."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(self.path, "w")
        self.lock = threading.Lock()
        self.followed = set()
        self.procs = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.loop, daemon=True)

    def __enter__(self):
        # only lines written from now on: `kubectl logs` would otherwise replay the whole container log
        self.since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time()))
        self.scan()
        self.thread.start()
        return self

    def __exit__(self, *exc):
        time.sleep(2)
        self.stop.set()
        self.thread.join()
        for p in self.procs:
            p.terminate()
        for p in self.procs:
            p.wait()
        self.file.close()

    def write(self, pod, restart, text):
        with self.lock:
            for line in text.splitlines():
                self.file.write(f"pod={pod} restart={restart} {line}\n")
            self.file.flush()

    def follow(self, pod, restart):
        proc = subprocess.Popen(["kubectl", "-n", NS, "logs", "-f", pod, "-c", "worker",
                                 f"--since-time={self.since}"],
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.procs.append(proc)

        def pump():
            for line in proc.stdout:
                self.write(pod, restart, line.rstrip("\n"))

        threading.Thread(target=pump, daemon=True).start()

    def scan(self):
        items = json.loads(kubectl("get", "pods", "-l", "app=worker", "-o", "json", check=False) or '{"items": []}')
        for p in items["items"]:
            pod = p["metadata"]["name"]
            for c in p["status"].get("containerStatuses", []):
                rc = c.get("restartCount", 0)
                if rc > 0 and (pod, rc - 1) not in self.followed:
                    self.followed.add((pod, rc - 1))
                    prev = subprocess.run(["kubectl", "-n", NS, "logs", pod, "-c", "worker", "--previous",
                                           f"--since-time={self.since}"],
                                          capture_output=True, text=True).stdout
                    self.write(pod, rc - 1, prev)
                if (pod, rc) not in self.followed and ("running" in c.get("state", {})
                                                       or "terminated" in c.get("state", {})):
                    self.followed.add((pod, rc))
                    self.follow(pod, rc)

    def loop(self):
        while not self.stop.wait(0.5):
            try:
                self.scan()
            except Exception:
                pass


class ExitWatcher:
    """Records every terminated worker container (exit code, reason) from a kubectl watch stream."""

    def __init__(self):
        self.exits = {}
        self.proc = None

    def __enter__(self):
        self.proc = subprocess.Popen(["kubectl", "-n", NS, "get", "pods", "-l", "app=worker", "-w", "-o", "json",
                                      "--output-watch-events"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                     text=True)
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        time.sleep(2)
        self.proc.terminate()
        self.proc.wait()

    def read(self):
        decoder, buf = json.JSONDecoder(), ""
        for chunk in iter(lambda: self.proc.stdout.read(4096), ""):
            buf += chunk
            while True:
                buf = buf.lstrip()
                try:
                    event, end = decoder.raw_decode(buf)
                except json.JSONDecodeError:
                    break
                buf = buf[end:]
                self.record(event.get("object", event))

    def record(self, pod):
        name = pod["metadata"]["name"]
        for c in pod.get("status", {}).get("containerStatuses", []):
            for state in (c.get("state", {}), c.get("lastState", {})):
                t = state.get("terminated")
                if t and t.get("containerID"):
                    self.exits[t["containerID"]] = {"pod": name, "exit_code": t.get("exitCode"),
                                                    "reason": t.get("reason"), "finished": t.get("finishedAt")}


def parse_log(path: Path) -> list[dict]:
    events = []
    for line in Path(path).read_text().splitlines():
        fields = {}
        for part in line.split(" "):
            k, sep, v = part.partition("=")
            if sep:
                fields[k] = v
        if "event" in fields:
            events.append(fields)
    return events


class LockSampler:
    """Backends waiting on a heavyweight lock, sampled from one psql session in postgres-0 with \\watch."""

    def __init__(self, interval=1.0, max_seconds=900):
        self.samples = []
        self.interval = interval
        self.count = int(max_seconds / interval)
        self.app = f"locksampler{int(interval * 1000)}ms"

    def __enter__(self):
        sql = ("SELECT extract(epoch FROM clock_timestamp()), count(*) FILTER (WHERE wait_event_type = 'Lock') "
               "FROM pg_stat_activity")
        self.proc = subprocess.Popen(
            ["kubectl", "-n", NS, "exec", "-i", "postgres-0", "--", "psql", "-XAtq", "-U", "vaccines",
             "-d", f"dbname=vaccines application_name={self.app}"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.proc.stdin.write(f"{sql} \\watch i={self.interval} c={self.count}\n")
        self.proc.stdin.close()
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()
        return self

    def read(self):
        for line in self.proc.stdout:
            t, _, n = line.strip().partition("|")
            if n:
                self.samples.append((float(t), int(n)))

    def __exit__(self, *exc):
        psql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = '{self.app}'")
        self.proc.wait(timeout=30)
        self.thread.join(timeout=5)

    @property
    def max_waiting(self):
        return max((n for _, n in self.samples), default=0)

    def share_waiting(self):
        return sum(n > 0 for _, n in self.samples) / len(self.samples) if self.samples else None


def worker_metrics() -> dict[str, float]:
    """Exact counters of every worker pod, read from its /metrics (sum over pods)."""
    from concurrent.futures import ThreadPoolExecutor

    def scrape(name):
        # through the API server proxy: nothing runs inside the worker container and eats its CPU quota
        return kubectl("get", "--raw", f"/api/v1/namespaces/{NS}/pods/{name}:9100/proxy/metrics", ns=False)

    total = {}
    pods = [p["name"] for p in worker_pods()]
    with ThreadPoolExecutor(len(pods) or 1) as pool:
        texts = list(pool.map(scrape, pods))
    for text in texts:
        for line in text.splitlines():
            if line.startswith("worker_") and not line.startswith("#"):
                key, _, value = line.rpartition(" ")
                total[key] = total.get(key, 0.0) + float(value)
    return total


def metrics_delta(before: dict, after: dict) -> dict:
    return {k: after[k] - before.get(k, 0.0) for k in after}


def claim_from_metrics(delta: dict) -> dict:
    buckets = []
    for key, v in delta.items():
        if key.startswith("worker_task_duration_seconds_bucket{") and 'step="claim"' in key:
            le = key.split('le="')[1].split('"')[0]
            buckets.append((math.inf if le == "+Inf" else float(le), v))
    buckets.sort()
    q = bucket_quantile(buckets, 0.99)
    return {"claim_p99_ms": round(q * 1000, 3) if q is not None else None,
            "claim_p50_ms": round(bucket_quantile(buckets, 0.5) * 1000, 3) if buckets else None,
            "claims_empty": delta.get('worker_claims_total{result="empty"}', 0.0),
            "claims_task": delta.get('worker_claims_total{result="task"}', 0.0),
            "claim_buckets": buckets}


# Prometheus -----------------------------------------------------------------------------------------------------
def prom_range(expr: str, start: float, end: float, step: float = 1.0) -> list[dict]:
    params = urllib.parse.urlencode({"query": expr, "start": start, "end": end, "step": step})
    with urllib.request.urlopen(f"{PROM}/api/v1/query_range?{params}") as response:
        return json.load(response)["data"]["result"]


def points(result: dict) -> list[tuple[float, float]]:
    return sorted((float(t), float(v)) for t, v in result["values"])


def value_at(pts, ts):
    i = bisect.bisect_right([p[0] for p in pts], ts + 1e-6) - 1
    return pts[i][1] if i >= 0 else None


def prom_rate(selector_metric: str, lo: float, hi: float, by: str = "") -> float:
    """sum(rate(m[window])) evaluated at the end of the window: Prometheus handles cAdvisor timestamps and resets."""
    window = max(int(round(hi - lo)), 1)
    expr = f"sum{f' by ({by})' if by else ''} (rate({selector_metric}[{window}s]))"
    res = prom_range(expr, hi, hi, 1)
    return sum(float(r["values"][-1][1]) for r in res) if res else 0.0


def counter_delta(expr_by_series: str, lo: float, hi: float) -> float:
    """Sum over series of (value at hi - value at lo); a series born inside the window counts from zero."""
    total = 0.0
    for s in prom_range(expr_by_series, lo - 10, hi + 10):
        pts = points(s)
        a, b = value_at(pts, lo), value_at(pts, hi)
        if b is None:
            continue
        total += b - (a or 0.0) if a is None or b >= a else b
    return total


def bucket_counts(metric: str, selector: str, lo: float, hi: float) -> list[tuple[float, float]]:
    counts = {}
    for s in prom_range(f"sum by (pod, le) ({metric}_bucket{{{selector}}})", lo - 10, hi + 10):
        pts = points(s)
        a, b = value_at(pts, lo), value_at(pts, hi)
        if b is None:
            continue
        d = b - (a or 0.0) if a is None or b >= a else b
        le = s["metric"]["le"]
        counts[le] = counts.get(le, 0.0) + d
    return sorted(((math.inf if le == "+Inf" else float(le), c) for le, c in counts.items()), key=lambda x: x[0])


def bucket_quantile(buckets, q=0.99):
    if not buckets or buckets[-1][1] <= 0:
        return None
    rank = q * buckets[-1][1]
    prev_b, prev_c = 0.0, 0.0
    finite = [b for b, _ in buckets if b != math.inf]
    for b, c in buckets:
        if c >= rank:
            if b == math.inf:
                return finite[-1]
            return prev_b + (b - prev_b) * (rank - prev_c) / (c - prev_c) if c > prev_c else b
        prev_b, prev_c = b, c
    return None


def write_json(path: Path, data):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + "\n")
