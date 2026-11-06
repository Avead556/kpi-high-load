import argparse
import csv
import json
import subprocess
import threading
import time
from pathlib import Path

NS = ["-n", "vaccines"]


def kubectl(*args, check=True):
    return subprocess.run(["kubectl", *args], capture_output=True, text=True, check=check).stdout


def web_pods():
    items = json.loads(kubectl(*NS, "get", "pods", "-l", "app=web", "-o", "json"))["items"]
    pods = []
    for p in items:
        ready = any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))
        pods.append({"name": p["metadata"]["name"], "node": p["spec"].get("nodeName"), "ready": ready,
                     "terminating": "deletionTimestamp" in p["metadata"], "phase": p["status"].get("phase")})
    return pods


def wait_ready(n):
    while True:
        pods = web_pods()
        if sum(p["ready"] and not p["terminating"] for p in pods) == n and len(pods) == n:
            return pods
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdb", choices=["yes", "no"], required=True)
    parser.add_argument("--rate", type=float, required=True)
    parser.add_argument("--replicas", type=int, default=3)
    parser.add_argument("--before", type=int, default=30)
    parser.add_argument("--duration", type=int, default=150)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    for node in ("lab3-worker", "lab3-worker2"):
        kubectl("uncordon", node)
    kubectl(*NS, "delete", "hpa", "web", "--ignore-not-found")
    kubectl(*NS, "scale", "deployment/web", f"--replicas={args.replicas}")
    if args.pdb == "yes":
        kubectl("apply", "-f", "k8s/web/pdb.yaml")
    else:
        kubectl(*NS, "delete", "pdb", "web", "--ignore-not-found")
    pods = wait_ready(args.replicas)
    by_node = {}
    for p in pods:
        by_node[p["node"]] = by_node.get(p["node"], 0) + 1
    target = max(("lab3-worker", "lab3-worker2"), key=lambda n: by_node.get(n, 0))
    print(f"pods by node {by_node}, draining {target}", flush=True)
    time.sleep(20)

    gen = subprocess.Popen([".venv/bin/python", "loadgen/load.py", "--profile",
                            f"{args.before}:{args.rate},{args.duration - args.before}:{args.rate}",
                            "--out", str(out / "load")], stdout=open(out / "load.log", "w"), stderr=subprocess.STDOUT)
    t_start = time.time() + 1.0
    timeline, drain = [], {}
    stop = threading.Event()

    def poll():
        while not stop.is_set():
            t = time.time()
            ps = web_pods()
            timeline.append({
                "t": round(t - t_start, 2), "epoch": round(t, 2),
                "ready": sum(p["ready"] and not p["terminating"] for p in ps),
                "total": len(ps), "terminating": sum(p["terminating"] for p in ps),
                **{f"ready_{n}": sum(p["ready"] and not p["terminating"] and p["node"] == n for p in ps)
                   for n in ("lab3-worker", "lab3-worker2")},
            })
            time.sleep(max(0.0, 1.0 - (time.time() - t)))

    poller = threading.Thread(target=poll)
    poller.start()
    time.sleep(max(0.0, t_start + args.before - time.time()))
    drain["start"] = time.time() - t_start
    res = subprocess.run(["kubectl", "drain", target, "--ignore-daemonsets", "--delete-emptydir-data",
                          "--timeout=150s"], capture_output=True, text=True)
    drain["end"] = time.time() - t_start
    drain["rc"] = res.returncode
    (out / "drain.txt").write_text(res.stdout + res.stderr)
    gen.wait()
    stop.set()
    poller.join()
    with open(out / "timeline.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(timeline[0]))
        w.writeheader()
        w.writerows(timeline)
    meta = {"pdb": args.pdb, "rate": args.rate, "node": target, "pods_before": by_node, "drain": drain,
            "start_epoch": t_start}
    (out / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(json.dumps(meta), flush=True)
    kubectl("uncordon", target)
    kubectl(*NS, "rollout", "restart", "deployment/web")
    kubectl(*NS, "rollout", "status", "deployment/web", "--timeout=180s")
    wait_ready(args.replicas)
    print("uncordoned, replicas ready:", [(p["name"], p["node"]) for p in web_pods()], flush=True)


if __name__ == "__main__":
    main()
