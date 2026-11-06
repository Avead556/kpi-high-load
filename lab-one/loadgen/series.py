import argparse
import json
import re
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
FIELDS = ["offered_rps", "achieved_rps", "p50_ms", "p95_ms", "p99_ms", "mean_ms", "non_2xx_share", "measured_L",
          "on_wire_mean"]


def container_cpu():
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}}"],
                         capture_output=True, text=True).stdout
    cpu = {}
    for line in out.splitlines():
        name, _, value = line.partition(" ")
        if name.startswith("vaccine-lots-"):
            cpu[name] = float(value.rstrip("%"))
    return cpu


def host_busy():
    out = subprocess.run(["top", "-l", "2", "-n", "0", "-s", "1"], capture_output=True, text=True).stdout
    idle = re.findall(r"CPU usage:.*?([\d.]+)% idle", out)
    return 100 - float(idle[-1]) if idle else None


def sample_cpu(start, stop, into):
    while time.time() < start:
        time.sleep(0.2)
    containers, host = {}, []
    while time.time() < stop:
        for name, value in container_cpu().items():
            containers.setdefault(name, []).append(value)
        busy = host_busy()
        if busy is not None:
            host.append(busy)
    into["container_cpu_pct"] = {k: round(statistics.fmean(v), 1) for k, v in sorted(containers.items())}
    into["host_cpu_busy_pct"] = round(statistics.fmean(host), 1) if host else None
    into["cpu_samples"] = len(host)


def main():
    parser = argparse.ArgumentParser(description="run the 10/100/500 rps series three times each")
    parser.add_argument("--url", default="http://127.0.0.1:8018")
    parser.add_argument("--path", default="/vaccine-lots/{id}")
    parser.add_argument("--label", required=True)
    parser.add_argument("--rates", default="10,100,500")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--ids", default="results/ids.txt")
    parser.add_argument("--outdir", default="results")
    args = parser.parse_args()
    ids = Path(args.ids).read_text().strip() if Path(args.ids).exists() else "0"
    raw = Path(args.outdir) / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    table = []
    for rate in [int(r) for r in args.rates.split(",")]:
        runs = []
        for rep in range(1, args.repeats + 1):
            stem = raw / f"{args.label}_{rate}rps_run{rep}"
            cpu = {}
            began = time.time()
            watcher = threading.Thread(target=sample_cpu, args=(began + 11, began + 39, cpu))
            watcher.start()
            subprocess.run(
                [sys.executable, str(HERE / "loadgen.py"), "--url", args.url, "--path", args.path, "--ids", ids,
                 "--rate", str(rate), "--out", f"{stem}.json", "--raw", f"{stem}.csv.gz"],
                check=True, stdout=subprocess.DEVNULL,
            )
            watcher.join()
            run = json.loads(Path(f"{stem}.json").read_text())
            run.update(cpu)
            run["run"] = rep
            Path(f"{stem}.json").write_text(json.dumps(run, ensure_ascii=False, indent=2) + "\n")
            runs.append(run)
            print(f"{args.label} {rate} rps run {rep}: achieved {run['achieved_rps']} p50 {run['p50_ms']} "
                  f"p95 {run['p95_ms']} p99 {run['p99_ms']} non2xx {run['non_2xx_share']} L {run['measured_L']} "
                  f"wire {run['on_wire_mean']} cpu {run['container_cpu_pct']} host {run['host_cpu_busy_pct']}",
                  flush=True)
            time.sleep(3)
        median = sorted(runs, key=lambda r: r["achieved_rps"])[len(runs) // 2]
        row = {"label": args.label, "target_rps": rate, "median_run": median["run"]}
        for field in FIELDS + ["container_cpu_pct", "host_cpu_busy_pct", "instances", "errors"]:
            row[field] = median[field]
        row["runs"] = [{k: r[k] for k in ["run"] + FIELDS} for r in runs]
        table.append(row)
        print(json.dumps({k: v for k, v in row.items() if k != "runs"}, ensure_ascii=False), flush=True)
    Path(args.outdir, f"series_{args.label}.json").write_text(json.dumps(table, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
