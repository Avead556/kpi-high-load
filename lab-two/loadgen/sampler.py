import argparse
import json
import subprocess
import time
from pathlib import Path


def docker_stats():
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"],
                         capture_output=True, text=True).stdout
    stats = {}
    for line in out.splitlines():
        row = json.loads(line)
        if row["Name"].startswith("vaccine-lots-"):
            stats[row["Name"]] = {"cpu_pct": float(row["CPUPerc"].rstrip("%")), "mem": row["MemUsage"].split(" / ")[0]}
    return stats


def generator_cpu():
    out = subprocess.run(["ps", "-A", "-o", "pid=,%cpu=,rss=,command="], capture_output=True, text=True).stdout
    cpu, rss, procs = 0.0, 0, 0
    for line in out.splitlines():
        if "loadgen/steps.py" in line and "ps -A" not in line:
            parts = line.split(None, 3)
            cpu += float(parts[1])
            rss += int(parts[2])
            procs += 1
    return {"cpu_pct": round(cpu, 1), "rss_mb": round(rss / 1024, 1), "procs": procs}


def main():
    parser = argparse.ArgumentParser(description="sample docker stats and generator CPU until a stop file appears")
    parser.add_argument("--out", required=True)
    parser.add_argument("--stop-file", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    with open(out, "w") as f:
        while not Path(args.stop_file).exists():
            began = time.time()
            containers = docker_stats()
            row = {"epoch": round((began + time.time()) / 2, 2), "containers": containers, "generator": generator_cpu()}
            f.write(json.dumps(row) + "\n")
            f.flush()


if __name__ == "__main__":
    main()
