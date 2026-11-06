import argparse
import json
import re
import subprocess
import time
from pathlib import Path

GROUPS = {
    "generator": ("loadgen/steps.py",),
    "docker_vm": ("com.apple.Virtualization.VirtualMachine",),
    "docker_backend": ("com.docker.backend", "com.docker.vpnkit", "vpnkit", "com.docker.krun"),
}


def host_busy():
    out = subprocess.run(["top", "-l", "2", "-n", "0", "-s", "1"], capture_output=True, text=True).stdout
    idle = re.findall(r"CPU usage:.*?([\d.]+)% idle", out)
    return round(100 - float(idle[-1]), 1) if idle else None


def processes():
    out = subprocess.run(["ps", "-A", "-o", "%cpu=,rss=,command="], capture_output=True, text=True).stdout
    groups = {name: {"cpu_pct": 0.0, "rss_mb": 0.0} for name in GROUPS}
    for line in out.splitlines():
        cpu, rss, command = line.strip().split(None, 2)
        for name, needles in GROUPS.items():
            if any(n in command for n in needles) and "ps -A" not in command:
                groups[name]["cpu_pct"] += float(cpu)
                groups[name]["rss_mb"] += int(rss) / 1024
    return {k: {"cpu_pct": round(v["cpu_pct"], 1), "rss_mb": round(v["rss_mb"], 1)} for k, v in groups.items()}


def main():
    parser = argparse.ArgumentParser(description="sample host CPU and Docker Desktop processes")
    parser.add_argument("--out", required=True)
    parser.add_argument("--seconds", type=float, required=True)
    args = parser.parse_args()
    end = time.time() + args.seconds
    with open(args.out, "w") as f:
        while time.time() < end:
            row = {"epoch": round(time.time(), 2), "host_cpu_busy_pct": host_busy(), "processes": processes()}
            f.write(json.dumps(row) + "\n")
            f.flush()


if __name__ == "__main__":
    main()
