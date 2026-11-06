import argparse
import csv
import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent


def sh(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()


def main():
    parser = argparse.ArgumentParser(description="stop one web container under load and check the client saw no errors")
    parser.add_argument("--rate", type=float, default=1000)
    parser.add_argument("--duration", type=float, default=40)
    parser.add_argument("--stop-after", type=float, default=20)
    parser.add_argument("--out", default="results/graceful_stop")
    args = parser.parse_args()
    containers = sh("docker", "compose", "ps", "-q", "web").split()
    if len(containers) < 2:
        sys.exit("start the stand with at least two web instances first")
    victim = containers[0]
    name = sh("docker", "inspect", "--format", "{{.Name}}", victim).lstrip("/")
    load = subprocess.Popen([sys.executable, str(HERE / "loadgen" / "steps.py"), "--constant", str(args.rate),
                             "--warmup", "5", "--duration", str(args.duration), "--out", args.out],
                            stdout=subprocess.DEVNULL)
    time.sleep(1 + 5 + args.stop_after)
    stop_begin = time.time()
    sh("docker", "stop", victim)
    stop_end = time.time()
    exit_code = sh("docker", "inspect", "--format", "{{.State.ExitCode}}", victim)
    load.wait()
    rows = list(csv.DictReader(open(Path(args.out) / "seconds.csv")))
    around = [r for r in rows if stop_begin - 10 <= float(r["epoch"]) <= stop_end + 10]
    result = {
        "stopped_container": name, "exit_code": int(exit_code),
        "docker_stop_seconds": round(stop_end - stop_begin, 2),
        "window_seconds": len(around),
        "achieved": sum(int(r["achieved"]) for r in around),
        "non_2xx": sum(int(r["non_2xx"]) for r in around),
        "conn_errors": sum(int(r["conn_errors"]) for r in around),
        "max_p99_ms": max(float(r["p99_ms"]) for r in around if r["p99_ms"]),
    }
    Path(args.out, "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(f"docker stop {name}: exit code {exit_code}, took {result['docker_stop_seconds']} s")
    print(f"10 s around the stop ({len(around)} s of records): {result['achieved']} x 2xx, "
          f"{result['non_2xx']} x non-2xx, {result['conn_errors']} connection errors, max p99 {result['max_p99_ms']} ms")
    sh("docker", "start", victim)


if __name__ == "__main__":
    main()
