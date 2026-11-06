import argparse
import json
import statistics
import subprocess
import time
import urllib.request


def healthy(url):
    try:
        with urllib.request.urlopen(url, timeout=0.5) as response:
            return response.status == 200
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description="time from docker compose up to the first 200 on /healthz")
    parser.add_argument("--url", default="http://127.0.0.1:8018/healthz")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--out", default="results/startup.json")
    args = parser.parse_args()
    times = []
    for i in range(args.repeats):
        subprocess.run(["docker", "compose", "down", "-v"], check=True, capture_output=True)
        start = time.perf_counter()
        up = subprocess.Popen(["docker", "compose", "up", "-d"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        while not healthy(args.url):
            time.sleep(0.05)
        elapsed = time.perf_counter() - start
        up.wait()
        times.append(round(elapsed, 2))
        print(f"run {i + 1}: first 200 on /healthz after {elapsed:.2f} s", flush=True)
    result = {"runs_s": times, "median_s": statistics.median(times)}
    print(f"median: {result['median_s']:.2f} s")
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()
