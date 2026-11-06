import argparse
import json
import urllib.parse
import urllib.request
from pathlib import Path

PATH = "/vaccine-lots/{lot_id}"
QUERIES = {
    "rps": f'sum by (instance) (rate(http_requests_total{{path="{PATH}"}}[15s]))',
    "cpu": 'rate(process_cpu_seconds_total{job="web"}[15s])',
    "in_progress": 'avg_over_time(http_requests_in_progress{job="web"}[15s])',
    "buckets": f'sum by (le) (increase(http_request_duration_seconds_bucket{{path="{PATH}"}}[15s]))',
    "buckets_by_instance": f'sum by (instance, le) (increase(http_request_duration_seconds_bucket{{path="{PATH}"}}[15s]))',
}


def query_range(base, expr, start, end, step):
    params = urllib.parse.urlencode({"query": expr, "start": start, "end": end, "step": step})
    with urllib.request.urlopen(f"{base}/api/v1/query_range?{params}") as response:
        return json.load(response)["data"]["result"]


def main():
    parser = argparse.ArgumentParser(description="export Prometheus series for a load run window")
    parser.add_argument("--summary", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--prometheus", default="http://127.0.0.1:9090")
    parser.add_argument("--step", default="5")
    args = parser.parse_args()
    summary = json.loads(Path(args.summary).read_text())
    start = summary["start_epoch"] - 5
    end = summary["start_epoch"] + summary["stopped_at_s"] + 15
    data = {name: query_range(args.prometheus, expr, start, end, args.step) for name, expr in QUERIES.items()}
    data["queries"] = QUERIES
    data["window"] = [start, end]
    Path(args.out).write_text(json.dumps(data) + "\n")
    print(f"exported {', '.join(f'{k}: {len(v)} series' for k, v in data.items() if isinstance(v, list) and k != 'window')}")


if __name__ == "__main__":
    main()
