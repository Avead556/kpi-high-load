import argparse
import json
import urllib.parse
import urllib.request
from pathlib import Path

NS = 'namespace="vaccines"'
PATH = 'path="/vaccine-lots/{lot_id}"'
QUERIES = {
    "requests": f'sum by (pod) (http_requests_total{{{NS},{PATH}}})',
    "buckets": f'sum by (pod, le) (http_request_duration_seconds_bucket{{{NS},{PATH}}})',
    "cfs_periods": f'sum by (pod) (container_cpu_cfs_periods_total{{{NS},container="web"}})',
    "cfs_throttled": f'sum by (pod) (container_cpu_cfs_throttled_periods_total{{{NS},container="web"}})',
    "cpu_seconds": f'sum by (pod) (container_cpu_usage_seconds_total{{{NS},container="web"}})',
    "cpu_rate": f'sum by (pod) (rate(container_cpu_usage_seconds_total{{{NS},container="web"}}[20s]))',
    "ready": f'max(kube_deployment_status_replicas_ready{{{NS},deployment="web"}})',
    "desired": f'max(kube_horizontalpodautoscaler_status_desired_replicas{{{NS},horizontalpodautoscaler="web"}})',
    "pod_node": f'max by (pod, node) (kube_pod_info{{{NS},created_by_kind="ReplicaSet"}})',
    "pg_connections": 'sum(pg_stat_activity_count)',
}


def query_range(base, expr, start, end, step):
    params = urllib.parse.urlencode({"query": expr, "start": start, "end": end, "step": step})
    with urllib.request.urlopen(f"{base}/api/v1/query_range?{params}") as response:
        return json.load(response)["data"]["result"]


def export(summary_path, out, prometheus="http://127.0.0.1:9090", step="1", tail=30):
    summary = json.loads(Path(summary_path).read_text())
    start = summary["start_epoch"] - 30
    end = summary["start_epoch"] + summary["stopped_at_s"] + tail
    data = {name: query_range(prometheus, expr, start, end, step) for name, expr in QUERIES.items()}
    data["queries"] = QUERIES
    data["window"] = [start, end]
    Path(out).write_text(json.dumps(data) + "\n")
    return {k: len(v) for k, v in data.items() if k not in ("queries", "window")}


def main():
    parser = argparse.ArgumentParser(description="export Prometheus series for a load run window")
    parser.add_argument("--summary", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--prometheus", default="http://127.0.0.1:9090")
    parser.add_argument("--tail", type=int, default=30)
    args = parser.parse_args()
    print("exported", export(args.summary, args.out, args.prometheus, tail=args.tail))


if __name__ == "__main__":
    main()
