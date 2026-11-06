#!/usr/bin/env bash
set -euo pipefail
PROM=${PROM:-http://localhost:9090}
q() {
  curl -s --get "$PROM/api/v1/query" --data-urlencode "query=$1" | python3 -c '
import json, sys
for x in json.load(sys.stdin)["data"]["result"]:
    m = {k: v for k, v in x["metric"].items() if k in ("pod", "container", "deployment", "__name__")}
    print("   ", m, x["value"][1])'
}
for i in $(seq 300); do curl -s -o /dev/null "http://localhost:8080/vaccine-lots/1"; done
sleep 12
echo "== targets"
curl -s "$PROM/api/v1/targets" | python3 -c 'import json,sys; from collections import Counter; c=Counter((t["scrapePool"],t["health"]) for t in json.load(sys.stdin)["data"]["activeTargets"]); [print("   ",k,v) for k,v in sorted(c.items())]'
echo "== requests per web pod";  q 'sum by (pod) (http_requests_total{namespace="vaccines"})'
echo "== CFS periods (web)";      q 'container_cpu_cfs_periods_total{namespace="vaccines",container="web"}'
echo "== CFS throttled (web)";    q 'container_cpu_cfs_throttled_periods_total{namespace="vaccines",container="web"}'
echo "== ready replicas";         q 'kube_deployment_status_replicas_ready{deployment="web"}'
echo "== postgres exporter";      q 'pg_up'; q 'pg_settings_max_connections'
echo "== kubectl top pods";       kubectl top pods -n vaccines
