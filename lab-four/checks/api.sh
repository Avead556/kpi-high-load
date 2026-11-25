#!/usr/bin/env bash
# step 1: task endpoints, the schema of the queue tables and the task gauges of the web service
set -euo pipefail
URL=${URL:-http://localhost:8084}
NS="-n vaccines"
req() { printf '%-34s ' "$1 $2"; curl -s -w ' -> %{http_code}\n' -X "$1" "$URL$2"; }
echo "== schema"
kubectl $NS exec postgres-0 -- psql -XU vaccines -d vaccines -c '\d tasks' -c '\d task_results'
echo "== endpoints"
req POST /vaccine-lots/1/tasks
req POST /vaccine-lots/999999/tasks
sleep 1
req GET /tasks/1
req GET /tasks/999999
req GET "/tasks?status=done&limit=2&offset=0"
req GET "/tasks?status=bogus"
echo "== cancel (D6): queued task with no workers, then a finished one"
kubectl $NS scale deployment/worker --replicas=0 >/dev/null
kubectl $NS wait --for=delete pod -l app=worker --timeout=60s >/dev/null 2>&1 || true
id=$(curl -s -X POST "$URL/vaccine-lots/2/tasks" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
req DELETE "/tasks/$id"
req DELETE "/tasks/$id"
req DELETE /tasks/1
req DELETE /tasks/999999
kubectl $NS scale deployment/worker --replicas=1 >/dev/null
kubectl $NS rollout status deployment/worker --timeout=120s >/dev/null
sleep 2
req GET "/tasks/$id"
echo "== web /metrics task gauges"
sleep 6
curl -s "$URL/metrics" | grep -E '^tasks_(queued|running|by_status)'
