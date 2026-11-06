#!/usr/bin/env bash
set -euo pipefail
URL=${URL:-http://localhost:8080}
body='{"lot_number":"LOT-PERSIST-0001","vaccine":"BCG","doses":200,"temperature_regime":"+2..+8","manufactured_on":"2026-02-01","expires_on":"2027-02-01"}'
id=$(curl -s -X POST "$URL/vaccine-lots" -H 'Content-Type: application/json' -d "$body" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
echo "created id=$id"
kubectl -n vaccines get pod postgres-0 -o jsonpath='before: uid={.metadata.uid} started={.status.startTime}{"\n"}'
kubectl -n vaccines delete pod postgres-0
kubectl -n vaccines wait --for=condition=ready pod/postgres-0 --timeout=120s
kubectl -n vaccines get pod postgres-0 -o jsonpath='after:  uid={.metadata.uid} started={.status.startTime}{"\n"}'
kubectl -n vaccines get pvc data-postgres-0
until curl -sf "$URL/healthz" >/dev/null; do sleep 1; done
echo "GET after restart:"; curl -s "$URL/vaccine-lots/$id"; echo
curl -s -o /dev/null -w "cleanup DELETE -> %{http_code}\n" -X DELETE "$URL/vaccine-lots/$id"
