#!/usr/bin/env bash
set -euo pipefail
URL=${URL:-http://localhost:8080}
show() { awk 'NR==1{printf "%s | ", $0} tolower($1)=="x-pod:"{printf "X-Pod: %s | ", $2} /^\r?$/{body=1; next} body{print}' | tr -d '\r'; }
body='{"lot_number":"LOT-K8S-0001","vaccine":"Comirnaty","doses":500,"temperature_regime":"-90..-60","manufactured_on":"2026-03-01","expires_on":"2026-12-01"}'
echo "POST";   created=$(curl -si -X POST "$URL/vaccine-lots" -H 'Content-Type: application/json' -d "$body"); echo "$created" | show
id=$(echo "$created" | tail -1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
echo "GET";    curl -si "$URL/vaccine-lots/$id" | show
echo "PUT";    curl -si -X PUT "$URL/vaccine-lots/$id" -H 'Content-Type: application/json' -d "${body/500/1000}" | show
echo "GET";    curl -si "$URL/vaccine-lots/$id" | show
echo "DELETE"; curl -si -X DELETE "$URL/vaccine-lots/$id" | show; echo
echo "GET";    curl -si "$URL/vaccine-lots/$id" | show
