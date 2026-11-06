#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PW=$(grep '^POSTGRES_PASSWORD=' .env | cut -d= -f2-)
printf "image /srv,/etc: "; docker run --rm --entrypoint sh vaccine-lots-web:3.0 -c "grep -rl -- '$PW' /srv /etc 2>/dev/null | wc -l"
printf "configmaps: "; kubectl get cm -A -o yaml | grep -c -- "$PW" || true
printf "project files except .env: "; { grep -rl --exclude=.env --exclude-dir=.venv --exclude-dir=results -- "$PW" . || true; } | wc -l | tr -d ' '
printf "secret db-credentials keys: "; kubectl -n vaccines get secret db-credentials -o jsonpath='{.data}' | python3 -c 'import json,sys; print(list(json.load(sys.stdin)))'
printf "web env DB_PASSWORD source: "; kubectl -n vaccines get deploy web -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="DB_PASSWORD")].valueFrom}'; echo
