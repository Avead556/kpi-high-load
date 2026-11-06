#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
kubectl -n vaccines delete hpa web --ignore-not-found
for N in ${@:-1 2 3}; do
  kubectl -n vaccines scale deployment/web --replicas="$N"
  kubectl -n vaccines rollout status deployment/web --timeout=3m
  until [ "$(kubectl -n vaccines get pods -l app=web --no-headers | wc -l | tr -d ' ')" = "$N" ]; do sleep 2; done
  kubectl -n vaccines get pods -l app=web -o wide --no-headers | awk '{print $1, $7}'
  sleep 30
  $PY loadgen/load.py --out "results/n$N" | tee "results/n$N.log"
  sleep 20
  $PY loadgen/prom_export.py --summary "results/n$N/summary.json" --out "results/n$N/prometheus.json"
  kubectl -n vaccines get pods -l app=web -o wide --no-headers | awk '{print $1, $7}' > "results/n$N/pods_nodes.txt"
done
