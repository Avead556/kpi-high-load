#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=$1 LOW=$2 JUMP=$3 OUT=${4:-results/$1}
PY=.venv/bin/python
NS="-n vaccines"
if [ "$MODE" = fixed ]; then
  kubectl $NS delete hpa web --ignore-not-found
  kubectl $NS scale deployment/web --replicas=3
  kubectl $NS rollout status deployment/web --timeout=3m
  sleep 30
else
  kubectl $NS scale deployment/web --replicas=1
  kubectl apply -f k8s/web/hpa.yaml
  stable=0
  while [ $stable -lt 60 ]; do
    cur=$(kubectl $NS get hpa web -o jsonpath='{.status.currentReplicas}')
    ready=$(kubectl $NS get deploy web -o jsonpath='{.status.readyReplicas}')
    if [ "$cur" = 1 ] && [ "$ready" = 1 ]; then stable=$((stable + 5)); else stable=0; fi
    sleep 5
  done
  kubectl $NS get hpa web
fi
$PY loadgen/load.py --profile "30:$LOW,60:$JUMP,90:$LOW" --out "$OUT" | tee "$OUT.log"
sleep 35
$PY loadgen/prom_export.py --summary "$OUT/summary.json" --out "$OUT/prometheus.json"
kubectl $NS get hpa 2>/dev/null | tee -a "$OUT.log" || true
