#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=lab3
IMAGE=vaccine-lots-web:3.0
NS=vaccines

[ -f .env ] || { echo "create .env from .env.example" >&2; exit 1; }
set -a; . ./.env; set +a
: "${POSTGRES_PASSWORD:?set POSTGRES_PASSWORD in .env}"

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --config k8s/kind-config.yaml
fi
kubectl config use-context "kind-$CLUSTER"
kubectl label node "$CLUSTER-control-plane" role=infra --overwrite

docker build -f .docker/Dockerfile -t "$IMAGE" .
kind load docker-image "$IMAGE" --name "$CLUSTER"

kubectl apply -f k8s/namespace.yaml
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NS" create secret generic db-credentials \
  --from-literal=password="$POSTGRES_PASSWORD" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create configmap db-schema --from-file=schema.sql=db/schema.sql \
  --dry-run=client -o yaml | kubectl apply -f -

helm upgrade --install metrics-server metrics-server/metrics-server --version 3.14.0 \
  -n kube-system -f k8s/monitoring/metrics-server.yaml --wait
helm upgrade --install kps prometheus-community/kube-prometheus-stack --version 92.21.1 \
  -n monitoring -f k8s/monitoring/kube-prometheus-stack.yaml --wait --timeout 10m

kubectl apply -f k8s/db/postgres.yaml
kubectl -n "$NS" rollout status statefulset/postgres --timeout=5m
kubectl -n "$NS" delete job migrate-schema --ignore-not-found
kubectl apply -f k8s/db/migrate-job.yaml
kubectl -n "$NS" wait --for=condition=complete job/migrate-schema --timeout=5m

helm upgrade --install pg-exporter prometheus-community/prometheus-postgres-exporter --version 8.2.3 \
  -n "$NS" -f k8s/monitoring/postgres-exporter.yaml --wait

kubectl apply -f k8s/web/web.yaml
kubectl apply -f k8s/monitoring/web-podmonitor.yaml
kubectl -n "$NS" rollout status deployment/web --timeout=5m

echo "web:        http://localhost:8080/healthz"
echo "prometheus: http://localhost:9090"
