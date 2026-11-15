#!/usr/bin/env bash
# Run Bronze, then Silver, as Kubernetes Jobs on the kind cluster.
set -euo pipefail
cd "$(dirname "$0")/.."

kubectl apply -f k8s/pipeline.yaml
kubectl -n pipeline delete job bronze-ingest silver-build --ignore-not-found

kubectl apply -f k8s/bronze-job.yaml
echo "Waiting for Bronze (up to 20 minutes)..."
kubectl -n pipeline wait --for=condition=complete job/bronze-ingest --timeout=20m

kubectl apply -f k8s/silver-job.yaml
echo "Waiting for Silver (up to 20 minutes)..."
kubectl -n pipeline wait --for=condition=complete job/silver-build --timeout=20m

kubectl -n pipeline get jobs