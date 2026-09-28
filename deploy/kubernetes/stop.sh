#!/usr/bin/env bash

# Stop application roles while preserving databases, telemetry, and PVC data.

set -euo pipefail

readonly NAMESPACE=trpc-agent-service

if [[ $# -ne 0 ]]; then
    echo "usage: $0" >&2
    exit 2
fi
if ! command -v kubectl >/dev/null 2>&1; then
    echo "error: kubectl is required" >&2
    exit 127
fi
if [[ "$(kubectl config current-context)" != "docker-desktop" ]]; then
    echo "error: current Kubernetes context must be docker-desktop" >&2
    exit 1
fi

# Stop the reconciler first so it cannot restore Worker replicas while the
# remaining application roles are intentionally scaled to zero.
kubectl scale deployment/worker-scaler --namespace "$NAMESPACE" --replicas=0
kubectl rollout status deployment/worker-scaler -n "$NAMESPACE" --timeout=120s
kubectl scale deployment/gateway deployment/agent-worker deployment/channel-runtime \
    --namespace "$NAMESPACE" --replicas=0
kubectl rollout status deployment/gateway -n "$NAMESPACE" --timeout=120s
kubectl rollout status deployment/agent-worker -n "$NAMESPACE" --timeout=120s
kubectl rollout status deployment/channel-runtime -n "$NAMESPACE" --timeout=120s

echo "Application roles stopped; stateful services and PVC data are preserved"
