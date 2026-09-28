#!/usr/bin/env bash

# Explicitly remove the local namespace, including all PVC-backed test data.

set -euo pipefail

if [[ $# -ne 1 || "$1" != "--confirm-delete-data" ]]; then
    echo "usage: $0 --confirm-delete-data" >&2
    echo "warning: this removes the namespace and all Kubernetes PVC data" >&2
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

kubectl delete namespace trpc-agent-service --wait=true
echo "Removed trpc-agent-service namespace and its Kubernetes-local data"
