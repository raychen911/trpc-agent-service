#!/usr/bin/env sh
set -eu

for command_name in docker kind kubectl; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        printf '%s\n' "$command_name is required" >&2
        exit 1
    fi
done

docker info >/dev/null
docker build -t trpc-agent-service:local .

if ! kind get clusters | grep -qx trpc-agent-service; then
    kind create cluster --name trpc-agent-service
fi
kind load docker-image trpc-agent-service:local --name trpc-agent-service

kubectl apply -f deploy/k8s/base/namespace.yaml
kubectl -n trpc-agent create secret generic trpc-agent-secrets \
    --from-literal=POSTGRES_PASSWORD=local-password \
    --from-literal=TRPC_SERVICE_DATABASE_URL=postgresql+asyncpg://trpc_agent:local-password@postgres:5432/trpc_agent \
    --from-literal=TRPC_QUEUE_REDIS_URL=redis://redis:6379/0 \
    --from-literal=TRPC_REDIS_URL=redis://redis:6379/1 \
    --from-literal=TRPC_AGENT_API_KEY=local-placeholder \
    --from-literal=TRPC_SERVICE_ADMIN_API_KEY=local-admin-key \
    --from-literal=TRPC_SERVICE_SESSION_HMAC_KEY=local-session-key-at-least-16 \
    --from-literal=TRPC_DEMO_TENANT_API_KEY=local-tenant-key \
    --from-literal=TRPC_TELEGRAM_BOT_TOKEN=local-placeholder \
    --from-literal=TRPC_WECOM_BOT_SECRET=local-placeholder \
    --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -k deploy/k8s/base
kubectl -n trpc-agent rollout status statefulset/postgres --timeout=180s
kubectl -n trpc-agent rollout status deployment/redis --timeout=180s
kubectl -n trpc-agent rollout status deployment/agent-gateway --timeout=180s
kubectl -n trpc-agent rollout status deployment/agent-worker --timeout=180s
