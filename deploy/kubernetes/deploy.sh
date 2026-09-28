#!/usr/bin/env bash

# Build and deploy the complete platform to Docker Desktop Kubernetes.

set -euo pipefail

readonly DEPLOY_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd -- "$DEPLOY_DIR/../.." && pwd)"
readonly NAMESPACE=trpc-agent-service
readonly IMAGE_TAG="${TRPC_K8S_IMAGE_TAG:-0.1.0-$(date -u +%Y%m%d%H%M%S)}"
readonly REUSE_IMAGE="${TRPC_K8S_REUSE_IMAGE:-}"
readonly IMAGE="${REUSE_IMAGE:-trpc-agent-service:${IMAGE_TAG}}"
readonly IMPORT_FLAG="${1:-}"

prune_unused_project_images() {
    local candidate_image
    local running_images

    # Immutable tags make every rollout deterministic, but old local tags must
    # not accumulate forever. Limit cleanup to this repository and preserve
    # every image still referenced by a normal or init container in the Namespace.
    running_images="$(kubectl get pods -n "$NAMESPACE" -o jsonpath='{range .items[*]}{range .spec.initContainers[*]}{.image}{"\n"}{end}{range .spec.containers[*]}{.image}{"\n"}{end}{end}')"
    while IFS= read -r candidate_image; do
        [[ -n "$candidate_image" ]] || continue
        if grep -Fxq "$candidate_image" <<<"$running_images"; then
            continue
        fi
        if ! docker image rm "$candidate_image"; then
            echo "warning: could not remove unused project image $candidate_image" >&2
        fi
    done < <(docker image ls --filter 'reference=trpc-agent-service:*' --format '{{.Repository}}:{{.Tag}}')
}

sync_database_password() {
    # PostgreSQL consumes POSTGRES_PASSWORD_FILE only while initializing a new
    # data directory. Read the authoritative local file instead of the
    # asynchronously updated Secret volume. psql's password command hashes the
    # value client-side, keeping plaintext out of SQL and PostgreSQL logs.
    local password
    password="$(<"$PROJECT_ROOT/.secrets/postgres_password")"
    printf '%s\n%s\n' "$password" "$password" \
        | kubectl exec -i -n "$NAMESPACE" postgres-0 -- \
            psql --username trpc --dbname trpc_agent --set ON_ERROR_STOP=1 \
            --command '\password trpc' >/dev/null
    unset password
}

sync_grafana_password() {
    # Grafana also persists the initial admin password in its database. Pass the
    # rotated value through stdin so it never appears in arguments or manifests.
    kubectl exec -i -n "$NAMESPACE" deployment/grafana -- \
        grafana cli --homepath /usr/share/grafana \
        --config /etc/grafana/grafana.ini \
        admin reset-admin-password --password-from-stdin \
        <"$PROJECT_ROOT/.secrets/grafana_admin_password" >/dev/null
}

if [[ $# -gt 1 || ($# -eq 1 && "$IMPORT_FLAG" != "--import-compose-data") ]]; then
    echo "usage: $0 [--import-compose-data]" >&2
    exit 2
fi
if [[ -z "$REUSE_IMAGE" && ! "$IMAGE_TAG" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
    echo "error: TRPC_K8S_IMAGE_TAG is not a valid container image tag" >&2
    exit 2
fi

for command_name in docker kubectl sed date grep tail openssl; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "error: $command_name is required" >&2
        exit 127
    fi
done

if [[ "$(kubectl config current-context)" != "docker-desktop" ]]; then
    echo "error: current Kubernetes context must be docker-desktop" >&2
    exit 1
fi
# Fail before building an image or changing Secrets when Docker Desktop's
# Kubernetes API is disabled or still starting.
if ! kubectl --request-timeout=5s get --raw=/readyz >/dev/null 2>&1; then
    echo "error: docker-desktop Kubernetes API is not ready; enable Kubernetes and retry" >&2
    exit 1
fi
if [[ ! -f "$PROJECT_ROOT/.env" ]]; then
    echo "error: .env is required" >&2
    exit 1
fi

# The tenant SecretStore key belongs to the platform deployment, not to an IM
# tenant. Generate it once when Kubernetes is the first startup mode and keep
# the ignored local file stable across rollouts so existing ciphertext remains
# decryptable.
umask 077
mkdir -p -- "$PROJECT_ROOT/.secrets"
if [[ ! -s "$PROJECT_ROOT/.secrets/tenant_secret_master_key" ]]; then
    openssl rand -hex 32 >"$PROJECT_ROOT/.secrets/tenant_secret_master_key"
fi

for secret_file in postgres_password admin_bootstrap_token tenant_secret_master_key grafana_admin_password; do
    if [[ ! -s "$PROJECT_ROOT/.secrets/$secret_file" ]]; then
        echo "error: missing non-empty .secrets/$secret_file" >&2
        exit 1
    fi
done

dashscope_api_key="$(sed -n 's/^DASHSCOPE_API_KEY=//p' "$PROJECT_ROOT/.env" | tail -n 1)"
dashscope_api_key="${dashscope_api_key%$'\r'}"
if [[ -z "$dashscope_api_key" ]]; then
    echo "error: DASHSCOPE_API_KEY is empty in .env" >&2
    exit 1
fi

if [[ -n "$REUSE_IMAGE" ]]; then
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        echo "error: TRPC_K8S_REUSE_IMAGE does not exist locally: $IMAGE" >&2
        exit 1
    fi
    echo "Reusing existing image $IMAGE"
else
    echo "Building $IMAGE"
    docker build --tag "$IMAGE" "$PROJECT_ROOT"
fi

kubectl apply -f "$DEPLOY_DIR/namespace.yaml"

# Secrets are generated from ignored local files and are never written to a
# manifest or printed. Direct environment variables override the non-secret
# Kubernetes ConfigMap at runtime.
kubectl create secret generic trpc-platform-files \
    --namespace "$NAMESPACE" \
    --from-file=postgres_password="$PROJECT_ROOT/.secrets/postgres_password" \
    --from-file=admin_bootstrap_token="$PROJECT_ROOT/.secrets/admin_bootstrap_token" \
    --from-file=tenant_secret_master_key="$PROJECT_ROOT/.secrets/tenant_secret_master_key" \
    --from-file=grafana_admin_password="$PROJECT_ROOT/.secrets/grafana_admin_password" \
    --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic trpc-runtime-secrets \
    --namespace "$NAMESPACE" \
    --from-literal=DASHSCOPE_API_KEY="$dashscope_api_key" \
    --dry-run=client -o yaml | kubectl apply -f -
unset dashscope_api_key

kubectl create configmap trpc-application-config \
    --namespace "$NAMESPACE" \
    --from-env-file="$DEPLOY_DIR/app.env" \
    --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trpc-otel-config \
    --namespace "$NAMESPACE" \
    --from-file=otel-collector.yaml="$PROJECT_ROOT/trpc_service/config/observability/otel-collector.yaml" \
    --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trpc-tempo-config \
    --namespace "$NAMESPACE" \
    --from-file=tempo.yaml="$PROJECT_ROOT/trpc_service/config/observability/tempo.yaml" \
    --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trpc-loki-config \
    --namespace "$NAMESPACE" \
    --from-file=loki.yaml="$PROJECT_ROOT/trpc_service/config/observability/loki.yaml" \
    --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trpc-prometheus-config \
    --namespace "$NAMESPACE" \
    --from-file=prometheus.yaml="$PROJECT_ROOT/trpc_service/config/observability/prometheus.yaml" \
    --from-file=rules.yaml="$PROJECT_ROOT/trpc_service/config/observability/prometheus-rules.yaml" \
    --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trpc-grafana-datasources \
    --namespace "$NAMESPACE" \
    --from-file="$PROJECT_ROOT/trpc_service/config/observability/grafana/provisioning/datasources/prometheus.yaml" \
    --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trpc-grafana-dashboard-provisioning \
    --namespace "$NAMESPACE" \
    --from-file="$PROJECT_ROOT/trpc_service/config/observability/grafana/provisioning/dashboards/platform.yaml" \
    --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trpc-grafana-dashboards \
    --namespace "$NAMESPACE" \
    --from-file="$PROJECT_ROOT/trpc_service/config/observability/grafana/dashboards/agent-platform.json" \
    --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trpc-alloy-config \
    --namespace "$NAMESPACE" \
    --from-file=alloy.config="$DEPLOY_DIR/alloy.config" \
    --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f "$DEPLOY_DIR/storage.yaml"
kubectl apply -f "$DEPLOY_DIR/observability.yaml"
# Projected ConfigMap files change without restarting their consumers. Restart
# every config-backed observability role so a redeploy cannot retain old rules,
# dashboards, pipelines, or storage configuration in memory.
kubectl rollout restart deployment/tempo deployment/loki \
    deployment/otel-collector deployment/prometheus \
    deployment/grafana deployment/alloy \
    --namespace "$NAMESPACE"
kubectl rollout status statefulset/postgres -n "$NAMESPACE" --timeout=240s
kubectl rollout status statefulset/redis -n "$NAMESPACE" --timeout=180s
kubectl rollout status statefulset/seaweedfs -n "$NAMESPACE" --timeout=240s
sync_database_password
for deployment_name in tempo loki otel-collector prometheus grafana alloy; do
    # A deployment is not usable merely because its manifest was accepted;
    # readiness here keeps the final success message truthful on first boot.
    kubectl rollout status "deployment/$deployment_name" \
        -n "$NAMESPACE" --timeout=240s
done
sync_grafana_password

if [[ "$IMPORT_FLAG" == "--import-compose-data" ]]; then
    if ! docker compose -f "$PROJECT_ROOT/compose.yaml" ps --status running postgres \
        --format '{{.Service}}' | grep -qx postgres; then
        echo "error: Compose postgres must be running for --import-compose-data" >&2
        exit 1
    fi
    echo "Importing existing Compose PostgreSQL data"
    docker compose -f "$PROJECT_ROOT/compose.yaml" exec --no-TTY postgres \
        pg_dump --username trpc --dbname trpc_agent --clean --if-exists \
        --no-owner --no-privileges | kubectl exec -i -n "$NAMESPACE" postgres-0 -- \
        psql --username trpc --dbname trpc_agent --set ON_ERROR_STOP=1
fi

# A one-shot Job owns schema migration; application replicas never race Alembic.
kubectl delete job database-migration -n "$NAMESPACE" --ignore-not-found
sed "s|trpc-agent-service:0.1.0|$IMAGE|g" "$DEPLOY_DIR/migration.yaml" | kubectl apply -f -
if ! kubectl wait --for=condition=complete job/database-migration \
    -n "$NAMESPACE" --timeout=240s; then
    kubectl logs -n "$NAMESPACE" job/database-migration >&2 || true
    exit 1
fi

sed "s|trpc-agent-service:0.1.0|$IMAGE|g" "$DEPLOY_DIR/application.yaml" | kubectl apply -f -
# A new immutable image tag already changes each Pod template and reloads the
# synchronized ConfigMaps and Secrets. Restart only when reusing an unchanged
# image, whose process-scoped Settings would otherwise retain old values.
if [[ -n "$REUSE_IMAGE" ]]; then
    kubectl rollout restart deployment/gateway deployment/agent-worker \
        deployment/channel-runtime deployment/worker-scaler \
        --namespace "$NAMESPACE"
fi
kubectl rollout status deployment/agent-worker -n "$NAMESPACE" --timeout=300s
kubectl rollout status deployment/gateway -n "$NAMESPACE" --timeout=300s
kubectl rollout status deployment/channel-runtime -n "$NAMESPACE" --timeout=300s
kubectl rollout status deployment/worker-scaler -n "$NAMESPACE" --timeout=180s
prune_unused_project_images

echo "Kubernetes deployment is ready"
echo "Admin/API: http://localhost:8000"
echo "Grafana:   http://localhost:3000"
echo "Prometheus:http://localhost:9090"
