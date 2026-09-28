#!/usr/bin/env bash

# Build the service, start Docker infrastructure, migrate, and launch the local API.

set -euo pipefail

readonly PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SECRET_DIR="$PROJECT_ROOT/.secrets"
readonly RUN_DIR="$PROJECT_ROOT/.run"
readonly DATA_DIR="$PROJECT_ROOT/data"
readonly API_PID_FILE="$RUN_DIR/trpc-agent-service.pid"
readonly WORKER_PID_DIR="$RUN_DIR/workers"
readonly SUPERVISOR_PID_FILE="$RUN_DIR/worker-supervisor.pid"
readonly CHANNEL_PID_FILE="$RUN_DIR/channel-runtime.pid"
readonly LOG_FILE="$RUN_DIR/trpc-agent-service.log"
readonly SUPERVISOR_LOG_FILE="$RUN_DIR/worker-supervisor.log"
readonly CHANNEL_LOG_FILE="$RUN_DIR/channel-runtime.log"
cd -- "$PROJECT_ROOT"

for command_name in curl docker uv openssl setsid; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "error: $command_name is required" >&2
        exit 127
    fi
done

umask 077
mkdir -p -- "$SECRET_DIR"
mkdir -p -- "$RUN_DIR"
mkdir -p -- "$WORKER_PID_DIR"
# Core storage uses explicit bind directories required by the project layout.
# Monitoring data remains in Docker-managed volumes because it is operational
# telemetry rather than application source data.
mkdir -p -- "$DATA_DIR/postgresql" "$DATA_DIR/redis" "$DATA_DIR/seaweedfs" \
    "$DATA_DIR/workspaces"
# These three directories are bind-mounted into containers that run with
# image-specific UIDs. A fresh clone combined with ``umask 077`` would leave
# them at 0700 and make PostgreSQL 18 fail its first initialization. They hold
# local test data only, so use portable writable permissions across Linux and
# Docker Desktop; the tenant Workspace remains private to the host process.
for container_data_dir in "$DATA_DIR/postgresql" "$DATA_DIR/redis" "$DATA_DIR/seaweedfs"; do
    # After first startup the image may own this directory. In that case its
    # permissions are already managed by the service and the host must not fail
    # a later restart merely because it cannot chmod a container-owned path.
    if [[ -O "$container_data_dir" ]]; then
        chmod 0777 "$container_data_dir"
    fi
done
chmod 0700 "$DATA_DIR/workspaces"

generate_secret() {
    local target="$1"
    local bytes="$2"
    if [[ ! -s "$target" ]]; then
        openssl rand -hex "$bytes" >"$target"
    fi
}

# Generate credentials once so restarts keep access to the existing data volume.
generate_secret "$SECRET_DIR/postgres_password" 24
generate_secret "$SECRET_DIR/admin_bootstrap_token" 32
generate_secret "$SECRET_DIR/tenant_secret_master_key" 32
generate_secret "$SECRET_DIR/grafana_admin_password" 24
# Local Compose implements file-backed secrets as bind mounts. The directory
# remains 0700 on the host, while the file must be readable by Grafana UID 472
# inside its isolated container.
chmod 0444 "$SECRET_DIR/grafana_admin_password"

if [[ -f "$API_PID_FILE" ]] && kill -0 "$(<"$API_PID_FILE")" 2>/dev/null; then
    echo "error: trpc-agent-service is already running" >&2
    exit 1
fi
if [[ -f "$CHANNEL_PID_FILE" ]] && kill -0 "$(<"$CHANNEL_PID_FILE")" 2>/dev/null; then
    echo "error: Channel Runtime is already running" >&2
    exit 1
fi
if [[ -f "$SUPERVISOR_PID_FILE" ]] && kill -0 "$(<"$SUPERVISOR_PID_FILE")" 2>/dev/null; then
    echo "error: Worker Supervisor is already running" >&2
    exit 1
fi

shopt -s nullglob
for worker_pid_file in "$WORKER_PID_DIR"/*.pid; do
    if kill -0 "$(<"$worker_pid_file")" 2>/dev/null; then
        echo "error: an Agent Worker is already running: $(<"$worker_pid_file")" >&2
        exit 1
    fi
    rm -f -- "$worker_pid_file"
done
shopt -u nullglob

readonly API_PORT="$(uv run python -c 'from trpc_service.config import Settings; print(Settings().port)')"
if curl --noproxy '*' --fail --silent --max-time 1 \
    "http://127.0.0.1:${API_PORT}/health" >/dev/null 2>&1; then
    echo "error: API port $API_PORT is already served by a process not owned by the PID file" >&2
    exit 1
fi

"$PROJECT_ROOT/build.sh"
if ! docker compose up --detach --wait --remove-orphans; then
    echo "error: Docker infrastructure did not become healthy" >&2
    # Compose's summary often reports only ``postgres is unhealthy``. Include
    # bounded diagnostics so a fresh-clone failure is actionable during a demo.
    docker compose ps --all >&2 || true
    docker compose logs --no-color --tail 80 postgres >&2 || true
    exit 1
fi

POSTGRES_ENDPOINT="$(docker compose port postgres 5432 2>/dev/null || true)"
if [[ ! "$POSTGRES_ENDPOINT" =~ ^127\.0\.0\.1:[0-9]+$ ]]; then
    # A stopped Docker container can occasionally retain HostConfig while losing
    # its active port publication. Recreating the container preserves its volume.
    docker compose up --detach --wait --force-recreate postgres
    POSTGRES_ENDPOINT="$(docker compose port postgres 5432)"
fi
readonly POSTGRES_ENDPOINT
readonly POSTGRES_PORT="${POSTGRES_ENDPOINT##*:}"

sync_database_password() {
    local service_name="$1"
    local database_name="$2"
    # PostgreSQL reads POSTGRES_PASSWORD_FILE only while initializing a new
    # volume. Synchronize an existing development volume after Secret rotation.
    docker compose exec --no-TTY "$service_name" sh -ceu '
        password="$(cat /run/secrets/postgres_password)"
        printf "ALTER ROLE trpc PASSWORD '\''%s'\'';\n" "$password" \
            | psql --username trpc --dbname "$1" --set ON_ERROR_STOP=1 >/dev/null
    ' sh "$database_name"
}

sync_database_password postgres trpc_agent

sync_grafana_password() {
    # Grafana only applies GF_SECURITY_ADMIN_PASSWORD during first-time database
    # initialization. Keep an existing volume synchronized with the password
    # file so the credential printed by this script is always authoritative.
    docker compose exec --no-TTY grafana sh -ceu '
        grafana cli --homepath /usr/share/grafana \
            --config /etc/grafana/grafana.ini \
            admin reset-admin-password --password-from-stdin \
            </run/secrets/grafana_admin_password >/dev/null
    '
}

sync_grafana_password
# The API runs on the host, so replace Compose's internal database hostname with
# the dynamically published loopback port while retaining file-based credentials.
export TRPC_SERVICE_DATABASE_URL="postgresql+asyncpg://trpc@127.0.0.1:${POSTGRES_PORT}/trpc_agent"
export TRPC_SERVICE_DATABASE_PASSWORD_FILE="$SECRET_DIR/postgres_password"
export TRPC_SERVICE_ADMIN_BOOTSTRAP_TOKEN_FILE="$SECRET_DIR/admin_bootstrap_token"
export TRPC_SERVICE_TENANT_SECRET_MASTER_KEY_FILE="$SECRET_DIR/tenant_secret_master_key"
export TRPC_SERVICE_ENVIRONMENT=development
# Storage Router configuration comes from .env or the caller environment. Do
# not overwrite it here: tenants may route Knowledge to this PostgreSQL,
# another pgvector service, or a future external Vector Store.

uv run alembic -c "$PROJECT_ROOT/trpc_service/storage/alembic.ini" upgrade head
uv run python -m trpc_service.storage.provision
readonly LOCAL_WORKER_NODES="$(uv run python -c \
    'from trpc_service.config import Settings; print(Settings().local_worker_nodes)')"
readonly WORKER_CONCURRENCY_PER_NODE="$(uv run python -c \
    'from trpc_service.config import Settings; print(Settings().worker_concurrency_per_node)')"
if [[ ! "$LOCAL_WORKER_NODES" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: TRPC_SERVICE_LOCAL_WORKER_NODES must be a positive integer" >&2
    exit 2
fi
if [[ ! "$WORKER_CONCURRENCY_PER_NODE" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: TRPC_SERVICE_WORKER_CONCURRENCY_PER_NODE must be a positive integer" >&2
    exit 2
fi

# Each Worker is an independent host-Python process. They share only external
# storage and can therefore be moved to separate hosts or Kubernetes Pods later.
# Detach long-lived processes from the launcher's process group as well as its
# terminal; IDE/task runners may clean up their entire group when a script exits.
launched_pids=()
cleanup_launched_processes() {
    local launched_pid
    for launched_pid in "${launched_pids[@]}"; do
        if kill -0 "$launched_pid" 2>/dev/null; then
            kill "$launched_pid" 2>/dev/null || true
        fi
    done
    for worker_pid_file in "$WORKER_PID_DIR"/*.pid; do
        if [[ -f "$worker_pid_file" ]] && kill -0 "$(<"$worker_pid_file")" 2>/dev/null; then
            kill "$(<"$worker_pid_file")" 2>/dev/null || true
        fi
    done
}

# The Supervisor owns Worker child processes. The admin console changes only
# durable desired capacity; this process performs actual expansion and drain.
nohup setsid env \
    TRPC_SERVICE_RUNTIME_ROLE=supervisor \
    TRPC_SERVICE_NODE_ID=local-worker-supervisor \
    TRPC_SERVICE_WORKER_CONCURRENCY=0 \
    TRPC_SERVICE_WORKER_SCALER_MODE=local_process \
    TRPC_SERVICE_LOG_FILE="$SUPERVISOR_LOG_FILE" \
    "$PROJECT_ROOT/.venv/bin/trpc-agent-service" >>"$SUPERVISOR_LOG_FILE" 2>&1 </dev/null &
supervisor_pid="$!"
launched_pids+=("$supervisor_pid")
echo "$supervisor_pid" >"$SUPERVISOR_PID_FILE"

# One Channel Runtime process owns all configured provider long connections.
# Agent execution remains on the independent Worker processes above.
nohup setsid env \
    TRPC_SERVICE_RUNTIME_ROLE=channel \
    TRPC_SERVICE_NODE_ID=local-channel-runtime \
    TRPC_SERVICE_WORKER_CONCURRENCY=0 \
    TRPC_SERVICE_LOG_FILE="$CHANNEL_LOG_FILE" \
    "$PROJECT_ROOT/.venv/bin/trpc-agent-service" >>"$CHANNEL_LOG_FILE" 2>&1 </dev/null &
channel_pid="$!"
launched_pids+=("$channel_pid")
echo "$channel_pid" >"$CHANNEL_PID_FILE"

# The Gateway owns HTTP only. It never relies on process-local Agent execution.
nohup setsid env \
    TRPC_SERVICE_RUNTIME_ROLE=api \
    TRPC_SERVICE_NODE_ID=local-gateway \
    TRPC_SERVICE_WORKER_CONCURRENCY=0 \
    TRPC_SERVICE_LOG_FILE="$LOG_FILE" \
    "$PROJECT_ROOT/.venv/bin/trpc-agent-service" >>"$LOG_FILE" 2>&1 </dev/null &
api_pid="$!"
launched_pids+=("$api_pid")
echo "$api_pid" >"$API_PID_FILE"

for _ in {1..30}; do
    if ! kill -0 "$api_pid" 2>/dev/null; then
        echo "error: Gateway exited during startup; see $LOG_FILE" >&2
        cleanup_launched_processes
        exit 1
    fi
    if ! kill -0 "$supervisor_pid" 2>/dev/null; then
        echo "error: Worker Supervisor exited; see $SUPERVISOR_LOG_FILE" >&2
        cleanup_launched_processes
        exit 1
    fi
    if ! kill -0 "$channel_pid" 2>/dev/null; then
        echo "error: Channel Runtime exited; see $CHANNEL_LOG_FILE" >&2
        cleanup_launched_processes
        exit 1
    fi
    if curl --noproxy '*' --fail --silent --max-time 2 \
        "http://127.0.0.1:${API_PORT}/ready" >/dev/null; then
        echo "trpc-agent-service is ready at http://127.0.0.1:${API_PORT}"
        echo "Agent Worker nodes: initial target $LOCAL_WORKER_NODES; managed from the admin console"
        echo "Channel Runtime: running (WeCom/Feishu bindings may be configured later)"
        echo "admin console: http://127.0.0.1:${API_PORT}/admin"
        echo "bootstrap token file: $SECRET_DIR/admin_bootstrap_token"
        echo "Grafana: http://127.0.0.1:${TRPC_GRAFANA_PORT:-3000}"
        echo "Grafana admin password file: $SECRET_DIR/grafana_admin_password"
        echo "Prometheus: http://127.0.0.1:${TRPC_PROMETHEUS_PORT:-9090}"
        echo "Tempo: http://127.0.0.1:${TRPC_TEMPO_PORT:-3200}"
        echo "Loki: http://127.0.0.1:${TRPC_LOKI_PORT:-3100}"
        exit 0
    fi
    sleep 1
done

echo "error: service did not become ready; see $LOG_FILE" >&2
cleanup_launched_processes
exit 1
