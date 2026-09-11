#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
service_python="${TRPC_SERVICE_PYTHON:-${project_dir}/.venv/bin/python}"

if [[ ! -x "${service_python}" ]]; then
    service_python="$(command -v python3)"
fi

# Source shared PID identity verification
# shellcheck source=scripts/pid_identity.sh
source "${project_dir}/scripts/pid_identity.sh"

if [[ -z "${TRPC_INTERNAL_TOKEN:-}" ]]; then
    echo "TRPC_INTERNAL_TOKEN is not set" >&2
    exit 1
fi

if [[ -z "${TRPC_ADMIN_TOKEN:-}" ]]; then
    echo "TRPC_ADMIN_TOKEN is not set" >&2
    exit 1
fi

worker_a_host="${TRPC_WORKER_A_HOST:-127.0.0.1}"
worker_a_port="${TRPC_WORKER_A_PORT:-8001}"
worker_a_pid_file="${TRPC_WORKER_A_PID_FILE:-${project_dir}/data/worker-a.pid}"
worker_a_log_file="${TRPC_WORKER_A_LOG_FILE:-${project_dir}/data/worker-a.log}"

worker_b_host="${TRPC_WORKER_B_HOST:-127.0.0.1}"
worker_b_port="${TRPC_WORKER_B_PORT:-8002}"
worker_b_pid_file="${TRPC_WORKER_B_PID_FILE:-${project_dir}/data/worker-b.pid}"
worker_b_log_file="${TRPC_WORKER_B_LOG_FILE:-${project_dir}/data/worker-b.log}"

gateway_host="${TRPC_GATEWAY_HOST:-127.0.0.1}"
gateway_port="${TRPC_GATEWAY_PORT:-8000}"
gateway_pid_file="${TRPC_GATEWAY_PID_FILE:-${project_dir}/data/gateway.pid}"
gateway_log_file="${TRPC_GATEWAY_LOG_FILE:-${project_dir}/data/gateway.log}"

admin_host="${TRPC_ADMIN_HOST:-127.0.0.1}"
admin_port="${TRPC_ADMIN_PORT:-8003}"
admin_pid_file="${TRPC_ADMIN_PID_FILE:-${project_dir}/data/admin.pid}"
admin_log_file="${TRPC_ADMIN_LOG_FILE:-${project_dir}/data/admin.log}"

redis_container_name="${TRPC_REDIS_CONTAINER_NAME:-trpc-dev-redis}"
redis_port="${TRPC_REDIS_PORT:-6379}"
redis_marker_file="${TRPC_REDIS_MARKER_FILE:-${project_dir}/data/redis.owned}"
started_redis_container=false

postgres_container_name="${TRPC_POSTGRES_CONTAINER_NAME:-trpc-dev-postgres}"
postgres_port="${TRPC_POSTGRES_PORT:-5432}"
postgres_user="${TRPC_POSTGRES_USER:-trpc}"
postgres_password="${TRPC_POSTGRES_PASSWORD:-trpc}"
postgres_db="${TRPC_POSTGRES_DB:-trpc}"
postgres_marker_file="${TRPC_POSTGRES_MARKER_FILE:-${project_dir}/data/postgres.owned}"
started_postgres_container=false

minio_container_name="${TRPC_MINIO_CONTAINER_NAME:-trpc-dev-minio}"
minio_port="${TRPC_MINIO_PORT:-9000}"
minio_marker_file="${TRPC_MINIO_MARKER_FILE:-${project_dir}/data/minio.owned}"
started_minio_container=false

mkdir -p "$(dirname "${worker_a_pid_file}")" "$(dirname "${worker_a_log_file}")"
mkdir -p "$(dirname "${worker_b_pid_file}")" "$(dirname "${worker_b_log_file}")"
mkdir -p "$(dirname "${gateway_pid_file}")" "$(dirname "${gateway_log_file}")"
mkdir -p "$(dirname "${admin_pid_file}")" "$(dirname "${admin_log_file}")"
cd "${project_dir}"

started_worker_a_pid=""
started_worker_b_pid=""
started_gateway_pid=""
started_admin_pid=""
created_worker_a_pid_file=false
created_worker_b_pid_file=false
created_gateway_pid_file=false
created_admin_pid_file=false

cleanup_on_failure() {
    if [[ -n "${started_admin_pid}" ]] && kill -0 "${started_admin_pid}" 2>/dev/null; then
        kill "${started_admin_pid}" 2>/dev/null || true
    fi
    if [[ -n "${started_gateway_pid}" ]] && kill -0 "${started_gateway_pid}" 2>/dev/null; then
        kill "${started_gateway_pid}" 2>/dev/null || true
    fi
    if [[ -n "${started_worker_b_pid}" ]] && kill -0 "${started_worker_b_pid}" 2>/dev/null; then
        kill "${started_worker_b_pid}" 2>/dev/null || true
    fi
    if [[ -n "${started_worker_a_pid}" ]] && kill -0 "${started_worker_a_pid}" 2>/dev/null; then
        kill "${started_worker_a_pid}" 2>/dev/null || true
    fi
    if [[ "${created_admin_pid_file}" == true ]]; then
        rm -f "${admin_pid_file}"
    fi
    if [[ "${created_gateway_pid_file}" == true ]]; then
        rm -f "${gateway_pid_file}"
    fi
    if [[ "${created_worker_b_pid_file}" == true ]]; then
        rm -f "${worker_b_pid_file}"
    fi
    if [[ "${created_worker_a_pid_file}" == true ]]; then
        rm -f "${worker_a_pid_file}"
    fi
    if [[ "${started_redis_container}" == true ]]; then
        docker rm -f "${redis_container_name}" 2>/dev/null || true
        rm -f "${redis_marker_file}"
    fi
    if [[ "${started_postgres_container}" == true ]]; then
        docker rm -f "${postgres_container_name}" 2>/dev/null || true
        rm -f "${postgres_marker_file}"
    fi
    if [[ "${started_minio_container}" == true ]]; then
        docker rm -f "${minio_container_name}" 2>/dev/null || true
        rm -f "${minio_marker_file}"
    fi
}

stop_existing_process() {
    local service_pid="$1"
    local label="$2"

    kill "${service_pid}" 2>/dev/null || true
    for _ in {1..50}; do
        if ! kill -0 "${service_pid}" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "${label} pid ${service_pid} did not stop after SIGTERM" >&2
    return 1
}

# Start Redis if TRPC_REDIS_URL is not set (development mode)
if [[ -z "${TRPC_REDIS_URL:-}" ]]; then
    if command -v docker >/dev/null 2>&1; then
        if docker ps -a --format '{{.Names}}' | grep -q "^${redis_container_name}$"; then
            if [[ ! -f "${redis_marker_file}" ]] \
                || [[ "$(<"${redis_marker_file}")" != "${redis_container_name}" ]]; then
                echo "Redis container exists but is not owned by start.sh; set TRPC_REDIS_URL" >&2
                exit 1
            fi
            if docker ps --format '{{.Names}}' | grep -q "^${redis_container_name}$"; then
                echo "redis container already running"
            else
                docker start "${redis_container_name}" >/dev/null
                echo "redis container started"
            fi
        else
            docker run -d --name "${redis_container_name}" -p "${redis_port}:6379" redis:7 >/dev/null
            started_redis_container=true
            mkdir -p "$(dirname "${redis_marker_file}")"
            echo "${redis_container_name}" > "${redis_marker_file}"
            echo "redis container started on port ${redis_port}"
        fi
        export TRPC_REDIS_URL="redis://127.0.0.1:${redis_port}"
    else
        echo "TRPC_REDIS_URL is not set and Docker is not available" >&2
        echo "Either set TRPC_REDIS_URL or install Docker for development" >&2
        exit 1
    fi
fi

# Artifact storage is shared across local Workers.  An explicitly supplied
# endpoint is never touched; it must carry explicit credentials.  For a
# marker-owned development MinIO, recover its container credentials on a
# later start instead of generating credentials that cannot authenticate to
# the existing container.
if [[ -z "${TRPC_S3_ENDPOINT:-}" ]]; then
    if ! command -v docker >/dev/null 2>&1; then
        echo "TRPC_S3_ENDPOINT is not set and Docker is not available" >&2
        cleanup_on_failure
        exit 1
    fi
    if docker ps -a --format '{{.Names}}' | grep -q "^${minio_container_name}$"; then
        if [[ ! -f "${minio_marker_file}" ]] \
            || [[ "$(<"${minio_marker_file}")" != "${minio_container_name}" ]]; then
            echo "MinIO container exists but is not owned by start.sh; set TRPC_S3_ENDPOINT and credentials" >&2
            cleanup_on_failure
            exit 1
        fi
        minio_environment="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "${minio_container_name}")"
        minio_access_key=""
        minio_secret_key=""
        while IFS= read -r minio_variable; do
            case "${minio_variable}" in
                MINIO_ROOT_USER=*) minio_access_key="${minio_variable#MINIO_ROOT_USER=}" ;;
                MINIO_ROOT_PASSWORD=*) minio_secret_key="${minio_variable#MINIO_ROOT_PASSWORD=}" ;;
            esac
        done <<<"${minio_environment}"
        if [[ -z "${minio_access_key}" || -z "${minio_secret_key}" ]]; then
            echo "owned MinIO container has no usable credentials" >&2
            cleanup_on_failure
            exit 1
        fi
        export TRPC_S3_ACCESS_KEY="${minio_access_key}"
        export TRPC_S3_SECRET_KEY="${minio_secret_key}"
        docker start "${minio_container_name}" >/dev/null 2>&1 || true
    else
        export TRPC_S3_ACCESS_KEY="${TRPC_S3_ACCESS_KEY:-$(${service_python} -c 'import secrets; print(secrets.token_urlsafe(18))')}"
        export TRPC_S3_SECRET_KEY="${TRPC_S3_SECRET_KEY:-$(${service_python} -c 'import secrets; print(secrets.token_urlsafe(36))')}"
        docker run -d --name "${minio_container_name}" -p "${minio_port}:9000" \
            -e "MINIO_ROOT_USER=${TRPC_S3_ACCESS_KEY}" -e "MINIO_ROOT_PASSWORD=${TRPC_S3_SECRET_KEY}" \
            minio/minio:RELEASE.2025-04-22T22-12-26Z server /data >/dev/null
        started_minio_container=true
        mkdir -p "$(dirname "${minio_marker_file}")"
        echo "${minio_container_name}" > "${minio_marker_file}"
        chmod 600 "${minio_marker_file}"
    fi
    export TRPC_S3_ENDPOINT="127.0.0.1:${minio_port}"
else
    if [[ -z "${TRPC_S3_ACCESS_KEY:-}" || -z "${TRPC_S3_SECRET_KEY:-}" ]]; then
        echo "external S3 endpoint requires TRPC_S3_ACCESS_KEY and TRPC_S3_SECRET_KEY" >&2
        cleanup_on_failure
        exit 1
    fi
fi

# Start PostgreSQL if TRPC_DATABASE_URL is not set (development mode)
if [[ -z "${TRPC_DATABASE_URL:-}" ]]; then
    if command -v docker >/dev/null 2>&1; then
        if docker ps -a --format '{{.Names}}' | grep -q "^${postgres_container_name}$"; then
            if docker ps --format '{{.Names}}' | grep -q "^${postgres_container_name}$"; then
                echo "postgres container already running"
            else
                docker start "${postgres_container_name}" >/dev/null
                echo "postgres container started"
            fi
        else
            docker run -d --name "${postgres_container_name}" \
                -p "${postgres_port}:5432" \
                -e "POSTGRES_USER=${postgres_user}" \
                -e "POSTGRES_PASSWORD=${postgres_password}" \
                -e "POSTGRES_DB=${postgres_db}" \
                postgres:16 >/dev/null
            started_postgres_container=true
            mkdir -p "$(dirname "${postgres_marker_file}")"
            echo "${postgres_container_name}" > "${postgres_marker_file}"
            echo "postgres container started on port ${postgres_port}"
        fi
        export TRPC_DATABASE_URL="postgresql+asyncpg://${postgres_user}:${postgres_password}@127.0.0.1:${postgres_port}/${postgres_db}"
    else
        echo "TRPC_DATABASE_URL is not set and Docker is not available" >&2
        echo "Either set TRPC_DATABASE_URL or install Docker for development" >&2
        exit 1
    fi
fi

# Wait for PostgreSQL to be ready (always, regardless of URL source)
postgres_ready=false
for _ in {1..60}; do
    if "${service_python}" -c "
import asyncio
from trpc_service.storage.database import DatabaseSettings, create_database_engine, check_database_readiness
async def _check():
    s = DatabaseSettings.from_env()
    e = create_database_engine(s)
    try:
        await check_database_readiness(e)
    finally:
        await e.dispose()
asyncio.run(_check())
" 2>/dev/null; then
        postgres_ready=true
        break
    fi
    sleep 0.5
done
if [[ "${postgres_ready}" != "true" ]]; then
    echo "database did not become ready" >&2
    cleanup_on_failure
    exit 1
fi

# Run migrations (always, regardless of URL source)
echo "running database migrations..."
if ! "${service_python}" -m trpc_service._cli db-migrate 2>/dev/null; then
    echo "database migration failed" >&2
    cleanup_on_failure
    exit 1
fi

# Import tenant configuration only on empty database (bootstrap mode)
tenant_config_path="${TRPC_TENANT_CONFIG_PATH:-${project_dir}/data/tenants.json}"
if [[ -f "${tenant_config_path}" ]]; then
    tenant_count=$("${service_python}" -c "
import asyncio, os
async def _check():
    from trpc_service.storage.database import DatabaseSettings, create_database_engine
    import sqlalchemy as sa
    from trpc_service.storage.schema import tenant_configs
    try:
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        async with engine.connect() as conn:
            result = await conn.execute(sa.select(sa.func.count()).select_from(tenant_configs))
            return result.scalar() or 0
    except Exception:
        return -1
print(asyncio.run(_check()))
" 2>/dev/null) || tenant_count="-1"

    if [[ "${tenant_count}" == "0" ]]; then
        echo "importing tenant configuration (bootstrap mode)..."
        if ! "${service_python}" -m trpc_service._cli tenant-config-import --path "${tenant_config_path}" 2>/dev/null; then
            echo "tenant configuration import failed" >&2
            cleanup_on_failure
            exit 1
        fi
    elif [[ "${tenant_count}" -gt 0 ]] 2>/dev/null; then
        echo "skipping tenant import (${tenant_count} tenants already exist)"
    else
        echo "tenant configuration import failed (database check error)" >&2
        cleanup_on_failure
        exit 1
    fi
fi

echo "initializing shared data backends..."
backends_ready=false
for _ in {1..60}; do
    if "${service_python}" -m trpc_service._cli backend-init 2>/dev/null; then
        backends_ready=true
        break
    fi
    sleep 0.5
done
if [[ "${backends_ready}" != "true" ]]; then
    echo "data backend initialization failed" >&2
    cleanup_on_failure
    exit 1
fi

_start_worker() {
    local label="$1"
    local host="$2"
    local port="$3"
    local pid_file="$4"
    local log_file="$5"

    if [[ -f "${pid_file}" ]]; then
        local existing_pid
        existing_pid="$(<"${pid_file}")"
        if [[ "${existing_pid}" =~ ^[0-9]+$ ]] && kill -0 "${existing_pid}" 2>/dev/null; then
            # Verify exact instance identity (worker on specific host:port)
            if _verify_pid_identity "${existing_pid}" "trpc_service._cli" "worker" "${host}" "${port}"; then
                if curl --noproxy '*' --connect-timeout 1 --max-time 1 -sf \
                    "http://${host}:${port}/health" >/dev/null 2>&1; then
                    echo "worker-${label} already running with pid ${existing_pid}"
                    return 0
                fi
                echo "worker-${label} pid ${existing_pid} exists but health check failed, stopping old process" >&2
                if ! stop_existing_process "${existing_pid}" "worker-${label}"; then
                    return 1
                fi
                rm -f "${pid_file}"
            else
                echo "pid ${existing_pid} does not match expected worker-${label} instance (${host}:${port}), removing stale pid file" >&2
            fi
        fi
        rm -f "${pid_file}"
    fi

    nohup "${service_python}" -m trpc_service._cli worker --host "${host}" --port "${port}" \
        >>"${log_file}" 2>&1 &
    local worker_pid=$!
    echo "${worker_pid}" >"${pid_file}"

    case "${label}" in
        a)
            started_worker_a_pid="${worker_pid}"
            created_worker_a_pid_file=true
            ;;
        b)
            started_worker_b_pid="${worker_pid}"
            created_worker_b_pid_file=true
            ;;
    esac
    echo "worker-${label} started with pid ${worker_pid} on ${host}:${port}"

    local i
    for _ in {1..30}; do
        if curl --noproxy '*' --connect-timeout 1 --max-time 1 -sf \
            "http://${host}:${port}/health" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    if ! curl --noproxy '*' --connect-timeout 1 --max-time 1 -sf \
        "http://${host}:${port}/health" >/dev/null 2>&1; then
        echo "worker-${label} did not become healthy" >&2
        return 1
    fi
}

_start_worker "a" "${worker_a_host}" "${worker_a_port}" "${worker_a_pid_file}" "${worker_a_log_file}" || {
    cleanup_on_failure
    exit 1
}
_start_worker "b" "${worker_b_host}" "${worker_b_port}" "${worker_b_pid_file}" "${worker_b_log_file}" || {
    cleanup_on_failure
    exit 1
}

export TRPC_WORKER_BASE_URLS="http://${worker_a_host}:${worker_a_port},http://${worker_b_host}:${worker_b_port}"

# Start Gateway
if [[ -f "${gateway_pid_file}" ]]; then
    existing_pid="$(<"${gateway_pid_file}")"
    if [[ "${existing_pid}" =~ ^[0-9]+$ ]] && kill -0 "${existing_pid}" 2>/dev/null; then
        # Verify exact instance identity (gateway on specific host:port)
        if _verify_pid_identity "${existing_pid}" "trpc_service._cli" "gateway" "${gateway_host}" "${gateway_port}"; then
            # Verify gateway is actually healthy
            if curl --noproxy '*' --connect-timeout 1 --max-time 1 -sf \
                "http://${gateway_host}:${gateway_port}/health" >/dev/null 2>&1; then
                echo "gateway already running with pid ${existing_pid}"
            else
                echo "gateway pid ${existing_pid} exists but health check failed, stopping old process" >&2
                if ! stop_existing_process "${existing_pid}" "gateway"; then
                    cleanup_on_failure
                    exit 1
                fi
                rm -f "${gateway_pid_file}"
            fi
        else
            echo "pid ${existing_pid} does not match expected gateway instance (${gateway_host}:${gateway_port}), removing stale pid file" >&2
            rm -f "${gateway_pid_file}"
        fi
    else
        rm -f "${gateway_pid_file}"
    fi
fi

if [[ ! -f "${gateway_pid_file}" ]]; then
    nohup "${service_python}" -m trpc_service._cli gateway --host "${gateway_host}" --port "${gateway_port}" \
        >>"${gateway_log_file}" 2>&1 &
    gateway_pid=$!
    started_gateway_pid="${gateway_pid}"
    echo "${gateway_pid}" >"${gateway_pid_file}"
    created_gateway_pid_file=true
    echo "gateway started with pid ${gateway_pid} on ${gateway_host}:${gateway_port}"
fi

for _ in {1..30}; do
    if curl --noproxy '*' --connect-timeout 1 --max-time 1 -sf \
        "http://${gateway_host}:${gateway_port}/health" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done
if ! curl --noproxy '*' --connect-timeout 1 --max-time 1 -sf \
    "http://${gateway_host}:${gateway_port}/health" >/dev/null 2>&1; then
    echo "gateway did not become healthy" >&2
    cleanup_on_failure
    exit 1
fi

# Start Admin API (last: Worker -> Gateway -> Admin)
if [[ -f "${admin_pid_file}" ]]; then
    existing_admin_pid="$(<"${admin_pid_file}")"
    if [[ "${existing_admin_pid}" =~ ^[0-9]+$ ]] && kill -0 "${existing_admin_pid}" 2>/dev/null; then
        # Verify exact instance identity (admin on specific host:port)
        if _verify_pid_identity "${existing_admin_pid}" "trpc_service._cli" "admin" "${admin_host}" "${admin_port}"; then
            # Verify admin is actually healthy
            if curl --noproxy '*' --connect-timeout 1 --max-time 1 -sf \
                "http://${admin_host}:${admin_port}/health" >/dev/null 2>&1; then
                echo "admin already running with pid ${existing_admin_pid}"
                echo "all services running"
                exit 0
            else
                echo "admin pid ${existing_admin_pid} exists but health check failed, stopping old process" >&2
                if ! stop_existing_process "${existing_admin_pid}" "admin"; then
                    cleanup_on_failure
                    exit 1
                fi
                rm -f "${admin_pid_file}"
            fi
        else
            echo "pid ${existing_admin_pid} does not match expected admin instance (${admin_host}:${admin_port}), removing stale pid file" >&2
            rm -f "${admin_pid_file}"
        fi
    else
        rm -f "${admin_pid_file}"
    fi
fi

nohup "${service_python}" -m trpc_service._cli admin --host "${admin_host}" --port "${admin_port}" \
    >>"${admin_log_file}" 2>&1 &
admin_pid=$!
started_admin_pid="${admin_pid}"
echo "${admin_pid}" >"${admin_pid_file}"
created_admin_pid_file=true
echo "admin started with pid ${admin_pid} on ${admin_host}:${admin_port}"

for _ in {1..30}; do
    if curl --noproxy '*' --connect-timeout 1 --max-time 1 -sf \
        "http://${admin_host}:${admin_port}/health" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done
if ! curl --noproxy '*' --connect-timeout 1 --max-time 1 -sf \
    "http://${admin_host}:${admin_port}/health" >/dev/null 2>&1; then
    echo "admin did not become healthy" >&2
    cleanup_on_failure
    exit 1
fi

echo "all services running"
