#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Source shared PID identity verification
# shellcheck source=scripts/pid_identity.sh
source "${project_dir}/scripts/pid_identity.sh"

admin_pid_file="${TRPC_ADMIN_PID_FILE:-${project_dir}/data/admin.pid}"
gateway_pid_file="${TRPC_GATEWAY_PID_FILE:-${project_dir}/data/gateway.pid}"
worker_a_pid_file="${TRPC_WORKER_A_PID_FILE:-${project_dir}/data/worker-a.pid}"
worker_b_pid_file="${TRPC_WORKER_B_PID_FILE:-${project_dir}/data/worker-b.pid}"
redis_container_name="${TRPC_REDIS_CONTAINER_NAME:-trpc-dev-redis}"
redis_marker_file="${TRPC_REDIS_MARKER_FILE:-${project_dir}/data/redis.owned}"
postgres_container_name="${TRPC_POSTGRES_CONTAINER_NAME:-trpc-dev-postgres}"
postgres_marker_file="${TRPC_POSTGRES_MARKER_FILE:-${project_dir}/data/postgres.owned}"
minio_container_name="${TRPC_MINIO_CONTAINER_NAME:-trpc-dev-minio}"
minio_marker_file="${TRPC_MINIO_MARKER_FILE:-${project_dir}/data/minio.owned}"

# Expected host/port for each service instance (from environment or defaults)
worker_a_host="${TRPC_WORKER_A_HOST:-127.0.0.1}"
worker_a_port="${TRPC_WORKER_A_PORT:-8001}"
worker_b_host="${TRPC_WORKER_B_HOST:-127.0.0.1}"
worker_b_port="${TRPC_WORKER_B_PORT:-8002}"
gateway_host="${TRPC_GATEWAY_HOST:-127.0.0.1}"
gateway_port="${TRPC_GATEWAY_PORT:-8000}"
admin_host="${TRPC_ADMIN_HOST:-127.0.0.1}"
admin_port="${TRPC_ADMIN_PORT:-8003}"

stop_pid() {
    local pid_file="$1"
    local name="$2"
    local expected_host="$3"
    local expected_port="$4"

    if [[ ! -f "${pid_file}" ]]; then
        echo "${name} is not running"
        return 0
    fi

    local service_pid
    service_pid="$(<"${pid_file}")"
    if [[ ! "${service_pid}" =~ ^[0-9]+$ ]]; then
        echo "invalid pid file: ${pid_file}" >&2
        rm -f "${pid_file}"
        return 0
    fi

    if kill -0 "${service_pid}" 2>/dev/null; then
        # Verify exact instance identity (module + subcommand + host + port)
        if ! _verify_pid_identity "${service_pid}" "trpc_service._cli" "${name%%-*}" "${expected_host}" "${expected_port}"; then
            echo "pid ${service_pid} does not match expected ${name} instance (${expected_host}:${expected_port}), removing stale pid file" >&2
            rm -f "${pid_file}"
            return 0
        fi
        kill "${service_pid}"
        for _ in {1..50}; do
            if ! kill -0 "${service_pid}" 2>/dev/null; then
                break
            fi
            sleep 0.1
        done
    fi

    if kill -0 "${service_pid}" 2>/dev/null; then
        echo "${name} pid ${service_pid} did not stop" >&2
        exit 1
    fi

    rm -f "${pid_file}"
    echo "${name} stopped"
}

# Stop order: Admin first, then Gateway, then Workers (reverse of start).
stop_pid "${admin_pid_file}" "admin" "${admin_host}" "${admin_port}"
stop_pid "${gateway_pid_file}" "gateway" "${gateway_host}" "${gateway_port}"
stop_pid "${worker_a_pid_file}" "worker-a" "${worker_a_host}" "${worker_a_port}"
stop_pid "${worker_b_pid_file}" "worker-b" "${worker_b_host}" "${worker_b_port}"

if [[ -f "${redis_marker_file}" ]]; then
    if command -v docker >/dev/null 2>&1; then
        if docker ps --format '{{.Names}}' | grep -q "^${redis_container_name}$"; then
            docker stop "${redis_container_name}" >/dev/null
            echo "redis container stopped"
        fi
    fi
    rm -f "${redis_marker_file}"
fi

if [[ -f "${postgres_marker_file}" ]]; then
    if command -v docker >/dev/null 2>&1; then
        if docker ps --format '{{.Names}}' | grep -q "^${postgres_container_name}$"; then
            docker stop "${postgres_container_name}" >/dev/null
            echo "postgres container stopped"
        fi
    fi
    rm -f "${postgres_marker_file}"
fi

if [[ -f "${minio_marker_file}" ]]; then
    if command -v docker >/dev/null 2>&1; then
        if docker ps --format '{{.Names}}' | grep -q "^${minio_container_name}$"; then
            docker stop "${minio_container_name}" >/dev/null
            echo "minio container stopped"
        fi
    fi
    rm -f "${minio_marker_file}"
fi
