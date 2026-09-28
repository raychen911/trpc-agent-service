#!/usr/bin/env bash

# Stop the local API and Docker infrastructure, preserving data unless requested.

set -euo pipefail

readonly PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly API_PID_FILE="$PROJECT_ROOT/.run/trpc-agent-service.pid"
readonly WORKER_PID_DIR="$PROJECT_ROOT/.run/workers"
readonly SUPERVISOR_PID_FILE="$PROJECT_ROOT/.run/worker-supervisor.pid"
readonly CHANNEL_PID_FILE="$PROJECT_ROOT/.run/channel-runtime.pid"
cd -- "$PROJECT_ROOT"

# Reject invalid invocations before sending a signal or touching Docker state.
if [[ $# -gt 1 || ($# -eq 1 && "$1" != "--volumes") ]]; then
    echo "usage: $0 [--volumes]" >&2
    exit 2
fi

signal_process() {
    local pid_file="$1"
    local process_label="$2"
    if [[ ! -f "$pid_file" ]]; then
        return
    fi
    local service_pid
    service_pid="$(<"$pid_file")"
    if kill -0 "$service_pid" 2>/dev/null; then
        local command_line
        command_line="$(tr '\0' ' ' <"/proc/$service_pid/cmdline" 2>/dev/null || true)"
        if [[ "$command_line" != *"$PROJECT_ROOT/.venv/bin/trpc-agent-service"* ]]; then
            echo "error: $process_label PID file does not identify this project: $service_pid" >&2
            exit 1
        fi
        kill "$service_pid"
    fi
}

wait_process() {
    local pid_file="$1"
    local process_label="$2"
    local graceful_seconds="$3"
    if [[ ! -f "$pid_file" ]]; then
        return
    fi
    local service_pid
    service_pid="$(<"$pid_file")"
    if kill -0 "$service_pid" 2>/dev/null; then
        # Worker shutdown first stops new claims, then waits for in-flight model
        # calls. Allow a full lease window before recoverable forced termination.
        local wait_steps=$((graceful_seconds * 4))
        for ((step = 0; step < wait_steps; step++)); do
            if ! kill -0 "$service_pid" 2>/dev/null; then
                break
            fi
            sleep 0.25
        done
        if kill -0 "$service_pid" 2>/dev/null; then
            # Durable queue and Session leases make the final forced fallback
            # recoverable, but only after the normal drain budget is exhausted.
            kill -KILL "$service_pid"
            for _ in {1..20}; do
                if ! kill -0 "$service_pid" 2>/dev/null; then
                    break
                fi
                sleep 0.25
            done
        fi
        if kill -0 "$service_pid" 2>/dev/null; then
            echo "error: $process_label did not stop: $service_pid" >&2
            exit 1
        fi
    fi
    rm -f -- "$pid_file"
}

stop_process() {
    local pid_file="$1"
    local process_label="$2"
    local graceful_seconds="$3"
    signal_process "$pid_file" "$process_label"
    wait_process "$pid_file" "$process_label" "$graceful_seconds"
}

# Stop the HTTP Gateway first, then notify the long-connection process before
# Workers drain so it rejects callbacks that no Worker could finish.
stop_process "$API_PID_FILE" "Gateway" 30
signal_process "$CHANNEL_PID_FILE" "Channel Runtime"
signal_process "$SUPERVISOR_PID_FILE" "Worker Supervisor"
shopt -s nullglob
# Signal every Worker before waiting so no peer continues claiming new tasks
# while an earlier PID drains.
for worker_pid_file in "$WORKER_PID_DIR"/*.pid; do
    signal_process "$worker_pid_file" "Agent Worker"
done
for worker_pid_file in "$WORKER_PID_DIR"/*.pid; do
    wait_process "$worker_pid_file" "Agent Worker" 120
done
shopt -u nullglob
wait_process "$SUPERVISOR_PID_FILE" "Worker Supervisor" 30

# Replies committed after the connector finishes its own drain remain durable
# Outbox work and are delivered after the next start.
wait_process "$CHANNEL_PID_FILE" "Channel Runtime" 90

if [[ "${1:-}" == "--volumes" ]]; then
    # Clear bind-mounted core data through the already configured service images;
    # this also works when container UIDs own files that the host user cannot delete.
    docker compose stop
    docker compose run --rm --no-deps --entrypoint sh postgres \
        -ceu 'find /var/lib/postgresql -mindepth 1 -delete'
    docker compose run --rm --no-deps --entrypoint sh redis \
        -ceu 'find /data -mindepth 1 -delete'
    docker compose run --rm --no-deps --entrypoint sh seaweedfs \
        -ceu 'find /data -mindepth 1 -delete'
    # Monitoring uses named volumes and is removed by Compose here.
    docker compose down --volumes --remove-orphans
else
    docker compose down --remove-orphans
fi
