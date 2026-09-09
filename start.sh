#!/usr/bin/env bash
set -euo pipefail

mkdir -p data

role_command() {
  case "$1" in
    api) printf '%s\n' "serve" ;;
    worker) printf '%s\n' "worker" ;;
    dispatcher) printf '%s\n' "dispatcher" ;;
    projector) printf '%s\n' "projector" ;;
    *)
      echo "Unsupported role: $1" >&2
      return 1
      ;;
  esac
}

uv run python -m trpc_service._cli migrate

if [[ "${FOREGROUND:-0}" == "1" ]]; then
  role="${ROLE:-api}"
  command="$(role_command "${role}")"
  if [[ "${role}" == "api" ]]; then
    exec uv run python -m trpc_service._cli serve \
      --host 0.0.0.0 --port "${PORT:-8000}"
  fi
  exec uv run python -m trpc_service._cli "${command}"
fi

IFS=',' read -r -a roles <<<"${ROLES:-api}"
if [[ "${#roles[@]}" -eq 0 ]]; then
  echo "ROLES must contain at least one of api,worker,dispatcher,projector." >&2
  exit 1
fi

started_pid_files=()
cleanup_started() {
  local pid_file pid
  for pid_file in "${started_pid_files[@]}"; do
    if [[ -f "${pid_file}" ]]; then
      pid="$(<"${pid_file}")"
      if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
        kill -TERM "${pid}" 2>/dev/null || true
      fi
      rm -f -- "${pid_file}"
    fi
  done
}
trap cleanup_started ERR INT TERM

for role in "${roles[@]}"; do
  command="$(role_command "${role}")"
  pid_file="data/trpc-agent-service-${role}.pid"
  log_file="data/trpc-agent-service-${role}.log"
  if [[ -f "${pid_file}" ]] && kill -0 "$(<"${pid_file}")" 2>/dev/null; then
    echo "Role ${role} is already running with PID $(<"${pid_file}")." >&2
    cleanup_started
    exit 1
  fi
  rm -f -- "${pid_file}"

  if [[ "${role}" == "api" ]]; then
    nohup uv run python -m trpc_service._cli serve \
      --host 0.0.0.0 --port "${PORT:-8000}" >"${log_file}" 2>&1 &
  else
    nohup uv run python -m trpc_service._cli "${command}" >"${log_file}" 2>&1 &
  fi
  role_pid=$!
  printf '%s\n' "${role_pid}" >"${pid_file}"
  started_pid_files+=("${pid_file}")

  sleep 1
  if ! kill -0 "${role_pid}" 2>/dev/null; then
    echo "Role ${role} exited during startup; inspect ${log_file}." >&2
    cleanup_started
    exit 1
  fi
  echo "Role ${role} started with PID ${role_pid}; log: ${log_file}"
done

if [[ " ${roles[*]} " == *" api "* ]]; then
  for _ in {1..30}; do
    if curl --fail --silent "http://127.0.0.1:${PORT:-8000}/health/ready" >/dev/null; then
      trap - ERR INT TERM
      echo "API readiness check passed."
      exit 0
    fi
    sleep 1
  done
  echo "API readiness deadline exceeded." >&2
  cleanup_started
  exit 1
fi

trap - ERR INT TERM
echo "All requested roles passed startup checks."
