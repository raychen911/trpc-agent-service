#!/usr/bin/env bash
set -euo pipefail

stop_role() {
  local role="$1"
  local pid_file="$2"
  local expected_command pid command_line
  expected_command="$(case "${role}" in api) echo serve ;; *) echo "${role}" ;; esac)"

  if [[ ! -f "${pid_file}" ]]; then
    return 0
  fi
  pid="$(<"${pid_file}")"
  if [[ ! "${pid}" =~ ^[0-9]+$ ]]; then
    echo "Refusing invalid PID file ${pid_file}." >&2
    return 1
  fi
  if ! kill -0 "${pid}" 2>/dev/null; then
    echo "Role ${role} PID ${pid} is stale; removing ${pid_file}."
    rm -f -- "${pid_file}"
    return 0
  fi

  command_line="$(ps -p "${pid}" -o command= 2>/dev/null || true)"
  if [[ "${command_line}" != *"trpc_service._cli ${expected_command}"* ]] && \
     [[ "${command_line}" != *"trpc-agent-service ${expected_command}"* ]]; then
    echo "Refusing to signal PID ${pid}: it does not match role ${role}." >&2
    return 1
  fi

  kill -TERM "${pid}"
  for _ in {1..40}; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      rm -f -- "${pid_file}"
      echo "Role ${role} stopped."
      return 0
    fi
    sleep 0.5
  done
  echo "Role ${role} did not stop within 20 seconds; PID file was preserved." >&2
  return 1
}

status=0
for role in api worker dispatcher projector; do
  stop_role "${role}" "data/trpc-agent-service-${role}.pid" || status=1
done

# Compatibility with the original single-API PID file.
stop_role api "data/trpc-agent-service.pid" || status=1

if [[ "${status}" -eq 0 ]]; then
  echo "All recorded service roles are stopped."
fi
exit "${status}"
