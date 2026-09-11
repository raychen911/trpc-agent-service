#!/usr/bin/env bash
# R3C bounded operational fault matrix.  It deliberately reuses focused
# contracts instead of starting seven duplicate application topologies.
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"
service_python="${TRPC_SERVICE_PYTHON:-${project_root}/.venv/bin/python}"

if [[ ! -x "${service_python}" ]]; then
  echo "python runtime unavailable" >&2
  exit 1
fi

run_case() {
  local name="$1"
  shift
  "${service_python}" -m pytest -q "$@"
  echo "PASS: ${name}"
}

# Each case pins fail-closed/replay behavior at its existing single boundary:
# no request reruns the model or a tool merely because infrastructure failed.
run_case "worker restart and receipt replay" tests/test_message_repository.py tests/test_execution_coordinator.py
run_case "redis unavailable fails closed" tests/test_channel_order_gate.py tests/test_state_backend.py
run_case "postgres unavailable fails closed" tests/test_sql_tenant_repository.py tests/test_execution_audit.py
run_case "model failure response remains bounded" tests/test_worker_service.py
run_case "tool execution failure remains bounded" tests/test_worker_approval_service.py
run_case "IM before/partial-send retry and terminal audit" tests/test_channel_delivery.py tests/test_wecom_service.py tests/test_feishu_service.py
run_case "collector/exporter outage does not affect business" tests/test_metrics.py tests/test_telemetry.py

# Compose topology and external-service checks are owned by the single final
# acceptance entry point, avoiding a second stage-specific deployment path.
echo "Compose outage checks: run scripts/acceptance_final.sh with operator credentials"
