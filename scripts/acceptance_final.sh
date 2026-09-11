#!/usr/bin/env bash
# The one production-topology final acceptance entry point.  It never reads
# local credential files: operators export values in their own shell first.
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"
service_python="${TRPC_SERVICE_PYTHON:-${project_root}/.venv/bin/python}"
report_path="${project_root}/artifacts/final-acceptance/report.json"
project_name="trpc-final-${RANDOM}${RANDOM}"
owned_topology=0
cleanup_recorded=0
preflight=0

for argument in "$@"; do
    case "${argument}" in
        --preflight) preflight=1 ;;
        *) echo "usage: bash scripts/acceptance_final.sh [--preflight]" >&2; exit 2 ;;
    esac
done

record() {
    local check="$1" result="$2" started="$3"
    shift 3
    local elapsed=$(( ($(date +%s%N) - started) / 1000000 ))
    "${service_python}" scripts/final_evidence.py record --path "${report_path}" \
        --check "${check}" --result "${result}" --duration-ms "${elapsed}" "$@"
}

cleanup() {
    local started cleanup_result
    started="$(date +%s%N)"
    cleanup_result="pass"
    if [[ "${owned_topology}" == "1" ]]; then
        if ! docker compose -p "${project_name}" --env-file /dev/null down -v --remove-orphans >/dev/null 2>&1; then
            cleanup_result="fail"
        fi
    fi
    # A preflight or missing-environment exit never owns a topology.  Do not
    # append a second cleanup record to an earlier preflight report in that
    # case; cleanup evidence belongs only to a full topology run.
    if [[ "${owned_topology}" == "1" && "${cleanup_recorded}" == "0" \
        && -x "${service_python}" && -f "${report_path}" ]]; then
        record cleanup "${cleanup_result}" "${started}" --counter workers=0 || true
        cleanup_recorded=1
    fi
}
trap cleanup EXIT

if [[ ! -x "${service_python}" ]]; then
    echo "required Python runtime is unavailable" >&2
    exit 1
fi
if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
    echo "Docker Compose is unavailable" >&2
    exit 1
fi

if [[ "${preflight}" == "1" ]]; then
    started="$(date +%s%N)"
    # Compose validates structure without interpolating or resolving service
    # env_files, so this path neither opens nor consumes local credentials.
    docker compose --env-file /dev/null config --quiet --no-env-resolution --no-interpolate
    mkdir -p "$(dirname "${report_path}")"
    rm -f "${report_path}"
    record preflight pass "${started}" --counter requests=0
    "${service_python}" scripts/final_evidence.py validate --path "${report_path}" >/dev/null 2>&1 || true
    echo "PASS: preflight (export required runtime variables before full acceptance)"
    exit 0
fi

required_environment=(
    TRPC_COMPOSE_PG_USER TRPC_COMPOSE_PG_PASSWORD TRPC_INTERNAL_TOKEN TRPC_ADMIN_TOKEN
    TRPC_S3_ACCESS_KEY TRPC_S3_SECRET_KEY TRPC_MODEL_PROVIDER TRPC_MODEL_NAME
    TRPC_MODEL_BASE_URL TRPC_MODEL_API_KEY
)
missing=0
for variable in "${required_environment[@]}"; do
    if [[ -z "${!variable:-}" ]]; then
        echo "required environment variable is not exported: ${variable}" >&2
        missing=1
    fi
done
if [[ "${missing}" == "1" ]]; then
    exit 1
fi

# The callback credentials are ephemeral acceptance inputs.  They are injected
# only into this Compose project and referenced by the short-lived binding
# created below; no local credential file is read or written.
export TRPC_WECOM_WEBHOOK_TOKEN="${TRPC_WECOM_WEBHOOK_TOKEN:-$(${service_python} -c 'import secrets; print(secrets.token_urlsafe(24))')}"
export TRPC_WECOM_WEBHOOK_AES_KEY="${TRPC_WECOM_WEBHOOK_AES_KEY:-$(${service_python} -c 'import base64, os; print(base64.b64encode(os.urandom(32)).decode().rstrip("="))')}"
read -r TRPC_GATEWAY_PUBLISH_PORT TRPC_ADMIN_PUBLISH_PORT < <("${service_python}" - <<'PY'
import socket

ports = []
for _ in range(2):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        ports.append(sock.getsockname()[1])
print(*ports)
PY
)
export TRPC_GATEWAY_PUBLISH_PORT TRPC_ADMIN_PUBLISH_PORT

rm -rf "${project_root}/artifacts/final-acceptance"
mkdir -p "$(dirname "${report_path}")"
started="$(date +%s%N)"
record preflight pass "${started}" --counter requests=0

started="$(date +%s%N)"
# The project name is unique to this invocation, so cleanup may safely own it
# before Compose creates the first resource.  This also covers partial starts.
owned_topology=1
docker compose -p "${project_name}" --env-file /dev/null up --build --wait --wait-timeout 240
record compose_health pass "${started}" --counter workers=2

started="$(date +%s%N)"
worker_count="$(docker compose -p "${project_name}" --env-file /dev/null ps --status running --services | grep -Ec '^worker-[ab]$' || true)"
if [[ "${worker_count}" != "2" ]]; then
    record worker_topology fail "${started}" --counter workers="${worker_count}"
    exit 1
fi

# Exercise the topology that was just started, including Gateway routing,
# one real model turn, Worker SSE and terminal response handling.  The fixed
# prompt and response remain in-process and are never printed as evidence.
"${service_python}" - "${project_name}" <<'PY'
import json
import os
import sys
import urllib.request

suffix = sys.argv[1].replace("-", "_")
payload = json.dumps({
    "tenant_id": "tenant_default",
    "user_id": "final_acceptance_user",
    "conversation_id": f"final_acceptance_{suffix}",
    "message_id": f"final_acceptance_{suffix}",
    "message": "Reply with a short acknowledgement.",
}).encode("utf-8")
request = urllib.request.Request(
    f"http://127.0.0.1:{os.environ['TRPC_GATEWAY_PUBLISH_PORT']}/api/console/messages/stream",
    data=payload,
    headers={"Content-Type": "application/json", "X-Tenant-ID": "tenant_default"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=180) as response:
    terminal = None
    for raw_line in response:
        line = raw_line.decode("utf-8").strip()
        if not line.startswith("data: "):
            continue
        event = json.loads(line[6:])
        if event.get("type") in {"done", "error"}:
            terminal = event["type"]
if terminal != "done":
    raise SystemExit("live console stream did not complete")
PY
record worker_topology pass "${started}" --counter workers=2 --counter requests=1

started="$(date +%s%N)"
# This is deliberately against the published Compose Gateway, not an in-process
# ASGI app: create its binding through Admin, then perform WeCom's encrypted
# GET challenge round-trip using the same Token/AES-key resolution path.
if "${service_python}" - <<'PY'
import base64
import hashlib
import json
import os
import struct
import urllib.parse
import urllib.request
import uuid

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

account = "wecom-acceptance"
token = os.environ["TRPC_WECOM_WEBHOOK_TOKEN"]
aes_key = os.environ["TRPC_WECOM_WEBHOOK_AES_KEY"]
admin_token = os.environ["TRPC_ADMIN_TOKEN"]
binding = {
    "binding": {
        "binding_id": str(uuid.uuid4()), "tenant_id": "tenant_default", "app_id": "app_demo",
        "channel": "wecom", "external_account_id": account, "secret_ref": "env:TRPC_WECOM_WEBHOOK_TOKEN",
        "webhook_token_ref": "env:TRPC_WECOM_WEBHOOK_TOKEN",
        "webhook_aes_key_ref": "env:TRPC_WECOM_WEBHOOK_AES_KEY", "enabled": True, "version": 1,
    }
}
request = urllib.request.Request(
    f"http://127.0.0.1:{os.environ['TRPC_ADMIN_PUBLISH_PORT']}/admin/v1/tenants/tenant_default/channel-bindings",
    data=json.dumps(binding).encode(), method="POST",
    headers={"Content-Type": "application/json", "X-TRPC-Admin-Token": admin_token},
)
with urllib.request.urlopen(request, timeout=15) as response:
    if response.status != 201:
        raise SystemExit("channel binding was not created")
plaintext = b"webhook-compose-challenge"
key = base64.b64decode(aes_key + "=")
raw = os.urandom(16) + struct.pack("!I", len(plaintext)) + plaintext + account.encode()
padding = 32 - len(raw) % 32
cipher = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
encrypted = base64.b64encode(cipher.update(raw + bytes([padding]) * padding) + cipher.finalize()).decode()
timestamp, nonce = "1700000000", "acceptance-nonce"
signature = hashlib.sha1("".join(sorted((token, timestamp, nonce, encrypted))).encode()).hexdigest()
query = urllib.parse.urlencode({"msg_signature": signature, "timestamp": timestamp, "nonce": nonce, "echostr": encrypted})
gateway_port = os.environ["TRPC_GATEWAY_PUBLISH_PORT"]
with urllib.request.urlopen(f"http://127.0.0.1:{gateway_port}/webhooks/wecom/{account}?{query}", timeout=15) as response:
    if response.status != 200 or response.read() != plaintext:
        raise SystemExit("webhook-compose-challenge failed")
PY
then
    record webhook_protocol pass "${started}" --counter requests=1
else
    record webhook_protocol fail "${started}" --counter failures=1
    exit 1
fi

# These contract groups use existing public tests once against the already
# initialized production configuration.  They are not historical acceptance
# scripts and never start a second topology.
run_contract() {
    local check="$1"
    shift
    local contract_started
    contract_started="$(date +%s%N)"
    if "${service_python}" -m pytest -q "$@"; then
        record "${check}" pass "${contract_started}" --counter requests=0
    else
        record "${check}" fail "${contract_started}" --counter failures=1
        exit 1
    fi
}

run_contract tenant_isolation tests/test_tenant_repository.py tests/test_channel_identity.py
# The complete integration suite uses real Redis/PostgreSQL/MinIO, two real
# Worker processes and the exported real-model configuration.  Keeping it in
# this single final entry point prevents mocked unit contracts from being
# mistaken for production-topology evidence.
run_contract state_backends tests/integration
run_contract idempotency_ordering tests/test_message_repository.py tests/test_channel_order_gate.py
run_contract governance tests/test_worker_content_governance.py tests/test_governance_tool_filter.py
run_contract approval tests/test_worker_approval_service.py
run_contract rate_budget tests/test_usage_accounting.py tests/test_tenant_limits.py
run_contract audit_usage_trace tests/test_audit_query.py tests/test_telemetry.py
run_contract rollout_rollback tests/test_rollout.py
run_contract capacity_faults tests/test_capacity_probe.py tests/test_stage6d_failure_contracts.py

for external in wecom feishu; do
    started="$(date +%s%N)"
    if [[ "${external}" == "wecom" && -n "${TRPC_WECOM_BOT_ID:-}" && -n "${TRPC_WECOM_BOT_SECRET:-}" ]]; then
        echo "ACTION REQUIRED: send one unique message from the configured WeCom account; inspect only aggregate audit counters."
        if [[ "${TRPC_FINAL_WECOM_CONFIRMED:-0}" != "1" ]]; then
            record external_wecom fail "${started}" --counter failures=1
            exit 1
        fi
        record external_wecom pass "${started}" --counter deliveries=1
    elif [[ "${external}" == "feishu" && -n "${TRPC_FEISHU_APP_ID:-}" && -n "${TRPC_FEISHU_APP_SECRET:-}" ]]; then
        echo "ACTION REQUIRED: send one unique message from the configured Feishu account; inspect only aggregate audit counters."
        if [[ "${TRPC_FINAL_FEISHU_CONFIRMED:-0}" != "1" ]]; then
            record external_feishu fail "${started}" --counter failures=1
            exit 1
        fi
        record external_feishu pass "${started}" --counter deliveries=1
    else
        record "external_${external}" external_unavailable "${started}" --counter deliveries=0
    fi
done

cleanup
trap - EXIT
"${service_python}" scripts/final_evidence.py validate --path "${report_path}"
echo "PASS: final acceptance evidence validated"
