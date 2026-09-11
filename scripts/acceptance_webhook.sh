#!/usr/bin/env bash
# Local Enterprise WeChat callback contract; never prints credential values.
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

./.venv/bin/python -m pytest -q \
  tests/test_webhook_verification.py \
  tests/test_webhook_routes.py \
  tests/integration/test_webhook_ingress.py

if [[ -z "${TRPC_WECOM_WEBHOOK_TOKEN:-}" || -z "${TRPC_WECOM_WEBHOOK_AES_KEY:-}" ]]; then
  echo "external_unavailable: WeCom webhook credentials are not configured"
else
  echo "external_configured: configure the persisted binding before platform verification"
fi
