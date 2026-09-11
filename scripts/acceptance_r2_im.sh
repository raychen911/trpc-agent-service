#!/usr/bin/env bash
# R2 IM acceptance: SDK retry/terminal audit plus real local PostgreSQL/Redis.
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

./.venv/bin/python -m pytest -q \
  tests/test_channel_delivery.py \
  tests/test_wecom_service.py \
  tests/test_feishu_service.py \
  tests/integration/test_r2_im_ingress_isolation.py \
  tests/integration/test_r2_im_isolation_delivery.py

if [[ -z "${TRPC_WECOM_BOT_ID:-}" || -z "${TRPC_WECOM_BOT_SECRET:-}" ]]; then
  echo "external unavailable: WeCom credentials are not configured"
else
  echo "external configured: start the Gateway with the persisted ChannelBinding to validate WeCom delivery"
fi
