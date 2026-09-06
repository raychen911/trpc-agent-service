"""Static gate for the production observability contract."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
METRICS = ROOT / "trpc_service" / "telemetry" / "metrics.py"
TRACE = ROOT / "trpc_service" / "telemetry" / "tracing.py"
APP = ROOT / "trpc_service" / "web" / "app.py"
ALERTS = ROOT / "deployment" / "prometheus-alerts.yml"


REQUIRED_METRICS = {
    "trpc_agent_requests_total",
    "trpc_agent_tokens_total",
    "trpc_agent_model_latency_seconds",
    "trpc_agent_tool_calls_total",
    "trpc_agent_tool_latency_seconds",
    "trpc_agent_im_deliveries_total",
    "trpc_agent_cost_total",
    "trpc_agent_session_backend_latency_seconds",
    "trpc_agent_errors_total",
}


def main() -> int:
    metrics = METRICS.read_text(encoding="utf-8")
    trace = TRACE.read_text(encoding="utf-8")
    app = APP.read_text(encoding="utf-8")
    alerts = ALERTS.read_text(encoding="utf-8")
    checks = {
        "required metrics": all(name in metrics for name in REQUIRED_METRICS),
        "trace retention bound": "TRACE_MAX_RETAINED_SPANS" in trace,
        "trace user/session hashing": "sha256:" in trace and "_trace_identifier" in trace,
        "liveness endpoint": '"/livez"' in app,
        "readiness endpoint": '"/readyz"' in app,
        "tenant audit endpoint": '"/admin/v1/tenants/{tenant_id}/audit"' in app,
        "alert rules": all(
            name in alerts
            for name in (
                "TrpcAgentGatewayDown",
                "TrpcAgentHighErrorRate",
                "TrpcAgentDeliveryFailures",
                "TrpcAgentBackendLatency",
            )
        ),
        "alert expressions avoid raw message labels": not re.search(
            r"(?:prompt|message|text|content|session_id|user_id)\s*[{]", alerts
        ),
    }
    for name, ok in checks.items():
        print(f"{name}: {'pass' if ok else 'fail'}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
