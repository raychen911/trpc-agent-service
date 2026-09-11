"""SLO calculations and external alert receiver validation."""

from __future__ import annotations

import json
import time
import urllib.request
import uuid


def report(snapshot: dict, *, availability_target: float = 0.999, latency_target_ms: float = 1500) -> dict:
    """Calculate bounded availability and p95 latency from the metrics registry."""
    def total(name):
        return sum(value.get("sum", 0) for key, value in snapshot.items() if key.startswith(name))

    requests = total("gateway.callback_total")
    failures = sum(value.get("sum", 0) for key, value in snapshot.items()
                   if key.startswith("gateway.callback_total") and "outcome=failed" in key)
    availability = 1.0 if requests == 0 else max(0.0, 1.0 - failures / requests)
    latency = max((value.get("p95", 0) for key, value in snapshot.items()
                   if key.startswith("gateway.inbox_latency_ms")), default=0)
    return {"availability": availability, "availability_target": availability_target,
            "latency_p95_ms": latency, "latency_target_ms": latency_target_ms,
            "availability_met": availability >= availability_target,
            "latency_met": latency <= latency_target_ms,
            "measured_at": time.time()}


def send_test(webhook_url: str, token: str | None = None) -> dict:
    """Send a non-sensitive synthetic alert and require a successful receiver response."""
    payload = {"alert": "trpc_slo_receiver_test", "nonce": uuid.uuid4().hex, "status": "firing"}
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(webhook_url, data=json.dumps(payload).encode(), method="POST", headers=headers)
    with urllib.request.urlopen(request, timeout=10) as response:
        if not 200 <= response.status < 300:
            raise RuntimeError(f"alert receiver returned HTTP {response.status}")
        return {"delivered": True, "status": response.status, "nonce": payload["nonce"]}
