"""Deterministic Phase 8 observability fixtures; never reads credentials or external services.

All values are stable, offline and non-secret. ``SENSITIVE_SENTINELS`` are fake
canary values (``.invalid`` domains, ``sentinel`` markers) used only to assert
zero-leak behaviour of telemetry outputs; they are not real credentials.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid5

FIXED_OBS_UTC = datetime(2026, 9, 10, 12, 0, 0, tzinfo=timezone.utc)
_OBS_FIXTURE_NAMESPACE = UUID("b1d2c3a4-5e6f-4a7b-8c9d-0e1f2a3b4c5d")

OBS_ROLES: tuple[str, ...] = (
    "gateway",
    "worker",
    "feishu_adapter",
    "wecom_adapter",
    "recovery",
)

OBS_CHANNELS: tuple[str, ...] = ("feishu", "wecom", "local_http")


@dataclass(frozen=True, slots=True)
class ObsTenant:
    tenant_id: str


@dataclass(frozen=True, slots=True)
class ObsNode:
    node_id: str
    role: str
    generation: int = 1


def obs_tenants() -> tuple[ObsTenant, ObsTenant]:
    return ObsTenant("tenant-alpha"), ObsTenant("tenant-beta")


def obs_nodes() -> tuple[ObsNode, ObsNode]:
    return (
        ObsNode("worker-a", "worker", generation=7),
        ObsNode("worker-b", "worker", generation=13),
    )


def obs_channel_nodes() -> tuple[ObsNode, ObsNode]:
    return (
        ObsNode("feishu-a", "feishu_adapter", generation=12),
        ObsNode("wecom-a", "wecom_adapter", generation=4),
    )


def stable_trace_id(label: str = "phase8") -> UUID:
    return uuid5(_OBS_FIXTURE_NAMESPACE, f"trace:{label}")


def stable_trace_digest(trace_id: UUID | str) -> str:
    """Safe external reference for a trace, format ``sha256:<16hex>``."""
    raw = sha256(f"trace-digest:{trace_id}".encode("utf-8")).hexdigest()
    return f"sha256:{raw[:16]}"


def stable_scope_digest(scope_key: str) -> str:
    """Digest of a tenant or platform scope; never the raw tenant id."""
    return sha256(f"scope:{scope_key}".encode("utf-8")).hexdigest()[:16]


def stable_fence(label: str = "phase8") -> str:
    return sha256(f"fence:{label}".encode("utf-8")).hexdigest()


def stable_span_id(label: str = "phase8") -> str:
    return sha256(f"span:{label}".encode("utf-8")).hexdigest()[:16]


SENSITIVE_SENTINELS: dict[str, str] = {
    "api_key": "sk-sentinel-a1b2c3d4e5f6g7h8i9j0",
    "im_token": "sentinel-im-token-9f8e7d6c5b4a3210",
    "database_password": "sentinel-db-password-4f5e6d7c8b9a",
    "response_url": "https://sentinel.invalid/wecom/callback?token=sentinel-callback-77aa",
    "phone": "13800000000",
    "email": "sentinel-user@example.invalid",
    "message_body": (
        "SENTINEL 正文：联系 13800000000 或 sentinel-user@example.invalid，"
        "凭证 sk-sentinel-a1b2c3d4e5f6g7h8i9j0，"
        "回调 https://sentinel.invalid/wecom/callback?token=sentinel-callback-77aa"
    ),
}


def sentinel_values() -> tuple[str, ...]:
    """Ordered unique sentinel values for zero-leak assertions."""
    seen: dict[str, None] = {}
    for value in SENSITIVE_SENTINELS.values():
        seen.setdefault(value, None)
    return tuple(seen.keys())


def stage_fixture(component: str = "gateway", stage: str = "gateway.accept") -> dict[str, Any]:
    return {
        "component": component,
        "stage": stage,
        "outcome": "success",
        "error_type": None,
        "retryable": False,
        "trace_id": str(stable_trace_id("stage")),
        "trace_digest": stable_trace_digest(stable_trace_id("stage")),
    }


def obs_fixture_summary() -> dict[str, Any]:
    alpha, beta = obs_tenants()
    node_a, node_b = obs_nodes()
    trace = stable_trace_id()
    return {
        "tenants": (alpha.tenant_id, beta.tenant_id),
        "nodes": (node_a.node_id, node_b.node_id),
        "roles": OBS_ROLES,
        "utc": FIXED_OBS_UTC.isoformat(),
        "trace": str(trace),
        "trace_digest": stable_trace_digest(trace),
        "scope_digest": stable_scope_digest(alpha.tenant_id),
        "fence_digest": stable_fence()[:16],
        "sentinel_count": len(sentinel_values()),
    }
