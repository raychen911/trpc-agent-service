"""Deterministic Phase 7 fixtures; never reads credentials or external services."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid5

FIXED_DATA_UTC = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
_FIXTURE_NAMESPACE = UUID("9f2f9f3d-7e79-4c52-91b0-5c9b4b9a0f7e")


@dataclass(frozen=True, slots=True)
class DataTenant:
    tenant_id: str


@dataclass(frozen=True, slots=True)
class DataNode:
    node_id: str
    generation: int = 1


def data_tenants() -> tuple[DataTenant, DataTenant]:
    return DataTenant("tenant-alpha"), DataTenant("tenant-beta")


def data_nodes() -> tuple[DataNode, DataNode]:
    return DataNode("data-node-a"), DataNode("data-node-b")


def stable_trace(label: str = "phase7") -> UUID:
    return uuid5(_FIXTURE_NAMESPACE, f"trace:{label}")


def stable_fence(label: str = "phase7") -> str:
    return sha256(f"fence:{label}".encode("utf-8")).hexdigest()


def canonical_fixture(value: Any | None = None) -> dict[str, Any]:
    return {"kind": "phase7-fixture", "value": value if value is not None else "deterministic"}


def fixture_summary() -> dict[str, Any]:
    alpha, beta = data_tenants()
    node_a, node_b = data_nodes()
    return {
        "tenants": (alpha.tenant_id, beta.tenant_id),
        "nodes": (node_a.node_id, node_b.node_id),
        "utc": FIXED_DATA_UTC.isoformat(),
        "trace": str(stable_trace()),
        "fence_digest": stable_fence()[:16],
    }
