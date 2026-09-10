"""Trusted correlation context: creation, binding, linking and propagation.

The correlation identity is owned by the platform.  External bodies and
untrusted metadata can never override it (FR-001); tenant scopes bind only
from verified contexts (FR-032); associations only append, never overwrite
(FR-004).  Propagation across processes uses W3C trace context on the
internal transport only.
"""

from __future__ import annotations

import re
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

from trpc_service.observability import taxonomy
from trpc_service.observability.contracts import CorrelationContextPort
from trpc_service.observability.models import TrustedCorrelationContext

_W3C_TRACEPARENT = re.compile(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$")
_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

PLATFORM_SCOPE = "platform"


def trace_digest(trace_id: UUID | str) -> str:
    """Safe external reference for a trace, format ``sha256:<16hex>``."""
    raw = sha256(f"trace-digest:{trace_id}".encode("utf-8")).hexdigest()
    return f"sha256:{raw[:16]}"


def scope_digest_of(tenant_id: str) -> str:
    """Digest of a tenant or platform scope; never the raw tenant id."""
    return sha256(f"scope:{tenant_id}".encode("utf-8")).hexdigest()[:16]


def _valid_external_trace(value: str) -> bool:
    if not isinstance(value, str) or not _UUID_PATTERN.match(value.lower()):
        return False
    parsed = UUID(value)
    return parsed.int != 0


def build_correlation(
    *,
    channel: str,
    external_message_digest: str,
    trace_id: str | None,
    role: str,
    node_id: str,
) -> TrustedCorrelationContext:
    """Synchronously build a root correlation context with validation.

    Accepts the external trace only when it complies with platform rules;
    otherwise a fresh identity is generated.  Message bodies and metadata
    never take part in this decision.
    """
    taxonomy.validate_role(role)
    if trace_id is not None and _valid_external_trace(str(trace_id)):
        request_trace = str(UUID(str(trace_id)))
    else:
        request_trace = str(uuid4())
    return TrustedCorrelationContext(
        request_trace_id=request_trace,
        tenant_scope=PLATFORM_SCOPE,
        trace_digest=trace_digest(request_trace),
        node_id=node_id,
        role=role,
    )


class CorrelationContextManager(CorrelationContextPort):
    """Async port implementation over the pure helpers above."""

    def __init__(self, *, role: str, node_id: str) -> None:
        taxonomy.validate_role(role)
        self.role = role
        self.node_id = node_id
        self.last_rebuild_reason: str | None = None

    async def start_root(
        self,
        channel: str,
        external_message_digest: str,
        *,
        untrusted_trace_id: str | None = None,
        untrusted_metadata: dict[str, Any] | None = None,
    ) -> TrustedCorrelationContext:
        # ``untrusted_metadata`` is intentionally unread: no field of an
        # untrusted payload may influence the correlation identity.
        del untrusted_metadata
        return build_correlation(
            channel=channel,
            external_message_digest=external_message_digest,
            trace_id=untrusted_trace_id,
            role=self.role,
            node_id=self.node_id,
        )

    async def bind_tenant(
        self, context: TrustedCorrelationContext, tenant_scope: str
    ) -> TrustedCorrelationContext:
        if context.tenant_scope != PLATFORM_SCOPE and context.tenant_scope != scope_digest_of(tenant_scope):
            raise ValueError("tenant_scope_invalid")
        return TrustedCorrelationContext(
            request_trace_id=context.request_trace_id,
            tenant_scope=scope_digest_of(tenant_scope),
            trace_digest=context.trace_digest,
            node_id=context.node_id,
            role=context.role,
            otel_trace_id=context.otel_trace_id,
            first_claim_trace_id=context.first_claim_trace_id,
            owner_trace_id=context.owner_trace_id,
            execution_trace_id=context.execution_trace_id,
            configuration_snapshot_id=context.configuration_snapshot_id,
            route_generation=context.route_generation,
        )

    async def link_attempt(
        self,
        context: TrustedCorrelationContext,
        execution_trace_id: str,
        *,
        first_claim_trace_id: str | None = None,
        owner_trace_id: str | None = None,
        generation: int | None = None,
    ) -> TrustedCorrelationContext:
        # Append-only: existing associations win; the root stays untouched.
        return TrustedCorrelationContext(
            request_trace_id=context.request_trace_id,
            tenant_scope=context.tenant_scope,
            trace_digest=context.trace_digest,
            node_id=context.node_id,
            role=context.role,
            otel_trace_id=context.otel_trace_id,
            first_claim_trace_id=context.first_claim_trace_id or first_claim_trace_id,
            owner_trace_id=context.owner_trace_id or owner_trace_id,
            execution_trace_id=context.execution_trace_id or execution_trace_id,
            configuration_snapshot_id=context.configuration_snapshot_id,
            route_generation=context.route_generation or generation,
        )

    async def inject(
        self,
        context: TrustedCorrelationContext,
        carrier: dict[str, str],
        *,
        transport: str = "internal",
    ) -> dict[str, str]:
        if transport != "internal":
            raise ValueError("correlation propagation is internal-transport only")
        trace_hex = _uuid_to_trace_hex(context.request_trace_id)
        span_hex = sha256(f"span:{context.request_trace_id}".encode("utf-8")).hexdigest()[:16]
        carrier = dict(carrier)
        carrier["traceparent"] = f"00-{trace_hex}-{span_hex}-01"
        return carrier

    async def extract(
        self, carrier: dict[str, str], *, transport: str = "internal"
    ) -> TrustedCorrelationContext:
        if transport != "internal":
            raise ValueError("correlation propagation is internal-transport only")
        traceparent = carrier.get("traceparent") if isinstance(carrier, dict) else None
        if traceparent is None or not _W3C_TRACEPARENT.match(str(traceparent)):
            self.last_rebuild_reason = "invalid_carrier"
            return build_correlation(
                channel="internal",
                external_message_digest="",
                trace_id=None,
                role=self.role,
                node_id=self.node_id,
            )
        trace_hex = str(traceparent).split("-")[1]
        request_trace = str(UUID(hex=trace_hex))
        self.last_rebuild_reason = None
        return TrustedCorrelationContext(
            request_trace_id=request_trace,
            tenant_scope=PLATFORM_SCOPE,
            trace_digest=trace_digest(request_trace),
            node_id=self.node_id,
            role=self.role,
        )


def bind_tenant_scope(
    context: TrustedCorrelationContext, tenant_id: str
) -> TrustedCorrelationContext:
    """Synchronously bind a verified tenant scope onto a correlation context."""
    digest = scope_digest_of(tenant_id)
    if context.tenant_scope == digest:
        return context
    return TrustedCorrelationContext(
        request_trace_id=context.request_trace_id,
        tenant_scope=digest,
        trace_digest=context.trace_digest,
        node_id=context.node_id,
        role=context.role,
        otel_trace_id=context.otel_trace_id,
        first_claim_trace_id=context.first_claim_trace_id,
        owner_trace_id=context.owner_trace_id,
        execution_trace_id=context.execution_trace_id,
        configuration_snapshot_id=context.configuration_snapshot_id,
        route_generation=context.route_generation,
    )


def _uuid_to_trace_hex(trace_id: str) -> str:
    return UUID(str(trace_id)).hex
