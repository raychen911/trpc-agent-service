"""T023 RED: DiagnosticQueryPort authorization, isolation and outage honesty.

Authorization happens BEFORE any store read; a minimal access audit is
written first; tenant scopes never see each other's data; a telemetry
outage yields an explicit ``partial_telemetry`` marker instead of a fake
complete trace (FR-016, FR-032, DEC-002).
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone

from trpc_service.observability import taxonomy
from trpc_service.operations import operations_errors


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


service_mod = _load("trpc_service.observability.service")

_NOW = datetime(2026, 9, 10, 12, 0, 0, tzinfo=timezone.utc)


def _span(scope_digest: str, trace_digest: str, stage: str, outcome: str):
    return service_mod.RecordedStage(
        trace_digest=trace_digest,
        scope_digest=scope_digest,
        stage=stage,
        outcome=outcome,
        component=taxonomy.stage_component(stage),
        error_type=None,
        retryable=False,
        role="gateway",
        node_digest="0123456789abcdef",
        start_ns=1,
        end_ns=2,
    )


class _FakeStore:
    def __init__(self, spans, *, degraded: bool = False) -> None:
        self.spans = spans
        self.degraded = degraded
        self.read_count = 0

    def spans_for(self, scope_digest: str, trace_digest: str | None = None):
        self.read_count += 1
        if self.degraded:
            return []
        return [
            span
            for span in self.spans
            if span.scope_digest == scope_digest
            and (trace_digest is None or span.trace_digest == trace_digest)
        ]


def test_service_module_exists() -> None:
    assert service_mod is not None, (
        "trpc_service.observability.service is not implemented yet"
    )


async def test_unauthorized_scope_is_denied_before_any_store_read() -> None:
    assert service_mod is not None
    store = _FakeStore([_span("scope-a", "sha256:aaaa", "gateway.accept", "success")])
    audits: list[str] = []

    async def _audit(scope_digest: str, trace_digest: str | None) -> None:
        audits.append(scope_digest)

    query = service_mod.DiagnosticQueryService(
        store, authorizer=lambda scope: scope == "scope-a", access_audit=_audit
    )
    try:
        await query.query("scope-b")
    except operations_errors.DiagnosticAccessDenied:
        pass
    else:
        raise AssertionError("unauthorized diagnostic query must be denied")
    assert store.read_count == 0, "authorization must precede any store access"


async def test_minimal_access_audit_precedes_the_store_read() -> None:
    assert service_mod is not None
    store = _FakeStore([_span("scope-a", "sha256:aaaa", "gateway.accept", "success")])
    order: list[str] = []

    async def _audit(scope_digest: str, trace_digest: str | None) -> None:
        order.append("audit")

    class _OrderedStore(_FakeStore):
        def spans_for(self, scope_digest, trace_digest=None):
            order.append("read")
            return super().spans_for(scope_digest, trace_digest)

    ordered = _OrderedStore(store.spans)
    query = service_mod.DiagnosticQueryService(ordered, access_audit=_audit)
    await query.query("scope-a")
    assert order == ["audit", "read"], "access audit must be written before querying"


async def test_tenant_scope_isolation_and_cross_tenant_emptiness() -> None:
    assert service_mod is not None
    spans = [
        _span("scope-alpha", "sha256:aaaa", "gateway.accept", "success"),
        _span("scope-alpha", "sha256:aaaa", "runner.invoke", "success"),
        _span("scope-beta", "sha256:bbbb", "gateway.accept", "success"),
    ]
    store = _FakeStore(spans)
    query = service_mod.DiagnosticQueryService(store)
    result = await query.query("scope-alpha")
    assert all(item["scope_digest"] == "scope-alpha" for item in result["stages"])
    assert {item["stage"] for item in result["stages"]} == {"gateway.accept", "runner.invoke"}
    empty = await query.query("scope-carol")
    assert empty["stages"] == [], "cross-tenant queries return an empty set"


async def test_telemetry_outage_reports_partial_telemetry_honestly() -> None:
    assert service_mod is not None
    degraded = _FakeStore([_span("scope-a", "sha256:aaaa", "gateway.accept", "success")], degraded=True)
    query = service_mod.DiagnosticQueryService(degraded)
    result = await query.query("scope-a", "sha256:aaaa")
    assert result["partial_telemetry"] is True
    assert result["stages"] == [], "an outage must not fabricate a complete trace"
    assert result["evidence_complete"] is False


async def test_per_trace_query_fills_unentered_stages_as_not_applicable() -> None:
    assert service_mod is not None
    store = _FakeStore([_span("scope-a", "sha256:aaaa", "gateway.accept", "success")])
    query = service_mod.DiagnosticQueryService(store)
    result = await query.query("scope-a", "sha256:aaaa")
    outcomes = {item["stage"]: item["outcome"] for item in result["stages"]}
    assert set(outcomes) == set(taxonomy.STAGES)
    assert outcomes["gateway.accept"] == "success"
    assert outcomes["runner.invoke"] == "not_applicable"
    assert outcomes["recovery.reconcile"] == "not_applicable"
    assert result["partial_telemetry"] is False
    assert result["evidence_complete"] is True
