"""T019 RED: trusted correlation context rules (FR-001, FR-004, FR-032)."""

from __future__ import annotations

import importlib
import re
from uuid import UUID

from tests.observability_support import stable_trace_id


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


context_mod = _load("trpc_service.observability.context")

TRACEPARENT_PATTERN = re.compile(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$")
TRACE_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{16}$")


def _manager():
    return context_mod.CorrelationContextManager(role="gateway", node_id="gateway-a")


async def _root(manager, **overrides):
    kwargs = {
        "channel": "local_http",
        "external_message_digest": "sha256:" + "ab" * 32,
        "untrusted_trace_id": None,
        "untrusted_metadata": {},
    }
    kwargs.update(overrides)
    return await manager.start_root(**kwargs)


def test_context_module_exists() -> None:
    assert context_mod is not None, (
        "trpc_service.observability.context is not implemented yet"
    )


async def test_start_root_rebuilds_invalid_external_trace() -> None:
    assert context_mod is not None
    manager = _manager()
    rebuilt = await _root(manager, untrusted_trace_id="not-a-uuid-at-all")
    # The invalid external value must never be accepted (FR-001).
    rebuilt_id = UUID(str(rebuilt.request_trace_id))
    assert str(rebuilt_id) != "not-a-uuid-at-all"


async def test_start_root_accepts_platform_compliant_trace() -> None:
    assert context_mod is not None
    manager = _manager()
    trace = stable_trace_id("accepted")
    accepted = await _root(manager, untrusted_trace_id=str(trace))
    assert str(accepted.request_trace_id) == str(trace)


async def test_message_body_and_untrusted_metadata_cannot_override_identity() -> None:
    assert context_mod is not None
    manager = _manager()
    trace = stable_trace_id("root")
    ctx = await _root(
        manager,
        untrusted_trace_id=str(trace),
        untrusted_metadata={
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "tenant_id": "spoof-tenant",
            "message_text": "SENTINEL 正文覆盖尝试",
            "request_trace_id": "spoof",
        },
    )
    assert str(ctx.request_trace_id) == str(trace)
    assert ctx.trace_digest == context_mod.trace_digest(trace)
    assert "spoof" not in str(ctx.request_trace_id)
    assert ctx.tenant_scope == "platform"


async def test_bind_tenant_rejects_scope_mismatch() -> None:
    assert context_mod is not None
    manager = _manager()
    ctx = await _root(manager)
    bound = await manager.bind_tenant(ctx, "tenant-alpha")
    assert bound.tenant_scope != "platform"
    try:
        await manager.bind_tenant(bound, "tenant-beta")
    except ValueError as error:
        assert "tenant_scope_invalid" in str(error)
    else:
        raise AssertionError("bind_tenant must reject a mismatched tenant scope")


async def test_link_attempt_appends_only_trusted_associations() -> None:
    assert context_mod is not None
    manager = _manager()
    ctx = await _root(manager)
    linked = await manager.link_attempt(ctx, "exec-trace-1")
    assert linked.execution_trace_id == "exec-trace-1"
    assert linked.request_trace_id == ctx.request_trace_id
    assert linked.trace_digest == ctx.trace_digest
    # Append-only: a second link must never overwrite the first association.
    again = await manager.link_attempt(linked, "exec-trace-2")
    assert again.execution_trace_id == "exec-trace-1"


async def test_inject_extract_roundtrip_on_internal_transport() -> None:
    assert context_mod is not None
    manager = _manager()
    ctx = await _root(manager)
    carrier = await manager.inject(ctx, {})
    traceparent = carrier["traceparent"]
    assert TRACEPARENT_PATTERN.match(traceparent), traceparent
    extracted = await manager.extract(carrier)
    assert extracted.request_trace_id == ctx.request_trace_id


async def test_extract_with_invalid_carrier_rebuilds_root() -> None:
    assert context_mod is not None
    manager = _manager()
    ctx = await _root(manager)
    extracted = await manager.extract({"traceparent": "garbage"})
    assert extracted.request_trace_id != ctx.request_trace_id
    UUID(str(extracted.request_trace_id))  # must be a valid new identity
    assert manager.last_rebuild_reason == "invalid_carrier"


async def test_inject_extract_reject_non_internal_transport() -> None:
    assert context_mod is not None
    manager = _manager()
    ctx = await _root(manager)
    for call in (
        lambda: manager.inject(ctx, {}, transport="external"),
        lambda: manager.extract({}, transport="external"),
    ):
        try:
            await call()
        except ValueError:
            continue
        raise AssertionError("correlation propagation must be internal-transport only")


def test_trace_digest_is_safe_reference_format() -> None:
    assert context_mod is not None
    digest = context_mod.trace_digest(stable_trace_id("digest-check"))
    assert TRACE_DIGEST_PATTERN.match(digest), digest
    other = context_mod.trace_digest(stable_trace_id("digest-check-2"))
    assert digest != other
