"""T022 RED: end-to-end stage reconstruction per correlation identity.

Covers the local HTTP entry (success / duplicate / conflict / post-start
failure), the dual-IM offline harness (feishu + wecom with scripted
delivery outcomes) and the IM delivery lifecycle states of FR-011.
Runs fully offline: in-memory adapters, repositories and providers.
Phase-8 modules are imported lazily so a missing implementation produces
test failures, not collection errors.
"""

from __future__ import annotations

import asyncio
import importlib
from datetime import datetime, timezone
from uuid import UUID

import pytest

from tests.support import FIXED_UTC, inbound_message_data
from tests.support_channels import (
    FakeProviderClient,
    feishu_text_event,
    wecom_text_event,
)
from trpc_service.channels.base import ProviderTransientError
from trpc_service.channels.contracts import Channel, InboundMessage
from trpc_service.channels.delivery import DeliveryService, InMemoryDeliveryRepository
from trpc_service.channels.feishu import FeishuChannelAdapter
from trpc_service.channels.identity import ChannelIdentity, RuntimeBotIdentity
from trpc_service.channels.service import ChannelMessageService
from trpc_service.channels.wecom import WeComChannelAdapter
from trpc_service.metrics.inmemory import InMemoryMetricsRecorder
from trpc_service.observability import taxonomy
from trpc_service.storage.contracts import (
    AgentExecutionFailed,
    ResolvedChannelBinding,
    SecretBytes,
)
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.session_backend import SessionBackendFactory
from trpc_service.tenant.models import (
    AgentApplication,
    ChannelBinding,
    ResourceStatus,
    Tenant,
)
from trpc_service.web.app import build_runtime

NOW = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


_context = _load("trpc_service.observability.context")


def _require_phase8():
    context = _context
    service = _load("trpc_service.observability.service")
    assert context is not None, "trpc_service.observability.context is not implemented yet"
    assert service is not None, "trpc_service.observability.service is not implemented yet"
    assert hasattr(service, "TelemetryRecorder"), "TelemetryRecorder is not implemented yet"
    assert hasattr(service, "DiagnosticQueryService"), "DiagnosticQueryService is not implemented yet"
    return context, service


def trace_digest(trace_id) -> str:
    return _context.trace_digest(trace_id)


def _stage_outcomes(result: dict) -> dict[str, str]:
    return {item["stage"]: item["outcome"] for item in result["stages"]}


async def test_local_http_success_reconstructs_every_stage(runtime_secret_env: dict[str, str]) -> None:
    _require_phase8()
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    try:
        message = InboundMessage(**inbound_message_data())
        reply = await runtime.gateway.handle_verified_message_for_test(message)
        assert reply.status.value == "succeeded"

        result = await runtime.diagnostics.query(
            runtime.telemetry.scope_digest("tenant-alpha"), trace_digest(message.trace_id)
        )
        assert result["partial_telemetry"] is False
        outcomes = _stage_outcomes(result)
        assert set(outcomes) == set(taxonomy.STAGES)
        assert outcomes["gateway.accept"] == "success"
        assert outcomes["binding.resolve"] == "success"
        assert outcomes["idempotency.claim"] == "success"
        assert outcomes["session.lock"] == "success"
        assert outcomes["worker.dispatch"] == "success"
        assert outcomes["runner.invoke"] == "success"
        assert outcomes["reply.compose"] == "success"
        assert outcomes["delivery.queue"] == "success"
        assert outcomes["delivery.attempt"] == "success"
        assert outcomes["delivery.result"] == "success"
        # Stages this request never entered are explicit, never missing.
        assert outcomes["governance.evaluate"] == "not_applicable"
        assert outcomes["data.access"] == "not_applicable"
        assert outcomes["recovery.reconcile"] == "not_applicable"
        assert outcomes["adapter.receive"] == "not_applicable"
    finally:
        await runtime.close()


async def test_duplicate_delivery_distinguishes_hit_and_single_terminal(runtime_secret_env: dict[str, str]) -> None:
    _require_phase8()
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    try:
        first = InboundMessage(**inbound_message_data())
        initial = await runtime.gateway.handle_verified_message_for_test(first)
        assert initial.status.value == "succeeded"

        duplicate = InboundMessage(
            **inbound_message_data(trace_id=UUID(int=10))
        )
        repeated = await runtime.gateway.handle_verified_message_for_test(duplicate)
        assert repeated.status.value == "duplicate"

        repeat_result = await runtime.diagnostics.query(
            runtime.telemetry.scope_digest("tenant-alpha"), trace_digest(UUID(int=10))
        )
        outcomes = _stage_outcomes(repeat_result)
        assert outcomes["gateway.accept"] == "success"
        assert outcomes["idempotency.claim"] == "recovered"
        assert outcomes["runner.invoke"] == "not_applicable"

        scope = runtime.tenant_scope("tenant-alpha")
        original = [
            item.decision.value
            for item in await runtime.adapters.audit.list_by_trace(scope, first.trace_id)
        ]
        assert original == ["authorized", "execution_started", "succeeded"]
        duplicate_audit = [
            item.decision.value
            for item in await runtime.adapters.audit.list_by_trace(scope, UUID(int=10))
        ]
        assert duplicate_audit == ["duplicate"], "one business terminal only"
    finally:
        await runtime.close()


async def test_conflicting_redelivery_is_rejected_with_clear_stages(runtime_secret_env: dict[str, str]) -> None:
    _require_phase8()
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    try:
        await runtime.gateway.handle_verified_message_for_test(
            InboundMessage(**inbound_message_data())
        )
        conflict = InboundMessage(
            **inbound_message_data(text="Different content entirely.", trace_id=UUID(int=9999))
        )
        reply = await runtime.gateway.handle_verified_message_for_test(conflict)
        assert reply.status.value == "conflict"

        result = await runtime.diagnostics.query(
            runtime.telemetry.scope_digest("tenant-alpha"), trace_digest(UUID(int=9999))
        )
        outcomes = _stage_outcomes(result)
        assert outcomes["gateway.accept"] == "success"
        assert outcomes["idempotency.claim"] == "rejected"
        assert outcomes["session.lock"] == "not_applicable"
        assert outcomes["runner.invoke"] == "not_applicable"
    finally:
        await runtime.close()


async def test_post_start_failure_records_failed_runner_stage(runtime_secret_env: dict[str, str]) -> None:
    _require_phase8()
    from trpc_service.config.settings import load_settings
    from trpc_service.gateway.service import GatewayService
    from trpc_service.observability.service import DiagnosticQueryService, TelemetryRecorder

    class _FailingExecution:
        async def execute(self, timeout_seconds: int = 30):
            raise AgentExecutionFailed()

    class _FailingWorker:
        async def prepare(self, context, identity, text):
            return _FailingExecution()

        async def close(self) -> None:  # pragma: no cover - symmetry
            pass

    telemetry = TelemetryRecorder()
    diagnostics = DiagnosticQueryService(telemetry)
    adapters = InMemoryPlatformAdapters(load_settings(runtime_secret_env))
    gateway = GatewayService(
        adapters, InMemoryMetricsRecorder(), _FailingWorker(), telemetry=telemetry, now=lambda: FIXED_UTC
    )
    message = InboundMessage(**inbound_message_data(external_message_id="failure-001"))
    reply = await gateway.handle_verified_message_for_test(message)
    assert reply.status.value == "failed"

    result = await diagnostics.query(telemetry.scope_digest("tenant-alpha"), trace_digest(message.trace_id))
    outcomes = _stage_outcomes(result)
    assert outcomes["gateway.accept"] == "success"
    assert outcomes["worker.dispatch"] == "success"
    assert outcomes["runner.invoke"] == "failed"
    failed_stage = next(item for item in result["stages"] if item["stage"] == "runner.invoke")
    assert failed_stage["error_type"] == "agent_failed"
    assert failed_stage["retryable"] is False
    assert outcomes["delivery.queue"] == "not_applicable"


# ---------------------------------------------------------------------------
# Dual-IM offline harness (pattern from test_dual_im_happy_path.py).
# ---------------------------------------------------------------------------


def _im_settings(identities: dict[Channel, ChannelIdentity]):
    tenant = Tenant(tenant_id="tenant-alpha", display_name="Alpha", status=ResourceStatus.ACTIVE, created_at=NOW, config_version=1)
    agent = AgentApplication(tenant_id="tenant-alpha", agent_id="agent-alpha", agent_name="Alpha Agent", status=ResourceStatus.ACTIVE, model_profile="deterministic-offline", instruction="deterministic", config_version=1)
    bindings = tuple(
        ChannelBinding(
            binding_id=f"binding-{channel.value}", tenant_id="tenant-alpha", agent_id="agent-alpha",
            channel=channel, status=ResourceStatus.ACTIVE, secret_ref=f"{channel.value.upper()}_SECRET",
            signature_version="v1", provider_tenant_key=identity.provider_tenant_key,
            provider_app_or_bot_id=identity.provider_app_or_bot_id,
            channel_identity_digest=identity.identity_digest, created_at=NOW,
        )
        for channel, identity in identities.items()
    )
    from trpc_service.config.settings import PlatformSettings

    return PlatformSettings(tenants=(tenant,), agents=(agent,), bindings=bindings)


class _Resolver:
    def __init__(self, adapters, identities):
        self.adapters, self.identities = adapters, identities

    async def resolve_by_channel_identity(self, identity, *, external_user_id, trace_id):
        scope = VerifiedBindingScope_issue(f"binding-{identity.channel.value}", identity.channel)
        context = await self.adapters.resolve_active_context(
            scope, external_user_id=external_user_id, trace_id=trace_id
        )
        return ResolvedChannelBinding(
            scope=scope, context=context,
            secret_ref=f"{identity.channel.value.upper()}_SECRET", config_version=1,
        )


def VerifiedBindingScope_issue(binding_id: str, channel):
    from trpc_service.channels.contracts import VerifiedBindingScope

    return VerifiedBindingScope._issue(binding_id=binding_id, channel=channel)


async def _drive_dual_im_with_delivery_states() -> dict:
    _require_phase8()
    from trpc_service.gateway.service import GatewayService
    from trpc_service.observability.service import DiagnosticQueryService, TelemetryRecorder
    from trpc_service.worker.service import AgentExecutor

    identities = {
        channel: ChannelIdentity(
            channel=channel, provider_tenant_key=f"tenant-{channel.value}",
            provider_app_or_bot_id=f"bot-{channel.value}",
        )
        for channel in (Channel.FEISHU, Channel.WECOM)
    }
    telemetry = TelemetryRecorder()
    diagnostics = DiagnosticQueryService(telemetry)
    platform = InMemoryPlatformAdapters(_im_settings(identities))
    worker = AgentExecutor(SessionBackendFactory())
    gateway = GatewayService(
        platform, InMemoryMetricsRecorder(), worker, telemetry=telemetry, now=lambda: NOW
    )
    delivery = DeliveryService(
        InMemoryDeliveryRepository(now=lambda: NOW),
        telemetry=telemetry, now=lambda: NOW, sleep=lambda _delay: asyncio.sleep(0),
    )
    service = ChannelMessageService(
        _Resolver(platform, identities), gateway, delivery, telemetry=telemetry, now=lambda: NOW
    )
    adapters = []
    try:
        # Feishu: clean success path.
        feishu_provider = FakeProviderClient(RuntimeBotIdentity(
            channel=Channel.FEISHU, sender_type="bot", sender_id="runtime-feishu",
            channel_identity_digest=identities[Channel.FEISHU].identity_digest,
            authenticated_at=NOW,
        ))
        feishu_adapter = FeishuChannelAdapter(
            provider=feishu_provider, credential_secret=SecretBytes(b"credential-placeholder"),
            message_service=service, now=lambda: NOW,
        )
        adapters.append(feishu_adapter)
        await feishu_adapter.start(identities[Channel.FEISHU], NodeIdentity(node_id="node-feishu"))
        feishu_result = await feishu_adapter.handle_provider_event(feishu_text_event(text="记住验证码 ALPHA"))
        assert feishu_result.safe_code == "reply_delivered"

        # WeCom: transient failure (retryable, stable code), then success.
        wecom_provider = FakeProviderClient(RuntimeBotIdentity(
            channel=Channel.WECOM, sender_type="bot", sender_id="runtime-wecom",
            channel_identity_digest=identities[Channel.WECOM].identity_digest,
            authenticated_at=NOW,
        ))
        wecom_provider.send_results = [ProviderTransientError(), _Ack(True)]
        wecom_adapter = WeComChannelAdapter(
            provider=wecom_provider, credential_secret=SecretBytes(b"credential-placeholder"),
            message_service=service, now=lambda: NOW,
        )
        adapters.append(wecom_adapter)
        await wecom_adapter.start(identities[Channel.WECOM], NodeIdentity(node_id="node-wecom"))
        wecom_event = wecom_text_event()
        wecom_event["body"]["text"]["content"] = "记住验证码 BETA"
        wecom_result = await wecom_adapter.handle_provider_event(wecom_event)
        assert wecom_result.safe_code == "reply_delivered"
    finally:
        for adapter in adapters:
            await adapter.stop("test_complete")
        await worker.close()

    return {"telemetry": telemetry, "diagnostics": diagnostics}


class _Ack:
    def __init__(self, acknowledged: bool) -> None:
        self.acknowledged = acknowledged


@pytest.mark.asyncio
async def test_dual_im_entries_record_full_lifecycle_with_delivery_states() -> None:
    harness = await _drive_dual_im_with_delivery_states()
    telemetry = harness["telemetry"]
    diagnostics = harness["diagnostics"]
    tenant_scope = telemetry.scope_digest("tenant-alpha")
    platform_scope = _context.scope_digest_of("platform")

    # Pre-auth adapter stage is recorded under the platform scope (FR-032).
    platform_result = await diagnostics.query(platform_scope)
    platform_stages = [(item["stage"], item["outcome"]) for item in platform_result["stages"]]
    assert ("adapter.receive", "success") in platform_stages

    tenant_result = await diagnostics.query(tenant_scope)
    tenant_stages = [(item["stage"], item["outcome"]) for item in tenant_result["stages"]]
    assert ("binding.resolve", "success") in tenant_stages
    assert ("runner.invoke", "success") in tenant_stages
    # IM reply lifecycle (FR-011): queued, attempted, transient failure and success.
    assert ("delivery.queue", "success") in tenant_stages
    assert ("delivery.attempt", "failed") in tenant_stages
    assert ("delivery.attempt", "success") in tenant_stages
    assert ("delivery.result", "success") in tenant_stages
    transient = next(
        item for item in tenant_result["stages"]
        if item["stage"] == "delivery.attempt" and item["outcome"] == "failed"
    )
    assert transient["retryable"] is True
    assert transient["error_type"] == "provider_unavailable"
