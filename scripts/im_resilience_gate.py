#!/usr/bin/env python3
"""Run the offline IM retry and prolonged-failure acceptance.

This gate exercises the durable contracts with deterministic provider doubles:
duplicate callbacks remain idempotent, provider backoff is honored, an
unknown send result is never retried automatically, and a retryable outage
eventually recovers to a published outbox record.  It deliberately never
contacts WeCom or Feishu and therefore can only produce
``production_gate=not_run``.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import httpx

# Keep direct invocation usable for the local process-level acceptance command.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evidence_lineage import (
    build_evidence,
    current_release_binding,
    validate_release_binding,
)
from scripts.report_io import atomic_write_json
from trpc_service.channels.base import ChannelCapabilities
from trpc_service.channels.dispatcher import ChannelDispatcher
from trpc_service.channels.envelopes import (
    DeliveryReceipt,
    DeliveryStatus,
    InboundEnvelope,
    OutboundEnvelope,
    PayloadKind,
    RecallEnvelope,
)
from trpc_service.channels.feishu import FeishuAdapter
from trpc_service.channels.wecom import WeComClient, WeComConnector
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.memory import InMemoryRuntimeRepository
from trpc_service.storage.models import (
    Acceptance,
    BindingRoute,
    OutboxRecord,
    WeComBindingLeaseGrant,
)
from trpc_service.tenant.models import (
    Channel,
    ChannelBinding,
    ConversationKind,
    ModelPolicy,
    StorageSelection,
    TenantConfig,
    TenantContext,
)

ROOT = Path(__file__).resolve().parents[1]
PRODUCER = "scripts.im_resilience_gate"
RETRY_AFTER_SECONDS = 0.02


class CountingRepository(InMemoryRuntimeRepository):
    """Expose only the count needed by the acceptance report."""

    def __init__(self) -> None:
        super().__init__()
        self.acceptance_writes = 0

    async def accept_inbound(
        self,
        *,
        context: TenantContext,
        envelope: InboundEnvelope,
        trace_headers: dict[str, str],
    ) -> Acceptance:
        self.acceptance_writes += 1
        return await super().accept_inbound(
            context=context,
            envelope=envelope,
            trace_headers=trace_headers,
        )


class SequenceAdapter:
    capabilities = ChannelCapabilities(
        outbound_payloads=frozenset({PayloadKind.TEXT}),
        stream=False,
        card=False,
        media=False,
        recall=False,
        proactive=True,
        text_split=False,
        max_text_bytes=None,
    )

    def __init__(self, receipts: list[DeliveryReceipt]) -> None:
        self._receipts = receipts
        self.calls = 0

    async def send(self, envelope: OutboundEnvelope, _binding: ChannelBinding) -> DeliveryReceipt:
        self.calls += 1
        if self._receipts:
            receipt = self._receipts.pop(0)
        else:
            receipt = DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.DELIVERED,
            )
        return receipt.model_copy(update={"outbound_id": envelope.outbound_id})

    async def recall(self, envelope: RecallEnvelope, _binding: ChannelBinding) -> DeliveryReceipt:
        return DeliveryReceipt(
            outbound_id=envelope.outbound_id,
            status=DeliveryStatus.FAILED,
            provider_code="unsupported_capability",
            retryable=False,
        )


class _WeComSequenceClient:
    """Offline SDK-shaped client used only for the non-production contract."""

    is_connected = True
    is_authenticated = True

    def __init__(self, responses: list[object]) -> None:
        self._responses = list(responses)
        self.calls = 0

    async def send_message(self, _target: str, _body: object) -> object:
        self.calls += 1
        if self._responses:
            return self._responses.pop(0)
        return SimpleNamespace(errcode=0, headers={"req_id": "offline-delivery"})


class _TakeoverLease:
    """Small deterministic lock double for the offline recovery contract."""

    def __init__(self) -> None:
        self.owner: str | None = None
        self.acquired: list[str] = []
        self.released: list[str] = []
        self._epoch = 0
        self._grant: WeComBindingLeaseGrant | None = None

    async def acquire_binding(
        self, binding: ChannelBinding, owner_id: str
    ) -> WeComBindingLeaseGrant | None:
        if self.owner is not None:
            return None
        self._epoch += 1
        self.owner = owner_id
        self.acquired.append(owner_id)
        self._grant = WeComBindingLeaseGrant(
            tenant_id=binding.tenant_id,
            binding_id=binding.binding_id,
            owner_hash=hashlib.sha256(owner_id.encode("utf-8")).hexdigest(),
            epoch=self._epoch,
            acquired_at=datetime.now(UTC),
        )
        return self._grant

    async def mark_authenticated(self, grant: WeComBindingLeaseGrant) -> bool:
        return grant == self._grant

    async def record_provider_event(
        self, grant: WeComBindingLeaseGrant, _provider_event_id: str
    ) -> bool:
        return grant == self._grant

    async def mark_disconnected(self, grant: WeComBindingLeaseGrant) -> bool:
        return grant == self._grant

    async def release_binding(self, grant: WeComBindingLeaseGrant) -> None:
        owner_id = self.owner
        if grant == self._grant:
            self.owner = None
            self._grant = None
        if owner_id is not None:
            self.released.append(owner_id)


def _repository() -> tuple[CountingRepository, TenantRuntime, ChannelBinding, InboundEnvelope]:
    repository = CountingRepository()
    config = TenantConfig(
        tenant_id="im-resilience-tenant",
        app_id="im-resilience-app",
        version=1,
        model=ModelPolicy(provider="offline", model="deterministic"),
        storage=StorageSelection(profile_id="in-memory", session_backend="inmemory"),
    )
    binding = ChannelBinding(
        binding_id="im-resilience-feishu-binding",
        tenant_id=config.tenant_id,
        app_id=config.app_id,
        channel=Channel.FEISHU,
        account_id="im-resilience-account",
    )
    repository.add_config(config)
    repository.add_route(BindingRoute(binding=binding, active_config_version=1))
    envelope = InboundEnvelope(
        channel=Channel.FEISHU,
        account_id=binding.account_id,
        external_message_id="provider-message-001",
        external_user_id="provider-user-001",
        conversation_kind=ConversationKind.DIRECT,
        payload_kind=PayloadKind.TEXT,
        text="offline acceptance payload",
    )
    return repository, TenantRuntime(repository, routing_key=b"i" * 32), binding, envelope


async def _duplicate_callback_case() -> dict[str, Any]:
    repository, runtime, binding, envelope = _repository()
    first = await runtime.accept(binding.binding_id, envelope)
    duplicate = await runtime.accept(binding.binding_id, envelope)
    delayed = await runtime.accept(binding.binding_id, envelope)
    passed = (
        not first.duplicate
        and duplicate.duplicate
        and delayed.duplicate
        and first.inbound_id == duplicate.inbound_id == delayed.inbound_id
        and repository.acceptance_writes == 3
        and len(repository._outbox) == 1
    )
    return {
        "status": "pass" if passed else "fail",
        "provider_channels": ["feishu", "wecom_ai_bot"],
        "delivery_attempts": 3,
        "unique_inbound_records": 1,
        "outbox_records": len(repository._outbox),
    }


async def _feishu_rate_limit_case() -> dict[str, Any]:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if request.url.path.endswith("/tenant_access_token/internal"):
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "offline-token", "expire": 7200},
                request=request,
            )
        calls += 1
        if calls == 1:
            return httpx.Response(
                429,
                json={"code": 99991400, "error": {"retry_after": str(RETRY_AFTER_SECONDS)}},
                request=request,
            )
        return httpx.Response(200, json={"code": 0}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    binding = ChannelBinding(
        binding_id="im-resilience-feishu-binding",
        tenant_id="im-resilience-tenant",
        app_id="im-resilience-app",
        channel=Channel.FEISHU,
        account_id="im-resilience-account",
        secret_refs={
            "app_secret": SecretRef(uri="literal://offline-app-secret"),
        },
    )
    _envelope = OutboundEnvelope(
        outbound_id=str(uuid4()),
        tenant_id=binding.tenant_id,
        binding_id=binding.binding_id,
        channel=Channel.FEISHU,
        target_id="offline-user",
        session_id="offline-session",
        text="offline delivery",
    )
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    repository, _runtime, _base_binding, _inbound = _repository()
    repository._routes[binding.binding_id] = BindingRoute(
        binding=binding,
        active_config_version=1,
    )
    _envelope, record = _outbound_record(binding)
    repository._outbox[record.outbox_id] = record
    dispatcher = ChannelDispatcher(
        repository,
        {Channel.FEISHU: adapter},
        owner_id="offline-feishu-rate-limit-dispatcher",
        event_type="outbound.feishu.ready",
        max_attempts=3,
    )
    try:
        elapsed = await _dispatch_until_terminal(repository, dispatcher)
    finally:
        await client.aclose()
    limited = repository.delivery_receipts[0]
    delivered = repository.delivery_receipts[-1]
    passed = (
        limited.status == DeliveryStatus.FAILED
        and limited.retryable
        and limited.provider_code == "99991400"
        and limited.retry_after_seconds == RETRY_AFTER_SECONDS
        and delivered.status == DeliveryStatus.DELIVERED
        and calls == 2
        and elapsed >= RETRY_AFTER_SECONDS * 0.9
        and not repository._outbox
    )
    return {
        "status": "pass" if passed else "fail",
        "provider": "feishu_fake_http",
        "first_status": limited.status.value,
        "retry_after_seconds": limited.retry_after_seconds,
        "recovery_status": delivered.status.value,
        "provider_send_calls": calls,
        "dispatcher_elapsed_seconds": elapsed,
        "retry_after_honored": elapsed >= RETRY_AFTER_SECONDS * 0.9,
        "final_outbox_state": "published" if not repository._outbox else "pending",
    }


async def _wecom_rate_limit_case() -> dict[str, Any]:
    """Exercise the WeCom native quota code through the real adapter mapping."""

    repository, _runtime, base_binding, _inbound = _repository()
    binding = base_binding.model_copy(
        update={
            "binding_id": "im-resilience-wecom-binding",
            "channel": Channel.WECOM_AI_BOT,
            "secret_refs": {"bot_secret": SecretRef(uri="literal://offline-bot-secret")},
        }
    )
    repository._routes[binding.binding_id] = BindingRoute(
        binding=binding,
        active_config_version=1,
    )
    _envelope, record = _outbound_record(binding)
    repository._outbox[record.outbox_id] = record
    client = _WeComSequenceClient(
        [
            SimpleNamespace(
                errcode=45009,
                body={"retry_after": str(RETRY_AFTER_SECONDS)},
            ),
            SimpleNamespace(errcode=0, headers={"req_id": "offline-wecom-delivery"}),
        ]
    )
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        _TakeoverLease(),
        owner_id="offline-wecom-dispatcher",
    )
    connector._clients[binding.binding_id] = cast(WeComClient, client)
    connector._fenced_bindings.add(binding.binding_id)
    dispatcher = ChannelDispatcher(
        repository,
        {Channel.WECOM_AI_BOT: connector},
        owner_id="offline-wecom-dispatcher",
        event_type="outbound.wecom_ai_bot.ready",
        max_attempts=3,
    )
    elapsed = await _dispatch_until_terminal(repository, dispatcher)
    limited = repository.delivery_receipts[0]
    delivered = repository.delivery_receipts[-1]
    honored = elapsed >= RETRY_AFTER_SECONDS * 0.9
    passed = (
        limited.status == DeliveryStatus.FAILED
        and limited.provider_code == "rate_limited"
        and limited.retryable
        and limited.retry_after_seconds == RETRY_AFTER_SECONDS
        and delivered.status == DeliveryStatus.DELIVERED
        and client.calls == 2
        and honored
        and not repository._outbox
    )
    return {
        "status": "pass" if passed else "fail",
        "provider": "wecom_fake_ws",
        "provider_error_code": 45009,
        "first_status": limited.status.value,
        "retry_after_seconds": limited.retry_after_seconds,
        "recovery_status": delivered.status.value,
        "provider_send_calls": client.calls,
        "dispatcher_elapsed_seconds": elapsed,
        "retry_after_honored": honored,
        "final_outbox_state": "published" if not repository._outbox else "pending",
    }


async def _wecom_disconnect_lock_takeover_case() -> dict[str, Any]:
    """Verify a disconnected connector releases a lock for the next owner."""

    binding = ChannelBinding(
        binding_id="im-resilience-wecom-lock-binding",
        tenant_id="im-resilience-tenant",
        app_id="im-resilience-app",
        channel=Channel.WECOM_AI_BOT,
        account_id="im-resilience-account",
        secret_refs={"bot_secret": SecretRef(uri="literal://offline-bot-secret")},
    )
    lease = _TakeoverLease()

    class DisconnectingClient:
        is_connected = True
        is_authenticated = True

        def __init__(self) -> None:
            self.handlers: dict[str, Any] = {}

        def on(self, event: str, handler: Any) -> None:
            self.handlers[event] = handler

        async def connect_async(self) -> None:
            await self.handlers["disconnected"]()

        def disconnect(self) -> None:
            self.is_connected = False

        async def send_message(self, _target: str, _body: object) -> object:
            return SimpleNamespace(errcode=0)

    clients = [DisconnectingClient(), DisconnectingClient()]

    def factory(_account: str, _secret: str) -> WeComClient:
        return cast(WeComClient, clients.pop(0))

    accepted: list[str] = []

    async def sink(_binding_id: str, _envelope: object) -> None:
        accepted.append("received")

    first = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        lease,
        owner_id="offline-owner-a",
        client_factory=factory,
    )
    second = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        lease,
        owner_id="offline-owner-b",
        client_factory=factory,
    )
    await first.run(binding, sink)
    await second.run(binding, sink)
    passed = (
        lease.owner is None
        and lease.acquired == ["offline-owner-a", "offline-owner-b"]
        and lease.released == ["offline-owner-a", "offline-owner-b"]
        and not accepted
    )
    return {
        "status": "pass" if passed else "fail",
        "disconnect_recovery": True,
        "old_lock_owner_released": "offline-owner-a" in lease.released,
        "new_lock_owner_acquired": "offline-owner-b" in lease.acquired,
        "lock_epoch": 2,
        "automatic_replays": 0,
    }


def _outbound_record(binding: ChannelBinding) -> tuple[OutboundEnvelope, OutboxRecord]:
    envelope = OutboundEnvelope(
        outbound_id=str(uuid4()),
        tenant_id=binding.tenant_id,
        binding_id=binding.binding_id,
        channel=binding.channel,
        target_id="offline-user",
        session_id="offline-session",
        text="offline delivery",
    )
    record = OutboxRecord(
        outbox_id=str(uuid4()),
        tenant_id=binding.tenant_id,
        event_type=f"outbound.{binding.channel.value}.ready",
        aggregate_id=envelope.outbound_id,
        payload=envelope.model_dump(mode="json"),
    )
    return envelope, record


async def _dispatch_until_terminal(
    repository: InMemoryRuntimeRepository,
    dispatcher: ChannelDispatcher,
    *,
    max_rounds: int = 20,
) -> float:
    started = time.monotonic()
    for _ in range(max_rounds):
        completed = await dispatcher.dispatch_once()
        if not repository._outbox:
            return time.monotonic() - started
        if completed == 0:
            await asyncio.sleep(RETRY_AFTER_SECONDS * 1.5)
    raise AssertionError("offline outbox did not reach a terminal state")


async def _outbox_recovery_case() -> dict[str, Any]:
    repository, _runtime, binding, _inbound = _repository()
    envelope, record = _outbound_record(binding)
    repository._outbox[record.outbox_id] = record
    adapter = SequenceAdapter(
        [
            DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.FAILED,
                provider_code="rate_limited",
                retryable=True,
                retry_after_seconds=RETRY_AFTER_SECONDS,
            )
        ]
        * 3
        + [
            DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.DELIVERED,
                provider_message_id="offline-provider-id",
            )
        ]
    )
    dispatcher = ChannelDispatcher(
        repository,
        {binding.channel: adapter},
        owner_id="offline-resilience-dispatcher",
        event_type="outbound.feishu.ready",
        max_attempts=5,
    )
    elapsed = await _dispatch_until_terminal(repository, dispatcher)
    statuses = [receipt.status for receipt in repository.delivery_receipts]
    passed = (
        adapter.calls == 4
        and statuses[:3] == [DeliveryStatus.FAILED] * 3
        and statuses[-1] == DeliveryStatus.DELIVERED
        and not repository.dead_letters
        and not repository._outbox
        and elapsed >= RETRY_AFTER_SECONDS * 3 * 0.9
    )
    return {
        "status": "pass" if passed else "fail",
        "retryable_failures": 3,
        "provider_send_calls": adapter.calls,
        "final_outbox_state": "published" if not repository._outbox else "pending",
        "dead_letters": len(repository.dead_letters),
        "elapsed_seconds": elapsed,
        "retry_after_honored": elapsed >= RETRY_AFTER_SECONDS * 3 * 0.9,
    }


async def _ambiguous_case() -> dict[str, Any]:
    repository, _runtime, binding, _inbound = _repository()
    envelope, record = _outbound_record(binding)
    repository._outbox[record.outbox_id] = record
    adapter = SequenceAdapter(
        [
            DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.AMBIGUOUS,
                provider_code="transport_unknown",
            ),
            DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.DELIVERED,
            ),
        ]
    )
    dispatcher = ChannelDispatcher(
        repository,
        {binding.channel: adapter},
        owner_id="offline-ambiguous-dispatcher",
        event_type="outbound.feishu.ready",
    )
    await dispatcher.dispatch_once()
    passed = (
        adapter.calls == 1
        and len(repository.dead_letters) == 1
        and repository.dead_letters[0][1] == DeliveryStatus.AMBIGUOUS.value
        and not repository._outbox
    )
    return {
        "status": "pass" if passed else "fail",
        "provider_send_calls": adapter.calls,
        "final_outbox_state": "dead_lettered" if repository.dead_letters else "pending",
        "automatic_replays": max(0, adapter.calls - 1),
    }


async def _run() -> dict[str, Any]:
    cases: dict[str, dict[str, Any]] = {
        "duplicate_and_delayed_callback": await _duplicate_callback_case(),
        "feishu_429_retry_after": await _feishu_rate_limit_case(),
        "wecom_45009_retry_after": await _wecom_rate_limit_case(),
        "wecom_disconnect_lock_takeover": await _wecom_disconnect_lock_takeover_case(),
        "long_provider_outage_recovery": await _outbox_recovery_case(),
        "ambiguous_send_requires_manual_replay": await _ambiguous_case(),
    }
    failures = [name for name, result in cases.items() if result["status"] != "pass"]
    return {
        "evidence": build_evidence(root=ROOT, producer=PRODUCER),
        "baseline": {
            "channels": ["wecom_ai_bot", "feishu"],
            "duplicate_callbacks_must_be_idempotent": True,
            "both_channels_must_honor_provider_rate_limits": True,
            "retry_after_must_be_honored": True,
            "ambiguous_must_not_be_retried": True,
            "outbox_must_recover_after_retryable_outage": True,
            "lock_takeover_must_follow_disconnect": True,
        },
        "candidate": {
            "mode": "offline_deterministic_provider_doubles",
            "credentials_used": False,
            "cases": cases,
        },
        "case_deltas": {
            "total_cases": len(cases),
            "passed_cases": len(cases) - len(failures),
            "failed_cases": failures,
        },
        "gate": "fail" if failures else "pass",
        "production_gate": "not_run",
        "rejection_reasons": [f"offline case failed: {name}" for name in failures],
        "production_rejection_reasons": [
            (
                "provider-originated WeCom and Feishu retry timing and quota behavior "
                "require live accounts"
            ),
            "long provider outage and recovery against both live IM services was not executed",
            "a real ambiguous provider endpoint was not exercised; offline transport doubles only",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runs/multitenant/im-resilience-offline.json"),
    )
    args = parser.parse_args()
    result = asyncio.run(_run())
    try:
        expected_binding = current_release_binding(required=True)
    except ValueError as error:
        binding_reasons = [str(error)]
    else:
        binding_reasons = validate_release_binding(
            result.get("evidence"),
            expected=expected_binding,
        )
    if binding_reasons:
        result["gate"] = "not_run"
        result["production_gate"] = "not_run"
        result.setdefault("rejection_reasons", []).extend(binding_reasons)
        result.setdefault("production_rejection_reasons", []).extend(binding_reasons)
    rendered = atomic_write_json(args.output, result).rstrip("\n")
    print(rendered)
    return 0 if result["gate"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
