from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from tests.support_channels import FakeProviderClient, dual_im_settings, feishu_text_event
from uuid import uuid4

from trpc_service.audit.models import AuditDecision, AuditRecord, PreAuthScope
from trpc_service.channels.contracts import Channel
from trpc_service.channels.feishu import FeishuChannelAdapter
from trpc_service.channels.identity import ChannelIdentity, RuntimeBotIdentity
from trpc_service.channels.service import ChannelMessageService
from trpc_service.storage.contracts import SecretBytes
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.repositories import PostgresAuditRepository


NOW = datetime(2026, 9, 8, 11, 0, tzinfo=timezone.utc)


def test_preauth_audit_schema_has_a_versioned_channel_column() -> None:
    migration = Path(
        "trpc_service/storage/postgres/migrations/004_preauth_audit_channel.sql"
    )
    sql = migration.read_text(encoding="utf-8").lower()
    assert "add column if not exists channel" in sql
    assert "drop column" not in sql


class NeverGateway:
    def __init__(self) -> None:
        self.calls = 0

    async def handle_verified_message(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError("pre-auth rejection must not reach Agent")


class NeverDelivery:
    async def deliver_reply(self, **kwargs):
        raise AssertionError("pre-auth rejection must not send")


@pytest.mark.asyncio
async def test_binding_and_sender_rejections_create_pseudonymous_preauth_audit() -> None:
    settings, identities = dual_im_settings()
    platform = InMemoryPlatformAdapters(settings)
    gateway = NeverGateway()
    service = ChannelMessageService(platform, gateway, NeverDelivery(), preauth_audit=platform.audit, now=lambda: NOW)

    unknown = ChannelIdentity(channel=Channel.FEISHU, provider_tenant_key="raw-unknown-tenant", provider_app_or_bot_id="raw-unknown-bot")
    provider = FakeProviderClient(RuntimeBotIdentity(channel=Channel.FEISHU, sender_type="bot", sender_id="runtime-bot", channel_identity_digest=unknown.identity_digest, authenticated_at=NOW))
    adapter = FeishuChannelAdapter(provider=provider, credential_secret=SecretBytes(b"credential-placeholder"), message_service=service, now=lambda: NOW)
    await adapter.start(unknown, NodeIdentity(node_id="adapter-node"))
    rejected = await adapter.handle_provider_event(feishu_text_event(message_id="raw-message-id", sender={"sender_type": "user", "sender_id": {"open_id": "raw-user-id"}}))
    assert rejected.safe_code == "binding_rejected"

    known = identities[Channel.FEISHU]
    provider2 = FakeProviderClient(RuntimeBotIdentity(channel=Channel.FEISHU, sender_type="bot", sender_id="runtime-bot", channel_identity_digest=known.identity_digest, authenticated_at=NOW))
    adapter2 = FeishuChannelAdapter(provider=provider2, credential_secret=SecretBytes(b"credential-placeholder"), message_service=service, now=lambda: NOW)
    await adapter2.start(known, NodeIdentity(node_id="adapter-node"))
    unverified = await adapter2.handle_provider_event(feishu_text_event(message_id="raw-message-id-2", sender={"sender_type": "unknown", "sender_id": {}}))
    assert unverified.safe_code == "sender_identity_unverified"

    records = await platform.audit.list_preauth(PreAuthScope())
    assert {record.decision for record in records} == {
        AuditDecision.BINDING_REJECTED,
        AuditDecision.SENDER_IDENTITY_UNVERIFIED,
    }
    assert all(record.tenant_id is None and record.audit_kind == "diagnostic" for record in records)
    rendered = "".join(record.model_dump_json() for record in records)
    for raw in ("raw-unknown-tenant", "raw-unknown-bot", "raw-user-id", "raw-message-id"):
        assert raw not in rendered
    assert gateway.calls == 0


@pytest.mark.shared_backend
@pytest.mark.asyncio
async def test_postgres_preauth_audit_round_trip_preserves_channel_and_only_digests(
    shared_database_url: str,
) -> None:
    database = PostgresDatabase(shared_database_url)
    trace_id = uuid4()
    try:
        await database.initialize_schema()
        repository = PostgresAuditRepository(database, node_id="adapter-node")
        record = AuditRecord(
            audit_id=uuid4(),
            trace_id=trace_id,
            tenant_id=None,
            channel=Channel.FEISHU,
            binding_id_digest="sha256:" + "a" * 64,
            decision=AuditDecision.BINDING_REJECTED,
            latency_ms=0,
            error_type="binding_rejected",
            cost=Decimal("0"),
            external_message_digest="sha256:" + "b" * 64,
            channel_identity_digest="c" * 64,
            provider_message_digest="sha256:" + "d" * 64,
            audit_kind="diagnostic",
            created_at=NOW,
        )
        await repository.append_diagnostic(PreAuthScope(), record)
        stored = [
            item
            for item in await repository.list_preauth(PreAuthScope())
            if item.trace_id == trace_id
        ]
        assert len(stored) == 1
        assert stored[0].channel == Channel.FEISHU
        assert stored[0].tenant_id is None
        assert stored[0].channel_identity_digest == "c" * 64
        assert stored[0].provider_message_digest == "sha256:" + "d" * 64
    finally:
        await database.close()
