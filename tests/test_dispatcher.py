from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tenant_agent.container import ApplicationContainer
from tenant_agent.models import (
    Attachment,
    AuditPolicy,
    BackendKind,
    BackendRef,
    ChannelType,
    RedactionPolicy,
    SecretRef,
    UsageDelta,
)
from tenant_agent.settings import Settings
from tests.helpers import make_envelope, make_tenant


async def test_container(node_id: str = "node-1") -> ApplicationContainer:
    settings = Settings(
        node_id=node_id,
        control_database_url="inmemory://",
        bootstrap_config_path=None,
        session_hmac_key="a-long-enough-test-session-hmac-key",
        admin_bearer_token="admin-test-token",
        internal_bearer_token="internal-test-token",
        model_timeout_seconds=5,
    )
    container = ApplicationContainer.build(settings)
    await container.initialize()
    return container


@pytest.mark.asyncio
async def test_end_to_end_turn_is_idempotent_and_updates_all_resources() -> None:
    container = await test_container()
    tenant = make_tenant(summary_every_events="2")
    await container.configs.create_version(tenant, actor="test", activate=True)
    envelope = make_envelope(
        tenant,
        text="email me at alice@example.com",
        message_id="message-1",
    )
    routed = container.gateway.route(envelope, tenant)

    first = await container.dispatcher.process(tenant=tenant, routed=routed)
    duplicate = await container.dispatcher.process(tenant=tenant, routed=routed)
    assert first.status == "processed"
    assert duplicate.status == "duplicate_completed"
    assert duplicate.responses == first.responses
    assert "alice@example.com" not in first.responses[0].text

    plane = await container.storage.for_tenant(tenant)
    events = await plane.sessions.list_events(tenant.tenant_id, routed.session_id)
    assert [event.sequence for event in events] == [1, 2]
    assert [event.kind for event in events] == ["user_message", "assistant_message"]
    summary = await plane.summaries.get_summary(tenant.tenant_id, routed.session_id)
    assert summary is not None and summary.through_event_sequence == 2
    memories = await plane.memories.search_memory(tenant.tenant_id, routed.internal_user_id, "Assistant")
    assert len(memories) == 1
    audit = await plane.audit.query_audit(tenant.tenant_id)
    assert audit[0].decision == "allowed"
    assert container.control._usage_reservations == {}  # type: ignore[attr-defined]
    await container.close()


@pytest.mark.asyncio
async def test_summary_and_memory_failures_enqueue_and_complete_durable_repairs() -> None:
    container = await test_container()
    base = make_tenant(summary_every_events="2")
    tenant = base.model_copy(
        update={
            "governance": base.governance.model_copy(
                update={
                    "redaction": base.governance.redaction.model_copy(update={"redact_before_model": True})
                }
            )
        }
    )
    await container.configs.create_version(tenant, actor="test", activate=True)
    plane = await container.storage.for_tenant(tenant)
    original_summary = plane.summaries.put_summary
    original_memory = plane.memories.put_memory

    async def fail_summary(record: object) -> None:
        del record
        raise RuntimeError("summary backend unavailable")

    async def fail_memory(record: object) -> None:
        del record
        raise RuntimeError("memory backend unavailable")

    plane.summaries.put_summary = fail_summary  # type: ignore[method-assign]
    plane.memories.put_memory = fail_memory  # type: ignore[method-assign]
    envelope = make_envelope(
        tenant,
        text="email alice@example.com",
        message_id="repair-message",
    ).model_copy(
        update={
            "attachments": (
                Attachment(
                    kind="file",
                    external_id="provider-file-id",
                    filename="report.pdf",
                    mime_type="application/pdf",
                    size_bytes=42,
                ),
            )
        }
    )
    routed = container.gateway.route(envelope, tenant)
    result = await container.dispatcher.process(tenant=tenant, routed=routed)
    assert result.status == "processed"
    repairs = [
        item
        for item in container.control._outbox.values()  # type: ignore[attr-defined]
        if item.kind == "auxiliary-repair"
    ]
    assert {item.payload["resource"] for item in repairs} == {"summary", "memory"}

    # Simulate a pre-effective_text event so repair must safely reconstruct the
    # governed input (including attachment projection) instead of using raw text.
    event_rows = plane.sessions._events[(tenant.tenant_id, routed.session_id)]  # type: ignore[attr-defined]
    inbound = event_rows[0]
    legacy_payload = dict(inbound.payload)
    legacy_payload.pop("effective_text")
    event_rows[0] = inbound.model_copy(update={"payload": legacy_payload})

    plane.summaries.put_summary = original_summary  # type: ignore[method-assign]
    plane.memories.put_memory = original_memory  # type: ignore[method-assign]
    assert await container.outbox.run_once(limit=10) == 2
    summary = await plane.summaries.get_summary(tenant.tenant_id, routed.session_id)
    assert summary is not None and summary.through_event_sequence == 2
    memories = await plane.memories.search_memory(
        tenant.tenant_id,
        routed.internal_user_id,
        "REDACTED",
    )
    assert len(memories) == 1
    assert "alice@example.com" not in memories[0].content
    assert "[REDACTED]" in memories[0].content
    assert "report.pdf" in memories[0].content
    assert "provider-file-id" not in memories[0].content
    assert all(
        container.control._outbox[item.outbox_id].status == "completed"  # type: ignore[attr-defined]
        for item in repairs
    )
    await container.close()


@pytest.mark.asyncio
async def test_concurrent_messages_for_one_session_are_serialized() -> None:
    container = await test_container()
    tenant = make_tenant(summary_every_events="100")
    await container.configs.create_version(tenant, actor="test", activate=True)
    routed_one = container.gateway.route(make_envelope(tenant, text="one", message_id="one"), tenant)
    routed_two = container.gateway.route(make_envelope(tenant, text="two", message_id="two"), tenant)
    assert routed_one.session_id == routed_two.session_id
    results = await asyncio.gather(
        container.dispatcher.process(tenant=tenant, routed=routed_one),
        container.dispatcher.process(tenant=tenant, routed=routed_two),
    )
    assert {result.status for result in results} == {"processed"}
    plane = await container.storage.for_tenant(tenant)
    events = await plane.sessions.list_events(tenant.tenant_id, routed_one.session_id)
    assert [event.sequence for event in events] == [1, 2, 3, 4]
    assert [event.kind for event in events] in (
        ["user_message", "assistant_message", "user_message", "assistant_message"],
    )
    await container.close()


@pytest.mark.asyncio
async def test_same_platform_message_id_on_two_bindings_cannot_collide_in_sql_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_binding = make_tenant().channels[0]
    second_binding = first_binding.model_copy(
        update={
            "binding_id": "web-binding-002",
            "external_account_id": "alpha-account-2",
        }
    )
    tenant = make_tenant().model_copy(update={"channels": (first_binding, second_binding)})
    sql_url = f"sqlite+aiosqlite:///{tmp_path}/events.db"
    monkeypatch.setenv("TENANT_ALPHA_EVENT_SQL", sql_url)
    monkeypatch.setenv(
        "TENANT_ALPHA_NATIVE_EVENT_SQL",
        f"sqlite+aiosqlite:///{tmp_path}/native-events.db",
    )
    sql_backend = BackendRef(
        kind=BackendKind.SQL,
        dsn_ref=SecretRef(uri="env://TENANT_ALPHA_EVENT_SQL"),
        native_dsn_ref=SecretRef(uri="env://TENANT_ALPHA_NATIVE_EVENT_SQL"),
    )
    tenant = tenant.model_copy(
        update={"data_backends": tenant.data_backends.model_copy(update={"session": sql_backend})}
    )
    container = await test_container()
    await container.configs.create_version(tenant, actor="test", activate=True)
    first_envelope = make_envelope(
        tenant,
        user_id="user-one",
        message_id="same-platform-id",
    )
    second_envelope = first_envelope.model_copy(
        update={
            "binding_id": second_binding.binding_id,
            "external_account_id": second_binding.external_account_id,
            "external_user_id": "user-two",
            "external_chat_id": "chat-two",
        }
    )
    first_routed = container.gateway.route(first_envelope, tenant)
    second_routed = container.gateway.route(second_envelope, tenant)

    await container.dispatcher.process(tenant=tenant, routed=first_routed)
    await container.dispatcher.process(tenant=tenant, routed=second_routed)

    plane = await container.storage.for_tenant(tenant)
    assert len(await plane.sessions.list_events("alpha", first_routed.session_id)) == 2
    assert len(await plane.sessions.list_events("alpha", second_routed.session_id)) == 2
    await container.close()


@pytest.mark.asyncio
async def test_external_im_response_uses_transactional_outbox() -> None:
    container = await test_container()
    tenant = make_tenant(channel=ChannelType.TELEGRAM)
    await container.configs.create_version(tenant, actor="test", activate=True)
    routed = container.gateway.route(
        make_envelope(tenant, text="hello", message_id="telegram-update-1"), tenant
    )
    result = await container.dispatcher.process(tenant=tenant, routed=routed)
    assert result.status == "processed"
    items = await container.control.claim_outbox(  # type: ignore[attr-defined]
        "delivery-test", limit=10, now=datetime.now(UTC)
    )
    assert len(items) == 1
    assert items[0].payload["message"]["channel"] == "telegram"
    await container.close()


@pytest.mark.asyncio
async def test_turn_audit_failure_preserves_completed_business_outcome() -> None:
    container = await test_container()
    tenant = make_tenant()
    await container.configs.create_version(tenant, actor="test", activate=True)
    plane = await container.storage.for_tenant(tenant)

    async def fail_audit(record: object) -> None:
        del record
        raise RuntimeError("audit unavailable")

    plane.audit.append_audit = fail_audit  # type: ignore[method-assign]
    routed = container.gateway.route(
        make_envelope(tenant, text="hello", message_id="turn-audit-failure"),
        tenant,
    )
    result = await container.dispatcher.process(tenant=tenant, routed=routed)
    duplicate = await container.dispatcher.process(tenant=tenant, routed=routed)

    assert result.status == "processed"
    assert duplicate.status == "duplicate_completed"
    assert duplicate.responses == result.responses
    assert len(await plane.sessions.list_events(tenant.tenant_id, routed.session_id)) == 2
    await container.close()


@pytest.mark.asyncio
async def test_audit_content_uses_mandatory_redaction_even_when_output_policy_is_disabled() -> None:
    container = await test_container()
    base = make_tenant()
    tenant = base.model_copy(
        update={
            "audit": AuditPolicy(include_content=True),
            "governance": base.governance.model_copy(
                update={
                    "redaction": RedactionPolicy(
                        redact_email=False,
                        redact_phone=False,
                        redact_credentials=False,
                    )
                }
            ),
        }
    )
    await container.configs.create_version(tenant, actor="test", activate=True)
    secret = "api_key=super-secret-value"
    routed = container.gateway.route(
        make_envelope(tenant, text=f"contact alice@example.com {secret}", message_id="audit-redact"),
        tenant,
    )
    await container.dispatcher.process(tenant=tenant, routed=routed)
    plane = await container.storage.for_tenant(tenant)
    audit = await plane.audit.query_audit(tenant.tenant_id)
    stored_prompt = str(audit[0].details["prompt"])
    assert "alice@example.com" not in stored_prompt
    assert "super-secret-value" not in stored_prompt
    await container.close()


@pytest.mark.asyncio
async def test_tenant_concurrency_limit_returns_retryable_capacity_message() -> None:
    container = await test_container()
    base = make_tenant()
    limited = base.model_copy(
        update={
            "governance": base.governance.model_copy(
                update={"budget": base.governance.budget.model_copy(update={"max_concurrent_sessions": 1})}
            )
        }
    )
    await container.configs.create_version(limited, actor="test", activate=True)
    assert await container.control.acquire_tenant_slot(  # type: ignore[attr-defined]
        tenant_id="alpha",
        owner="held",
        limit=1,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=30),
    )
    routed = container.gateway.route(make_envelope(limited, message_id="capacity-message"), limited)
    result = await container.dispatcher.process(tenant=limited, routed=routed)
    assert result.status == "capacity_limited"
    assert "concurrent-session limit" in result.responses[0].text
    assert container.control._usage_reservations == {}  # type: ignore[attr-defined]
    await container.control.release_tenant_slot(  # type: ignore[attr-defined]
        tenant_id="alpha", owner="held"
    )
    await container.close()


@pytest.mark.asyncio
async def test_post_model_recovery_does_not_rerun_agent_or_double_usage() -> None:
    container = await test_container()
    tenant = make_tenant(summary_every_events="100")
    await container.configs.create_version(tenant, actor="test", activate=True)
    envelope = make_envelope(tenant, message_id="recover-message", text="recover me")
    routed = container.gateway.route(envelope, tenant)
    first = await container.dispatcher.process(tenant=tenant, routed=routed)
    assert first.status == "processed"
    later_envelope = make_envelope(
        tenant,
        message_id="recover-later-message",
        text="later turn",
    )
    later_routed = container.gateway.route(later_envelope, tenant)
    assert later_routed.session_id == routed.session_id
    assert (await container.dispatcher.process(tenant=tenant, routed=later_routed)).status == "processed"

    dedupe_key = container.identities.dedupe_key(envelope)
    container.control._receipts.pop((tenant.tenant_id, dedupe_key))  # type: ignore[attr-defined]
    container.control._usage.clear()  # type: ignore[attr-defined]
    period = datetime.now(UTC).strftime("%Y-%m")
    stale_reservation = await container.control.reserve_usage(  # type: ignore[attr-defined]
        tenant_id=tenant.tenant_id,
        reservation_id=dedupe_key,
        period=period,
        reserved_tokens=1,
        reserved_cost_usd=0.0,
        token_limit=tenant.governance.budget.monthly_tokens * 2,
        cost_limit_usd=tenant.governance.budget.monthly_cost_usd,
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    assert stale_reservation.acquired
    await container.control.add_usage(  # type: ignore[attr-defined]
        tenant.tenant_id,
        period,
        UsageDelta(input_tokens=tenant.governance.budget.monthly_tokens),
    )
    plane = await container.storage.for_tenant(tenant)
    # Remove the first projection so recovery must rebuild it while the Session
    # snapshot already contains the later turn.
    first_memory_key = next(
        key
        for key, record in plane.memories._memories.items()  # type: ignore[attr-defined]
        if record.metadata.get("message_id") == envelope.message_id
    )
    plane.memories._memories.pop(first_memory_key)  # type: ignore[attr-defined]

    class MustNotRun:
        async def stream(self, **kwargs: object) -> object:
            del kwargs
            raise AssertionError("agent was rerun after its output event committed")
            yield

        async def close(self) -> None:
            return None

    container.engines.deterministic = MustNotRun()
    async with plane.leases.acquire_session(
        tenant_id=tenant.tenant_id,
        session_id=routed.session_id,
        owner="blocked-newer-turn",
        wait_timeout=1,
        lease_seconds=5,
    ):
        recovery_task = asyncio.create_task(container.dispatcher.process(tenant=tenant, routed=routed))
        await asyncio.sleep(0.01)
        assert not recovery_task.done()
    recovered = await recovery_task
    assert recovered.status == "recovered"
    assert recovered.responses == first.responses
    assert len(await plane.sessions.list_events(tenant.tenant_id, routed.session_id)) == 4
    recovered_memory = next(
        record
        for record in plane.memories._memories.values()  # type: ignore[attr-defined]
        if record.metadata.get("message_id") == envelope.message_id
    )
    assert recovered_memory.metadata["through_event_sequence"] == 2
    usage = await plane.usage.get_usage(tenant.tenant_id, period)
    assert usage.total_tokens > tenant.governance.budget.monthly_tokens
    assert container.control._usage_reservations == {}  # type: ignore[attr-defined]
    await container.close()
