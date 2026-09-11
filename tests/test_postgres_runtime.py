"""Optional real-service tests for the shared PostgreSQL + Redis runtime.

Run only against an isolated database that has already been migrated:

``TRPC_TEST_DATABASE_URL=... TRPC_TEST_REDIS_URL=... pytest -m integration``.
"""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest

from trpc_service.channels import CallbackRequest
from trpc_service.config import AppSettings
from trpc_service.db import PostgresControlPlane
from trpc_service.runtime import (
    BudgetAccount,
    CommitInput,
    InboundEnvelope,
    PlatformRuntime,
    PostgresRuntimeStore,
    RedisStreamMessageBus,
    ReplyDraft,
    TenantContext,
)
from trpc_service.web.app import ServiceContainer
from trpc_service.web.schemas import AgentCreate, ChannelCreate, ReleaseCreate, TenantCreate

pytestmark = pytest.mark.integration


def _settings() -> tuple[str, str]:
    database_url = os.environ.get("TRPC_TEST_DATABASE_URL")
    redis_url = os.environ.get("TRPC_TEST_REDIS_URL")
    if not database_url or not redis_url:
        pytest.skip("set TRPC_TEST_DATABASE_URL and TRPC_TEST_REDIS_URL to run integration tests")
    # A source-only psycopg install without libpq raises ImportError during
    # module initialisation.  Treat that exactly like an absent optional
    # integration dependency; the test will still run in the production image
    # and explicitly configured integration environments.
    pytest.importorskip("psycopg", exc_type=ImportError)
    redis = pytest.importorskip("redis")
    try:
        redis.Redis.from_url(redis_url).ping()
    except Exception as exc:
        pytest.skip(f"Redis test endpoint is unavailable: {exc}")
    return database_url, redis_url


def _seed(control: PostgresControlPlane, tenant_id: str) -> None:
    control.create_tenant(tenant_id, "Postgres integration tenant")
    control.create_agent(tenant_id, "agent", "Integration agent")
    control.create_release(
        tenant_id,
        "agent",
        1,
        app_config={"system_prompt": "test"},
        model_config={"mode": "mock"},
        tool_policy={},
        knowledge_config={},
        created_by="test",
        change_reason="integration seed",
    )
    control.activate_release(tenant_id, "agent", 1)
    control.create_binding(
        tenant_id,
        binding_id="binding",
        agent_id="agent",
        provider="mock",
        # The schema protects a provider account from being bound to multiple
        # tenants, so every integration fixture needs its own account identity.
        external_account_id=f"integration-{tenant_id}",
        webhook_key=f"integration-{uuid4().hex}",
        secret_ref="test-secret",
        capabilities={},
    )


def test_postgres_inbox_outbox_redis_and_delivery_transition_are_shared():
    database_url, redis_url = _settings()
    tenant_id = f"it_{uuid4().hex}"
    control = PostgresControlPlane(database_url)
    _seed(control, tenant_id)
    store = PostgresRuntimeStore(database_url)
    bus = RedisStreamMessageBus(redis_url, stream=f"trpc-agent:test:{uuid4().hex}")
    runtime = PlatformRuntime(store, bus)
    context = TenantContext(tenant_id, request_id="integration", trace_id="a" * 32)
    runtime.put_budget_account(
        context, BudgetAccount(tenant_id, "model_tokens", "tokens", limit_units=100)
    )
    envelope = InboundEnvelope(
        tenant_id=tenant_id,
        channel_binding_id="binding",
        agent_id="agent",
        session_id="session",
        idempotency_key="provider:1",
        external_message_id="provider:1",
        subject_id="sandbox-user",
        payload={"text": "hello", "recipient_id": "sandbox-user"},
        config_version=1,
        request_id="integration",
        trace_id="a" * 32,
    )
    accepted = runtime.accept_inbound(context, envelope)
    duplicate = runtime.accept_inbound(context, envelope)
    assert duplicate.duplicate is True
    assert duplicate.inbox.inbox_id == accepted.inbox.inbox_id

    published = runtime.dispatch_once(context, "dispatcher-it")
    assert [event.outbox_id for event in published] == [accepted.outbox.outbox_id]
    group = f"test-workers-{uuid4().hex}"
    stream_rows = bus.consume(group, "worker-it", block_ms=1)
    assert any(record["outbox_id"] == accepted.outbox.outbox_id for _, record in stream_rows)
    for message_id, _ in stream_rows:
        bus.acknowledge(group, message_id)

    claim = runtime.claim_execution(
        context, accepted.inbox.inbox_id, "worker-it", budget_estimates={"model_tokens": 3}
    )
    committed = runtime.commit_execution(
        context,
        claim,
        CommitInput(
            expected_session_version=claim.session_version,
            new_state={"completed": True},
            reply=ReplyDraft(
                blocks=[{"type": "text", "text": "hello from the shared runtime"}],
                channel_binding_id="binding",
                recipient_id="sandbox-user",
            ),
            actual_budget_units={"model_tokens": 3},
        ),
    )
    assert committed.reply_outbox is not None
    runtime.dispatch_once(context, "dispatcher-it")
    runtime.mark_outbox_delivered(context, committed.reply_outbox.outbox_id)

    reopened = PlatformRuntime(PostgresRuntimeStore(database_url), bus)
    snapshot = reopened.snapshot(context)
    assert snapshot["inboxes"][0]["status"] == "delivered"
    assert any(row["status"] == "delivered" for row in snapshot["outbox"])


def test_postgres_rls_cannot_read_another_tenant():
    database_url, _ = _settings()
    psycopg = pytest.importorskip("psycopg", exc_type=ImportError)
    first_tenant, second_tenant = f"rls_{uuid4().hex}", f"rls_{uuid4().hex}"
    control = PostgresControlPlane(database_url)
    _seed(control, first_tenant)
    _seed(control, second_tenant)

    with psycopg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://", 1)) as conn:
        with conn.transaction():
            conn.execute("SET LOCAL ROLE agent_worker")
            conn.execute("SELECT pg_catalog.set_config('app.tenant_id', %s, true)", (first_tenant,))
            first_rows = conn.execute("SELECT tenant_id FROM tenant ORDER BY tenant_id").fetchall()
            conn.execute("SELECT pg_catalog.set_config('app.tenant_id', %s, true)", (second_tenant,))
            second_rows = conn.execute("SELECT tenant_id FROM tenant ORDER BY tenant_id").fetchall()
    assert [row[0] for row in first_rows] == [first_tenant]
    assert [row[0] for row in second_rows] == [second_tenant]


def test_postgres_service_container_wires_callback_worker_dispatcher_and_delivery():
    """Exercise the same durable adapters used by the API, worker, and dispatcher CLIs."""

    database_url, redis_url = _settings()
    tenant_id = f"service_{uuid4().hex}"
    webhook_key = f"service-{uuid4().hex}"
    services = ServiceContainer(
        AppSettings(
            runtime_backend="postgres",
            database_url=database_url,
            redis_url=redis_url,
            mock_model=True,
            worker_id=f"worker-{uuid4().hex}",
            dispatcher_id=f"dispatcher-{uuid4().hex}",
        )
    )
    services.create_tenant(TenantCreate(tenant_id=tenant_id, display_name="Service integration"))
    services.create_agent(tenant_id, AgentCreate(agent_id="agent", name="Integration agent"))
    services.create_release(
        tenant_id,
        "agent",
        ReleaseCreate(version=1, model_config={"mode": "mock"}),
    )
    services.activate_release(tenant_id, "agent", 1)
    services.create_binding(
        tenant_id,
        ChannelCreate(
            binding_id="binding",
            agent_id="agent",
            provider="mock",
            external_account_id=f"service-{tenant_id}",
            webhook_key=webhook_key,
            capabilities={"callback_secret": "test-secret"},
        ),
    )

    async def scenario() -> None:
        accepted = await services.accept_callback(
            "mock",
            webhook_key,
            CallbackRequest(
                body={"message_id": "message-1", "user_id": "sandbox-user", "text": "ticket 42"},
                headers={"x-mock-secret": "test-secret"},
            ),
            request_id="service-integration",
            trace_id="b" * 32,
        )
        assert accepted["duplicate"] is False
        assert tenant_id in services.dispatch_all()
        processed = await services.process_published()
        assert any(row["inbox_id"] == accepted["inbox_id"] for row in processed)
        services.dispatch_all()
        deliveries = await services.process_delivery_published()
        assert any(row["status"] == "accepted" for row in deliveries)

    asyncio.run(scenario())
    snapshot = services.runtime.snapshot(TenantContext(tenant_id))
    assert snapshot["inboxes"][0]["status"] == "delivered"
