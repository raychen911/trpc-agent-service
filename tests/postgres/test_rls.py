"""PostgreSQL-only RLS and append-only audit contracts."""

from __future__ import annotations

import asyncio
import hashlib
import os

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError

from trpc_service.reliability import (
    EventData,
    InboxEnvelope,
    ReliabilityRepository,
    StaleClaimError,
    canonical_json_hash,
)
from trpc_service.runtime import EventStoreError, SqlEventObjectStore
from trpc_service.storage import Database
from trpc_service.storage.models import AuditLog, EventObject, Session, Tenant, new_id
from trpc_service.tenant.models import TenantSpec
from trpc_service.tenant.service import TenantConfigService

pytestmark = pytest.mark.postgres


def _runtime_url() -> str:
    url = os.environ.get("TEST_POSTGRES_URL")
    if not url:
        pytest.skip("TEST_POSTGRES_URL is not configured")
    return url


def _spec(tenant_id: str) -> TenantSpec:
    suffix = tenant_id.removeprefix("tenant-")
    return TenantSpec.model_validate(
        {
            "tenant_id": tenant_id,
            "revision": 1,
            "display_name": tenant_id,
            "apps": [
                {
                    "app_id": "assistant",
                    "revision": 1,
                    "name": "assistant_agent",
                    "prompt": "Safe prompt",
                    "model": {"provider": "mock", "model": "deterministic"},
                }
            ],
            "channels": [
                {
                    "binding_id": f"binding-{suffix}",
                    "app_id": "assistant",
                    "app_revision": 1,
                    "channel": "telegram",
                    "external_account_id": f"bot-{suffix}",
                    "callback_path": f"/v1/channels/telegram/callback-{suffix}/callback",
                    "public_callback_id": f"callback-{suffix}",
                    "secret_refs": {
                        "bot_token": "secret://env/TEST_BOT_TOKEN",
                        "webhook_secret": "secret://env/TEST_WEBHOOK_SECRET",
                    },
                }
            ],
        }
    )


@pytest.fixture
async def database():
    value = Database(_runtime_url())
    try:
        yield value
    finally:
        await value.dispose()


@pytest.fixture
async def seeded_database(database: Database):
    service = TenantConfigService(database.session_factory)
    await service.publish(_spec("tenant-rls-a"), actor="test")
    await service.publish(_spec("tenant-rls-b"), actor="test")
    return database


async def test_force_rls_hides_other_tenants_and_unscoped_reads(
    seeded_database: Database,
) -> None:
    database = seeded_database

    async with database.tenant_transaction("tenant-rls-a") as session:
        assert await session.scalar(select(func.count()).select_from(Tenant)) == 1
        assert await session.get(Tenant, "tenant-rls-b") is None
    async with database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Tenant)) == 0


async def test_rls_rejects_cross_tenant_write(seeded_database: Database) -> None:
    with pytest.raises(DBAPIError):
        async with seeded_database.tenant_transaction("tenant-rls-a") as session:
            session.add(
                Tenant(
                    tenant_id="tenant-forged",
                    display_name="forged",
                    status="active",
                    audit_policy={},
                    budget_policy={},
                )
            )
            await session.flush()


async def test_audit_log_is_append_only(seeded_database: Database) -> None:
    database = seeded_database
    audit_id = new_id()
    async with database.tenant_transaction("tenant-rls-a") as session:
        session.add(
            AuditLog(
                audit_id=audit_id,
                tenant_id="tenant-rls-a",
                channel="telegram",
                user_id="u_test",
                session_id="s_test",
                agent_name="assistant",
                decision="allow",
                latency_ms=1,
                cost_micros=0,
                trace_id="0" * 32,
                request_id="req-test",
                action="run",
                resource="agent:assistant",
                config_revision=1,
                policy_revision=1,
                detail={},
                record_hash="a" * 64,
            )
        )

    with pytest.raises(DBAPIError):
        async with database.tenant_transaction("tenant-rls-a") as session:
            await session.execute(
                update(AuditLog).where(AuditLog.audit_id == audit_id).values(decision="deny")
            )


async def test_event_object_is_rls_scoped_and_append_only(seeded_database: Database) -> None:
    ciphertext = "v1.postgresql-test-ciphertext"
    digest = hashlib.sha256(ciphertext.encode()).hexdigest()
    object_key = f"sdk-events/v1/{'a' * 64}/{digest}"
    store = SqlEventObjectStore(seeded_database, max_object_bytes=1_024)
    await store.put_if_absent(
        object_key,
        ciphertext,
        tenant_id="tenant-rls-a",
        ciphertext_sha256=digest,
    )
    assert await store.get(object_key, tenant_id="tenant-rls-a") == ciphertext
    with pytest.raises(EventStoreError, match="unavailable"):
        await store.get(object_key, tenant_id="tenant-rls-b")

    with pytest.raises(DBAPIError):
        async with seeded_database.tenant_transaction("tenant-rls-a") as session:
            await session.execute(
                update(EventObject)
                .where(EventObject.object_key == object_key)
                .values(ciphertext="v1.mutated")
            )


async def test_postgresql_skip_locked_and_fencing_reject_stale_worker(
    database: Database,
) -> None:
    """Exercise the PostgreSQL-only row-lock behavior, not a SQLite approximation."""

    tenant_id = "tenant-pg-fence"
    await TenantConfigService(database.session_factory).publish(
        _spec(tenant_id),
        actor="test",
    )
    repository = ReliabilityRepository(database.session_factory)
    payload = {"text": "one"}
    await repository.accept_inbox(
        InboxEnvelope(
            tenant_id=tenant_id,
            binding_id="binding-pg-fence",
            session_id="session-shared",
            app_id="assistant",
            app_revision=1,
            config_revision=1,
            scope="private",
            principal_id="principal-pg",
            external_delivery_id="delivery-pg-1",
            payload=payload,
            payload_hash=canonical_json_hash(payload),
            request_id="request-pg-1",
            trace_id="1" * 32,
        )
    )

    first, skipped = await asyncio.gather(
        repository.claim_next(
            tenant_id,
            "worker-first",
        ),
        repository.claim_next(
            tenant_id,
            "worker-skipped",
        ),
    )
    claims = [claim for claim in (first, skipped) if claim is not None]
    assert len(claims) == 1
    old_claim = claims[0]

    async with database.tenant_transaction(tenant_id) as scoped:
        await scoped.execute(
            update(Session)
            .where(
                Session.tenant_id == tenant_id,
                Session.session_id == old_claim.session_id,
            )
            .values(lease_expires_at=func.clock_timestamp() - text("INTERVAL '1 second'"))
        )
    new_claim = await repository.claim_next(tenant_id, "worker-takeover")
    assert new_claim is not None
    assert new_claim.fencing_token > old_claim.fencing_token
    with pytest.raises(StaleClaimError):
        await repository.append_event_cas(
            old_claim,
            old_claim.expected_version,
            EventData(
                event_id="stale-event",
                event_key="stale-event",
                event_type="assistant",
                payload={"text": "must not commit"},
            ),
        )
