"""Tenant-scoped admission limits and fair worker claiming."""

import pytest
from trpc_service.persistence import Database
from trpc_service.persistence.models import TenantRow
from trpc_service.reliability.inbox import InboxRepository
from trpc_service.tenant import ResourcePolicy
from trpc_service.management import ConfigConflict


def values(tenant, external):
    return dict(tenant_id=tenant, channel_binding_id="binding", external_message_id=external,
                request_id="request-" + external, trace_id="trace-" + external,
                partition_key="user", config_version=1, storage_revision=1,
                payload={"text": external}, identity_payload={"text": external})


def test_rate_limit_deduplicates_without_consuming_quota(tmp_path, monkeypatch):
    db = Database(f"sqlite+pysqlite:///{tmp_path / 'admission.db'}")
    db.create_schema()
    with db.sessions.begin() as session:
        session.add(TenantRow(tenant_id="tenant", name="Tenant", status="active", active_config_version=1))
    monkeypatch.setattr(InboxRepository, "_resource_policy",
                        staticmethod(lambda session, tenant_id: ResourcePolicy(requests_per_minute=1)))
    inbox = InboxRepository(db)
    first = inbox.accept(**values("tenant", "event-1"))
    duplicate = inbox.accept(**values("tenant", "event-1"))
    assert first.disposition.value == "accepted"
    assert duplicate.disposition.value == "duplicate"
    with pytest.raises(ConfigConflict, match="rate limit"):
        inbox.accept(**values("tenant", "event-2"))
    db.close()


def test_claim_skips_tenant_at_concurrency_cap(tmp_path, monkeypatch):
    db = Database(f"sqlite+pysqlite:///{tmp_path / 'claim.db'}")
    db.create_schema()
    with db.sessions.begin() as session:
        session.add_all([
            TenantRow(tenant_id="a", name="A", status="active", active_config_version=1),
            TenantRow(tenant_id="b", name="B", status="active", active_config_version=1),
        ])
    monkeypatch.setattr(
        InboxRepository, "_resource_policy",
        staticmethod(lambda session, tenant_id: ResourcePolicy(max_concurrent_runs=1,
                                                               priority=100 if tenant_id == "b" else 1)))
    inbox = InboxRepository(db)
    inbox.accept(**values("a", "a-processing"))
    work = inbox.claim(worker_id="worker-a", lease_seconds=60)
    assert work and work.tenant_id == "a"
    inbox.accept(**values("a", "a-pending"))
    inbox.accept(**values("b", "b-pending"))
    next_work = inbox.claim(worker_id="worker-b", lease_seconds=60)
    assert next_work and next_work.tenant_id == "b"
    db.close()
