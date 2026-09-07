from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from tenant_agent.container import ApplicationContainer
from tenant_agent.models import (
    AuditPolicy,
    AuditRecord,
    BackendKind,
    BackendRef,
    SecretRef,
    TenantStatus,
)
from tenant_agent.settings import Settings
from tests.helpers import make_tenant


def audit_settings() -> Settings:
    return Settings(
        control_database_url="inmemory://",
        bootstrap_config_path=None,
        session_hmac_key="a-long-enough-test-session-hmac-key",
        admin_bearer_token="admin-test-token",
        internal_bearer_token="internal-test-token",
        audit_export_batch_size=2,
    )


def audit_record(audit_id: str, occurred_at: datetime) -> AuditRecord:
    return AuditRecord(
        audit_id=audit_id,
        occurred_at=occurred_at,
        tenant_id="alpha",
        channel="web",
        user_id="user",
        session_id="session",
        agent_name="agent",
        decision="allowed",
        trace_id="0" * 32,
    )


@pytest.mark.asyncio
async def test_audit_retention_prunes_expired_records() -> None:
    container = ApplicationContainer.build(audit_settings())
    await container.initialize()
    tenant = make_tenant().model_copy(update={"audit": AuditPolicy(enabled=True, retention_days=1)})
    await container.configs.create_version(tenant, actor="test", activate=True)
    plane = await container.storage.for_tenant(tenant)
    now = datetime.now(UTC)
    await plane.audit.append_audit(audit_record("old", now - timedelta(days=2)))
    await plane.audit.append_audit(audit_record("new", now))

    result = await container.audit_maintenance.run_once()
    assert result == {"exported": 0, "pruned": 1, "failed_tenants": 0}
    assert [row.audit_id for row in await plane.audit.query_audit("alpha")] == ["new"]
    await container.close()


@pytest.mark.asyncio
async def test_retention_still_runs_after_audit_disable_and_tenant_suspension() -> None:
    container = ApplicationContainer.build(audit_settings())
    await container.initialize()
    tenant = make_tenant().model_copy(
        update={
            "status": TenantStatus.SUSPENDED,
            "audit": AuditPolicy(enabled=False, retention_days=1),
        }
    )
    await container.configs.create_version(tenant, actor="test", activate=True)
    plane = await container.storage.for_tenant(tenant)
    await plane.audit.append_audit(audit_record("old-disabled", datetime.now(UTC) - timedelta(days=2)))

    result = await container.audit_maintenance.run_once()
    assert result == {"exported": 0, "pruned": 1, "failed_tenants": 0}
    assert await plane.audit.query_audit("alpha") == ()
    await container.close()


@pytest.mark.asyncio
async def test_audit_maintenance_does_not_initialize_unrelated_broken_backends() -> None:
    container = ApplicationContainer.build(audit_settings())
    await container.initialize()
    base = make_tenant()
    tenant = base.model_copy(
        update={
            "audit": AuditPolicy(retention_days=1),
            "data_backends": base.data_backends.model_copy(
                update={
                    "session": BackendRef(
                        kind=BackendKind.REDIS,
                        dsn_ref=SecretRef(uri="env://TENANT_ALPHA_MISSING_UNRELATED_REDIS"),
                    )
                }
            ),
        }
    )
    await container.configs.create_version(tenant, actor="test", activate=True)
    audit = await container.storage.audit_for_tenant(tenant)
    await audit.append_audit(audit_record("old-with-broken-session", datetime.now(UTC) - timedelta(days=2)))

    result = await container.audit_maintenance.run_once()
    assert result == {"exported": 0, "pruned": 1, "failed_tenants": 0}
    assert await audit.query_audit("alpha") == ()
    await container.close()


@pytest.mark.asyncio
async def test_audit_maintenance_bounds_work_and_prunes_oldest_first() -> None:
    settings = audit_settings().model_copy(update={"audit_maintenance_max_batches_per_tenant": 1})
    container = ApplicationContainer.build(settings)
    await container.initialize()
    tenant = make_tenant().model_copy(update={"audit": AuditPolicy(enabled=True, retention_days=1)})
    await container.configs.create_version(tenant, actor="test", activate=True)
    plane = await container.storage.for_tenant(tenant)
    now = datetime.now(UTC)
    await plane.audit.append_audit(audit_record("oldest", now - timedelta(days=4)))
    await plane.audit.append_audit(audit_record("middle", now - timedelta(days=3)))
    await plane.audit.append_audit(audit_record("newest-expired", now - timedelta(days=2)))

    result = await container.audit_maintenance.run_once()
    assert result == {"exported": 0, "pruned": 2, "failed_tenants": 0}
    remaining = await plane.audit.query_audit("alpha")
    assert [record.audit_id for record in remaining] == ["newest-expired"]
    await container.close()


@pytest.mark.asyncio
async def test_audit_export_is_idempotent_and_deletes_only_exported_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TENANT_ALPHA_AUDIT_EXPORT_AUTH", "Bearer audit-secret")
    container = ApplicationContainer.build(audit_settings())
    await container.initialize()
    tenant = make_tenant().model_copy(
        update={
            "audit": AuditPolicy(
                enabled=True,
                retention_days=1,
                export_sink="https://audit.example/v1/batches",
                export_auth_ref=SecretRef(uri="env://TENANT_ALPHA_AUDIT_EXPORT_AUTH"),
            )
        }
    )
    await container.configs.create_version(tenant, actor="test", activate=True)
    plane = await container.storage.for_tenant(tenant)
    now = datetime.now(UTC)
    for index in range(3):
        await plane.audit.append_audit(audit_record(f"old-{index}", now - timedelta(days=2)))
    await plane.audit.append_audit(audit_record("new", now))
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["authorization"] == "Bearer audit-secret"
        assert request.headers["idempotency-key"]
        return httpx.Response(204)

    import tenant_agent.services.audit as audit_module

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        audit_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler)),
    )

    result = await container.audit_maintenance.run_once()
    assert result == {"exported": 3, "pruned": 3, "failed_tenants": 0}
    assert len(requests) == 2
    assert [row.audit_id for row in await plane.audit.query_audit("alpha")] == ["new"]
    await container.close()


@pytest.mark.asyncio
async def test_failed_audit_export_preserves_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = ApplicationContainer.build(audit_settings())
    await container.initialize()
    tenant = make_tenant().model_copy(
        update={
            "audit": AuditPolicy(
                enabled=True,
                retention_days=1,
                export_sink="https://audit.example/v1/batches",
            )
        }
    )
    await container.configs.create_version(tenant, actor="test", activate=True)
    plane = await container.storage.for_tenant(tenant)
    await plane.audit.append_audit(audit_record("preserved", datetime.now(UTC) - timedelta(days=2)))
    import tenant_agent.services.audit as audit_module

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        audit_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(
            transport=httpx.MockTransport(lambda request: httpx.Response(503, request=request))
        ),
    )
    result = await container.audit_maintenance.run_once()
    assert result["failed_tenants"] == 1
    assert [row.audit_id for row in await plane.audit.query_audit("alpha")] == ["preserved"]
    await container.close()
