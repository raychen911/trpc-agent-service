from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select

from trpc_service.admin.models import ManagementAuditLog
from trpc_service.channels.models import ChannelBinding
from trpc_service.config import Settings
from trpc_service.storage.runtime_orm import OutboxMessageRow
from tests.conftest import create_test_app


@pytest.mark.anyio
async def test_delivery_failures_are_tenant_scoped_and_replay_is_audited(tmp_path: Path, ) -> None:
    """A terminal delivery can be replayed once without exposing its payload."""

    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'delivery-recovery.db'}",
        auto_create_schema=True,
        worker_concurrency=0,
        admin_bootstrap_token=SecretStr("delivery-recovery-admin"),
    )
    app = create_test_app(settings)
    headers = {
        "Authorization": "Bearer delivery-recovery-admin",
        "X-Support-Reason": "delivery recovery API integration test",
    }
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
                transport=transport,
                base_url="http://test",
                headers=headers,
        ) as client:
            tenant = await client.post("/api/v1/tenants", json={"name": "Delivery Owner"})
            other = await client.post("/api/v1/tenants", json={"name": "Delivery Other"})
            tenant_id = UUID(tenant.json()["tenant_id"])
            agent = await client.post(
                f"/api/v1/tenants/{tenant_id}/agents",
                json={"name": "Delivery Recovery Agent"},
            )
            agent_id = UUID(agent.json()["agent_app_id"])
            binding_id = uuid4()
            async with app.state.session_factory.begin() as database:
                database.add(
                    ChannelBinding(
                        binding_id=binding_id,
                        tenant_id=tenant_id,
                        agent_app_id=agent_id,
                        channel_type="wecom",
                        external_account_hash="delivery-recovery-account",
                    ))
                database.add(
                    OutboxMessageRow(
                        tenant_id=tenant_id,
                        agent_app_id=agent_id,
                        outbox_id="failed-reply",
                        request_id="failed-request",
                        category="IM_REPLY",
                        destination="wecom",
                        binding_id=binding_id,
                        idempotency_key="failed-reply",
                        payload={"private": "must not be returned"},
                        status="DEAD_LETTER",
                        attempt_count=8,
                        retry_count=3,
                        last_error_code="InvalidRecipient",
                        last_error_summary="Channel delivery cannot be retried",
                    ))

            listed = await client.get(f"/api/v1/tenants/{tenant_id}/delivery-failures")
            cross_tenant = await client.get(
                f"/api/v1/tenants/{other.json()['tenant_id']}/delivery-failures")
            replayed = await client.post(
                f"/api/v1/tenants/{tenant_id}/delivery-failures/failed-reply/replay",
                json={"reason": "recipient configuration was corrected"},
            )
            duplicate = await client.post(
                f"/api/v1/tenants/{tenant_id}/delivery-failures/failed-reply/replay",
                json={"reason": "duplicate replay must be rejected"},
            )

            assert listed.status_code == 200
            assert listed.json()["total"] == 1
            assert "payload" not in listed.json()["items"][0]
            assert cross_tenant.json()["total"] == 0
            assert replayed.status_code == 200
            assert replayed.json() == {"outbox_id": "failed-reply", "status": "PENDING"}
            assert duplicate.status_code == 404
            async with app.state.session_factory() as database:
                replayed_row = await database.scalar(
                    select(OutboxMessageRow).where(
                        OutboxMessageRow.tenant_id == tenant_id,
                        OutboxMessageRow.outbox_id == "failed-reply",
                    ))
                assert replayed_row is not None
                # Historical provider attempts remain addressable, while the
                # manual replay receives a fresh retry budget.
                assert replayed_row.attempt_count == 8
                assert replayed_row.retry_count == 0
                audit = await database.scalar(
                    select(ManagementAuditLog).where(
                        ManagementAuditLog.action == "delivery_failure.replay",
                        ManagementAuditLog.tenant_id == tenant_id,
                    ))
                assert audit is not None
                assert audit.resource_id == "failed-reply"
