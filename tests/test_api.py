from __future__ import annotations

import asyncio

import httpx
import pytest

from trpc_service.config import AppSettings
from trpc_service.runtime import InboundEnvelope, TenantContext
from trpc_service.web.app import ServiceContainer, create_app
from trpc_service.web.schemas import TenantCreate


def test_admin_callback_and_direct_run_share_the_durable_runtime() -> None:
    """Exercise the documented HTTP boundary without opening a network socket."""

    async def scenario() -> None:
        services = ServiceContainer(AppSettings(admin_api_key="admin-test-key"))
        app = create_app(services)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://platform.test",
            headers={"x-admin-key": "admin-test-key"},
        ) as client:
            tenant = await client.post(
                "/admin/v1/tenants", json={"tenant_id": "tenant-api", "display_name": "API tenant"}
            )
            assert tenant.status_code == 201
            assert tenant.json()["tenant_id"] == "tenant-api"

            agent = await client.post(
                "/admin/v1/tenants/tenant-api/agents",
                json={"agent_id": "support", "name": "Support"},
            )
            assert agent.status_code == 201
            release = await client.post(
                "/admin/v1/tenants/tenant-api/agents/support/releases",
                json={
                    "version": 1,
                    "model_config": {"mode": "mock"},
                    "tool_policy": {"allow": ["ticket.lookup"]},
                },
            )
            assert release.status_code == 201
            activated = await client.post(
                "/admin/v1/tenants/tenant-api/agents/support/releases/1/activate"
            )
            assert activated.status_code == 200

            binding = await client.post(
                "/admin/v1/tenants/tenant-api/channels",
                json={
                    "binding_id": "mock-main",
                    "agent_id": "support",
                    "provider": "mock",
                    "external_account_id": "test-account",
                    "webhook_key": "test-webhook-key-123456",
                    "capabilities": {"callback_secret": "test-secret"},
                },
            )
            assert binding.status_code == 201

            callback = await client.post(
                "/callbacks/mock/test-webhook-key-123456",
                headers={"x-mock-secret": "test-secret", "x-request-id": "callback-1"},
                json={"message_id": "message-1", "user_id": "alice", "text": "ticket 42"},
            )
            assert callback.status_code == 200
            first = callback.json()
            assert first["duplicate"] is False
            duplicate = await client.post(
                "/callbacks/mock/test-webhook-key-123456",
                headers={"x-mock-secret": "test-secret"},
                json={"message_id": "message-1", "user_id": "alice", "text": "ticket 42"},
            )
            assert duplicate.json()["duplicate"] is True
            assert duplicate.json()["inbox_id"] == first["inbox_id"]

            direct = await client.post(
                "/v1/tenants/tenant-api/agents/support:run",
                headers={"x-request-id": "run-1"},
                json={"input": "ticket 42", "idempotency_key": "direct-1"},
            )
            assert direct.status_code == 202
            assert direct.json()["status"] == "queued"
            assert direct.headers["x-request-id"] == "run-1"
            services.dispatch("tenant-api", "run-1")
            processed = await services.process_published("tenant-api", "run-1")
            direct_processed = next(
                row for row in processed if row["inbox_id"] == direct.json()["inbox_id"]
            )
            assert direct_processed["reply_outbox_id"] is None
            operation = await client.get("/v1/operations/run-1?tenant_id=tenant-api")
            assert operation.status_code == 200
            assert operation.json()["items"][0]["status"] == "committed"
            assert "ticket" in operation.json()["items"][0]["reply"].lower()

    asyncio.run(scenario())


def test_http_authentication_fails_closed_and_tenant_keys_are_scoped() -> None:
    async def scenario() -> None:
        settings = AppSettings(
            admin_api_key="admin-test-key",
            tenant_api_keys={"tenant-a": "tenant-a-key", "tenant-b": "tenant-b-key"},
        )
        app = create_app(ServiceContainer(settings))
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://platform.test") as client:
            health = await client.get("/health/live")
            assert health.status_code == 200

            anonymous_admin = await client.post(
                "/admin/v1/tenants", json={"tenant_id": "tenant-a", "display_name": "A"}
            )
            assert anonymous_admin.status_code == 401

            admin_headers = {"authorization": "Bearer admin-test-key"}
            for tenant_id in ("tenant-a", "tenant-b"):
                created = await client.post(
                    "/admin/v1/tenants",
                    headers=admin_headers,
                    json={"tenant_id": tenant_id, "display_name": tenant_id},
                )
                assert created.status_code == 201
                agent = await client.post(
                    f"/admin/v1/tenants/{tenant_id}/agents",
                    headers=admin_headers,
                    json={"agent_id": "support", "name": "Support"},
                )
                assert agent.status_code == 201
                release = await client.post(
                    f"/admin/v1/tenants/{tenant_id}/agents/support/releases",
                    headers=admin_headers,
                    json={"version": 1, "model_config": {"mode": "mock"}},
                )
                assert release.status_code == 201
                active = await client.post(
                    f"/admin/v1/tenants/{tenant_id}/agents/support/releases/1/activate",
                    headers=admin_headers,
                )
                assert active.status_code == 200

            own_tenant = await client.post(
                "/v1/tenants/tenant-a/agents/support:run",
                headers={"x-api-key": "tenant-a-key"},
                json={"input": "hello", "idempotency_key": "tenant-a-run"},
            )
            assert own_tenant.status_code == 202
            other_tenant = await client.post(
                "/v1/tenants/tenant-b/agents/support:run",
                headers={"x-api-key": "tenant-a-key"},
                json={"input": "hello", "idempotency_key": "tenant-b-run"},
            )
            assert other_tenant.status_code == 403

    asyncio.run(scenario())


def test_unconfigured_authentication_never_allows_admin_access() -> None:
    async def scenario() -> None:
        app = create_app(ServiceContainer(AppSettings()))
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://platform.test") as client:
            response = await client.post(
                "/admin/v1/tenants", json={"tenant_id": "tenant-a", "display_name": "A"}
            )
            assert response.status_code == 503
            assert response.json()["detail"]["code"] == "authentication_unconfigured"

    asyncio.run(scenario())


def test_production_app_requires_an_admin_authentication_configuration() -> None:
    try:
        create_app(ServiceContainer(AppSettings(environment="production")))
    except RuntimeError as exc:
        assert "TRPC_SERVICE_ADMIN_API_KEY" in str(exc)
    else:  # pragma: no cover - keeps the assertion clear if startup validation regresses.
        raise AssertionError("production startup accepted an unconfigured admin API")


def test_non_http_production_process_requires_restricted_database_role() -> None:
    settings = AppSettings(
        environment="production",
        runtime_backend="postgres",
        database_role=None,
    )
    with pytest.raises(RuntimeError, match="DATABASE_ROLE"):
        settings.validate_startup(require_http_auth=False)

    AppSettings(
        environment="production",
        runtime_backend="postgres",
        database_role="agent_worker",
    ).validate_startup(require_http_auth=False)


def test_accepted_inbox_keeps_its_pinned_release_after_activation_changes() -> None:
    async def scenario() -> None:
        services = ServiceContainer(AppSettings(admin_api_key="admin-test-key"))
        app = create_app(services)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://platform.test",
            headers={"x-admin-key": "admin-test-key"},
        ) as client:
            assert (
                await client.post(
                    "/admin/v1/tenants", json={"tenant_id": "tenant-pinned", "display_name": "Pinned"}
                )
            ).status_code == 201
            assert (
                await client.post(
                    "/admin/v1/tenants/tenant-pinned/agents",
                    json={"agent_id": "support", "name": "Support"},
                )
            ).status_code == 201
            for version in (1, 2):
                assert (
                    await client.post(
                        "/admin/v1/tenants/tenant-pinned/agents/support/releases",
                        json={"version": version, "model_config": {"mode": "mock"}},
                    )
                ).status_code == 201
            assert (
                await client.post(
                    "/admin/v1/tenants/tenant-pinned/agents/support/releases/1/activate"
                )
            ).status_code == 200
            assert (
                await client.post(
                    "/admin/v1/tenants/tenant-pinned/channels",
                    json={
                        "binding_id": "mock-pinned",
                        "agent_id": "support",
                        "provider": "mock",
                        "external_account_id": "pinned-account",
                        "webhook_key": "pinned-webhook-key-123456",
                        "capabilities": {"callback_secret": "pinned-secret"},
                    },
                )
            ).status_code == 201
            accepted = await client.post(
                "/callbacks/mock/pinned-webhook-key-123456",
                headers={"x-mock-secret": "pinned-secret"},
                json={"message_id": "pinned-message", "user_id": "alice", "text": "hello"},
            )
            assert accepted.status_code == 200
            # This moves only the mutable active pointer. The accepted Inbox
            # must continue to execute the release version it recorded above.
            assert (
                await client.post(
                    "/admin/v1/tenants/tenant-pinned/agents/support/releases/2/activate"
                )
            ).status_code == 200
            services.dispatch("tenant-pinned")
            await services.process_published("tenant-pinned")

        snapshot = services.runtime.snapshot(TenantContext("tenant-pinned"))
        started = next(row for row in snapshot["events"] if row["event_type"] == "runner.started")
        assert started["payload"]["release_version"] == 1

    asyncio.run(scenario())


def test_worker_heartbeat_renews_a_live_execution_lease() -> None:
    async def scenario() -> None:
        services = ServiceContainer(
            AppSettings(
                admin_api_key="admin-test-key",
                execution_lease_seconds=2,
                execution_heartbeat_seconds=1,
            )
        )
        services.create_tenant(TenantCreate(tenant_id="tenant-heartbeat", display_name="Heartbeat"))
        context = TenantContext("tenant-heartbeat", request_id="heartbeat", trace_id="a" * 32)
        inbox = services.runtime.accept_inbound(
            context,
            InboundEnvelope(
                tenant_id="tenant-heartbeat",
                channel_binding_id="test-binding",
                agent_id="test-agent",
                session_id="test-session",
                idempotency_key="heartbeat-message",
                external_message_id="heartbeat-message",
                payload={"text": "heartbeat"},
                config_version=1,
            ),
        ).inbox
        claim = services.runtime.claim_execution(
            context, inbox.inbox_id, "worker-heartbeat", lease_seconds=2
        )
        _, renewed = await services._run_with_execution_heartbeat(
            context, claim, asyncio.sleep(1.05, result="done")
        )
        assert renewed.lease_expires_at > claim.lease_expires_at

    asyncio.run(scenario())
