from collections.abc import AsyncIterator
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import insert, select, update

from trpc_service.admin.auth import WEB_CSRF_COOKIE
from trpc_service.admin.models import ChannelAdapterType, ManagementPrincipal, TenantSecret
from trpc_service.admin.secret_store import TenantSecretStore
from trpc_service.channels.models import ChannelBinding
from trpc_service.config import Settings
from trpc_service.config.storage import LocalSecretNotReadyError
from tests.conftest import create_test_app


@pytest.fixture
async def tenant_console_clients(
    tmp_path: Path, ) -> AsyncIterator[tuple[httpx.AsyncClient, httpx.AsyncClient, FastAPI]]:
    """Run platform and browser clients against one isolated control plane."""

    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'tenant-console.db'}",
        auto_create_schema=True,
        admin_bootstrap_token=SecretStr("test-platform-admin-token"),
        tenant_secret_master_key=SecretStr("02" * 32),
        worker_concurrency=0,
    )
    app = create_test_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with app.state.engine.begin() as connection:
            await connection.execute(
                insert(ChannelAdapterType).values(
                    channel_type="wecom",
                    display_name="企业微信智能机器人",
                    adapter_version="test",
                    config_schema={
                        "type": "object",
                        "properties": {
                            "bot_id": {
                                "type": "string",
                                "minLength": 1
                            }
                        },
                        "required": ["bot_id"],
                        "additionalProperties": False,
                    },
                    secret_schema={
                        "type": "object",
                        "properties": {
                            "bot_secret": {
                                "type": "string",
                                "minLength": 1
                            }
                        },
                        "required": ["bot_secret"],
                        "additionalProperties": False,
                    },
                ))
        async with (
                httpx.AsyncClient(
                    transport=transport,
                    base_url="http://test",
                    headers={
                        "Authorization": "Bearer test-platform-admin-token",
                        "X-Support-Reason": "automated control-plane test",
                    },
                ) as platform,
                httpx.AsyncClient(transport=transport, base_url="http://test") as browser,
        ):
            yield platform, browser, app


@pytest.mark.anyio
async def test_tenant_console_auth_channel_secrets_and_knowledge_are_scoped(
    tenant_console_clients: tuple[httpx.AsyncClient, httpx.AsyncClient, FastAPI], ) -> None:
    """Exercise the explicit tenant-admin flow without exposing any IM secret."""

    platform, browser, app = tenant_console_clients
    tenant = (await platform.post("/api/v1/tenants", json={"name": "Console Tenant"})).json()
    tenant_id = tenant["tenant_id"]
    catalog = await platform.post(
        "/api/v1/admin/model-catalog",
        json={
            "provider": "bailian",
            "model_name": "qwen-max",
            "display_name": "Qwen Max",
        },
    )
    credential = await platform.post(
        "/api/v1/admin/model-credentials",
        json={
            "provider": "bailian",
            "name": "tenant-console-test",
            "secret_ref": "env://DASHSCOPE_API_KEY",
        },
    )
    profile = await platform.post(
        f"/api/v1/tenants/{tenant_id}/model-profiles",
        json={
            "name": "primary",
            "model_catalog_id": catalog.json()["model_catalog_id"],
            "credential_id": credential.json()["model_credential_id"],
        },
    )
    assert catalog.status_code == credential.status_code == profile.status_code == 201
    agent = (await platform.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={
            "name": "Console Agent",
            "knowledge_config": {
                "knowledge_base_names": ["handbook"]
            },
        },
    )).json()
    account = await platform.post(
        "/api/v1/admin/tenant-accounts",
        json={
            "tenant_id": tenant_id,
            "username": "owner",
            "password": "correct horse battery staple",
        },
    )
    assert account.status_code == 201

    invalid = await browser.post(
        "/api/v1/auth/login",
        json={
            "username": "owner",
            "password": "wrong password"
        },
    )
    login = await browser.post(
        "/api/v1/auth/login",
        json={
            "username": "owner",
            "password": "correct horse battery staple"
        },
    )
    assert invalid.status_code == 401
    assert login.status_code == 200
    assert login.json()["tenant_roles"][tenant_id] == ["tenant_admin"]
    assert "HttpOnly" in login.headers.get_list("set-cookie")[0]
    catalog = await browser.get(f"/api/v1/tenants/{tenant_id}/channel-adapter-types")
    assert catalog.status_code == 200
    assert catalog.json()["items"][0]["channel_type"] == "wecom"

    no_csrf = await browser.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json={},
    )
    assert no_csrf.status_code == 403
    csrf = browser.cookies.get(WEB_CSRF_COOKIE)
    assert csrf is not None
    headers = {"X-CSRF-Token": csrf}
    binding = await browser.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        headers=headers,
        json={
            "agent_app_id": agent["agent_app_id"],
            "channel_type": "wecom",
            "account_config": {
                "bot_id": "tenant-bot"
            },
            "secret_values": {
                "bot_secret": "tenant-private-secret"
            },
        },
    )
    assert binding.status_code == 201
    assert binding.json()["secret_fields"] == ["bot_secret"]
    assert "secret_ref_map" not in binding.json()
    assert "tenant-private-secret" not in binding.text
    async with app.state.session_factory() as database:
        binding_row = await database.scalar(select(ChannelBinding))
        secret_row = await database.scalar(select(TenantSecret))
    assert binding_row is not None
    assert secret_row is not None
    secret_ref = binding_row.secret_ref_map["bot_secret"]
    assert secret_ref.startswith(f"secret-manager://tenants/{tenant_id}/channels/")
    assert "tenant-private-secret" not in secret_row.ciphertext
    assert await app.state.container.tenant_secrets.resolve(
        secret_ref, binding_row.tenant_id) == "tenant-private-secret"
    rotated = await browser.patch(
        f"/api/v1/tenants/{tenant_id}/channel-bindings/{binding.json()['binding_id']}",
        headers=headers,
        json={"secret_values": {
            "bot_secret": "rotated-private-secret"
        }},
    )
    assert rotated.status_code == 200
    assert await app.state.container.tenant_secrets.resolve(
        secret_ref, binding_row.tenant_id) == "rotated-private-secret"

    knowledge_path = (f"/api/v1/tenants/{tenant_id}/knowledge-bases/handbook/documents"
                      f"?agent_app_id={agent['agent_app_id']}&filename=policy.md")
    uploaded = await browser.post(
        knowledge_path,
        headers={
            **headers, "Content-Type": "text/markdown"
        },
        content="白兔公司的年假为十五天。".encode(),
    )
    assert uploaded.status_code == 201
    document_id = uploaded.json()["document_id"]
    listed = await browser.get(f"/api/v1/tenants/{tenant_id}/knowledge-bases/handbook/documents"
                               f"?agent_app_id={agent['agent_app_id']}")
    assert listed.status_code == 200
    assert listed.json()["items"][0]["filename"] == "policy.md"
    replaced = await browser.put(
        f"/api/v1/tenants/{tenant_id}/knowledge-bases/handbook/documents/{document_id}"
        f"?agent_app_id={agent['agent_app_id']}&filename=policy-v2.md",
        headers={
            **headers, "Content-Type": "text/markdown"
        },
        content="白兔公司的年假更新为十八天。".encode(),
    )
    assert replaced.status_code == 200
    assert replaced.json()["version"] == 2
    document_id = replaced.json()["document_id"]
    forbidden_base = await browser.get(
        f"/api/v1/tenants/{tenant_id}/knowledge-bases/private/documents"
        f"?agent_app_id={agent['agent_app_id']}")
    missing_agent = await browser.get(
        f"/api/v1/tenants/{tenant_id}/knowledge-bases/handbook/documents"
        "?agent_app_id=00000000-0000-0000-0000-000000000000")
    assert forbidden_base.status_code == 403
    assert missing_agent.status_code == 404
    unsupported = await browser.post(
        f"/api/v1/tenants/{tenant_id}/knowledge-bases/handbook/documents"
        f"?agent_app_id={agent['agent_app_id']}&filename=policy.exe",
        headers={
            **headers, "Content-Type": "application/octet-stream"
        },
        content=b"not a knowledge document",
    )
    missing_document = await browser.delete(
        f"/api/v1/tenants/{tenant_id}/knowledge-bases/handbook/documents/"
        "00000000-0000-0000-0000-000000000000"
        f"?agent_app_id={agent['agent_app_id']}",
        headers=headers,
    )
    assert unsupported.status_code == 422
    assert missing_document.status_code == 404

    # Simulate an account retained from the legacy role model. Password rotation
    # must repair its display/login subject based on the role, not a name prefix.
    async with app.state.session_factory.begin() as database:
        await database.execute(
            update(ManagementPrincipal).where(ManagementPrincipal.management_principal_id == UUID(
                account.json()["management_principal_id"])).values(
                    external_subject="local:legacy-owner"))
    rotated_password = await platform.put(
        f"/api/v1/admin/principals/{account.json()['management_principal_id']}/password",
        json={
            "username": "owner-renamed",
            "password": "a newer correct horse password"
        },
    )
    assert rotated_password.status_code == 204
    assert (await browser.get("/api/v1/admin/me")).status_code == 401
    principals = await platform.get("/api/v1/admin/principals")
    rotated_account = next(
        item for item in principals.json()["items"]
        if item["management_principal_id"] == account.json()["management_principal_id"])
    assert rotated_account["external_subject"] == "tenant-console:owner-renamed"
    relogin = await browser.post(
        "/api/v1/auth/login",
        json={
            "username": "owner-renamed",
            "password": "a newer correct horse password"
        },
    )
    assert relogin.status_code == 200
    csrf = browser.cookies.get(WEB_CSRF_COOKIE)
    assert csrf is not None
    headers = {"X-CSRF-Token": csrf}
    deleted = await browser.delete(
        f"/api/v1/tenants/{tenant_id}/knowledge-bases/handbook/documents/{document_id}"
        f"?agent_app_id={agent['agent_app_id']}",
        headers=headers,
    )
    assert deleted.status_code == 204
    logout = await browser.post("/api/v1/auth/logout", headers=headers)
    assert logout.status_code == 204
    assert (await browser.get("/api/v1/admin/me")).status_code == 401


@pytest.mark.anyio
async def test_tenant_console_cannot_cross_tenant_boundary(
    tenant_console_clients: tuple[httpx.AsyncClient, httpx.AsyncClient, FastAPI], ) -> None:
    platform, browser, _ = tenant_console_clients
    first = (await platform.post("/api/v1/tenants", json={"name": "First"})).json()
    second = (await platform.post("/api/v1/tenants", json={"name": "Second"})).json()
    account = await platform.post(
        "/api/v1/admin/tenant-accounts",
        json={
            "tenant_id": first["tenant_id"],
            "username": "first-owner",
            "password": "correct horse battery staple",
        },
    )
    assert account.status_code == 201
    duplicate = await platform.post(
        "/api/v1/admin/tenant-accounts",
        json={
            "tenant_id": first["tenant_id"],
            "username": "second-owner",
            "password": "another correct horse password",
        },
    )
    missing_tenant = await platform.post(
        "/api/v1/admin/tenant-accounts",
        json={
            "tenant_id": "00000000-0000-0000-0000-000000000000",
            "username": "missing-tenant",
            "password": "another correct horse password",
        },
    )
    assert duplicate.status_code == 409
    assert missing_tenant.status_code == 404
    await browser.post(
        "/api/v1/auth/login",
        json={
            "username": "first-owner",
            "password": "correct horse battery staple"
        },
    )
    response = await browser.get(f"/api/v1/tenants/{second['tenant_id']}")
    assert response.status_code == 403


@pytest.mark.anyio
async def test_tenant_secret_store_fails_closed_for_invalid_or_missing_data(
    tenant_console_clients: tuple[httpx.AsyncClient, httpx.AsyncClient, FastAPI], ) -> None:
    """Reject invalid keys, empty values, unknown rows, and corrupt ciphertext."""

    platform, _, app = tenant_console_clients
    tenant = (await platform.post("/api/v1/tenants", json={"name": "Secret Errors"})).json()
    tenant_id = tenant["tenant_id"]
    scope = UUID(tenant_id)
    with pytest.raises(ValueError, match="32-byte"):
        TenantSecretStore(app.state.session_factory, b"short")
    without_key = TenantSecretStore(app.state.session_factory, None)
    async with app.state.session_factory() as database:
        with pytest.raises(ValueError, match="name"):
            await without_key.put(database, scope, "", "value")
        with pytest.raises(ValueError, match="must contain"):
            await without_key.put(database, scope, "channels/test/key", "")
        with pytest.raises(RuntimeError, match="not configured"):
            await without_key.put(database, scope, "channels/test/key", "value")
        await database.rollback()
    with pytest.raises(ValueError, match="identifier"):
        await app.state.container.tenant_secrets.resolve(
            f"secret-manager://tenants/{scope}/channels/not-a-uuid", scope)
    with pytest.raises(LocalSecretNotReadyError, match="unavailable"):
        await app.state.container.tenant_secrets.resolve(
            f"secret-manager://tenants/{scope}/channels/{uuid4()}", scope)
    async with app.state.session_factory.begin() as database:
        reference = await app.state.container.tenant_secrets.put(
            database,
            scope,
            "channels/test/corrupt",
            "valid-before-corruption",
        )
    async with app.state.session_factory.begin() as database:
        row = await database.scalar(select(TenantSecret).where(TenantSecret.tenant_id == scope))
        assert row is not None
        row.ciphertext = "not-base64"
    with pytest.raises(LocalSecretNotReadyError, match="cannot be decrypted"):
        await app.state.container.tenant_secrets.resolve(reference, scope)
