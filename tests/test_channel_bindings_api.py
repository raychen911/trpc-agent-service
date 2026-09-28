from uuid import UUID

import httpx
import pytest


async def _create_tenant(client: httpx.AsyncClient, name: str) -> str:
    response = await client.post("/api/v1/tenants", json={"name": name})
    assert response.status_code == 201
    tenant_id = response.json()["tenant_id"]

    catalogs = (await client.get("/api/v1/admin/model-catalog")).json()["items"]
    if catalogs:
        catalog_id = catalogs[0]["model_catalog_id"]
    else:
        catalog = await client.post(
            "/api/v1/admin/model-catalog",
            json={
                "provider": "bailian",
                "model_name": "qwen-max",
                "display_name": "Qwen Max",
            },
        )
        assert catalog.status_code == 201
        catalog_id = catalog.json()["model_catalog_id"]
    credentials = (await client.get("/api/v1/admin/model-credentials")).json()["items"]
    if credentials:
        credential_id = credentials[0]["model_credential_id"]
    else:
        credential = await client.post(
            "/api/v1/admin/model-credentials",
            json={
                "provider": "bailian",
                "name": "channel-test",
                "secret_ref": "env://DASHSCOPE_API_KEY",
            },
        )
        assert credential.status_code == 201
        credential_id = credential.json()["model_credential_id"]
    profile = await client.post(
        f"/api/v1/tenants/{tenant_id}/model-profiles",
        json={
            "name": "primary",
            "model_catalog_id": catalog_id,
            "credential_id": credential_id,
        },
    )
    assert profile.status_code == 201
    return tenant_id


async def _create_agent(client: httpx.AsyncClient, tenant_id: str, name: str) -> str:
    response = await client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={"name": name},
    )
    assert response.status_code == 201
    return response.json()["agent_app_id"]


@pytest.mark.anyio
async def test_active_binding_rejects_agent_without_model_policy(
    api_client: httpx.AsyncClient, ) -> None:
    """Reject broken IM wiring before a real provider can route messages to it."""

    tenant = await api_client.post("/api/v1/tenants", json={"name": "Unconfigured IM"})
    tenant_id = tenant.json()["tenant_id"]
    agent = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={"name": "Agent Without Model Policy"},
    )

    response = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json={
            "agent_app_id": agent.json()["agent_app_id"],
            "channel_type": "web",
            "external_account_hash": "sha256:unconfigured-agent",
        },
    )

    assert agent.status_code == 201
    assert response.status_code == 409
    assert response.json()["error"]["message"] == (
        "Agent is not executable: platform model profile is not configured")


@pytest.mark.anyio
async def test_channel_binding_crud_is_scoped_to_its_tenant(
    api_client: httpx.AsyncClient, ) -> None:
    tenant_id = await _create_tenant(api_client, "Customer Service")
    agent_id = await _create_agent(api_client, tenant_id, "Support Agent")

    create_response = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json={
            "agent_app_id": agent_id,
            "channel_type": "wecom",
            "external_account_hash": "sha256:example-account",
            "account_config": {
                "corp_id": "example-corp"
            },
            "secret_ref_map": {
                "token":
                (f"env://TRPC_TENANT_{tenant_id.replace('-', '_').upper()}_CHANNEL_WECOM_TOKEN")
            },
            "capabilities": {
                "streaming": False
            },
        },
    )

    assert create_response.status_code == 201
    created = create_response.json()
    UUID(created["binding_id"])
    assert len(created["binding_public_id"]) >= 22
    assert created["tenant_id"] == tenant_id
    assert created["agent_app_id"] == agent_id
    assert created["status"] == "active"

    list_response = await api_client.get(f"/api/v1/tenants/{tenant_id}/channel-bindings")
    assert list_response.status_code == 200
    assert list_response.json()["total"] == 1

    update_response = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/channel-bindings/{created['binding_id']}",
        json={"capabilities": {
            "streaming": True
        }},
    )
    assert update_response.status_code == 200
    assert update_response.json()["capabilities"] == {"streaming": True}

    delete_response = await api_client.delete(
        f"/api/v1/tenants/{tenant_id}/channel-bindings/{created['binding_id']}")
    assert delete_response.status_code == 204

    restore_response = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/channel-bindings/{created['binding_id']}",
        json={"status": "active"},
    )
    assert restore_response.status_code == 200

    get_response = await api_client.get(
        f"/api/v1/tenants/{tenant_id}/channel-bindings/{created['binding_id']}")
    assert get_response.status_code == 200
    assert get_response.json()["status"] == "active"


@pytest.mark.anyio
async def test_channel_binding_rejects_agent_from_another_tenant(
    api_client: httpx.AsyncClient, ) -> None:
    owner_id = await _create_tenant(api_client, "Owner")
    other_id = await _create_tenant(api_client, "Other")
    agent_id = await _create_agent(api_client, owner_id, "Private Agent")

    response = await api_client.post(
        f"/api/v1/tenants/{other_id}/channel-bindings",
        json={
            "agent_app_id": agent_id,
            "channel_type": "web",
            "external_account_hash": "sha256:other-account",
        },
    )

    assert response.status_code == 404


@pytest.mark.anyio
async def test_external_channel_account_can_only_have_one_binding(
    api_client: httpx.AsyncClient, ) -> None:
    tenant_id = await _create_tenant(api_client, "Channel Account")
    agent_id = await _create_agent(api_client, tenant_id, "Binding Agent")
    payload = {
        "agent_app_id": agent_id,
        "channel_type": "wecom",
        "external_account_hash": "sha256:unique-account",
    }

    first_response = await api_client.post(f"/api/v1/tenants/{tenant_id}/channel-bindings",
                                           json=payload)
    second_response = await api_client.post(f"/api/v1/tenants/{tenant_id}/channel-bindings",
                                            json=payload)

    assert first_response.status_code == 201
    assert second_response.status_code == 409


@pytest.mark.anyio
async def test_wecom_identity_ignores_non_identity_thinking_message(
    api_client: httpx.AsyncClient, ) -> None:
    """Changing presentation settings must not duplicate one provider Bot."""

    first_tenant = await _create_tenant(api_client, "First Bot Owner")
    second_tenant = await _create_tenant(api_client, "Second Bot Owner")
    first_agent = await _create_agent(api_client, first_tenant, "First Bot Agent")
    second_agent = await _create_agent(api_client, second_tenant, "Second Bot Agent")
    first = await api_client.post(
        f"/api/v1/tenants/{first_tenant}/channel-bindings",
        json={
            "agent_app_id": first_agent,
            "channel_type": "wecom",
            "account_config": {
                "bot_id": "shared-provider-bot",
                "thinking_message": "正在思考...",
            },
        },
    )
    duplicate = await api_client.post(
        f"/api/v1/tenants/{second_tenant}/channel-bindings",
        json={
            "agent_app_id": second_agent,
            "channel_type": "wecom",
            "account_config": {
                "bot_id": "shared-provider-bot",
                "thinking_message": "处理中，请稍候",
            },
        },
    )
    disabled = await api_client.delete(
        f"/api/v1/tenants/{first_tenant}/channel-bindings/{first.json()['binding_id']}")
    moved = await api_client.post(
        f"/api/v1/tenants/{second_tenant}/channel-bindings",
        json={
            "agent_app_id": second_agent,
            "channel_type": "wecom",
            "account_config": {
                "bot_id": "shared-provider-bot",
                "thinking_message": "处理中，请稍候",
            },
        },
    )
    conflicting_restore = await api_client.patch(
        f"/api/v1/tenants/{first_tenant}/channel-bindings/{first.json()['binding_id']}",
        json={"status": "active"},
    )

    assert first.status_code == 201
    assert duplicate.status_code == 409
    assert disabled.status_code == 204
    assert moved.status_code == 201
    assert conflicting_restore.status_code == 409


@pytest.mark.anyio
async def test_binding_update_rejects_cross_tenant_agent_and_unknown_binding(
    api_client: httpx.AsyncClient, ) -> None:
    owner_id = await _create_tenant(api_client, "Binding Owner")
    other_id = await _create_tenant(api_client, "Binding Other")
    owner_agent = await _create_agent(api_client, owner_id, "Owner Agent")
    other_agent = await _create_agent(api_client, other_id, "Other Agent")
    created = await api_client.post(
        f"/api/v1/tenants/{owner_id}/channel-bindings",
        json={
            "agent_app_id": owner_agent,
            "channel_type": "web",
            "external_account_hash": "sha256:update-account",
        },
    )

    cross_tenant = await api_client.patch(
        f"/api/v1/tenants/{owner_id}/channel-bindings/{created.json()['binding_id']}",
        json={"agent_app_id": other_agent},
    )
    missing = await api_client.get(
        f"/api/v1/tenants/{owner_id}/channel-bindings/00000000-0000-0000-0000-000000000000")

    assert cross_tenant.status_code == 404
    assert missing.status_code == 404


@pytest.mark.anyio
async def test_binding_rejects_plaintext_secrets_and_null_updates(
    api_client: httpx.AsyncClient, ) -> None:
    tenant_id = await _create_tenant(api_client, "Binding Secrets")
    agent_id = await _create_agent(api_client, tenant_id, "Secure Agent")
    plaintext = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json={
            "agent_app_id": agent_id,
            "channel_type": "web",
            "external_account_hash": "sha256:plaintext-secret",
            "secret_ref_map": {
                "token": "plaintext-token"
            },
        },
    )
    created = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json={
            "agent_app_id": agent_id,
            "channel_type": "web",
            "external_account_hash": "sha256:null-update",
        },
    )
    null_update = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/channel-bindings/{created.json()['binding_id']}",
        json={"capabilities": None},
    )

    assert plaintext.status_code == 422
    assert null_update.status_code == 422


@pytest.mark.anyio
async def test_binding_rejects_cross_scope_channel_secret(api_client: httpx.AsyncClient) -> None:
    tenant_id = await _create_tenant(api_client, "Binding Secret Scope")
    agent_id = await _create_agent(api_client, tenant_id, "Scoped Agent")

    response = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json={
            "agent_app_id": agent_id,
            "channel_type": "web",
            "external_account_hash": "sha256:cross-scope-secret",
            "secret_ref_map": {
                "token": "env://DASHSCOPE_API_KEY"
            },
        },
    )

    assert response.status_code == 422
