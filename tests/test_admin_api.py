from uuid import UUID, uuid4

import httpx
import pytest


async def _create_tenant(client: httpx.AsyncClient, name: str) -> str:
    response = await client.post("/api/v1/tenants", json={"name": name})
    assert response.status_code == 201
    return str(response.json()["tenant_id"])


async def _issue_tenant_admin(
    client: httpx.AsyncClient,
    tenant_id: str,
    name: str = "Tenant Administrator",
) -> tuple[str, str]:
    del name  # The tenant name determines the single account's display label.
    account_response = await client.post(
        "/api/v1/admin/tenant-accounts",
        json={
            "tenant_id": tenant_id,
            "username": f"admin-{tenant_id}",
            "password": "correct horse battery staple",
        },
    )
    assert account_response.status_code == 201
    principal_id = str(account_response.json()["management_principal_id"])
    # API credentials keep route tests independent from browser cookie/CSRF flow.
    credential = await client.post(
        f"/api/v1/admin/principals/{principal_id}/credentials",
        json={"name": "test credential"},
    )
    assert credential.status_code == 201
    body = credential.json()
    assert body["token"].startswith("trpc_admin_")
    assert "token_hash" not in body
    return principal_id, str(body["token"])


@pytest.mark.anyio
async def test_admin_console_remains_available_without_temporary_webui(
    api_client: httpx.AsyncClient, ) -> None:
    """The control-plane page remains public while the test IM is absent."""

    page = await api_client.get("/admin")
    tenant_page = await api_client.get("/tenant")
    stylesheet = await api_client.get("/admin/assets/admin.css")
    script = await api_client.get("/admin/assets/admin.js")
    tenant_stylesheet = await api_client.get("/tenant/assets/tenant.css")
    tenant_script = await api_client.get("/tenant/assets/tenant.js")
    shared_stylesheet = await api_client.get("/console/assets/console.css")
    shared_script = await api_client.get("/console/assets/console.js")
    unguarded_console = await api_client.get("/console/assets/admin/index.html")
    unguarded_admin = await api_client.get("/admin/assets/index.html")
    unguarded_tenant = await api_client.get("/tenant/assets/index.html")
    webui_page = await api_client.get("/webui")
    webui_asset = await api_client.get("/webui/assets/webui.js")
    webui_api = await api_client.post("/api/v1/channels/webui/not-installed/messages", json={})

    assert page.status_code == 200
    assert "系统管理员登录" in page.text
    assert "Worker Pool 容量" in page.text
    assert "租户管理员登录" in tenant_page.text
    assert 'id="channel-cards"' not in page.text
    assert 'id="model-rows"' not in tenant_page.text
    assert "租户所有者" not in page.text
    assert "成员权限" not in tenant_page.text
    assert "management-members" not in tenant_script.text
    assert "待系统管理员配置模型" in tenant_script.text
    assert "只读直通" in tenant_script.text
    assert "写操作确认" in tenant_script.text
    assert "尚未授权给 Agent" in tenant_script.text
    assert "配置 Agent" in tenant_script.text
    assert "刷新成功不等于 Agent 已获授权" in tenant_page.text
    assert 'value="http.get"' in tenant_page.text
    assert 'value="http_get"' not in tenant_page.text
    assert 'data-view="mcp"' in tenant_page.text
    assert "body?.error?.message" in shared_script.text
    assert 'id="tenant-select"' not in tenant_page.text
    assert "test-platform-admin-token" not in page.text
    assert "/admin/worker-pool" in script.text
    assert 'id="tenant-edit-dialog"' in page.text
    assert 'id="agent-profile-dialog"' in page.text
    assert 'id="profile-agent-rows"' in page.text
    assert "model_catalog_id" in script.text
    assert 'method: "DELETE"' in script.text
    assert page.headers["cache-control"] == "no-store"
    assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
    assert tenant_page.headers["cache-control"] == "no-store"
    assert "frame-ancestors 'none'" in tenant_page.headers["content-security-policy"]
    assert stylesheet.status_code == 200
    assert script.status_code == 200
    assert tenant_stylesheet.status_code == 200
    assert tenant_script.status_code == 200
    assert shared_stylesheet.status_code == 200
    assert shared_script.status_code == 200
    assert unguarded_console.status_code == 404
    assert unguarded_admin.status_code == 404
    assert unguarded_tenant.status_code == 404
    assert webui_page.status_code == 404
    assert webui_asset.status_code == 404
    assert webui_api.status_code == 404


@pytest.mark.anyio
async def test_platform_admin_can_observe_runtime_nodes(api_client: httpx.AsyncClient) -> None:
    nodes = await api_client.get("/api/v1/admin/runtime-nodes")
    audit = await api_client.get("/api/v1/admin/audit")

    assert nodes.status_code == 200
    assert nodes.json()["total"] == 1
    assert nodes.json()["items"][0]["role"] == "api_worker"
    assert nodes.json()["items"][0]["health"] == "active"
    assert any(item["action"] == "runtime_node.list" for item in audit.json()["items"])


@pytest.mark.anyio
async def test_platform_admin_can_scale_worker_pool_with_generation_guard(
        api_client: httpx.AsyncClient) -> None:
    initial = await api_client.get("/api/v1/admin/worker-pool")
    assert initial.status_code == 200
    assert initial.json()["desired_nodes"] == 2

    expanded = await api_client.put(
        "/api/v1/admin/worker-pool",
        json={
            "desired_nodes": 5,
            "expected_generation": initial.json()["generation"],
        },
    )
    stale = await api_client.put(
        "/api/v1/admin/worker-pool",
        json={
            "desired_nodes": 3,
            "expected_generation": initial.json()["generation"],
        },
    )
    audit = await api_client.get("/api/v1/admin/audit")

    assert expanded.status_code == 200
    assert expanded.json()["desired_nodes"] == 5
    assert expanded.json()["reconciling"]
    assert stale.status_code == 409
    assert any(item["action"] == "worker_pool.scale" for item in audit.json()["items"])


@pytest.mark.anyio
async def test_only_platform_admin_can_query_usage_ledger(api_client: httpx.AsyncClient, ) -> None:
    tenant_id = await _create_tenant(api_client, "Usage Tenant")
    _, token = await _issue_tenant_admin(api_client, tenant_id, "Usage Owner")

    platform_view = await api_client.get(
        "/api/v1/admin/usage",
        params={"tenant_id": tenant_id},
    )
    tenant_view = await api_client.get(
        "/api/v1/admin/usage",
        headers={
            "Authorization": f"Bearer {token}",
            "X-Support-Reason": ""
        },
    )

    assert platform_view.status_code == 200
    assert platform_view.json()["summary"]["total_tokens"] == 0
    assert tenant_view.status_code == 403


@pytest.mark.anyio
async def test_admin_endpoints_require_authentication_and_support_reason(
    api_client: httpx.AsyncClient, ) -> None:
    missing = await api_client.get(
        "/api/v1/tenants",
        headers={
            "Authorization": "",
            "X-Support-Reason": ""
        },
    )
    invalid = await api_client.get(
        "/api/v1/tenants",
        headers={
            "Authorization": "Bearer invalid",
            "X-Support-Reason": ""
        },
    )
    tenant_id = await _create_tenant(api_client, "Support Boundary")
    silent_support = await api_client.get(
        f"/api/v1/tenants/{tenant_id}/agents",
        headers={"X-Support-Reason": ""},
    )
    explicit_support = await api_client.get(
        f"/api/v1/tenants/{tenant_id}/agents",
        headers={"X-Support-Reason": "incident-2026-001 tenant approved support"},
    )
    audit = await api_client.get("/api/v1/admin/audit")

    assert missing.status_code == 401
    assert invalid.status_code == 401
    assert silent_support.status_code == 403
    assert explicit_support.status_code == 200
    assert any(item["action"] == "platform_support.access"
               and item["reason"] == "incident-2026-001 tenant approved support"
               for item in audit.json()["items"])


@pytest.mark.anyio
async def test_tenant_admin_credential_is_scoped_and_revocable(
    api_client: httpx.AsyncClient, ) -> None:
    owned_tenant = await _create_tenant(api_client, "Owned Tenant")
    other_tenant = await _create_tenant(api_client, "Other Tenant")
    principal_id, token = await _issue_tenant_admin(api_client, owned_tenant)
    tenant_headers = {"Authorization": f"Bearer {token}", "X-Support-Reason": ""}

    own_agent = await api_client.post(
        f"/api/v1/tenants/{owned_tenant}/agents",
        json={"name": "Tenant Agent"},
        headers=tenant_headers,
    )
    cross_tenant = await api_client.get(
        f"/api/v1/tenants/{other_tenant}/agents",
        headers=tenant_headers,
    )
    credentials = await api_client.get(f"/api/v1/admin/principals/{principal_id}/credentials", )
    credential_id = credentials.json()["items"][0]["credential_id"]
    revoked = await api_client.delete(f"/api/v1/admin/credentials/{credential_id}")
    after_revoke = await api_client.get(
        f"/api/v1/tenants/{owned_tenant}/agents",
        headers=tenant_headers,
    )

    assert own_agent.status_code == 201
    assert cross_tenant.status_code == 403
    assert credentials.status_code == 200
    assert "token" not in credentials.json()["items"][0]
    assert revoked.status_code == 204
    assert after_revoke.status_code == 401


@pytest.mark.anyio
async def test_platform_owns_model_credentials_and_tenant_profiles(
    api_client: httpx.AsyncClient, ) -> None:
    tenant_id = await _create_tenant(api_client, "Model Tenant")
    _, token = await _issue_tenant_admin(api_client, tenant_id, "Model Owner")
    tenant_headers = {"Authorization": f"Bearer {token}", "X-Support-Reason": ""}
    catalog = await api_client.post(
        "/api/v1/admin/model-catalog",
        json={
            "provider": "bailian",
            "model_name": "qwen-max",
            "display_name": "Qwen Max",
            "capabilities": {
                "text": True
            },
            "default_limits": {
                "max_output_tokens": 4096
            },
        },
    )
    assert catalog.status_code == 201
    catalog_id = catalog.json()["model_catalog_id"]
    credential = await api_client.post(
        "/api/v1/admin/model-credentials",
        json={
            "provider": "bailian",
            "name": "default-bailian",
            "secret_ref": "env://DASHSCOPE_API_KEY",
        },
    )
    assert credential.status_code == 201
    assert "secret_ref" not in credential.json()
    credential_id = credential.json()["model_credential_id"]
    wrong_provider = await api_client.post(
        "/api/v1/admin/model-credentials",
        json={
            "provider": "bailian_openai",
            "name": "wrong-provider",
            "secret_ref": "env://DASHSCOPE_API_KEY",
        },
    )
    provider_mismatch = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/model-profiles",
        json={
            "name": "wrong-provider",
            "model_catalog_id": catalog_id,
            "credential_id": wrong_provider.json()["model_credential_id"],
        },
    )

    tenant_attempt = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/model-profiles",
        json={
            "name": "tenant-cannot-configure",
            "model_catalog_id": catalog_id,
            "credential_id": credential_id,
        },
        headers=tenant_headers,
    )
    unsafe_budget = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/model-profiles",
        json={
            "name": "unsafe-context-window",
            "model_catalog_id": catalog_id,
            "credential_id": credential_id,
            "parameter_config": {
                "context_window_tokens": 2048
            },
            "limits": {
                "daily_tokens": 100000
            },
        },
    )
    created = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/model-profiles",
        json={
            "name": "primary",
            "model_catalog_id": catalog_id,
            "credential_id": credential_id,
            "parameter_config": {
                "temperature": 0.2,
                "context_window_tokens": 32768
            },
            "limits": {
                "daily_tokens": 100000
            },
        },
    )

    assert tenant_attempt.status_code == 403
    assert unsafe_budget.status_code == 409
    assert provider_mismatch.status_code == 409
    assert created.status_code == 201
    assert created.json()["tenant_id"] == tenant_id
    assert created.json()["secret_configured"] is True
    assert "secret_ref" not in created.json()
    assert created.json()["credential_id"] == credential_id

    tenant_linked_agent = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={
            "name": "Profile Agent",
            "model_profile_id": created.json()["model_profile_id"],
        },
        headers=tenant_headers,
    )
    linked_agent = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={
            "name": "Profile Agent",
            "model_profile_id": created.json()["model_profile_id"],
        },
    )
    assert tenant_linked_agent.status_code == 403
    assert linked_agent.status_code == 201
    assert linked_agent.json()["model_profile_id"] == created.json()["model_profile_id"]

    # Model parameters are centralized in the platform-owned Profile even for
    # platform administrators; Agent snapshots cannot introduce local drift.
    local_override = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/agents/{linked_agent.json()['agent_app_id']}",
        json={"model_config": {
            "temperature": 1.8
        }},
    )
    assert local_override.status_code == 409


@pytest.mark.anyio
async def test_disabled_model_credential_blocks_profile_reactivation(
    api_client: httpx.AsyncClient, ) -> None:
    """A disabled SecretRef must not re-enter the runtime through an old profile."""

    tenant_id = await _create_tenant(api_client, "Credential Lifecycle Tenant")
    catalog = await api_client.post(
        "/api/v1/admin/model-catalog",
        json={
            "provider": "bailian",
            "model_name": "qwen-max",
            "display_name": "Qwen Max Lifecycle",
        },
    )
    credential = await api_client.post(
        "/api/v1/admin/model-credentials",
        json={
            "provider": "bailian",
            "name": "lifecycle-credential",
            "secret_ref": "env://DASHSCOPE_API_KEY",
        },
    )
    profile = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/model-profiles",
        json={
            "name": "lifecycle-profile",
            "model_catalog_id": catalog.json()["model_catalog_id"],
            "credential_id": credential.json()["model_credential_id"],
        },
    )
    profile_id = profile.json()["model_profile_id"]

    disabled_profile = await api_client.delete(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{profile_id}")
    disabled_credential = await api_client.patch(
        f"/api/v1/admin/model-credentials/{credential.json()['model_credential_id']}",
        json={"status": "disabled"},
    )
    reactivated = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{profile_id}",
        json={"status": "active"},
    )

    assert disabled_profile.status_code == 204
    assert disabled_credential.status_code == 200
    assert reactivated.status_code == 409
    assert reactivated.json()["error"] == {
        "code": "conflict",
        "message": "model credential is not active",
    }


@pytest.mark.anyio
async def test_model_catalog_rejects_provider_without_runtime(
    api_client: httpx.AsyncClient, ) -> None:
    response = await api_client.post(
        "/api/v1/admin/model-catalog",
        json={
            "provider": "not-installed",
            "model_name": "unrunnable-model",
            "display_name": "Unrunnable Model",
        },
    )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_model_catalog_rejects_fractional_token_limits(
    api_client: httpx.AsyncClient, ) -> None:
    response = await api_client.post(
        "/api/v1/admin/model-catalog",
        json={
            "provider": "bailian",
            "model_name": "invalid-token-default",
            "display_name": "Invalid Token Default",
            "default_limits": {
                "max_output_tokens": 4096.5
            },
        },
    )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_channel_binding_requires_an_active_adapter_type(
    api_client: httpx.AsyncClient, ) -> None:
    tenant_id = await _create_tenant(api_client, "Channel Tenant")
    _, token = await _issue_tenant_admin(api_client, tenant_id, "Channel Owner")
    tenant_headers = {"Authorization": f"Bearer {token}", "X-Support-Reason": ""}
    agent = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={"name": "Channel Agent"},
        headers=tenant_headers,
    )
    agent_id = agent.json()["agent_app_id"]
    payload = {
        "agent_app_id": agent_id,
        "channel_type": "custom_im",
        "external_account_hash": "sha256:managed-wecom",
        "secret_ref_map": {
            "token": f"secret-manager://tenants/{tenant_id}/channels/wecom-token"
        },
        "account_config": {
            "region": "cn"
        },
    }
    unknown = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json=payload,
        headers=tenant_headers,
    )
    adapter = await api_client.post(
        "/api/v1/admin/channel-adapter-types",
        json={
            "channel_type": "custom_im",
            "display_name": "Custom IM",
            "adapter_version": "1.0",
            "config_schema": {
                "type": "object",
                "properties": {
                    "region": {
                        "type": "string"
                    }
                },
                "required": ["region"],
                "additionalProperties": False,
            },
            "secret_schema": {
                "type": "object",
                "properties": {
                    "token": {
                        "type": "string"
                    }
                },
                "required": ["token"],
                "additionalProperties": False,
            },
            "capabilities": {
                "text": True
            },
        },
    )
    still_inactive = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json=payload,
        headers=tenant_headers,
    )
    activated = await api_client.patch(
        "/api/v1/admin/channel-adapter-types/custom_im",
        json={"status": "active"},
    )
    assert unknown.status_code == 409
    assert adapter.status_code == 201
    assert adapter.json()["status"] == "disabled"
    assert still_inactive.status_code == 409
    assert activated.status_code == 409
    assert "channel adapter implementation is not registered" in str(activated.json())


@pytest.mark.anyio
async def test_management_mutations_are_audited(api_client: httpx.AsyncClient) -> None:
    tenant_id = await _create_tenant(api_client, "Audited Tenant")
    response = await api_client.get("/api/v1/admin/audit")

    assert response.status_code == 200
    records = response.json()["items"]
    assert any(record["action"] == "tenant.create" and record["resource_id"] == tenant_id
               for record in records)
    assert any(record["action"] == "platform_management.read" for record in records)
    assert all("details_redacted" in record for record in records)
    UUID(records[0]["audit_id"])


@pytest.mark.anyio
async def test_admin_api_rejects_conflicts_and_unknown_resources(
    api_client: httpx.AsyncClient, ) -> None:
    """Keep management failures stable instead of leaking database errors."""

    missing_id = uuid4()
    principal_payload = {
        "display_name": "Boundary Operator",
        "principal_type": "human",
        "external_subject": "local:boundary-operator",
    }
    principal = await api_client.post("/api/v1/admin/principals", json=principal_payload)
    principal_id = principal.json()["management_principal_id"]
    reserved_subject = await api_client.post(
        "/api/v1/admin/principals",
        json={
            **principal_payload,
            "external_subject": "tenant-console:not-a-tenant-account",
        },
    )
    tenant_id = await _create_tenant(api_client, "Boundary Tenant")
    role_payload = {"role": "platform_admin"}
    assigned = await api_client.post(
        f"/api/v1/admin/principals/{principal_id}/role-assignments",
        json=role_payload,
    )

    responses = [
        await api_client.post("/api/v1/admin/principals", json=principal_payload),
        await api_client.patch(f"/api/v1/admin/principals/{missing_id}",
                               json={"display_name": "Missing"}),
        await api_client.post(f"/api/v1/admin/principals/{missing_id}/role-assignments",
                              json=role_payload),
        await api_client.post(
            f"/api/v1/admin/principals/{principal_id}/role-assignments",
            json={
                "role": "tenant_admin",
                "tenant_id": str(missing_id)
            },
        ),
        await api_client.post(f"/api/v1/admin/principals/{principal_id}/role-assignments",
                              json=role_payload),
        await api_client.delete(f"/api/v1/admin/role-assignments/{missing_id}"),
        await api_client.post(f"/api/v1/admin/principals/{missing_id}/credentials",
                              json={"name": "missing"}),
        await api_client.get(f"/api/v1/admin/principals/{missing_id}/credentials"),
        await api_client.delete(f"/api/v1/admin/credentials/{missing_id}"),
        await api_client.patch(f"/api/v1/admin/model-catalog/{missing_id}",
                               json={"status": "disabled"}),
        await api_client.patch("/api/v1/admin/channel-adapter-types/missing_im",
                               json={"status": "disabled"}),
    ]

    assert principal.status_code == 201
    assert reserved_subject.status_code == 422
    assert assigned.status_code == 201
    assert [response.status_code for response in responses] == [
        409,
        404,
        404,
        422,
        409,
        404,
        404,
        404,
        404,
        404,
        404,
    ]
    disabled = await api_client.patch(f"/api/v1/admin/principals/{principal_id}",
                                      json={"status": "disabled"})
    rejected_legacy_role = await api_client.post(
        f"/api/v1/admin/principals/{principal_id}/role-assignments",
        json={
            "role": "tenant_owner",
            "tenant_id": tenant_id
        },
    )
    assert disabled.status_code == 200
    assert rejected_legacy_role.status_code == 422


@pytest.mark.anyio
async def test_management_resources_support_lifecycle_queries(
    api_client: httpx.AsyncClient, ) -> None:
    """Exercise the read and update paths used by the lightweight admin console."""

    tenant_id = await _create_tenant(api_client, "Lifecycle Tenant")
    principal_id, tenant_token = await _issue_tenant_admin(
        api_client,
        tenant_id,
        "Lifecycle Owner",
    )
    actor = await api_client.get(
        "/api/v1/admin/me",
        headers={
            "Authorization": f"Bearer {tenant_token}",
            "X-Support-Reason": "",
        },
    )
    principals = await api_client.get("/api/v1/admin/principals")
    expired = await api_client.post(
        f"/api/v1/admin/principals/{principal_id}/credentials",
        json={
            "name": "already expired",
            "expires_at": "2020-01-01T00:00:00Z",
        },
    )
    catalog = await api_client.post(
        "/api/v1/admin/model-catalog",
        json={
            "provider": "bailian",
            "model_name": "qwen-long",
            "display_name": "Qwen Long",
        },
    )
    catalog_id = catalog.json()["model_catalog_id"]
    updated_catalog = await api_client.patch(
        f"/api/v1/admin/model-catalog/{catalog_id}",
        json={
            "display_name": "Qwen Long Context",
            "default_limits": {
                "max_output_tokens": 8192,
                "context_window_tokens": 32768
            },
        },
    )
    catalogs = await api_client.get("/api/v1/admin/model-catalog")
    model_credential = await api_client.post(
        "/api/v1/admin/model-credentials",
        json={
            "provider": "bailian",
            "name": "lifecycle-bailian",
            "secret_ref": "env://DASHSCOPE_API_KEY",
        },
    )
    profile = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/model-profiles",
        json={
            "name": "platform-default",
            "model_catalog_id": catalog_id,
            "credential_id": model_credential.json()["model_credential_id"],
        },
    )
    profile_id = profile.json()["model_profile_id"]
    updated_profile = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{profile_id}",
        json={
            "name": "platform-primary",
            "limits": {
                "daily_tokens": 200000
            },
        },
    )
    profiles = await api_client.get(f"/api/v1/tenants/{tenant_id}/model-profiles")
    available_models = await api_client.get(
        f"/api/v1/tenants/{tenant_id}/available-models",
        headers={
            "Authorization": f"Bearer {tenant_token}",
            "X-Support-Reason": "",
        },
    )
    disabled_profile = await api_client.delete(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{profile_id}")
    adapter = await api_client.post(
        "/api/v1/admin/channel-adapter-types",
        json={
            "channel_type": "telegram",
            "display_name": "Telegram",
            "adapter_version": "1.0",
        },
    )
    updated_adapter = await api_client.patch(
        "/api/v1/admin/channel-adapter-types/telegram",
        json={
            "adapter_version": "1.1",
            "status": "disabled",
        },
    )
    adapters = await api_client.get("/api/v1/admin/channel-adapter-types")
    roles = await api_client.get(f"/api/v1/admin/principals/{principal_id}/role-assignments")
    role_assignment_id = roles.json()["items"][0]["role_assignment_id"]
    revoked_role = await api_client.delete(f"/api/v1/admin/role-assignments/{role_assignment_id}")
    after_role_revoke = await api_client.get(
        f"/api/v1/tenants/{tenant_id}/available-models",
        headers={
            "Authorization": f"Bearer {tenant_token}",
            "X-Support-Reason": "",
        },
    )
    disabled_principal = await api_client.patch(
        f"/api/v1/admin/principals/{principal_id}",
        json={"status": "disabled"},
    )
    after_principal_disable = await api_client.get(
        "/api/v1/admin/me",
        headers={
            "Authorization": f"Bearer {tenant_token}",
            "X-Support-Reason": "",
        },
    )

    assert actor.status_code == 200
    assert actor.json()["tenant_roles"][tenant_id] == ["tenant_admin"]
    assert principals.json()["total"] == 1
    assert expired.status_code == 422
    assert updated_catalog.json()["display_name"] == "Qwen Long Context"
    assert catalogs.json()["total"] == 1
    assert profile.json()["secret_configured"] is True
    assert updated_profile.json()["name"] == "platform-primary"
    assert profiles.json()["total"] == 1
    assert available_models.json()["total"] == 1
    assert disabled_profile.status_code == 204
    assert adapter.status_code == 201
    assert updated_adapter.json()["status"] == "disabled"
    assert adapters.json()["total"] == 3
    assert roles.json()["total"] == 1
    assert revoked_role.status_code == 409
    assert after_role_revoke.status_code == 200
    assert disabled_principal.status_code == 409
    assert after_principal_disable.status_code == 200


@pytest.mark.anyio
async def test_each_tenant_has_one_administrator_and_no_member_management_api(
    api_client: httpx.AsyncClient, ) -> None:
    tenant_id = await _create_tenant(api_client, "Single Administrator Tenant")
    legacy_account = await api_client.post(
        "/api/v1/admin/tenant-accounts",
        json={
            "tenant_id": tenant_id,
            "display_name": "Legacy Owner",
            "username": "legacy-owner",
            "password": "another correct horse password",
            "role": "tenant_owner",
        },
    )
    principal_id, admin_token = await _issue_tenant_admin(
        api_client,
        tenant_id,
        "Tenant Administrator",
    )
    second_principal = await api_client.post(
        "/api/v1/admin/principals",
        json={
            "display_name": "Second Tenant Administrator",
            "principal_type": "human",
            "external_subject": "local:second-tenant-administrator",
        },
    )
    second_principal_id = second_principal.json()["management_principal_id"]
    duplicate_assignment = await api_client.post(
        f"/api/v1/admin/principals/{second_principal_id}/role-assignments",
        json={
            "role": "tenant_admin",
            "tenant_id": tenant_id
        },
    )
    other_tenant_id = await _create_tenant(api_client, "Other Single Administrator Tenant")
    reused_principal = await api_client.post(
        f"/api/v1/admin/principals/{principal_id}/role-assignments",
        json={
            "role": "tenant_admin",
            "tenant_id": other_tenant_id
        },
    )
    mixed_management_scope = await api_client.post(
        f"/api/v1/admin/principals/{principal_id}/role-assignments",
        json={"role": "platform_admin"},
    )
    duplicate_account = await api_client.post(
        "/api/v1/admin/tenant-accounts",
        json={
            "tenant_id": tenant_id,
            "username": "second-admin",
            "password": "another correct horse password",
        },
    )
    admin_headers = {
        "Authorization": f"Bearer {admin_token}",
        "X-Support-Reason": "",
    }
    removed_members_api = await api_client.get(
        f"/api/v1/tenants/{tenant_id}/management-members",
        headers=admin_headers,
    )
    visible = await api_client.get(
        f"/api/v1/tenants/{tenant_id}/available-models",
        headers=admin_headers,
    )
    roles = await api_client.get(f"/api/v1/admin/principals/{principal_id}/role-assignments")
    blocked_revoke = await api_client.delete(
        f"/api/v1/admin/role-assignments/{roles.json()['items'][0]['role_assignment_id']}")

    assert legacy_account.status_code == 422
    assert duplicate_assignment.status_code == 422
    assert reused_principal.status_code == 422
    assert mixed_management_scope.status_code == 409
    assert duplicate_account.status_code == 409
    assert removed_members_api.status_code == 404
    assert visible.status_code == 200
    assert blocked_revoke.status_code == 409
