"""Control-plane boundary and lifecycle tests through the public HTTP API."""

from uuid import uuid4

import httpx
import pytest


async def _tenant(client: httpx.AsyncClient, name: str) -> dict[str, object]:
    response = await client.post("/api/v1/tenants", json={"name": name})
    assert response.status_code == 201
    return response.json()


async def _agent(client: httpx.AsyncClient, tenant_id: str, name: str) -> dict[str, object]:
    response = await client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={"name": name},
    )
    assert response.status_code == 201
    return response.json()


async def _model_resources(
    client: httpx.AsyncClient,
    tenant_id: str,
    suffix: str,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    catalog = await client.post(
        "/api/v1/admin/model-catalog",
        json={
            "provider": "bailian",
            "model_name": f"qwen-{suffix}",
            "display_name": f"Qwen {suffix}",
            "default_limits": {
                "max_output_tokens": 1024,
                "context_window_tokens": 8192,
            },
        },
    )
    credential = await client.post(
        "/api/v1/admin/model-credentials",
        json={
            "provider": "bailian",
            "name": f"credential-{suffix}",
            "secret_ref": "env://DASHSCOPE_API_KEY",
        },
    )
    assert catalog.status_code == credential.status_code == 201
    profile = await client.post(
        f"/api/v1/tenants/{tenant_id}/model-profiles",
        json={
            "name": f"profile-{suffix}",
            "model_catalog_id": catalog.json()["model_catalog_id"],
            "credential_id": credential.json()["model_credential_id"],
        },
    )
    assert profile.status_code == 201
    return catalog.json(), credential.json(), profile.json()


@pytest.mark.anyio
async def test_tenant_http_boundaries_and_name_conflict(api_client: httpx.AsyncClient) -> None:
    """Tenant updates remain deterministic for missing and duplicate resources."""

    first = await _tenant(api_client, "Boundary First")
    second = await _tenant(api_client, "Boundary Second")
    missing = str(uuid4())

    listed = await api_client.get("/api/v1/tenants", params={"offset": 1, "limit": 1})
    fetched = await api_client.get(f"/api/v1/tenants/{first['tenant_id']}")
    renamed = await api_client.patch(
        f"/api/v1/tenants/{first['tenant_id']}",
        json={"name": "Boundary Renamed"},
    )
    duplicate = await api_client.patch(
        f"/api/v1/tenants/{first['tenant_id']}",
        json={"name": second["name"]},
    )
    missing_get = await api_client.get(f"/api/v1/tenants/{missing}")
    missing_update = await api_client.patch(f"/api/v1/tenants/{missing}", json={"name": "Missing"})
    missing_delete = await api_client.delete(f"/api/v1/tenants/{missing}")

    assert listed.status_code == 200
    assert listed.json()["total"] == 2
    assert len(listed.json()["items"]) == 1
    assert fetched.json()["name"] == "Boundary First"
    assert renamed.json()["name"] == "Boundary Renamed"
    assert duplicate.status_code == 409
    assert [missing_get.status_code, missing_update.status_code, missing_delete.status_code] == [
        404,
        404,
        404,
    ]


@pytest.mark.anyio
async def test_model_catalog_and_credential_full_lifecycle(api_client: httpx.AsyncClient) -> None:
    """Platform resources expose metadata but never return a secret reference."""

    catalog_payload = {
        "provider": "bailian",
        "model_name": "qwen-control-boundary",
        "display_name": "Qwen Control Boundary",
        "platform_secret_ref": "env://DASHSCOPE_API_KEY",
    }
    catalog = await api_client.post("/api/v1/admin/model-catalog", json=catalog_payload)
    duplicate_catalog = await api_client.post("/api/v1/admin/model-catalog", json=catalog_payload)
    updated_catalog = await api_client.patch(
        f"/api/v1/admin/model-catalog/{catalog.json()['model_catalog_id']}",
        json={
            "model_name": "qwen-control-boundary-v2",
            "status": "disabled",
            "capabilities": {
                "text": True
            }
        },
    )
    catalogs = await api_client.get("/api/v1/admin/model-catalog")

    credential_payload = {
        "provider": "bailian",
        "name": "control-boundary",
        "secret_ref": "env://DASHSCOPE_API_KEY",
    }
    credential = await api_client.post("/api/v1/admin/model-credentials", json=credential_payload)
    duplicate_credential = await api_client.post("/api/v1/admin/model-credentials",
                                                 json=credential_payload)
    second = await api_client.post(
        "/api/v1/admin/model-credentials",
        json={
            **credential_payload, "name": "control-boundary-second"
        },
    )
    conflict = await api_client.patch(
        f"/api/v1/admin/model-credentials/{second.json()['model_credential_id']}",
        json={"name": "control-boundary"},
    )
    updated_credential = await api_client.patch(
        f"/api/v1/admin/model-credentials/{credential.json()['model_credential_id']}",
        json={
            "name": "control-boundary-renamed",
            "status": "disabled"
        },
    )
    credentials = await api_client.get("/api/v1/admin/model-credentials")
    deleted_catalog = await api_client.delete(
        f"/api/v1/admin/model-catalog/{catalog.json()['model_catalog_id']}")
    deleted_credential = await api_client.delete(
        f"/api/v1/admin/model-credentials/{credential.json()['model_credential_id']}")
    missing = await api_client.patch(f"/api/v1/admin/model-credentials/{uuid4()}",
                                     json={"status": "disabled"})

    assert catalog.status_code == 201
    assert catalog.json()["platform_credential_configured"] is True
    assert duplicate_catalog.status_code == 409
    assert updated_catalog.json()["model_name"] == "qwen-control-boundary-v2"
    assert updated_catalog.json()["status"] == "disabled"
    assert catalogs.json()["total"] == 1
    assert credential.status_code == 201
    assert duplicate_credential.status_code == 409
    assert conflict.status_code == 409
    assert updated_credential.json()["status"] == "disabled"
    assert deleted_catalog.status_code == 204
    assert deleted_credential.status_code == 204
    assert credentials.json()["total"] == 2
    assert all("secret_ref" not in item for item in credentials.json()["items"])
    assert missing.status_code == 404


@pytest.mark.anyio
async def test_model_profile_rejects_inactive_dependencies_and_invalid_budgets(
    api_client: httpx.AsyncClient, ) -> None:
    tenant = await _tenant(api_client, "Profile Boundary")
    tenant_id = str(tenant["tenant_id"])
    catalog, credential, profile = await _model_resources(api_client, tenant_id, "profile-boundary")
    missing = str(uuid4())

    missing_tenant_list = await api_client.get(f"/api/v1/tenants/{missing}/model-profiles")
    missing_tenant_models = await api_client.get(f"/api/v1/tenants/{missing}/available-models")
    missing_profile_update = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{missing}",
        json={"status": "disabled"},
    )
    duplicate = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/model-profiles",
        json={
            "name": profile["name"],
            "model_catalog_id": catalog["model_catalog_id"],
            "credential_id": credential["model_credential_id"],
        },
    )
    too_small_daily_budget = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{profile['model_profile_id']}",
        json={
            "parameter_config": {
                "max_output_tokens": 1024,
                "context_window_tokens": 4096,
            },
            "limits": {
                "daily_tokens": 2048
            },
        },
    )
    missing_credential = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{profile['model_profile_id']}",
        json={"credential_id": missing},
    )

    replacement_catalog = await api_client.post(
        "/api/v1/admin/model-catalog",
        json={
            "provider": "bailian",
            "model_name": "qwen-profile-replacement",
            "display_name": "Qwen Profile Replacement",
        },
    )
    changed_model = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{profile['model_profile_id']}",
        json={"model_catalog_id": replacement_catalog.json()["model_catalog_id"]},
    )

    await api_client.patch(
        f"/api/v1/admin/model-catalog/{replacement_catalog.json()['model_catalog_id']}",
        json={"status": "disabled"},
    )
    await api_client.delete(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{profile['model_profile_id']}")
    inactive_catalog = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{profile['model_profile_id']}",
        json={"status": "active"},
    )

    assert missing_tenant_list.status_code == 404
    assert missing_tenant_models.status_code == 404
    assert missing_profile_update.status_code == 404
    assert duplicate.status_code == 409
    assert too_small_daily_budget.status_code == 409
    assert missing_credential.status_code == 409
    assert changed_model.status_code == 200
    assert changed_model.json()["model_catalog_id"] == replacement_catalog.json(
    )["model_catalog_id"]
    assert inactive_catalog.status_code == 409


@pytest.mark.anyio
async def test_platform_admin_can_reassign_agent_model_profile(
    api_client: httpx.AsyncClient, ) -> None:
    """The console workflow updates Agent policy before retiring an old profile."""

    tenant = await _tenant(api_client, "Agent Model Assignment")
    tenant_id = str(tenant["tenant_id"])
    _, _, first_profile = await _model_resources(api_client, tenant_id, "assignment-one")
    _, _, second_profile = await _model_resources(api_client, tenant_id, "assignment-two")
    agent = await _agent(api_client, tenant_id, "Assignment Agent")
    agent_id = str(agent["agent_app_id"])

    assigned = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/agents/{agent_id}",
        json={"model_profile_id": second_profile["model_profile_id"]},
    )
    protected = await api_client.delete(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{second_profile['model_profile_id']}")
    restored = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/agents/{agent_id}",
        json={"model_profile_id": first_profile["model_profile_id"]},
    )
    removed = await api_client.delete(
        f"/api/v1/tenants/{tenant_id}/model-profiles/{second_profile['model_profile_id']}")

    assert assigned.status_code == 200
    assert assigned.json()["model_profile_id"] == second_profile["model_profile_id"]
    assert assigned.json()["stable_config_version"] == agent["stable_config_version"] + 1
    assert protected.status_code == 409
    assert restored.status_code == 200
    assert removed.status_code == 204


@pytest.mark.anyio
async def test_agent_version_routes_cover_stable_release_and_invalid_targets(
    api_client: httpx.AsyncClient, ) -> None:
    tenant = await _tenant(api_client, "Agent Release Boundary")
    tenant_id = str(tenant["tenant_id"])
    agent = await _agent(api_client, tenant_id, "Release Agent")
    agent_id = str(agent["agent_app_id"])
    missing = str(uuid4())

    draft = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents/{agent_id}/config-versions",
        json={
            "application_config": {
                "instruction": "new stable behavior"
            },
            "tool_permissions": {
                "allowlist": ["calculate"]
            },
            "reason": "publish a tested stable configuration",
        },
    )
    rollback_draft = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents/{agent_id}/config-versions/2/rollback",
        json={"reason": "verify draft rollback rejection"},
    )
    stable = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents/{agent_id}/config-versions/2/release",
        json={
            "mode": "stable",
            "reason": "promote verified stable configuration"
        },
    )
    fetched = await api_client.get(f"/api/v1/tenants/{tenant_id}/agents/{agent_id}")
    patched = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/agents/{agent_id}",
        json={"backend_config": {
            "session": "inmemory"
        }},
    )
    versions = await api_client.get(f"/api/v1/tenants/{tenant_id}/agents/{agent_id}/config-versions"
                                    )
    missing_responses = [
        await api_client.get(f"/api/v1/tenants/{tenant_id}/agents/{missing}"),
        await api_client.patch(f"/api/v1/tenants/{tenant_id}/agents/{missing}",
                               json={"name": "Missing"}),
        await api_client.delete(f"/api/v1/tenants/{tenant_id}/agents/{missing}"),
        await api_client.post(
            f"/api/v1/tenants/{tenant_id}/agents/{missing}/config-versions",
            json={
                "application_config": {},
                "reason": "missing agent configuration draft",
            },
        ),
        await api_client.post(
            f"/api/v1/tenants/{tenant_id}/agents/{missing}/config-versions/1/release",
            json={
                "mode": "stable",
                "reason": "missing agent stable release"
            },
        ),
        await api_client.post(
            f"/api/v1/tenants/{tenant_id}/agents/{missing}/config-versions/1/rollback",
            json={"reason": "missing agent stable rollback"},
        ),
        await api_client.post(
            f"/api/v1/tenants/{tenant_id}/agents/{agent_id}/config-versions/99/release",
            json={
                "mode": "stable",
                "reason": "missing configuration release"
            },
        ),
    ]

    assert draft.status_code == 201
    assert rollback_draft.status_code == 409
    assert stable.status_code == 200
    assert stable.json()["stable_config_version"] == 2
    assert fetched.json()["application_config"] == {"instruction": "new stable behavior"}
    assert patched.json()["stable_config_version"] == 3
    assert versions.json()["total"] == 3
    assert [response.status_code for response in missing_responses] == [404] * 7


@pytest.mark.anyio
async def test_agent_and_channel_lifecycle_fail_closed_for_disabled_tenant(
    api_client: httpx.AsyncClient, ) -> None:
    tenant = await _tenant(api_client, "Disabled Runtime Resources")
    tenant_id = str(tenant["tenant_id"])
    await _model_resources(api_client, tenant_id, "disabled-runtime")
    agent = await _agent(api_client, tenant_id, "Disabled Resource Agent")
    agent_id = str(agent["agent_app_id"])
    binding = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json={
            "agent_app_id": agent_id,
            "channel_type": "web",
            "external_account_hash": "sha256:disabled-boundary",
        },
    )
    assert binding.status_code == 201
    binding_id = binding.json()["binding_id"]
    await api_client.delete(f"/api/v1/tenants/{tenant_id}/agents/{agent_id}")
    await api_client.delete(f"/api/v1/tenants/{tenant_id}")

    agent_reactivation = await api_client.patch(f"/api/v1/tenants/{tenant_id}/agents/{agent_id}",
                                                json={"status": "active"})
    binding_creation = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json={
            "agent_app_id": agent_id,
            "channel_type": "web",
            "external_account_hash": "sha256:disabled-new",
        },
    )
    missing_tenant = str(uuid4())
    missing_binding_responses = [
        await api_client.get(f"/api/v1/tenants/{missing_tenant}/channel-bindings"),
        await api_client.get(f"/api/v1/tenants/{tenant_id}/channel-bindings/{uuid4()}"),
        await api_client.patch(
            f"/api/v1/tenants/{tenant_id}/channel-bindings/{uuid4()}",
            json={"status": "disabled"},
        ),
        await api_client.delete(f"/api/v1/tenants/{tenant_id}/channel-bindings/{uuid4()}"),
    ]

    assert agent_reactivation.status_code == 409
    assert binding_creation.status_code == 409
    assert [response.status_code for response in missing_binding_responses] == [404] * 4
    # Existing disabled-tenant bindings remain inspectable for operations/audit.
    existing = await api_client.get(f"/api/v1/tenants/{tenant_id}/channel-bindings/{binding_id}")
    assert existing.status_code == 200


@pytest.mark.anyio
async def test_channel_adapter_schema_and_registration_boundaries(
    api_client: httpx.AsyncClient, ) -> None:
    tenant = await _tenant(api_client, "Schema Channel")
    tenant_id = str(tenant["tenant_id"])
    await _model_resources(api_client, tenant_id, "schema-channel")
    agent = await _agent(api_client, tenant_id, "Schema Agent")
    agent_id = str(agent["agent_app_id"])
    tenant_env = tenant_id.replace("-", "_").upper()

    unsupported_active = await api_client.post(
        "/api/v1/admin/channel-adapter-types",
        json={
            "channel_type": "not_deployed",
            "display_name": "Not Deployed",
            "adapter_version": "1",
            "status": "active",
        },
    )
    invalid_schema_adapter = await api_client.post(
        "/api/v1/admin/channel-adapter-types",
        json={
            "channel_type": "broken_schema",
            "display_name": "Broken Schema",
            "adapter_version": "1",
            "config_schema": {
                "type": "definitely-not-a-json-schema-type"
            },
        },
    )
    assert unsupported_active.status_code == 409
    assert invalid_schema_adapter.status_code == 422

    # The test fixture publishes ``web`` as an active, node-local adapter. Its
    # catalog schemas can therefore be tightened through the management API.
    configured = await api_client.patch(
        "/api/v1/admin/channel-adapter-types/web",
        json={
            "config_schema": {
                "type": "object",
                "required": ["region"],
                "properties": {
                    "region": {
                        "type": "string"
                    }
                },
                "additionalProperties": False,
            },
            "secret_schema": {
                "type": "object",
                "required": ["token"],
                "properties": {
                    "token": {
                        "type": "string"
                    }
                },
                "additionalProperties": False,
            },
        },
    )
    invalid_binding = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json={
            "agent_app_id": agent_id,
            "channel_type": "web",
            "external_account_hash": "sha256:schema-invalid",
            "account_config": {
                "unexpected": True
            },
            "secret_ref_map": {},
        },
    )
    valid_binding = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/channel-bindings",
        json={
            "agent_app_id": agent_id,
            "channel_type": "web",
            "external_account_hash": "sha256:schema-valid",
            "account_config": {
                "region": "cn"
            },
            "secret_ref_map": {
                "token": f"env://TRPC_TENANT_{tenant_env}_CHANNEL_WEB_TOKEN"
            },
        },
    )

    assert configured.status_code == 200
    assert invalid_binding.status_code == 422
    assert invalid_binding.json()["error"] == {
        "code": "http_error",
        "message": "request failed",
    }
    assert valid_binding.status_code == 201
