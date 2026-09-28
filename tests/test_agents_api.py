from uuid import UUID

import httpx
import pytest
from pydantic import ValidationError

from trpc_service.agent.schemas import AgentAppCreate


async def _create_tenant(client: httpx.AsyncClient, name: str) -> str:
    response = await client.post("/api/v1/tenants", json={"name": name})
    assert response.status_code == 201
    return response.json()["tenant_id"]


@pytest.mark.anyio
async def test_agent_crud_is_scoped_to_its_tenant(api_client: httpx.AsyncClient) -> None:
    tenant_id = await _create_tenant(api_client, "Customer Service")
    create_response = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={
            "name": "Support Agent",
            "application_config": {
                "description": "Answers product questions"
            },
            "tool_permissions": {
                "allowlist": ["ticket.read"]
            },
            "knowledge_config": {
                "knowledge_base_ids": []
            },
            "backend_config": {
                "session": "inmemory"
            },
        },
    )

    assert create_response.status_code == 201
    created = create_response.json()
    UUID(created["agent_app_id"])
    assert created["tenant_id"] == tenant_id
    assert created["status"] == "active"

    list_response = await api_client.get(f"/api/v1/tenants/{tenant_id}/agents")
    assert list_response.status_code == 200
    assert list_response.json()["total"] == 1

    update_response = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/agents/{created['agent_app_id']}",
        json={"name": "Senior Support Agent"},
    )
    assert update_response.status_code == 200
    assert update_response.json()["name"] == "Senior Support Agent"

    delete_response = await api_client.delete(
        f"/api/v1/tenants/{tenant_id}/agents/{created['agent_app_id']}")
    assert delete_response.status_code == 204

    restore_response = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/agents/{created['agent_app_id']}",
        json={"status": "active"},
    )
    assert restore_response.status_code == 200

    get_response = await api_client.get(
        f"/api/v1/tenants/{tenant_id}/agents/{created['agent_app_id']}")
    assert get_response.status_code == 200
    assert get_response.json()["status"] == "active"


@pytest.mark.anyio
async def test_agent_rejects_malformed_capability_policy_before_runtime(
        api_client: httpx.AsyncClient) -> None:
    """Bad tenant policy must fail at save time instead of breaking a later IM turn."""

    tenant_id = await _create_tenant(api_client, "Invalid Capability")
    response = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={
            "name": "Broken Agent",
            "tool_permissions": {
                "grants": [{
                    "kind": "mcp",
                    "name": "remote_read",
                    "actions": ["execute"],
                    "resources": ["not-a-connection-id"],
                    "risk_level": 0,
                }]
            },
        },
    )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_agent_rejects_unregistered_backend_at_save_time(
    api_client: httpx.AsyncClient, ) -> None:
    tenant_id = await _create_tenant(api_client, "Unknown Backend")
    response = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={
            "name": "Broken Agent",
            "backend_config": {
                "session": "missing"
            }
        },
    )
    assert response.status_code == 422
    agents = await api_client.get(f"/api/v1/tenants/{tenant_id}/agents")
    assert agents.json()["total"] == 0


@pytest.mark.parametrize(
    "permissions",
    [
        {
            "allowlist": "calculate"
        },
        {
            "allowlist": [""]
        },
        {
            "http_allowed_hosts": ["https://api.example.com"]
        },
        {
            "grants": "invalid"
        },
        {
            "grants": ["invalid"]
        },
        {
            "grants": [{
                "kind": "unknown",
                "name": "x",
                "actions": ["execute"]
            }]
        },
        {
            "grants": [{
                "kind": "tool",
                "name": "x",
                "actions": ["load"]
            }]
        },
        {
            "grants": [{
                "kind": "skill",
                "name": "x",
                "actions": ["load"],
                "resources": ["x"]
            }]
        },
        {
            "grants": [{
                "kind": "mcp",
                "name": "x",
                "actions": ["execute"]
            }]
        },
        {
            "grants": [{
                "kind": "mcp",
                "name": "x",
                "actions": ["execute"],
                "resources": ["12345678-1234-5678-1234-567812345678"],
                "risk_level": 1,
            }]
        },
        {
            "grants": [{
                "kind": "tool",
                "name": "x",
                "actions": ["execute"],
                "risk_level": True,
            }]
        },
    ],
)
def test_agent_schema_rejects_each_malformed_capability_shape(
        permissions: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        AgentAppCreate(name="Invalid", tool_permissions=permissions)


def test_agent_schema_accepts_typed_tool_skill_and_mcp_grants() -> None:
    connection_id = str(UUID("12345678-1234-5678-1234-567812345678"))
    model = AgentAppCreate(
        name="Typed",
        tool_permissions={
            "http_allowed_hosts": ["api.example.com"],
            "grants": [
                {
                    "kind": "tool",
                    "name": "calculate",
                    "actions": ["execute"],
                    "resources": [],
                },
                {
                    "kind": "skill",
                    "name": "code-review",
                    "actions": ["load"],
                    "resources": [],
                },
                {
                    "kind": "mcp",
                    "name": "mcp_read",
                    "actions": ["execute"],
                    "resources": [connection_id],
                    "risk_level": 0,
                },
                {
                    "kind": "mcp",
                    "name": "mcp_write",
                    "actions": ["execute"],
                    "resources": [connection_id],
                    "risk_level": 2,
                },
            ],
        },
    )

    assert len(model.tool_permissions["grants"]) == 4


@pytest.mark.anyio
async def test_agent_cannot_be_read_through_another_tenant(api_client: httpx.AsyncClient) -> None:
    owner_id = await _create_tenant(api_client, "Owner")
    other_id = await _create_tenant(api_client, "Other")
    create_response = await api_client.post(
        f"/api/v1/tenants/{owner_id}/agents",
        json={"name": "Private Agent"},
    )
    agent_id = create_response.json()["agent_app_id"]

    response = await api_client.get(f"/api/v1/tenants/{other_id}/agents/{agent_id}")

    assert response.status_code == 404


@pytest.mark.anyio
async def test_agent_name_is_unique_within_a_tenant(api_client: httpx.AsyncClient) -> None:
    tenant_id = await _create_tenant(api_client, "Agent Names")
    first_response = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={"name": "Duplicate"},
    )
    second_response = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={"name": "Duplicate"},
    )

    assert first_response.status_code == 201
    assert second_response.status_code == 409


@pytest.mark.anyio
async def test_disabled_tenant_rejects_agents_and_agent_update_conflicts(
    api_client: httpx.AsyncClient, ) -> None:
    tenant_id = await _create_tenant(api_client, "Agent Policies")
    first = await api_client.post(f"/api/v1/tenants/{tenant_id}/agents", json={"name": "First"})
    second = await api_client.post(f"/api/v1/tenants/{tenant_id}/agents", json={"name": "Second"})

    conflict = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}/agents/{second.json()['agent_app_id']}",
        json={"name": "First"},
    )
    await api_client.delete(f"/api/v1/tenants/{tenant_id}")
    disabled = await api_client.post(f"/api/v1/tenants/{tenant_id}/agents",
                                     json={"name": "Rejected"})

    assert first.status_code == 201
    assert conflict.status_code == 409
    assert disabled.status_code == 409


@pytest.mark.anyio
async def test_agent_config_rejects_plaintext_secrets(api_client: httpx.AsyncClient) -> None:
    tenant_id = await _create_tenant(api_client, "Agent Secrets")

    response = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={
            "name": "Unsafe Agent",
            "model_config": {
                "api_key": "plaintext-key"
            },
        },
    )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_agent_config_versions_support_canary_and_atomic_rollback(
    api_client: httpx.AsyncClient, ) -> None:
    """Published snapshots remain immutable while rollout pointers change."""

    tenant_id = await _create_tenant(api_client, "Versioned Agent Tenant")
    created = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents",
        json={
            "name": "Versioned Agent",
            "application_config": {
                "instruction": "stable"
            },
        },
    )
    agent_id = created.json()["agent_app_id"]
    initial = await api_client.get(f"/api/v1/tenants/{tenant_id}/agents/{agent_id}/config-versions")
    drafted = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents/{agent_id}/config-versions",
        json={
            "application_config": {
                "instruction": "canary"
            },
            "reason": "validate new prompt on a bounded cohort",
        },
    )
    released = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents/{agent_id}/config-versions/2/release",
        json={
            "mode": "canary",
            "canary_percent": 25,
            "reason": "start controlled rollout",
        },
    )
    rolled_back = await api_client.post(
        f"/api/v1/tenants/{tenant_id}/agents/{agent_id}/config-versions/1/rollback",
        json={"reason": "canary error rate exceeded threshold"},
    )

    assert initial.status_code == 200
    assert initial.json()["items"][0]["version"] == 1
    assert initial.json()["items"][0]["snapshot"]["application_config"] == {"instruction": "stable"}
    assert drafted.status_code == 201
    assert drafted.json()["version"] == 2
    assert released.status_code == 200
    assert released.json()["stable_config_version"] == 1
    assert released.json()["canary_config_version"] == 2
    assert released.json()["canary_percent"] == 25
    assert rolled_back.status_code == 200
    assert rolled_back.json()["stable_config_version"] == 1
    assert rolled_back.json()["canary_config_version"] is None
    assert rolled_back.json()["canary_percent"] == 0
