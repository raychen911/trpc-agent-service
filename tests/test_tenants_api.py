from uuid import UUID

import httpx
import pytest


@pytest.mark.anyio
async def test_tenant_can_be_created_and_retrieved(api_client: httpx.AsyncClient) -> None:
    create_response = await api_client.post(
        "/api/v1/tenants",
        json={
            "name": "Customer Service",
            "isolation_mode": "shared",
            "audit_policy": {
                "retention_days": 180,
            },
        },
    )

    assert create_response.status_code == 201
    created = create_response.json()
    UUID(created["tenant_id"])
    assert created["name"] == "Customer Service"
    assert created["status"] == "active"
    assert created["isolation_mode"] == "shared"
    assert created["audit_policy"] == {"retention_days": 180}

    get_response = await api_client.get(f"/api/v1/tenants/{created['tenant_id']}")

    assert get_response.status_code == 200
    assert get_response.json() == created


@pytest.mark.anyio
async def test_tenant_can_be_listed_updated_and_disabled(api_client: httpx.AsyncClient) -> None:
    create_response = await api_client.post(
        "/api/v1/tenants",
        json={"name": "Operations"},
    )
    tenant_id = create_response.json()["tenant_id"]

    list_response = await api_client.get("/api/v1/tenants")

    assert list_response.status_code == 200
    assert list_response.json()["total"] == 1
    assert list_response.json()["items"][0]["tenant_id"] == tenant_id

    update_response = await api_client.patch(
        f"/api/v1/tenants/{tenant_id}",
        json={"name": "Platform Operations"},
    )

    assert update_response.status_code == 200
    assert update_response.json()["name"] == "Platform Operations"

    delete_response = await api_client.delete(f"/api/v1/tenants/{tenant_id}")

    assert delete_response.status_code == 204

    disabled_response = await api_client.get(f"/api/v1/tenants/{tenant_id}")
    assert disabled_response.status_code == 200
    assert disabled_response.json()["status"] == "disabled"


@pytest.mark.anyio
async def test_duplicate_tenant_name_returns_conflict(api_client: httpx.AsyncClient) -> None:
    first_response = await api_client.post("/api/v1/tenants", json={"name": "Duplicate"})
    second_response = await api_client.post("/api/v1/tenants", json={"name": "Duplicate"})

    assert first_response.status_code == 201
    assert second_response.status_code == 409


@pytest.mark.anyio
async def test_tenant_update_conflict_and_unknown_tenant(api_client: httpx.AsyncClient) -> None:
    first = await api_client.post("/api/v1/tenants", json={"name": "First"})
    second = await api_client.post("/api/v1/tenants", json={"name": "Second"})

    conflict = await api_client.patch(
        f"/api/v1/tenants/{second.json()['tenant_id']}",
        json={"name": "First"},
    )
    unknown_id = "00000000-0000-0000-0000-000000000000"
    missing_get = await api_client.get(f"/api/v1/tenants/{unknown_id}")
    missing_patch = await api_client.patch(f"/api/v1/tenants/{unknown_id}",
                                           json={"name": "Missing"})
    missing_delete = await api_client.delete(f"/api/v1/tenants/{unknown_id}")

    assert first.status_code == 201
    assert conflict.status_code == 409
    assert missing_get.status_code == 404
    assert missing_patch.status_code == 404
    assert missing_delete.status_code == 404
