"""A published protected config must satisfy the same contract as worker startup."""

from copy import deepcopy

from tests.reliability.test_management import managed as managed_fixture
from trpc_service.protected_demo import documents

managed = managed_fixture


def test_protected_config_rejected_before_it_can_reach_workers(managed, monkeypatch):
    client, _ = managed
    monkeypatch.setenv("TRPC_RUNTIME_MODE", "protected")
    profiles, tenants = documents("acceptance_wecom", "acceptance_feishu")
    for profile in profiles:
        assert client.post("/admin/backend-profiles", json=profile.model_dump(mode="json")).status_code == 201
    body = {
        "config": tenants[0].model_dump(mode="json"),
        "profile_revisions": {
            role: 1
            for role in type(tenants[0].data_backends).model_fields
        }
    }
    path = "/admin/tenants/" + tenants[0].tenant_id + "/configs"
    invalid = deepcopy(body)
    invalid["config"]["models"][0]["max_retries"] = 1
    assert client.post(path, json=invalid).status_code == 422
    invalid = deepcopy(body)
    invalid["config"]["applications"][0]["tool_policy"]["allow"].append("unregistered_tool")
    assert client.post(path, json=invalid).status_code == 422
    assert client.post(path, json=body).status_code == 201
    assert client.post(path + "/1/publish", json={"expected_active_version": 0}).status_code == 200


def test_legacy_draft_is_revalidated_at_protected_publication(managed, monkeypatch):
    client, _ = managed
    profiles, tenants = documents("acceptance_wecom", "acceptance_feishu")
    for profile in profiles:
        assert client.post("/admin/backend-profiles", json=profile.model_dump(mode="json")).status_code == 201
    body = {
        "config": tenants[0].model_dump(mode="json"),
        "profile_revisions": {
            role: 1
            for role in type(tenants[0].data_backends).model_fields
        }
    }
    body["config"]["models"][0]["max_output_tokens"] = 256
    path = "/admin/tenants/" + tenants[0].tenant_id + "/configs"
    assert client.post(path, json=body).status_code == 201
    monkeypatch.setenv("TRPC_RUNTIME_MODE", "protected")
    assert client.post(path + "/1/publish", json={"expected_active_version": 0}).status_code == 422
