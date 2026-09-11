"""Tests for tenant configuration model and JSON repository."""

from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from trpc_service.config.tenant import TenantConfigError
from trpc_service.config.tenant_repository import JsonTenantConfigRepository

PROJECT_ROOT = Path(__file__).resolve().parents[1]

_GOV_JSON = {
    "allowed_channels": ["web", "web_console"],
    "allowed_user_ids": [],
    "tool_decisions": {},
    "content_policy": {
        "enabled": True,
        "input_action": "block",
        "output_action": "block",
    },
    "limits": None,
}

# R1A: the default profile mirrors migration 0008's explicit backfill value.
_BACKEND_PROFILE_JSON = {
    "state_backend": "redis",
    "artifact_backend": "s3",
    "knowledge_backend": "sql",
    "audit_backend": "sql",
}

_AUDIT_POLICY_JSON = {"retention_days": 365, "delivery_events": "all"}

_VALID_TENANT = {
    "tenant_id": "tenant_default",
    "enabled": True,
    "version": 1,
    "app": {
        "app_id": "app_demo",
        "instruction": "You are a helpful assistant.",
        "model_profile": "default",
        "allowed_tools": ["get_current_time"],
    },
    "governance": {
        "allowed_channels": ["web", "web_console"],
        "allowed_user_ids": [],
        "tool_decisions": {},
        "content_policy": {
            "enabled": True,
            "input_action": "block",
            "output_action": "block",
        },
        "limits": None,
    },
    "backend_profile": dict(_BACKEND_PROFILE_JSON),
    "audit_policy": dict(_AUDIT_POLICY_JSON),
}


def _valid_doc(**root_overrides):
    doc = {"schema_version": 1, "tenants": [_VALID_TENANT]}
    doc.update(root_overrides)
    return doc


def _mutate_tenant(field, value):
    tenant = copy.deepcopy(_VALID_TENANT)
    tenant[field] = value
    return {"schema_version": 1, "tenants": [tenant]}


def _write_json(tmp_path, data, filename="tenants.json"):
    path = tmp_path / filename
    path.write_text(json.dumps(data))
    return path


def _load(tmp_path, data):
    path = _write_json(tmp_path, data)
    return JsonTenantConfigRepository.from_path(path)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_valid_config(tmp_path):
    repo = _load(
        tmp_path, {
            "schema_version":
            1,
            "tenants": [
                _VALID_TENANT,
                {
                    "tenant_id": "tenant_a",
                    "enabled": True,
                    "version": 2,
                    "app": {
                        "app_id": "app_demo",
                        "instruction": "Tenant A assistant.",
                        "model_profile": "default",
                        "allowed_tools": ["get_current_time"],
                    },
                    "governance": dict(_GOV_JSON),
                    "backend_profile": dict(_BACKEND_PROFILE_JSON),
                    "audit_policy": dict(_AUDIT_POLICY_JSON),
                },
                {
                    "tenant_id": "tenant_b",
                    "enabled": True,
                    "version": 1,
                    "app": {
                        "app_id": "app_demo",
                        "instruction": "Tenant B assistant.",
                        "model_profile": "default",
                        "allowed_tools": [],
                    },
                    "governance": dict(_GOV_JSON),
                    "backend_profile": dict(_BACKEND_PROFILE_JSON),
                    "audit_policy": dict(_AUDIT_POLICY_JSON),
                },
                {
                    "tenant_id": "tenant_disabled",
                    "enabled": False,
                    "version": 1,
                    "app": {
                        "app_id": "app_demo",
                        "instruction": "Disabled tenant.",
                        "model_profile": "default",
                        "allowed_tools": ["get_current_time"],
                    },
                    "governance": dict(_GOV_JSON),
                    "backend_profile": dict(_BACKEND_PROFILE_JSON),
                    "audit_policy": dict(_AUDIT_POLICY_JSON),
                },
            ],
        })

    default = await repo.get("tenant_default")
    assert default is not None
    assert default.enabled is True
    assert default.version == 1
    assert default.app.allowed_tools == ("get_current_time", )

    a = await repo.get("tenant_a")
    b = await repo.get("tenant_b")
    assert a.app.instruction == "Tenant A assistant."
    assert b.app.instruction == "Tenant B assistant."
    assert b.app.allowed_tools == ()

    disabled = await repo.get("tenant_disabled")
    assert disabled is not None
    assert disabled.enabled is False

    assert await repo.get("nonexistent") is None


@pytest.mark.asyncio
async def test_shipped_default_is_streaming_with_block_mode_available():
    repo = JsonTenantConfigRepository.from_path(PROJECT_ROOT / "data" / "tenants.json")
    default = await repo.get("tenant_default")
    protected = await repo.get("tenant_a")

    assert default is not None
    assert default.governance.content_policy.output_action == "allow"
    assert protected is not None
    assert protected.governance.content_policy.output_action == "block"


def test_config_and_snapshot_are_immutable(tmp_path):
    repo = _load(tmp_path, _valid_doc())
    config = asyncio.run(repo.get("tenant_default"))
    with pytest.raises(ValidationError):
        config.enabled = False
    with pytest.raises(ValidationError):
        config.app.app_id = "other"
    with pytest.raises(TypeError):
        repo._configs["new"] = None


@pytest.mark.asyncio
async def test_get_does_not_read_disk(tmp_path):
    path = _write_json(tmp_path, _valid_doc())
    repo = JsonTenantConfigRepository.from_path(path)
    path.unlink()
    config = await repo.get("tenant_default")
    assert config is not None


# ---------------------------------------------------------------------------
# File-level errors
# ---------------------------------------------------------------------------


def test_missing_file_raises(tmp_path):
    with pytest.raises(TenantConfigError, match="not readable"):
        JsonTenantConfigRepository.from_path(tmp_path / "missing.json")


def test_invalid_json_raises(tmp_path):
    path = tmp_path / "tenants.json"
    path.write_text("not json")
    with pytest.raises(TenantConfigError, match="not valid JSON"):
        JsonTenantConfigRepository.from_path(path)


def test_json_root_must_be_dict(tmp_path):
    path = tmp_path / "tenants.json"
    path.write_text("[1, 2, 3]")
    with pytest.raises(TenantConfigError, match="not valid JSON"):
        JsonTenantConfigRepository.from_path(path)


def test_error_messages_do_not_leak_paths_or_values(tmp_path):
    with pytest.raises(TenantConfigError) as exc_info:
        JsonTenantConfigRepository.from_path(tmp_path / "secret_path" / "x.json")
    assert "secret_path" not in str(exc_info.value)


def test_invalid_utf8_raises_tenant_config_error(tmp_path):
    path = tmp_path / "tenants.json"
    path.write_bytes(b"\xff\xfe invalid utf-8")
    with pytest.raises(TenantConfigError, match="not readable"):
        JsonTenantConfigRepository.from_path(path)


def test_os_error_during_read_raises_tenant_config_error(tmp_path):
    path = tmp_path / "tenants.json"
    path.write_text("{}")
    path.chmod(0o000)
    try:
        with pytest.raises(TenantConfigError, match="not readable"):
            JsonTenantConfigRepository.from_path(path)
    finally:
        path.chmod(0o644)


# ---------------------------------------------------------------------------
# schema_version: must be strict integer 1
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_schema", [
    "1",
    1.0,
    True,
    False,
    2,
    None,
])
def test_schema_version_must_be_strict_int_1(tmp_path, bad_schema):
    with pytest.raises(TenantConfigError, match="schema"):
        _load(tmp_path, {"schema_version": bad_schema, "tenants": [_VALID_TENANT]})


# ---------------------------------------------------------------------------
# Root object: must reject unknown fields
# ---------------------------------------------------------------------------


def test_root_rejects_unknown_field_with_valid_tenants(tmp_path):
    doc = _valid_doc()
    doc["extra"] = True
    with pytest.raises(TenantConfigError, match="invalid entries"):
        _load(tmp_path, doc)


def test_empty_tenants_list_rejected(tmp_path):
    with pytest.raises(TenantConfigError, match="invalid entries"):
        _load(tmp_path, {"schema_version": 1, "tenants": []})


# ---------------------------------------------------------------------------
# Tenant-level errors: single-field mutations
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("field,value", [
    ("tenant_id", "INVALID"),
    ("enabled", "yes"),
    ("enabled", 1),
    ("enabled", 0),
    ("version", "1"),
    ("version", 1.0),
    ("version", True),
    ("version", 0),
    ("version", -1),
    ("app_id", ""),
])
def test_tenant_field_rejection(tmp_path, field, value):
    if field == "app_id":
        data = _mutate_tenant("app", {**_VALID_TENANT["app"], "app_id": value})
    else:
        data = _mutate_tenant(field, value)
    with pytest.raises(TenantConfigError, match="invalid entries"):
        _load(tmp_path, data)


def test_nested_unknown_field_rejected(tmp_path):
    tenant = copy.deepcopy(_VALID_TENANT)
    tenant["app"]["extra"] = True
    with pytest.raises(TenantConfigError, match="invalid entries"):
        _load(tmp_path, {"schema_version": 1, "tenants": [tenant]})


def test_duplicate_tenant_ids_rejected(tmp_path):
    with pytest.raises(TenantConfigError, match="duplicate"):
        _load(tmp_path, {
            "schema_version": 1,
            "tenants": [_VALID_TENANT, _VALID_TENANT],
        })


# ---------------------------------------------------------------------------
# allowed_tools
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_tools", [None, ["valid", None], ["valid", ""], ["valid", "valid"]])
def test_invalid_allowed_tools_rejected(tmp_path, bad_tools):
    tenant = copy.deepcopy(_VALID_TENANT)
    tenant["app"]["allowed_tools"] = bad_tools
    with pytest.raises(TenantConfigError, match="invalid entries"):
        _load(tmp_path, {"schema_version": 1, "tenants": [tenant]})


# ---------------------------------------------------------------------------
# from_env
# ---------------------------------------------------------------------------


def test_from_env_resolves_relative_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_json(tmp_path, _valid_doc(), "rel.json")
    repo = JsonTenantConfigRepository.from_env({"TRPC_TENANT_CONFIG_PATH": "rel.json"})
    config = asyncio.run(repo.get("tenant_default"))
    assert config is not None


class TestGovernanceInJsonSnapshot:
    """Stage 6A1: JSON import must explicitly provide governance."""

    def test_missing_governance_rejected(self, tmp_path):
        tenant = copy.deepcopy(_VALID_TENANT)
        del tenant["governance"]
        path = _write_json(tmp_path, {"schema_version": 1, "tenants": [tenant]})
        with pytest.raises(TenantConfigError):
            JsonTenantConfigRepository.from_path(path)

    def test_governance_loaded(self, tmp_path):
        path = _write_json(tmp_path, _valid_doc())
        repo = JsonTenantConfigRepository.from_path(path)
        config = asyncio.run(repo.get("tenant_default"))
        assert config is not None
        assert config.governance.allowed_channels == ("web", "web_console")
        assert config.governance.allowed_user_ids == ()
        assert config.governance.tool_decisions == {}

    def test_governance_unknown_field_rejected(self, tmp_path):
        tenant = copy.deepcopy(_VALID_TENANT)
        tenant["governance"]["extra_rule"] = True
        path = _write_json(tmp_path, {"schema_version": 1, "tenants": [tenant]})
        with pytest.raises(TenantConfigError):
            JsonTenantConfigRepository.from_path(path)

    def test_decisions_outside_allowed_tools_rejected(self, tmp_path):
        tenant = copy.deepcopy(_VALID_TENANT)
        tenant["app"]["allowed_tools"] = []
        tenant["governance"]["tool_decisions"] = {"get_current_time": "deny"}
        path = _write_json(tmp_path, {"schema_version": 1, "tenants": [tenant]})
        with pytest.raises(TenantConfigError):
            JsonTenantConfigRepository.from_path(path)


class TestBackendProfileInJsonSnapshot:
    """R1A: JSON import must explicitly provide the backend profile."""

    def test_missing_backend_profile_rejected(self, tmp_path):
        tenant = copy.deepcopy(_VALID_TENANT)
        del tenant["backend_profile"]
        path = _write_json(tmp_path, {"schema_version": 1, "tenants": [tenant]})
        with pytest.raises(TenantConfigError):
            JsonTenantConfigRepository.from_path(path)

    def test_backend_profile_loaded(self, tmp_path):
        path = _write_json(tmp_path, _valid_doc())
        repo = JsonTenantConfigRepository.from_path(path)
        config = asyncio.run(repo.get("tenant_default"))
        assert config is not None
        assert config.backend_profile.model_dump(mode="json") == _BACKEND_PROFILE_JSON

    def test_backend_profile_unknown_field_rejected(self, tmp_path):
        tenant = copy.deepcopy(_VALID_TENANT)
        tenant["backend_profile"]["object_store"] = "minio"
        path = _write_json(tmp_path, {"schema_version": 1, "tenants": [tenant]})
        with pytest.raises(TenantConfigError):
            JsonTenantConfigRepository.from_path(path)
