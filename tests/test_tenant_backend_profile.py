"""R1A tests: strict ``TenantBackendProfile`` model and required nesting.

The profile is the typed per-tenant data-backend bundle: frozen,
extra-forbid, all four fields REQUIRED, strict literals (no bool/int/other
strings), and stable JSON round-trips.  ``TenantConfig`` and
``TenantConfigDraft`` must demand it (mirrors the governance precedent).
"""

from __future__ import annotations

from typing import get_args

import pytest
from pydantic import ValidationError

from trpc_service.config.tenant import StateBackendKind
from trpc_service.config.tenant import TenantBackendProfile
from trpc_service.config.tenant import TenantConfig
from trpc_service.config.tenant import TenantConfigDraft

_FULL_PROFILE = {
    "state_backend": "redis",
    "artifact_backend": "s3",
    "knowledge_backend": "sql",
    "audit_backend": "sql",
}

_AUDIT_POLICY = {
    "retention_days": 365,
    "delivery_events": "all",
}

_GOV = {
    "allowed_channels": ["web"],
    "allowed_user_ids": [],
    "tool_decisions": {},
    "content_policy": {
        "enabled": True,
        "input_action": "block",
        "output_action": "block",
    },
    "limits": None,
}

_APP = {
    "app_id": "app_demo",
    "instruction": "You are a helpful assistant.",
    "model_profile": "default",
    "allowed_tools": ["get_current_time"],
}


def _profile(**overrides) -> TenantBackendProfile:
    data = dict(_FULL_PROFILE)
    data.update(overrides)
    return TenantBackendProfile.model_validate(data)


def _tenant_payload(**root_overrides) -> dict:
    payload = {
        "tenant_id": "tenant_default",
        "enabled": True,
        "version": 1,
        "app": dict(_APP),
        "governance": dict(_GOV),
        "backend_profile": dict(_FULL_PROFILE),
        "audit_policy": dict(_AUDIT_POLICY),
    }
    payload.update(root_overrides)
    return payload


class TestStateBackendKind:

    def test_kind_is_exactly_redis_or_sql(self):
        assert get_args(StateBackendKind) == ("redis", "sql")


class TestTenantBackendProfileShape:

    @pytest.mark.parametrize("kind", ["redis", "sql"])
    def test_valid_state_backends(self, kind):
        profile = _profile(state_backend=kind)
        assert profile.state_backend == kind

    @pytest.mark.parametrize(
        "field, bad_value",
        [
            ("state_backend", "mysql"),
            ("state_backend", "Redis"),
            ("state_backend", "REDIS"),
            ("state_backend", "inmemory"),
            ("state_backend", ""),
            ("artifact_backend", "minio"),
            ("artifact_backend", "file"),
            ("artifact_backend", "redis"),
            ("knowledge_backend", "milvus"),
            ("knowledge_backend", "redis"),
            ("audit_backend", "file"),
            ("audit_backend", "s3"),
        ],
    )
    def test_literals_are_strict(self, field, bad_value):
        with pytest.raises(ValidationError):
            _profile(**{field: bad_value})

    @pytest.mark.parametrize("field", ["state_backend", "artifact_backend", "knowledge_backend", "audit_backend"])
    @pytest.mark.parametrize("bad_value", [True, False, 0, 1, None])
    def test_bool_int_none_rejected(self, field, bad_value):
        with pytest.raises(ValidationError):
            _profile(**{field: bad_value})

    @pytest.mark.parametrize("field", ["state_backend", "artifact_backend", "knowledge_backend", "audit_backend"])
    def test_every_field_is_required(self, field):
        data = dict(_FULL_PROFILE)
        del data[field]
        with pytest.raises(ValidationError) as exc_info:
            TenantBackendProfile.model_validate(data)
        assert any(err["loc"] == (field, ) for err in exc_info.value.errors())

    def test_none_is_not_a_valid_missing_value(self):
        with pytest.raises(ValidationError):
            TenantBackendProfile.model_validate(dict(_FULL_PROFILE, state_backend=None))

    def test_unknown_keys_rejected(self):
        with pytest.raises(ValidationError):
            _profile(extra_backend="redis")

    def test_frozen(self):
        profile = _profile()
        with pytest.raises(ValidationError):
            profile.state_backend = "sql"

    def test_json_round_trip_is_stable(self):
        profile = _profile()
        dumped = profile.model_dump(mode="json")
        assert dumped == _FULL_PROFILE
        assert TenantBackendProfile.model_validate(dumped) == profile
        # second round must not drift either
        assert TenantBackendProfile.model_validate(
            TenantBackendProfile.model_validate(dumped).model_dump(mode="json")) == profile


class TestTenantConfigNesting:

    def test_config_requires_backend_profile(self):
        payload = _tenant_payload()
        del payload["backend_profile"]
        with pytest.raises(ValidationError) as exc_info:
            TenantConfig.model_validate(payload)
        assert any(err["loc"] == ("backend_profile", ) and err["type"] == "missing" for err in exc_info.value.errors())

    def test_draft_requires_backend_profile(self):
        payload = {
            "enabled": True,
            "app": dict(_APP),
            "governance": dict(_GOV),
        }
        with pytest.raises(ValidationError) as exc_info:
            TenantConfigDraft.model_validate(payload)
        assert any(err["loc"] == ("backend_profile", ) and err["type"] == "missing" for err in exc_info.value.errors())

    def test_config_accepts_and_preserves_profile(self):
        config = TenantConfig.model_validate(_tenant_payload())
        assert config.backend_profile == _profile()
        assert config.model_dump(mode="json")["backend_profile"] == _FULL_PROFILE

    def test_draft_accepts_profile(self):
        draft = TenantConfigDraft.model_validate({
            "enabled": True,
            "app": dict(_APP),
            "governance": dict(_GOV),
            "backend_profile": dict(_FULL_PROFILE, state_backend="sql"),
            "audit_policy": dict(_AUDIT_POLICY),
        })
        assert draft.backend_profile.state_backend == "sql"

    def test_config_rejects_nested_unknown_key(self):
        payload = _tenant_payload(backend_profile=dict(_FULL_PROFILE, state_pool_size=1))
        with pytest.raises(ValidationError):
            TenantConfig.model_validate(payload)

    def test_config_sql_profile_round_trips(self):
        payload = _tenant_payload(backend_profile=dict(_FULL_PROFILE, state_backend="sql"))
        config = TenantConfig.model_validate(payload)
        assert TenantConfig.model_validate(config.model_dump(mode="json")) == config
