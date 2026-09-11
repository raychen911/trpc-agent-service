"""Strict governance domain-model tests.

Covers ``TenantGovernanceConfig`` validation rules: channels non-empty+dedup
via validate_channel, internal user id pattern
``usr_v1_[0-9a-f]{48}`` + dedup, decisions restricted to allow/deny/review,
tool keys must belong to the same config's allowed_tools, and governance is a
required part of TenantConfig/TenantConfigDraft.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from trpc_service.config.tenant import AgentAppConfig

VALID_USER_A = "usr_v1_" + "0123456789abcdef0123456789abcdef0123456789abcdef"
VALID_USER_B = "usr_v1_" + "fedcba9876543210fedcba9876543210fedcba9876543210"


def _app(**overrides: object) -> AgentAppConfig:
    defaults: dict = {
        "app_id": "app_demo",
        "instruction": "You are helpful.",
        "model_profile": "default",
        "allowed_tools": ("get_current_time", ),
    }
    defaults.update(overrides)
    return AgentAppConfig(**defaults)


def _governance_cls():
    from trpc_service.config.tenant import TenantGovernanceConfig
    return TenantGovernanceConfig


def _gov(**overrides: object):
    from trpc_service.governance.content_policy import ContentPolicyConfig
    cls = _governance_cls()
    defaults: dict = {
        "allowed_channels": ("web_console", "wecom"),
        "allowed_user_ids": (),
        "tool_decisions": {},
        "content_policy": ContentPolicyConfig(),
        "limits": None,
    }
    defaults.update(overrides)
    return cls(**defaults)


def _config_cls():
    from trpc_service.config.tenant import TenantConfig
    return TenantConfig


def _draft_cls():
    from trpc_service.config.tenant import TenantConfigDraft
    return TenantConfigDraft


def _backend():
    from trpc_service.config.tenant import TenantBackendProfile
    return TenantBackendProfile(
        state_backend="redis",
        artifact_backend="s3",
        knowledge_backend="sql",
        audit_backend="sql",
    )


def _audit_policy():
    from trpc_service.config.tenant import TenantAuditPolicy
    return TenantAuditPolicy(retention_days=365, delivery_events="all")


class TestTenantGovernanceConfig:

    def test_valid_minimal(self):
        gov = _gov()
        assert gov.allowed_channels == ("web_console", "wecom")
        assert gov.allowed_user_ids == ()
        assert gov.tool_decisions == {}

    def test_is_frozen(self):
        gov = _gov()
        with pytest.raises(ValidationError):
            gov.allowed_channels = ("web", )  # type: ignore[misc]

    def test_extra_field_forbidden(self):
        cls = _governance_cls()
        from trpc_service.governance.content_policy import ContentPolicyConfig
        with pytest.raises(ValidationError):
            cls(
                allowed_channels=("web", ),
                allowed_user_ids=(),
                tool_decisions={},
                content_policy=ContentPolicyConfig(),
                limits=None,
                admin_secret="nope",
            )

    def test_channels_required_non_empty(self):
        with pytest.raises(ValidationError):
            _gov(allowed_channels=())

    def test_channels_dedup_rejected(self):
        with pytest.raises(ValidationError):
            _gov(allowed_channels=("web", "web"))

    def test_channels_validated_by_channel_rules(self):
        with pytest.raises(ValidationError):
            _gov(allowed_channels=("Web", ))
        with pytest.raises(ValidationError):
            _gov(allowed_channels=("bad channel", ))
        with pytest.raises(ValidationError):
            _gov(allowed_channels=("", ))

    def test_channels_normalized_stripped(self):
        gov = _gov(allowed_channels=["  web  ", "feishu"])
        assert gov.allowed_channels == ("web", "feishu")

    def test_user_ids_accept_valid_projections(self):
        gov = _gov(allowed_user_ids=(VALID_USER_A, VALID_USER_B))
        assert gov.allowed_user_ids == (VALID_USER_A, VALID_USER_B)

    def test_user_ids_empty_means_allow_all(self):
        assert _gov(allowed_user_ids=()).allowed_user_ids == ()

    @pytest.mark.parametrize(
        "bad",
        [
            "usr_v1_" + "0" * 47,  # too short
            "usr_v1_" + "0" * 49,  # too long
            "usr_v2_" + "0" * 48,  # wrong version
            "usr_v1_" + "Z" * 48,  # non hex
            "usr_v1_" + "0" * 47 + "G",  # non hex tail
            "ou_raw_external_id",  # raw IM id rejected
            "",
        ],
    )
    def test_user_ids_pattern_enforced(self, bad: str):
        with pytest.raises(ValidationError):
            _gov(allowed_user_ids=(bad, ))

    def test_user_ids_dedup_rejected(self):
        with pytest.raises(ValidationError):
            _gov(allowed_user_ids=(VALID_USER_A, VALID_USER_A))

    def test_decisions_accept_three_values(self):
        gov = _gov(tool_decisions={
            "get_current_time": "allow",
            "x": "deny",
            "y": "review",
        })
        assert gov.tool_decisions["y"] == "review"

    def test_decisions_reject_unknown_value(self):
        with pytest.raises(ValidationError):
            _gov(tool_decisions={"get_current_time": "approve"})

    def test_decisions_reject_non_string_key(self):
        with pytest.raises(ValidationError):
            _gov(tool_decisions={"": "allow"})

    def test_decisions_normalized(self):
        gov = _gov(tool_decisions={"get_current_time": "deny"})
        # immutable-ish access for the runtime filter
        assert dict(gov.tool_decisions) == {"get_current_time": "deny"}


class TestGovernanceOnTenantConfig:

    def test_tenant_config_requires_governance(self):
        cls = _config_cls()
        with pytest.raises(ValidationError):
            cls(
                tenant_id="tenant_default",
                enabled=True,
                version=1,
                app=_app(),
            )  # missing governance

    def test_tenant_config_with_governance_roundtrips(self):
        cls = _config_cls()
        cfg = cls(
            tenant_id="tenant_default",
            enabled=True,
            version=1,
            app=_app(),
            governance=_gov(),
            backend_profile=_backend(),
            audit_policy=_audit_policy(),
        )
        assert cfg.governance.allowed_channels == ("web_console", "wecom")
        assert cfg.model_dump(mode="json")["backend_profile"]["state_backend"] == "redis"

    def test_decision_tool_must_be_in_allowed_tools(self):
        cls = _config_cls()
        with pytest.raises(ValidationError):
            cls(
                tenant_id="tenant_default",
                enabled=True,
                version=1,
                app=_app(allowed_tools=()),
                governance=_gov(tool_decisions={"get_current_time": "deny"}),
            )

    def test_draft_requires_governance(self):
        cls = _draft_cls()
        with pytest.raises(ValidationError):
            cls(enabled=True, app=_app())

    def test_draft_decision_tool_must_be_in_allowed_tools(self):
        cls = _draft_cls()
        with pytest.raises(ValidationError):
            cls(
                enabled=True,
                app=_app(allowed_tools=("other_tool", )),
                governance=_gov(tool_decisions={"get_current_time": "review"}),
            )

    def test_draft_accepts_matching_decisions(self):
        cls = _draft_cls()
        draft = cls(
            enabled=True,
            app=_app(),
            governance=_gov(tool_decisions={"get_current_time": "review"}),
            backend_profile=_backend(),
            audit_policy=_audit_policy(),
        )
        assert draft.governance.tool_decisions == {"get_current_time": "review"}


class TestToolDecisionsTrulyImmutable:
    """P1-1 (Codex review): frozen=True only blocks rebinding; the dict
    itself must reject in-place mutation, otherwise a request path could
    rewrite a cached TenantConfig's policy behind the version/history
    boundary."""

    def test_setitem_rejected(self):
        gov = _gov(tool_decisions={"get_current_time": "deny"})
        with pytest.raises(TypeError):
            gov.tool_decisions["get_current_time"] = "allow"

    def test_delitem_rejected(self):
        gov = _gov(tool_decisions={"get_current_time": "deny"})
        with pytest.raises(TypeError):
            del gov.tool_decisions["get_current_time"]

    def test_update_pop_setdefault_clear_rejected(self):
        gov = _gov(tool_decisions={"get_current_time": "deny"})
        with pytest.raises(TypeError):
            gov.tool_decisions.update({"other": "allow"})
        with pytest.raises(TypeError):
            gov.tool_decisions.pop("get_current_time", None)
        with pytest.raises(TypeError):
            gov.tool_decisions.setdefault("x", "allow")
        with pytest.raises(TypeError):
            gov.tool_decisions.clear()

    def test_reachable_via_tenant_config_same_object(self):
        cls = _config_cls()
        cfg = cls(
            tenant_id="tenant_default",
            enabled=True,
            version=1,
            app=_app(),
            governance=_gov(tool_decisions={"get_current_time": "deny"}),
            backend_profile=_backend(),
            audit_policy=_audit_policy(),
        )
        with pytest.raises(TypeError):
            cfg.governance.tool_decisions["get_current_time"] = "allow"
        # the policy actually in force is unchanged
        assert cfg.governance.tool_decisions == {"get_current_time": "deny"}

    def test_model_dump_json_is_plain_object(self):
        gov = _gov(tool_decisions={"get_current_time": "deny"})
        dumped = gov.model_dump(mode="json")
        assert dumped["tool_decisions"] == {"get_current_time": "deny"}
        assert type(dumped["tool_decisions"]) is dict

    def test_json_input_still_object_and_roundtrips(self):
        cls = _governance_cls()
        gov = cls.model_validate({
            "allowed_channels": ["web_console"],
            "allowed_user_ids": [],
            "tool_decisions": {
                "get_current_time": "review"
            },
            "content_policy": {
                "enabled": True,
                "input_action": "block",
                "output_action": "block",
            },
            "limits": None,
        })
        assert gov.tool_decisions["get_current_time"] == "review"
        assert gov.model_dump(mode="json")["tool_decisions"] == {"get_current_time": "review"}

    def test_reads_and_views_still_work(self):
        gov = _gov(tool_decisions={"get_current_time": "deny"})
        assert dict(gov.tool_decisions) == {"get_current_time": "deny"}
        assert list(gov.tool_decisions.keys()) == ["get_current_time"]
        assert "get_current_time" in gov.tool_decisions
        assert gov.tool_decisions.get("missing", "allow") == "allow"


class TestToolDecisionKeyNormalization:
    """P2-1: keys differing only by surrounding whitespace must be a hard
    error, never a silent last-wins overwrite."""

    @pytest.mark.parametrize(
        "conflicting",
        [
            {
                " tool": "deny",
                "tool": "allow"
            },
            {
                "tool ": "allow",
                "tool": "deny"
            },
            {
                "  tool": "review",
                "\ttool": "allow"
            },
        ],
    )
    def test_whitespace_collision_rejected(self, conflicting: dict):
        with pytest.raises(ValidationError):
            _gov(tool_decisions=conflicting)

    def test_whitespace_key_still_normalized_when_unique(self):
        gov = _gov(tool_decisions={" get_current_time ": "deny"})
        assert dict(gov.tool_decisions) == {"get_current_time": "deny"}

    def test_non_string_key_rejected(self):
        with pytest.raises(ValidationError):
            _gov(tool_decisions={1: "deny"})
