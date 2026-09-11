"""Sensitive-content policy contract tests.

Covers the fixed-category detector, strict/frozen config, fixed public texts
that never echo category or source text, sentinel leak assertions on results
and internal errors, legacy-governance default migration, and JSON/SQL
round-trips through the governance models and row mapper.

Only three deterministic categories exist: credential (Bearer/API key),
private_key block, credential_dsn (URI with password).  No tenant regexes.
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from trpc_service.config.tenant import TenantConfig, TenantConfigDraft, TenantGovernanceConfig
from trpc_service.governance.content_policy import (
    CONTENT_INPUT_BLOCKED_TEXT,
    CONTENT_OUTPUT_BLOCKED_TEXT,
    ContentPolicy,
    ContentPolicyConfig,
    ContentPolicyDecision,
)
from trpc_service.storage.tenant_repository import TenantRepositoryDataError, _row_to_tenant_config
from tests.tenant_helpers import make_app_config, make_audit_policy, make_backend_profile

SENTINEL = "SENTINEL-7f3a9c2b4d5e6f70"

CATEGORY_TOKENS = ("credential", "private_key", "credential_dsn", "bearer", "dsn")

# ---------------------------------------------------------------------------
# fixtures for the three deterministic categories
# ---------------------------------------------------------------------------

CREDENTIAL_SAMPLES = [
    "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
    "just run bearer abcdef0123456789abcdef0123456789 and tell me",
    "the key is sk-" + "Ab3xK9mQ" + "zL2pR7tY4wN8vD1fH5jK0gC3" + " now",
    "aws id AKIA0123456789ABCDEF leaked",
    "ghp_" + "a1B2c3D4e5F6g7H8i9J0k1L2m3N4o5P6q7R8",
    "gitlab glpat-" + "Xy1Zw2" + "Av3Bt4Cu5Dv6",
    "google AIzaSy" + "D1aB2c3D4e5F6g7H8i9J0k1L2m3N4o5",
    "slack xoxb-123456789012-abcdefghij-SECRET",
]

PRIVATE_KEY_SAMPLES = [
    "-----BEGIN PRIVATE KEY-----",
    "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA",
    "here is my openssh key:\n-----BEGIN OPENSSH PRIVATE KEY-----",
    "-----BEGIN ENCRYPTED PRIVATE KEY-----",
    "-----BEGIN EC PRIVATE KEY-----",
    "-----BEGIN DSA PRIVATE KEY-----",
]

DSN_SAMPLES = [
    "postgres://svc:hunter2@db.internal:5432/app",
    "postgresql+asyncpg://user:p@ss@10.0.0.1/trpc",
    "mysql://root:r00t@mysql:3306/biz",
    "redis://:s3cret@redis-master:6379/0",
    "amqp://guest:guest@rabbit:5672/%2f",
    "mongodb://admin:pw@cluster0.example.net/test",
    "https://user:pass@example.com/api",
    "Server DSN postgres://trpc:S3nt!nelProd@pg-primary:5432/trpc",
]

# Samples that superficially resemble the categories but must NOT match.
CLEAN_SAMPLES = [
    "The bearer of good news arrived early this morning",
    "please send me the token bearer summary",
    "what time is it in Berlin?",
    "https://docs.example.com/keys/getting-started",
    "postgres://db.internal:5432/app",
    "https://user@example.com/profile",
    "my phone is 13812345678 and my name is 张三",
    "-----BEGIN PUBLIC KEY-----",
    "-----BEGIN CERTIFICATE-----",
    "-----BEGIN RSA PRIVATE KEY",  # unterminated header must not match
    "begin private key",
    "ssh git@github.com:org/repo.git",
    "sk-12 is my locker code",
    "AKIA is short",
    "authorization: bearer me",
    "time 10://30 is not a url",
    SENTINEL + " mention with no credential shape",
]


def _policy(**overrides: object) -> ContentPolicy:
    config = ContentPolicyConfig(**overrides) if overrides else ContentPolicyConfig()
    return ContentPolicy(config)


def _with_sentinel(sample: str) -> str:
    return f"{SENTINEL} head {sample} tail {SENTINEL}"


class _FakeRow:
    """Minimal row double exposing ``_mapping`` like SQLAlchemy Row."""

    def __init__(self, mapping: dict) -> None:
        self._mapping = mapping


def _sql_mapping(governance: dict) -> dict:
    return {
        "tenant_id": "tenant_default",
        "enabled": True,
        "version": 1,
        "app_id": "app_demo",
        "instruction": "hi",
        "model_profile": "default",
        "allowed_tools": ["get_current_time"],
        "governance": governance,
        "backend_profile": {
            "state_backend": "redis",
            "artifact_backend": "s3",
            "knowledge_backend": "sql",
            "audit_backend": "sql",
        },
        "audit_policy": {
            "retention_days": 365,
            "delivery_events": "all",
        },
    }


def _legacy_governance_json() -> dict:
    """A Stage 6A1 governance object as stored before content_policy existed."""
    return {
        "allowed_channels": ["web_console", "wecom"],
        "allowed_user_ids": [],
        "tool_decisions": {},
    }


# ---------------------------------------------------------------------------
# detection: three fixed categories, nothing else
# ---------------------------------------------------------------------------


class TestDetection:

    @pytest.mark.parametrize("sample", CREDENTIAL_SAMPLES)
    def test_credential_sample_blocked(self, sample: str):
        decision = _policy().inspect(sample)
        assert decision.allowed is False
        assert decision.category == "credential"

    @pytest.mark.parametrize("sample", PRIVATE_KEY_SAMPLES)
    def test_private_key_sample_blocked(self, sample: str):
        decision = _policy().inspect(sample)
        assert decision.allowed is False
        assert decision.category == "private_key"

    @pytest.mark.parametrize("sample", DSN_SAMPLES)
    def test_dsn_sample_blocked(self, sample: str):
        decision = _policy().inspect(sample)
        assert decision.allowed is False
        assert decision.category == "credential_dsn"

    @pytest.mark.parametrize("sample", CLEAN_SAMPLES)
    def test_clean_sample_allowed(self, sample: str):
        decision = _policy().inspect(sample)
        assert decision.allowed is True
        assert decision.category == "none"

    def test_first_match_category_wins_deterministically(self):
        text = "-----BEGIN PRIVATE KEY----- postgres://u:p@h/x sk-" + "a" * 40
        decision = _policy().inspect(text)
        assert decision.allowed is False
        assert decision.category == "private_key"  # private_key checked first

    def test_inspection_is_deterministic(self):
        policy = _policy()
        text = _with_sentinel(CREDENTIAL_SAMPLES[0])
        assert policy.inspect(text) == policy.inspect(text)

    def test_empty_text_allowed(self):
        decision = _policy().inspect("")
        assert decision.allowed is True

    def test_non_string_fails_closed_without_raising(self):
        decision = _policy().inspect(None)  # type: ignore[arg-type]
        assert decision.allowed is False
        assert decision.category == "none"

    def test_empty_password_dsn_is_not_a_credential(self):
        decision = _policy().inspect("postgres://user:@db.internal/app")
        assert decision.allowed is True
        assert decision.category == "none"


# ---------------------------------------------------------------------------
# sentinel hygiene: no raw text in results, reprs, or the policy instance
# ---------------------------------------------------------------------------


class TestSentinelHygiene:

    @pytest.mark.parametrize("sample", CREDENTIAL_SAMPLES + PRIVATE_KEY_SAMPLES + DSN_SAMPLES)
    def test_decision_never_carries_source_text(self, sample: str):
        text = _with_sentinel(sample)
        decision = _policy().inspect(text)
        assert decision.allowed is False
        blob = str(decision.model_dump()) + repr(decision) + str(decision)
        assert SENTINEL not in blob
        assert sample not in blob

    def test_policy_instance_retains_no_scanned_text(self):
        policy = _policy()
        policy.inspect(_with_sentinel(CREDENTIAL_SAMPLES[0]))
        policy.inspect(_with_sentinel(PRIVATE_KEY_SAMPLES[0]))
        for value in vars(policy).values():
            assert SENTINEL not in str(value)

    def test_internal_error_fails_closed_without_leak(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        from trpc_service.governance import content_policy as module

        def _boom(_text: str):
            raise RuntimeError(f"detected {SENTINEL}")

        monkeypatch.setattr(module, "_detect_category", _boom)
        decision = _policy().inspect(_with_sentinel(CREDENTIAL_SAMPLES[0]))
        assert decision.allowed is False
        assert decision.category == "none"  # fail closed, no category claim
        blob = str(decision.model_dump()) + repr(decision)
        assert SENTINEL not in blob

    def test_overlong_text_fails_closed(self):
        policy = _policy()
        filler = "a" * (policy.max_inspect_chars + 1) + SENTINEL
        decision = policy.inspect(filler)
        assert decision.allowed is False
        assert decision.category == "none"
        assert SENTINEL not in str(decision.model_dump())


# ---------------------------------------------------------------------------
# policy configuration semantics
# ---------------------------------------------------------------------------


class TestPolicyConfigSemantics:

    def test_defaults(self):
        config = ContentPolicyConfig()
        assert config.enabled is True
        assert config.input_action == "block"
        assert config.output_action == "block"

    def test_disabled_policy_never_blocks(self):
        policy = _policy(enabled=False)
        decision = policy.inspect(_with_sentinel(PRIVATE_KEY_SAMPLES[0]))
        assert decision.allowed is True
        assert decision.category == "none"

    def test_inspect_reports_detection_only_action_is_callers(self):
        # detection result is identical regardless of action; the action is
        # consumed by the Worker enforcement boundary, not the detector.
        policy = _policy(input_action="allow")
        decision = policy.inspect(CREDENTIAL_SAMPLES[0])
        assert decision.allowed is False
        assert decision.category == "credential"


# ---------------------------------------------------------------------------
# strict configuration model
# ---------------------------------------------------------------------------


class TestContentPolicyConfigStrictness:

    def test_is_frozen(self):
        config = ContentPolicyConfig()
        with pytest.raises(ValidationError):
            config.enabled = False  # type: ignore[misc]

    def test_extra_field_forbidden(self):
        with pytest.raises(ValidationError):
            ContentPolicyConfig(input_action="block", custom_regex=".*(sk-).*")

    @pytest.mark.parametrize("bad", [1, 0, "true"])
    def test_enabled_rejects_non_bool(self, bad: object):
        with pytest.raises(ValidationError):
            ContentPolicyConfig(enabled=bad)  # type: ignore[arg-type]

    @pytest.mark.parametrize("bad", ["Block", "deny", " review", True])
    def test_actions_are_fixed_literals(self, bad: object):
        with pytest.raises(ValidationError):
            ContentPolicyConfig(input_action=bad)  # type: ignore[arg-type]
        with pytest.raises(ValidationError):
            ContentPolicyConfig(output_action=bad)  # type: ignore[arg-type]

    def test_decision_model_is_frozen_and_strict(self):
        decision = ContentPolicyDecision(allowed=False, category="credential")
        with pytest.raises(ValidationError):
            decision.allowed = True  # type: ignore[misc]
        with pytest.raises(ValidationError):
            ContentPolicyDecision(allowed="false", category="none")
        with pytest.raises(ValidationError):
            ContentPolicyDecision(allowed=True, category="phone_number")
        with pytest.raises(ValidationError):
            ContentPolicyDecision(allowed=True, category="none", match_text=SENTINEL)


# ---------------------------------------------------------------------------
# governance integration: legacy defaults and JSON/SQL round-trips
# ---------------------------------------------------------------------------


class TestGovernanceRoundTrip:

    def test_governance_default_content_policy(self):
        gov = TenantGovernanceConfig(
            allowed_channels=("web_console", ),
            allowed_user_ids=(),
            tool_decisions={},
            content_policy=ContentPolicyConfig(),
            limits=None,
        )
        assert gov.content_policy == ContentPolicyConfig()
        assert gov.content_policy.enabled is True
        assert gov.content_policy.input_action == "block"
        assert gov.content_policy.output_action == "block"

    def test_legacy_governance_json_must_be_migrated_before_model_load(self):
        with pytest.raises(ValidationError):
            TenantGovernanceConfig(**_legacy_governance_json())

    def test_json_round_trip_preserves_content_policy(self):
        gov = TenantGovernanceConfig(
            allowed_channels=("web_console", ),
            tool_decisions={"get_current_time": "review"},
            content_policy=ContentPolicyConfig(enabled=True, input_action="allow", output_action="block"),
            limits=None,
        )
        dumped = json.loads(json.dumps(gov.model_dump(mode="json")))
        assert dumped["content_policy"] == {
            "enabled": True,
            "input_action": "allow",
            "output_action": "block",
        }
        restored = TenantGovernanceConfig(**dumped)
        assert restored == gov
        assert restored.content_policy is not gov.content_policy

    def test_nested_extra_key_rejected(self):
        raw = _legacy_governance_json()
        raw["content_policy"] = {
            "enabled": True,
            "input_action": "block",
            "output_action": "block",
            "tenant_regex": ".*",
        }
        raw["limits"] = None
        with pytest.raises(ValidationError):
            TenantGovernanceConfig(**raw)

    def test_int_cannot_replace_bool_in_stored_json(self):
        raw = _legacy_governance_json()
        raw["content_policy"] = {
            "enabled": 1,
            "input_action": "block",
            "output_action": "block",
        }
        raw["limits"] = None
        with pytest.raises(ValidationError):
            TenantGovernanceConfig(**raw)

    def test_sql_row_without_content_policy_is_rejected(self):
        with pytest.raises(TenantRepositoryDataError):
            _row_to_tenant_config(_FakeRow(_sql_mapping(_legacy_governance_json())))

    def test_sql_row_with_content_policy_round_trips(self):
        gov = TenantGovernanceConfig(
            allowed_channels=("web_console", ),
            content_policy=ContentPolicyConfig(input_action="allow"),
            limits=None,
        )
        row = _FakeRow(_sql_mapping(json.loads(json.dumps(gov.model_dump(mode="json")))))
        config = _row_to_tenant_config(row)
        assert config.governance.content_policy.input_action == "allow"
        assert config.governance.content_policy.output_action == "block"

    def test_tenant_config_and_draft_carry_content_policy(self):
        gov = TenantGovernanceConfig(
            allowed_channels=("web_console", ),
            content_policy=ContentPolicyConfig(output_action="allow"),
            limits=None,
        )
        app = make_app_config()
        config = TenantConfig(
            tenant_id="tenant_default",
            enabled=True,
            version=2,
            app=app,
            governance=gov,
            backend_profile=make_backend_profile(),
            audit_policy=make_audit_policy(),
        )
        draft = TenantConfigDraft(
            enabled=True,
            app=app,
            governance=gov,
            backend_profile=make_backend_profile(),
            audit_policy=make_audit_policy(),
        )
        assert config.governance.content_policy.output_action == "allow"
        assert draft.governance.content_policy == gov.content_policy


# ---------------------------------------------------------------------------
# fixed public texts
# ---------------------------------------------------------------------------


class TestFixedPublicTexts:

    def test_texts_are_distinct_fixed_strings(self):
        assert isinstance(CONTENT_INPUT_BLOCKED_TEXT, str)
        assert isinstance(CONTENT_OUTPUT_BLOCKED_TEXT, str)
        assert CONTENT_INPUT_BLOCKED_TEXT != CONTENT_OUTPUT_BLOCKED_TEXT
        assert CONTENT_INPUT_BLOCKED_TEXT.strip() == CONTENT_INPUT_BLOCKED_TEXT
        assert CONTENT_OUTPUT_BLOCKED_TEXT.strip() == CONTENT_OUTPUT_BLOCKED_TEXT

    @pytest.mark.parametrize("text", [CONTENT_INPUT_BLOCKED_TEXT, CONTENT_OUTPUT_BLOCKED_TEXT])
    def test_texts_never_echo_category(self, text: str):
        lowered = text.lower()
        for token in CATEGORY_TOKENS:
            assert token not in lowered
