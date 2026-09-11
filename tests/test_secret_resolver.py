"""R2A secret-reference and tenant audit-policy contracts."""

from __future__ import annotations

import traceback

import pytest
from pydantic import ValidationError


def _policy(**overrides: object):
    from trpc_service.config.tenant import TenantAuditPolicy

    values: dict[str, object] = {"retention_days": 365, "delivery_events": "all"}
    values.update(overrides)
    return TenantAuditPolicy(**values)


class TestTenantAuditPolicy:

    def test_is_strict_frozen_and_forbids_extra_fields(self) -> None:
        policy = _policy()
        assert policy.retention_days == 365
        with pytest.raises(ValidationError):
            policy.retention_days = 1  # type: ignore[misc]
        with pytest.raises(ValidationError):
            _policy(unexpected=True)
        with pytest.raises(ValidationError):
            _policy(retention_days=True)

    def test_tenant_config_and_draft_require_the_versioned_policy(self) -> None:
        from trpc_service.config.tenant import TenantConfig, TenantConfigDraft

        assert TenantConfig.model_fields["audit_policy"].is_required()
        assert TenantConfigDraft.model_fields["audit_policy"].is_required()

    @pytest.mark.parametrize("days", [0, 3651, 1.0, "365"])
    def test_requires_strict_bounded_retention_days(self, days: object) -> None:
        with pytest.raises(ValidationError):
            _policy(retention_days=days)

    @pytest.mark.parametrize("delivery_events", ["", "failed", True, None])
    def test_requires_known_delivery_event_policy(self, delivery_events: object) -> None:
        with pytest.raises(ValidationError):
            _policy(delivery_events=delivery_events)


class TestEnvSecretResolver:

    def test_resolves_only_allowed_non_empty_env_reference(self) -> None:
        from trpc_service.config.secret_resolver import EnvSecretResolver

        resolver = EnvSecretResolver({"TRPC_CHANNEL_TOKEN_2": "a-secret-value"})
        assert resolver.resolve("env:TRPC_CHANNEL_TOKEN_2") == "a-secret-value"

    @pytest.mark.parametrize(
        "reference",
        ["", "env:", "env:OTHER_TOKEN", "env:TRPC_lower", "env:TRPC_", "file:TRPC_TOKEN"],
    )
    def test_rejects_invalid_reference_without_disclosure(self, reference: str) -> None:
        self._assert_redacted_failure(reference, {"TRPC_REAL_TOKEN": "sentinel-secret"})

    def test_missing_or_empty_value_is_redacted(self) -> None:
        self._assert_redacted_failure("env:TRPC_MISSING_TOKEN", {"TRPC_REAL_TOKEN": "sentinel-secret"})
        self._assert_redacted_failure("env:TRPC_EMPTY_TOKEN", {"TRPC_EMPTY_TOKEN": ""})

    @staticmethod
    def _assert_redacted_failure(reference: str, environ: dict[str, str]) -> None:
        from trpc_service.config.secret_resolver import EnvSecretResolver
        from trpc_service.config.secret_resolver import SecretResolutionError

        with pytest.raises(SecretResolutionError) as caught:
            EnvSecretResolver(environ).resolve(reference)
        rendered = "\n".join([
            str(caught.value),
            repr(caught.value),
            "".join(traceback.format_exception(caught.type, caught.value, caught.tb)),
        ])
        assert "TRPC_" not in rendered
        assert "sentinel-secret" not in rendered
        assert caught.value.__cause__ is None
        assert caught.value.__suppress_context__ is True
