from __future__ import annotations

import pytest

from trpc_service.governance import principal


def test_principal_digest_is_tenant_and_channel_scoped() -> None:
    first = principal.issue_principal(tenant_id="tenant-a", channel="feishu", binding_id="b", provider_subject="same")
    second = principal.issue_principal(tenant_id="tenant-a", channel="wecom", binding_id="b", provider_subject="same")
    assert first.subject_digest != second.subject_digest
    assert first.provider_subject == "same"


def test_display_name_cannot_construct_principal() -> None:
    with pytest.raises(ValueError):
        principal.issue_principal(tenant_id="tenant-a", channel="feishu", binding_id="b", provider_subject="")


def test_grant_evaluation_defaults_to_denied() -> None:
    repository = principal.InMemoryPrincipalGrantRepository()
    subject = principal.issue_principal(tenant_id="tenant-a", channel="feishu", binding_id="b", provider_subject="u")
    decision = repository.evaluate_sync(subject, agent_name="agent", binding_id="b")
    assert decision.allowed is False
    assert decision.reason_code == "principal_unauthorized"
