"""Tenant configuration validation tests."""

import pytest
from pydantic import ValidationError

from trpc_service.tenant import SecretRef
from trpc_service.tenant import ToolPolicy

from .conftest import build_tenant


def test_complete_tenant_model_is_immutable_and_reference_safe(tenant):
    assert tenant.tenant_id == "tenant_acme"
    assert tenant.applications[0].model_config_id == "model_primary"
    assert tenant.channel_bindings[0].tenant_id == tenant.tenant_id
    assert tenant.data_backends.session.namespace == tenant.tenant_id

    with pytest.raises(ValidationError):
        tenant.name = "changed"


def test_plaintext_secret_is_rejected():
    with pytest.raises(ValidationError, match="reference URI"):
        SecretRef(uri="sk-plaintext-secret")


def test_tool_policy_requires_confirmation_tools_to_be_allowed():
    with pytest.raises(ValidationError, match="must also be present in allow"):
        ToolPolicy(require_confirmation=frozenset({"execute_refund"}))


def test_cross_tenant_channel_binding_is_rejected(tenant):
    data = tenant.model_dump()
    data["channel_bindings"][0]["tenant_id"] = "tenant_other"
    with pytest.raises(ValidationError, match="belongs to another tenant"):
        type(tenant).model_validate(data)


def test_unknown_model_reference_is_rejected(tenant):
    data = tenant.model_dump()
    data["applications"][0]["model_config_id"] = "missing_model"
    with pytest.raises(ValidationError, match="unknown model"):
        type(tenant).model_validate(data)


@pytest.mark.parametrize("backend_role", ["session", "memory", "summary", "knowledge", "artifact", "audit"])
def test_backend_namespace_must_match_tenant_id(tenant, backend_role):
    data = tenant.model_dump()
    data["data_backends"][backend_role]["namespace"] = "tenant_other"

    with pytest.raises(ValidationError, match=rf"{backend_role} backend namespace must equal tenant_id"):
        type(tenant).model_validate(data)


def test_duplicate_callback_identity_is_rejected():
    tenant = build_tenant()
    data = tenant.model_dump()
    bindings = list(data["channel_bindings"])
    bindings.append(dict(bindings[0], binding_id="binding_second"))
    data["channel_bindings"] = bindings
    with pytest.raises(ValidationError, match="duplicate webhook public"):
        type(tenant).model_validate(data)


def test_summary_must_follow_session_backend(tenant):
    data = tenant.model_dump()
    data["data_backends"]["summary"]["kind"] = "sql"
    data["data_backends"]["summary"]["profile_id"] = "sql_prod"
    with pytest.raises(ValidationError, match="summary must use"):
        type(tenant).model_validate(data)
