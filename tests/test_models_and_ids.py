from __future__ import annotations

import hashlib

import pytest
from pydantic import ValidationError

from tenant_agent.ids import IdentityDeriver
from tenant_agent.models import (
    ArtifactRecord,
    AuditPolicy,
    BackendKind,
    BackendRef,
    ChannelBindingConfig,
    ChatType,
    ModelConfig,
    SecretRef,
    tenant_environment_prefix,
)
from tests.helpers import make_envelope, make_tenant


def test_secret_values_must_be_references() -> None:
    with pytest.raises(ValidationError):
        SecretRef(uri="plain-text-password")


def test_ids_are_stable_and_tenant_scoped() -> None:
    deriver = IdentityDeriver("a-long-test-hmac-key-value")
    alpha = make_tenant("alpha")
    beta = make_tenant("beta")
    alpha_message = make_envelope(alpha, user_id="same-user", chat_id="same-chat")
    beta_message = make_envelope(beta, user_id="same-user", chat_id="same-chat")

    assert deriver.user_id(alpha_message) == deriver.user_id(alpha_message)
    assert deriver.user_id(alpha_message) != deriver.user_id(beta_message)
    assert deriver.session_id(alpha_message) != deriver.session_id(beta_message)
    assert "same-user" not in deriver.user_id(alpha_message)
    assert deriver.content_fingerprint("alpha", "prompt") != deriver.content_fingerprint("beta", "prompt")


def test_direct_group_and_per_user_group_session_rules() -> None:
    deriver = IdentityDeriver("a-long-test-hmac-key-value")
    tenant = make_tenant()
    direct_one = make_envelope(tenant, user_id="u1", chat_id="dm", chat_type=ChatType.DIRECT)
    direct_two = make_envelope(tenant, user_id="u2", chat_id="dm", chat_type=ChatType.DIRECT)
    group_one = make_envelope(tenant, user_id="u1", chat_id="g1", chat_type=ChatType.GROUP)
    group_two = make_envelope(tenant, user_id="u2", chat_id="g1", chat_type=ChatType.GROUP)

    assert deriver.session_id(direct_one) != deriver.session_id(direct_two)
    assert deriver.session_id(group_one) == deriver.session_id(group_two)
    assert deriver.session_id(group_one, group_scope="per_user") != deriver.session_id(
        group_two, group_scope="per_user"
    )


def test_tenant_rejects_app_tools_outside_tenant_allowlist() -> None:
    with pytest.raises(ValidationError):
        make_tenant(tools=frozenset({"unregistered"})).model_copy(
            update={"governance": make_tenant().governance}
        ).model_validate(
            {
                **make_tenant(tools=frozenset({"unregistered"})).model_dump(mode="json"),
                "governance": make_tenant().governance.model_dump(mode="json"),
            }
        )


def test_backend_channel_and_model_reject_inline_credentials() -> None:
    with pytest.raises(ValidationError):
        BackendRef(kind=BackendKind.INMEMORY, options={"password": "plain"})
    with pytest.raises(ValidationError):
        BackendRef(
            kind=BackendKind.INMEMORY,
            options={"servers": [{"password": "nested-plain-secret"}]},
        )
    with pytest.raises(ValidationError):
        ChannelBindingConfig(
            binding_id="binding-001",
            channel="web",
            app_id="assistant",
            external_account_id="account",
            settings={"access_token": "plain"},
        )
    with pytest.raises(ValidationError):
        ModelConfig(
            provider="openai-compatible",
            model_name="model",
            api_key_ref=SecretRef(uri="env://MODEL_KEY"),
            base_url="https://user:password@example.com/v1",
        )
    with pytest.raises(ValidationError):
        ModelConfig(
            provider="openai-compatible",
            model_name="model",
            api_key_ref=SecretRef(uri="env://MODEL_KEY"),
            base_url="http://model.example/v1",
        )
    with pytest.raises(ValidationError, match="context window"):
        ModelConfig(
            provider="deterministic",
            model_name="deterministic",
            max_output_tokens=2_048,
            context_window_tokens=1_024,
        )
    with pytest.raises(ValidationError):
        BackendRef(kind=BackendKind.INMEMORY, namespace="../escape")
    with pytest.raises(ValidationError):
        AuditPolicy(export_sink="https://user:password@audit.example/v1")
    artifact_values = {
        "tenant_id": "alpha",
        "session_id": "session",
        "artifact_id": "artifact",
        "filename": "file",
        "content_type": "text/plain",
        "size_bytes": 0,
        "checksum_sha256": hashlib.sha256(b"").hexdigest(),
        "storage_uri": "memory://artifact",
    }
    with pytest.raises(ValidationError):
        ArtifactRecord(**artifact_values, version=-1)
    with pytest.raises(ValidationError):
        ArtifactRecord(**artifact_values, version=10**20)

    tenant = make_tenant()
    leaked_model = tenant.models["offline"].model_copy(
        update={
            "provider": "openai-compatible",
            "api_key_ref": SecretRef(uri="env://TAP_INTERNAL_BEARER_TOKEN"),
        }
    )
    with pytest.raises(ValidationError, match="tenant environment references"):
        type(tenant).model_validate(
            {
                **tenant.model_dump(mode="json"),
                "models": {"offline": leaked_model.model_dump(mode="json")},
            }
        )

    cross_tenant_channel = tenant.channels[0].model_copy(
        update={"credential_refs": {"webhook_token": SecretRef(uri="vault://kv/data/tenants/beta/web#token")}}
    )
    with pytest.raises(ValidationError, match="tenant namespace"):
        type(tenant).model_validate(
            {
                **tenant.model_dump(mode="json"),
                "channels": [cross_tenant_channel.model_dump(mode="json")],
            }
        )

    for unsafe_reference, message in (
        (SecretRef(uri="file://alpha/../beta/secret"), "tenant file references"),
        (SecretRef(uri="file://alpha/%2e%2e/beta/secret"), "tenant file references"),
        (
            SecretRef(uri="vault://kv/data/tenants/alpha/%2e%2e/beta/secret#token"),
            "canonical path",
        ),
        (
            SecretRef(uri="vault://kv/data/tenants/beta/shadow/tenants/alpha/key#value"),
            "tenant namespace",
        ),
        (SecretRef(uri="env://TAP_REDIS_URL"), "tenant environment references"),
        (SecretRef(uri="env://TENANT_BETA_WEBHOOK_TOKEN"), "tenant environment references"),
    ):
        unsafe_channel = tenant.channels[0].model_copy(
            update={"credential_refs": {"webhook_token": unsafe_reference}}
        )
        with pytest.raises(ValidationError, match=message):
            type(tenant).model_validate(
                {
                    **tenant.model_dump(mode="json"),
                    "channels": [unsafe_channel.model_dump(mode="json")],
                }
            )

    hyphen_prefix = tenant_environment_prefix("a-b")
    underscore_prefix = tenant_environment_prefix("a_b")
    assert hyphen_prefix != underscore_prefix
    for tenant_id, wrong_prefix in (
        ("a-b", underscore_prefix),
        ("a_b", hyphen_prefix),
    ):
        scoped = make_tenant(tenant_id)
        unsafe_channel = scoped.channels[0].model_copy(
            update={"credential_refs": {"webhook_token": SecretRef(uri=f"env://{wrong_prefix}WEBHOOK_TOKEN")}}
        )
        with pytest.raises(ValidationError, match="tenant environment references"):
            type(scoped).model_validate(
                {
                    **scoped.model_dump(mode="json"),
                    "channels": [unsafe_channel.model_dump(mode="json")],
                }
            )
