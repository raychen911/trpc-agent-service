"""Tenant model routes cannot choose arbitrary providers, endpoints, or secrets."""

from __future__ import annotations

from dataclasses import replace

import pytest
from pydantic import SecretStr, ValidationError
from trpc_agent_sdk.models import OpenAIModel

from trpc_service.agent import ModelRouteError, TenantModelResolver
from trpc_service.tenant.context import ConversationScope, TenantContext
from trpc_service.tenant.models import AgentAppSpec, ModelRoute


class StaticSecretResolver:
    def resolve(self, reference: str) -> SecretStr:
        if reference != "secret://env/TENANT_MODEL_KEY":
            raise RuntimeError("unexpected secret reference")
        return SecretStr("synthetic-model-key")


def _context() -> TenantContext:
    return TenantContext(
        tenant_id="tenant-a",
        app_id="app-a",
        app_revision=1,
        binding_id="binding-a",
        binding_revision=1,
        principal_id="principal-a",
        session_id="session-a",
        scope=ConversationScope.PRIVATE,
        request_id="request-a",
        trace_id="0" * 32,
    )


def _app(provider: str = "openai") -> AgentAppSpec:
    return AgentAppSpec(
        app_id="app-a",
        revision=1,
        name="safe_agent",
        prompt="Answer concisely.",
        model=ModelRoute(
            provider=provider,
            model="gpt-example",
            api_key_ref="secret://env/TENANT_MODEL_KEY",
            token_ceiling=512,
            temperature=0.1,
        ),
    )


def test_resolver_builds_real_sdk_model_without_exposing_endpoint_control() -> None:
    resolver = TenantModelResolver(
        provider="openai",
        secret_resolver=StaticSecretResolver(),
    )

    model = resolver(_context(), _app())

    assert isinstance(model, OpenAIModel)
    assert model.name == "gpt-example"
    assert "synthetic-model-key" not in repr(model)


def test_resolver_rejects_provider_and_revision_mismatch() -> None:
    resolver = TenantModelResolver(
        provider="openai",
        secret_resolver=StaticSecretResolver(),
    )
    with pytest.raises(ModelRouteError, match="outside"):
        resolver(_context(), _app("anthropic"))
    wrong_context = replace(_context(), app_revision=2)
    with pytest.raises(ModelRouteError, match="revision"):
        resolver(wrong_context, _app())


def test_model_route_requires_secret_reference_syntax() -> None:
    with pytest.raises(ValidationError, match="secret://"):
        ModelRoute(
            provider="openai",
            model="gpt-example",
            api_key_ref="plaintext-key",
        )
