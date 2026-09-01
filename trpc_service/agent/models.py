# mypy: disable-error-code="import-untyped"
"""Fail-closed resolution of tenant model routes to tRPC model instances."""

from __future__ import annotations

from pydantic import SecretStr
from trpc_agent_sdk.models import LLMModel, OpenAIModel
from trpc_agent_sdk.types import GenerateContentConfig

from trpc_service.security import SecretResolver
from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import AgentAppSpec


class ModelRouteError(RuntimeError):
    """A published tenant model route cannot be safely instantiated."""


class TenantModelResolver:
    """Resolve a tenant model without accepting tenant-controlled network targets.

    The endpoint is platform configuration. A tenant chooses only an approved
    provider/model and an allowlisted secret reference, preventing a malicious
    TenantSpec from exfiltrating credentials through an arbitrary base URL.
    """

    def __init__(
        self,
        *,
        provider: str,
        secret_resolver: SecretResolver,
        default_api_key: SecretStr | None = None,
        base_url: str | None = None,
    ) -> None:
        normalized_provider = provider.strip().casefold()
        if normalized_provider not in {"openai", "openai-compatible"}:
            raise ModelRouteError("unsupported platform model provider")
        if base_url is not None and not base_url.startswith("https://"):
            raise ModelRouteError("model base URL must use HTTPS")
        self._provider = normalized_provider
        self._secret_resolver = secret_resolver
        self._default_api_key = default_api_key or SecretStr("")
        self._base_url = base_url

    def __call__(self, tenant_context: TenantContext, app: AgentAppSpec) -> LLMModel:
        """Build one request-owned model from an immutable published route."""

        if tenant_context.app_id != app.app_id or tenant_context.app_revision != app.revision:
            raise ModelRouteError("TenantContext and model route revision differ")
        if app.model.provider.strip().casefold() != self._provider:
            raise ModelRouteError("tenant requested a provider outside the platform route")
        if app.model.api_key_ref is not None:
            api_key = self._secret_resolver.resolve(app.model.api_key_ref).get_secret_value()
        else:
            api_key = self._default_api_key.get_secret_value()
        if not api_key:
            raise ModelRouteError("model credential is unavailable")

        kwargs: dict[str, object] = {
            "api_key": api_key,
            "generate_content_config": GenerateContentConfig(
                max_output_tokens=app.model.token_ceiling,
                temperature=app.model.temperature,
            ),
        }
        if self._base_url is not None:
            kwargs["base_url"] = self._base_url
        return OpenAIModel(model_name=app.model.model, **kwargs)
