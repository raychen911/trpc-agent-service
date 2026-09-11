"""Real model provider configuration loaded without persisting secrets."""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Mapping

from trpc_agent_sdk.models import LLMModel
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.models import close_shared_http_clients
from trpc_agent_sdk.models import shared_http_client_provider_factory

logger = logging.getLogger(__name__)

# Public knob so shutdown tests can shrink the bound without waiting 5s.
HTTP_CLIENT_CLOSE_TIMEOUT_SECONDS = 5.0


class ModelConfigurationError(ValueError):
    """Raised when real-model configuration is incomplete or unsupported."""


@dataclass(frozen=True, slots=True)
class ModelSettings:
    """Non-secret settings for an OpenAI-compatible model provider."""

    provider: str
    model_name: str
    base_url: str | None
    api_key_env: str

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "ModelSettings":
        values = os.environ if environ is None else environ
        provider = values.get("TRPC_MODEL_PROVIDER", "openai-compatible").strip()
        if provider != "openai-compatible":
            raise ModelConfigurationError("TRPC_MODEL_PROVIDER must be 'openai-compatible' during Stage 0")

        model_name = values.get("TRPC_MODEL_NAME", "").strip()
        if not model_name:
            raise ModelConfigurationError("TRPC_MODEL_NAME is required")

        api_key_env = values.get("TRPC_MODEL_API_KEY_ENV", "TRPC_MODEL_API_KEY").strip()
        if not api_key_env:
            raise ModelConfigurationError("TRPC_MODEL_API_KEY_ENV must not be empty")

        base_url = values.get("TRPC_MODEL_BASE_URL", "").strip() or None
        return cls(
            provider=provider,
            model_name=model_name,
            base_url=base_url,
            api_key_env=api_key_env,
        )

    def resolve_api_key(self, environ: Mapping[str, str] | None = None) -> str:
        values = os.environ if environ is None else environ
        api_key = values.get(self.api_key_env, "").strip()
        if not api_key:
            raise ModelConfigurationError(f"{self.api_key_env} is required")
        return api_key


def build_model(
    settings: ModelSettings,
    environ: Mapping[str, str] | None = None,
) -> LLMModel:
    """Build the SDK's real model implementation without making a network call.

    The SDK's shared HTTP client provider is injected explicitly so back-to-back
    model calls reuse keep-alive connections instead of paying fresh
    DNS/TCP/TLS per request.  Shutdown must call :func:`close_model_http_clients`.
    """
    return OpenAIModel(
        model_name=settings.model_name,
        api_key=settings.resolve_api_key(environ),
        base_url=settings.base_url,
        http_client_provider_factory=shared_http_client_provider_factory,
    )


async def close_model_http_clients() -> None:
    """Boundedly close the SDK's shared model HTTP clients; never raises.

    Repeatable: safe to call from every shutdown hook that may run.  Failures
    are logged with a fixed message only — no URLs, keys, or exception bodies.
    """
    try:
        await asyncio.wait_for(
            close_shared_http_clients(),
            timeout=HTTP_CLIENT_CLOSE_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.warning("model http client cleanup failed (component=model_http)")
