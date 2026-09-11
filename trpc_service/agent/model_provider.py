"""Lazy model provider for tenant agent runtimes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Protocol

from trpc_agent_sdk.models import LLMModel

from trpc_service.agent.errors import TenantAgentConfigurationError


class ModelProvider(Protocol):
    """Resolves a model profile name to an LLMModel instance."""

    def get_model(self, model_profile: str) -> LLMModel:
        ...


class DefaultModelProvider:
    """Product model provider supporting only the ``default`` profile.

    Model creation is deferred until the first ``get_model("default")`` call
    so that ``/health`` never depends on model credentials.
    """

    def __init__(self, model_factory: Callable[[], LLMModel]) -> None:
        self._factory = model_factory
        self._model: LLMModel | None = None

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> DefaultModelProvider:
        """Build a provider that defers ``ModelSettings.from_env()`` + ``build_model()``."""

        def _factory() -> LLMModel:
            from trpc_service.config import ModelSettings, build_model

            settings = ModelSettings.from_env(environ)
            return build_model(settings, environ)

        return cls(model_factory=_factory)

    def get_model(self, model_profile: str) -> LLMModel:
        if model_profile != "default":
            raise TenantAgentConfigurationError()
        if self._model is None:
            self._model = self._factory()
        return self._model


__all__ = [
    "DefaultModelProvider",
    "ModelProvider",
]
