# mypy: disable-error-code="import-untyped"
"""Tenant-aware construction of immutable tRPC-Agent graphs."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.filter import BaseFilter
from trpc_agent_sdk.models import LLMModel
from trpc_agent_sdk.tools import BaseTool

from trpc_service.agent.compat import require_sdk_compatibility
from trpc_service.agent.governance import TenantGovernanceFilter
from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import AgentAppSpec
from trpc_service.tool import TenantToolSet


class AgentConfigurationError(ValueError):
    """An immutable app revision cannot produce a safe Agent graph."""


class ModelResolver(Protocol):
    """Resolve one tenant/app revision to an SDK model or dynamic model factory."""

    def __call__(
        self,
        tenant_context: TenantContext,
        app: AgentAppSpec,
    ) -> LLMModel | Callable[..., Any]:
        """Return a model without placing credentials in AgentContext metadata."""


class FilterFactory(Protocol):
    """Create a fresh tenant-policy filter for one Agent graph."""

    def __call__(
        self,
        tenant_context: TenantContext,
        app: AgentAppSpec,
    ) -> BaseFilter:
        """Return a new filter instance without request state from another turn."""


@dataclass(frozen=True, slots=True)
class AgentBuild:
    """One request-safe Agent graph and its internal namespace."""

    tenant_id: str
    app_id: str
    app_revision: int
    app_name: str
    agent: LlmAgent
    tool_set: TenantToolSet


class AgentFactory:
    """Build an Agent only after binding it to a verified TenantContext."""

    def __init__(
        self,
        *,
        model_resolver: ModelResolver,
        registered_tools: Mapping[str, BaseTool | Callable[..., Any]]
        | Iterable[BaseTool | Callable[..., Any]] = (),
        filter_factories: Iterable[FilterFactory] = (),
    ) -> None:
        require_sdk_compatibility()
        self._model_resolver = model_resolver
        self._registered_tools = (
            dict(registered_tools)
            if isinstance(registered_tools, Mapping)
            else tuple(registered_tools)
        )
        self._filter_factories = tuple(filter_factories)

    def build_for_context(
        self,
        *,
        tenant_context: TenantContext,
        app: AgentAppSpec,
        approved_tools: Iterable[str] = (),
    ) -> AgentBuild:
        """Build a fresh graph for one immutable app revision.

        A fresh graph avoids mutable request state leaking through an SDK agent or
        filter instance. Long-lived HTTP clients may still be shared inside the model
        provider, keyed by tenant-safe credential references.
        """

        if tenant_context.app_id != app.app_id:
            raise AgentConfigurationError("TenantContext app_id does not match AgentAppSpec")
        if tenant_context.app_revision != app.revision:
            raise AgentConfigurationError(
                "TenantContext app_revision does not match AgentAppSpec revision"
            )
        if not app.prompt.strip():
            raise AgentConfigurationError("Agent instruction must not be empty")

        tool_set = TenantToolSet(
            tenant_id=tenant_context.tenant_id,
            policy=app.tools,
            registered_tools=self._registered_tools,
            approved_tools=approved_tools,
        )
        resolved_model = self._model_resolver(tenant_context, app)
        if not isinstance(resolved_model, LLMModel) and not callable(resolved_model):
            raise AgentConfigurationError(
                "model_resolver must return LLMModel or an SDK dynamic model factory"
            )
        filters = [TenantGovernanceFilter(tenant_context, app.governance)]
        filters.extend(factory(tenant_context, app) for factory in self._filter_factories)
        if any(not isinstance(item, BaseFilter) for item in filters):
            raise AgentConfigurationError("filter factory must return BaseFilter")

        namespace = _internal_namespace(
            tenant_context.tenant_id,
            app.app_id,
            app.revision,
        )
        agent = LlmAgent(
            name=f"agent_{namespace}",
            description=app.name,
            instruction=app.prompt,
            model=resolved_model,
            tools=[tool_set] if tool_set.tool_names else [],
            filters=filters,
        )
        return AgentBuild(
            tenant_id=tenant_context.tenant_id,
            app_id=app.app_id,
            app_revision=app.revision,
            app_name=f"tenant_app_{namespace}",
            agent=agent,
            tool_set=tool_set,
        )


def _internal_namespace(tenant_id: str, app_id: str, revision: int) -> str:
    """Create a non-reversible SDK-safe namespace from external identifiers."""

    canonical = f"{tenant_id}\x1f{app_id}\x1f{revision}".encode()
    return hashlib.sha256(canonical).hexdigest()[:24]
