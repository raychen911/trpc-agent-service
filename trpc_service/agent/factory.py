"""Build public tRPC-Agent model/agent objects from immutable tenant config."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncGenerator, Callable, Collection
from typing import Any, cast

from trpc_agent_sdk.agents import BaseAgent, LlmAgent
from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.models import (
    AnthropicModel,
    LiteLLMModel,
    LLMModel,
    LlmRequest,
    LlmResponse,
    OpenAIModel,
)
from trpc_agent_sdk.planners import BuiltInPlanner
from trpc_agent_sdk.tools import BaseTool
from trpc_agent_sdk.types import GenerateContentConfig, HttpOptions, ThinkingConfig

from trpc_service.agent.fake import DeterministicAgent, DeterministicToolCallModel
from trpc_service.config.secrets import SecretProvider, SecretRef
from trpc_service.config.settings import Environment
from trpc_service.tenant.models import ModelPolicy, TenantConfig, validate_model_base_url
from trpc_service.tool.execution import ToolExecutor
from trpc_service.tool.governance import GovernancePipeline
from trpc_service.tool.integration import GovernedTool
from trpc_service.tool.test_tool import DETERMINISTIC_FAULT_TOOL_NAME


class FallbackModel(LLMModel):
    """Fallback only before the primary emits a usable response."""

    def __init__(self, primary: LLMModel, fallback: LLMModel) -> None:
        super().__init__(primary.name)
        self._primary = primary
        self._fallback = fallback

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r".*"]

    def validate_request(self, request: LlmRequest) -> None:
        self._primary.validate_request(request)

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx: InvocationContext | None = None,
    ) -> AsyncGenerator[LlmResponse, None]:
        emitted = False
        try:
            async for response in self._primary.generate_async(request, stream=stream, ctx=ctx):
                if response.error_code and not emitted:
                    break
                emitted = True
                yield response
            else:
                return
        except Exception:
            if emitted:
                raise
        async for response in self._fallback.generate_async(request, stream=stream, ctx=ctx):
            yield response


class ProductionAgentLoader:
    def __init__(
        self,
        secrets: SecretProvider,
        *,
        tools: dict[str, Any] | None = None,
        governance: GovernancePipeline | None = None,
        tool_executor: ToolExecutor | None = None,
        allowed_model_hosts: Collection[str] | None = None,
    ) -> None:
        self._secrets = secrets
        self._tools = dict(tools or {})
        self._governance = governance
        self._tool_executor = tool_executor
        self._allowed_model_hosts = (
            frozenset(host.lower().rstrip(".") for host in allowed_model_hosts)
            if allowed_model_hosts is not None
            else None
        )

    async def __call__(self, config: TenantConfig) -> BaseAgent:
        return self._build_agent(config, self._model(config.model))

    def _build_agent(self, config: TenantConfig, model: LLMModel) -> LlmAgent:
        selected_tools = self._selected_tools(config)
        digest = hashlib.sha256(
            f"{config.tenant_id}:{config.app_id}:{config.version}".encode()
        ).hexdigest()[:12]
        agent_kwargs: dict[str, Any] = {}
        thinking_budget = config.model.thinking_budget_tokens
        if thinking_budget not in {None, 0}:
            agent_kwargs["planner"] = BuiltInPlanner(
                thinking_config=ThinkingConfig(
                    include_thoughts=True,
                    thinking_budget=thinking_budget,
                )
            )
        generation_kwargs: dict[str, Any] = {
            "max_output_tokens": config.budget.max_tokens_per_turn,
            "http_options": HttpOptions(timeout=max(1, round(config.model.timeout_seconds * 1000))),
        }
        if config.model.temperature is not None:
            generation_kwargs["temperature"] = config.model.temperature
        if config.model.top_p is not None:
            generation_kwargs["top_p"] = config.model.top_p
        if config.model.stop_sequences:
            generation_kwargs["stop_sequences"] = list(config.model.stop_sequences)
        return LlmAgent(
            name=f"tenant_agent_{digest}",
            description=f"Tenant agent revision {config.version}",
            model=model,
            instruction=config.instructions,
            tools=selected_tools,
            generate_content_config=GenerateContentConfig(**generation_kwargs),
            **agent_kwargs,
        )

    def _selected_tools(self, config: TenantConfig) -> list[BaseTool]:
        selected = [self._tools[name] for name in sorted(config.tools.allow) if name in self._tools]
        if not selected:
            return []
        if self._governance is None or self._tool_executor is None:
            raise ValueError("configured tools require governance and execution ledger")
        if not all(isinstance(tool, BaseTool) for tool in selected):
            raise TypeError("production tools must implement the tRPC-Agent BaseTool API")
        return [
            GovernedTool(
                tool,
                config=config,
                governance=self._governance,
                executor=self._tool_executor,
            )
            for tool in selected
        ]

    def _model(self, policy: ModelPolicy) -> LLMModel:
        api_key = self._resolve_model_secret(policy.api_key_ref) if policy.api_key_ref else ""
        kwargs: dict[str, Any] = {"api_key": api_key}
        if policy.base_url:
            # An explicit literal key is available only to offline/test
            # providers, where a synthetic endpoint is useful for contract
            # tests.  Every tenant-resolvable key follows the production
            # endpoint allow-list and fails closed when it is absent.
            offline_literal = policy.api_key_ref is not None and policy.api_key_ref.uri.startswith(
                "literal://"
            )
            if not self._allowed_model_hosts and not offline_literal:
                raise ValueError("approved model endpoint allowlist is required")
            validate_model_base_url(policy.base_url, allowed_hosts=self._allowed_model_hosts)
            kwargs["base_url"] = policy.base_url
        provider = policy.provider.lower()
        if provider == "openai":
            if policy.reasoning_effort is not None:
                kwargs["use_responses_api"] = True
                kwargs["responses_api_params"] = {"reasoning": {"effort": policy.reasoning_effort}}
            primary: LLMModel = OpenAIModel(model_name=policy.model, **kwargs)
        elif provider == "anthropic":
            primary = AnthropicModel(model_name=policy.model, **kwargs)
        elif provider == "litellm":
            primary = LiteLLMModel(model_name=policy.model, **kwargs)
        else:
            raise ValueError(f"unsupported model provider: {provider}")
        if not policy.fallback_model:
            return primary
        fallback_policy = policy.model_copy(
            update={"model": policy.fallback_model, "fallback_model": None}
        )
        return FallbackModel(primary, self._model(fallback_policy))

    def _resolve_model_secret(self, ref: SecretRef) -> str:
        resolver = getattr(self._secrets, "resolve_tenant", None)
        # Literal references remain available only to explicit offline/test
        # providers.  Environment and mounted-file references must pass the
        # tenant-scoped resolver so a tenant cannot read arbitrary process data.
        if not ref.uri.startswith("literal://"):
            if resolver is None:
                raise ValueError("tenant-scoped secret resolver is required")
            return cast(Callable[[SecretRef], str], resolver)(ref)
        return self._secrets.resolve(ref)


class DevelopmentAgentLoader:
    """Permit an offline deterministic agent only in explicitly non-production wiring."""

    def __init__(
        self,
        production: ProductionAgentLoader,
        *,
        delay_seconds: float = 0.05,
        environment: Environment = Environment.DEVELOPMENT,
        fault_injection_enabled: bool = False,
        deterministic_tool_call: bool = False,
    ) -> None:
        if fault_injection_enabled and environment != Environment.TEST:
            raise ValueError("fault-stage test tools are restricted to test environment")
        if deterministic_tool_call and not (
            environment == Environment.TEST and fault_injection_enabled
        ):
            raise ValueError("deterministic tool calls require test fault injection")
        self._production = production
        self._delay_seconds = delay_seconds
        self._deterministic_tool_call = deterministic_tool_call

    async def __call__(self, config: TenantConfig) -> BaseAgent:
        if self._deterministic_tool_call:
            selected_tools = self._production._selected_tools(config)
            if any(tool.name == DETERMINISTIC_FAULT_TOOL_NAME for tool in selected_tools):
                return self._production._build_agent(
                    config,
                    DeterministicToolCallModel(
                        DETERMINISTIC_FAULT_TOOL_NAME,
                        delay_seconds=self._delay_seconds,
                    ),
                )
            return DeterministicAgent(
                name=_offline_agent_name(config),
                response="offline deterministic response",
                delay_seconds=self._delay_seconds,
            )
        if config.model.provider.lower() != "offline":
            return await self._production(config)
        return DeterministicAgent(
            name=_offline_agent_name(config),
            response="offline deterministic response",
            delay_seconds=self._delay_seconds,
        )


def _offline_agent_name(config: TenantConfig) -> str:
    digest = hashlib.sha256(
        f"{config.tenant_id}:{config.app_id}:{config.version}".encode()
    ).hexdigest()[:12]
    return f"offline_agent_{digest}"


__all__ = ["DevelopmentAgentLoader", "FallbackModel", "ProductionAgentLoader"]
