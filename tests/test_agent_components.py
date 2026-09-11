"""Tests for ModelProvider, AllowedToolRegistry, and TenantAgentConfigurationError."""

from __future__ import annotations

import asyncio

import pytest
from trpc_agent_sdk.models import LLMModel
from trpc_agent_sdk.tools import BaseTool

from trpc_service.agent.errors import TenantAgentConfigurationError

# ---------------------------------------------------------------------------
# TenantAgentConfigurationError
# ---------------------------------------------------------------------------


def test_configuration_error_is_value_error():
    assert issubclass(TenantAgentConfigurationError, ValueError)


def test_configuration_error_has_fixed_message():
    err = TenantAgentConfigurationError()
    assert str(err) == "Tenant agent configuration is not available."
    assert "profile" not in str(err).lower()
    assert "tool" not in str(err).lower()


# ---------------------------------------------------------------------------
# DefaultModelProvider
# ---------------------------------------------------------------------------


class _FakeModel(LLMModel):

    def __init__(self, name: str = "fake"):
        super().__init__(model_name=name)

    @classmethod
    def supported_models(cls):
        return [r".*"]

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        yield  # pragma: no cover


class _CountingFactory:

    def __init__(self, model=None, error=None):
        self._model = model or _FakeModel()
        self._error = error
        self.call_count = 0

    def __call__(self):
        self.call_count += 1
        if self._error:
            raise self._error
        return self._model


def test_default_provider_lazy_creation():
    from trpc_service.agent.model_provider import DefaultModelProvider

    factory = _CountingFactory()
    provider = DefaultModelProvider(model_factory=factory)
    assert factory.call_count == 0

    model = provider.get_model("default")
    assert factory.call_count == 1
    assert isinstance(model, LLMModel)


def test_default_provider_caches_model():
    from trpc_service.agent.model_provider import DefaultModelProvider

    factory = _CountingFactory()
    provider = DefaultModelProvider(model_factory=factory)
    first = provider.get_model("default")
    second = provider.get_model("default")
    assert first is second
    assert factory.call_count == 1


def test_default_provider_unknown_profile_raises():
    from trpc_service.agent.model_provider import DefaultModelProvider

    factory = _CountingFactory()
    provider = DefaultModelProvider(model_factory=factory)
    with pytest.raises(TenantAgentConfigurationError):
        provider.get_model("nonexistent")
    assert factory.call_count == 0


def test_default_provider_factory_failure_not_cached():
    from trpc_service.agent.model_provider import DefaultModelProvider

    factory = _CountingFactory(error=RuntimeError("boom"))
    provider = DefaultModelProvider(model_factory=factory)
    with pytest.raises(RuntimeError, match="boom"):
        provider.get_model("default")
    assert factory.call_count == 1

    factory._error = None
    model = provider.get_model("default")
    assert isinstance(model, LLMModel)
    assert factory.call_count == 2


def test_default_provider_from_env_defers_creation():
    from trpc_service.agent.model_provider import DefaultModelProvider

    provider = DefaultModelProvider.from_env(
        environ={
            "TRPC_MODEL_PROVIDER": "openai-compatible",
            "TRPC_MODEL_NAME": "mimo-v2.5",
            "TRPC_MODEL_BASE_URL": "http://localhost:1/v1",
            "TRPC_MODEL_API_KEY": "test-key",
        })
    assert isinstance(provider, DefaultModelProvider)


def test_default_provider_from_env_creates_real_model_on_get():
    from trpc_agent_sdk.models import OpenAIModel
    from trpc_service.agent.model_provider import DefaultModelProvider

    provider = DefaultModelProvider.from_env(
        environ={
            "TRPC_MODEL_PROVIDER": "openai-compatible",
            "TRPC_MODEL_NAME": "test-model",
            "TRPC_MODEL_BASE_URL": "http://localhost:1/v1",
            "TRPC_MODEL_API_KEY": "test-key",
        })
    model = provider.get_model("default")
    assert isinstance(model, OpenAIModel)
    assert model.name == "test-model"


# ---------------------------------------------------------------------------
# AllowedToolRegistry
# ---------------------------------------------------------------------------


def test_default_registry_builds_get_current_time():
    from trpc_service.agent.tool_registry import AllowedToolRegistry

    registry = AllowedToolRegistry.default()
    tools = registry.build_tools(("get_current_time", ), {})
    assert len(tools) == 1
    assert isinstance(tools[0], BaseTool)
    assert tools[0].name == "get_current_time"


def test_default_registry_empty_tools():
    from trpc_service.agent.tool_registry import AllowedToolRegistry

    registry = AllowedToolRegistry.default()
    tools = registry.build_tools((), {})
    assert tools == []


def test_default_registry_unknown_tool_raises():
    from trpc_service.agent.tool_registry import AllowedToolRegistry

    registry = AllowedToolRegistry.default()
    with pytest.raises(TenantAgentConfigurationError):
        registry.build_tools(("nonexistent_tool", ), {})


def test_knowledge_search_requires_tenant_bound_knowledge_base():
    from trpc_service.agent.tool_registry import AllowedToolRegistry

    registry = AllowedToolRegistry.default()
    with pytest.raises(TenantAgentConfigurationError):
        registry.build_tools(("knowledge_search", ), {})


def test_knowledge_search_is_a_tenant_private_allowlisted_tool():
    from trpc_agent_sdk.knowledge import KnowledgeBase
    from trpc_agent_sdk.server.knowledge.tools import LangchainKnowledgeSearchTool

    from trpc_service.agent.tool_registry import AllowedToolRegistry

    class TenantKnowledge(KnowledgeBase):

        async def search(self, ctx, req):  # pragma: no cover - construction contract only
            raise AssertionError("search is not invoked during construction")

    knowledge = TenantKnowledge()
    tool = AllowedToolRegistry.default().build_tools(
        ("knowledge_search", ),
        {"knowledge_search": "allow"},
        knowledge_base=knowledge,
    )[0]
    assert isinstance(tool, LangchainKnowledgeSearchTool)
    assert tool.rag is knowledge


def test_knowledge_search_review_policy_fails_closed():
    from trpc_agent_sdk.knowledge import KnowledgeBase

    from trpc_service.agent.tool_registry import AllowedToolRegistry

    class TenantKnowledge(KnowledgeBase):

        async def search(self, ctx, req):  # pragma: no cover - validation contract only
            raise AssertionError("search is not invoked during validation")

    with pytest.raises(TenantAgentConfigurationError):
        AllowedToolRegistry.default().build_tools(
            ("knowledge_search", ),
            {"knowledge_search": "review"},
            knowledge_base=TenantKnowledge(),
        )


def test_registry_builds_in_configured_order():
    from trpc_service.agent.tool_registry import AllowedToolRegistry

    call_log = []

    def factory_a(filters, long_running):
        call_log.append("a")

        class T(BaseTool):

            def __init__(self):
                super().__init__(name="a", description="")

            async def _run_async_impl(self, *args, **kwargs):
                yield  # pragma: no cover

        return T()

    def factory_b(filters, long_running):
        call_log.append("b")

        class T(BaseTool):

            def __init__(self):
                super().__init__(name="b", description="")

            async def _run_async_impl(self, *args, **kwargs):
                yield  # pragma: no cover

        return T()

    registry = AllowedToolRegistry(factories={"a": factory_a, "b": factory_b})
    tools = registry.build_tools(("b", "a"), {})
    assert [t.name for t in tools] == ["b", "a"]
    assert call_log == ["b", "a"]


def test_registry_produces_independent_objects():
    from trpc_service.agent.tool_registry import AllowedToolRegistry

    registry = AllowedToolRegistry.default()
    first = registry.build_tools(("get_current_time", ), {})
    second = registry.build_tools(("get_current_time", ), {})
    assert first[0] is not second[0]


def test_registry_error_does_not_leak_tool_name():
    from trpc_service.agent.tool_registry import AllowedToolRegistry

    registry = AllowedToolRegistry.default()
    with pytest.raises(TenantAgentConfigurationError) as exc_info:
        registry.build_tools(("secret_tool_name", ), {})
    assert "secret_tool_name" not in str(exc_info.value)


# ── Stage 6A2: registry review tooling (RED) ────────────────────────────────


class TestRegistryReviewTools:

    def test_default_registry_accepts_originals_and_builds_long_running_for_review(self):
        from trpc_agent_sdk.tools import LongRunningFunctionTool

        from trpc_service.agent.tool_registry import AllowedToolRegistry
        from trpc_service.config.tenant import TenantGovernanceConfig
        from trpc_service.governance.content_policy import ContentPolicyConfig

        registry = AllowedToolRegistry.default()
        tools = registry.build_tools(
            ("get_current_time", ),
            TenantGovernanceConfig(
                allowed_channels=("web", ),
                allowed_user_ids=(),
                tool_decisions={
                    "get_current_time": "review"
                },
                content_policy=ContentPolicyConfig(),
                limits=None,
            ).tool_decisions,
        )
        assert isinstance(tools[0], LongRunningFunctionTool)
        assert tools[0].is_long_running is True

    def test_registry_still_rejects_unknown_tools_and_bad_decisions(self):
        from trpc_service.agent.errors import TenantAgentConfigurationError
        from trpc_service.agent.tool_registry import AllowedToolRegistry

        registry = AllowedToolRegistry.default()
        with pytest.raises(TenantAgentConfigurationError):
            registry.build_tools(("ghost", ), {})
        with pytest.raises(TenantAgentConfigurationError):
            registry.build_tools(("get_current_time", ), {"get_current_time": "maybe"})

    def test_execute_approved_only_from_originals(self):
        from trpc_service.agent.errors import TenantAgentConfigurationError
        from trpc_service.agent.tool_registry import AllowedToolRegistry

        registry = AllowedToolRegistry.default()
        result = asyncio.run(registry.execute_approved("get_current_time", {}))
        assert "T" in result and "+" in result  # ISO-8601 tz-aware string
        with pytest.raises(TenantAgentConfigurationError):
            asyncio.run(registry.execute_approved("not_a_tool", {}))
        with pytest.raises(TenantAgentConfigurationError):
            asyncio.run(registry.execute_approved("get_current_time", "not-a-dict"))
