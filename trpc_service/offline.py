"""Explicit no-network SDK model and runtime factory for demos/tests only."""

from __future__ import annotations

import asyncio

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.models import LLMModel
from trpc_agent_sdk.models import LlmResponse
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part

from trpc_service.agent.runtime import TenantRuntime
from trpc_service.gateway.identity import sdk_app_name
from trpc_service.storage import StorageProviderFactory
from trpc_service.config import StoragePolicy
from trpc_service.tenant.filters import TenantBoundaryAgentFilter
from trpc_service.agent.post_turn import configure_summary
from trpc_service.agent.model_observer import instrument_model_call_accounting


class OfflineModel(LLMModel):
    """Exercises the real SDK Agent/Runner lifecycle without HTTP/model charges."""

    calls: int = 0

    @classmethod
    def supported_models(cls):
        return [r"offline-.*"]

    def validate_request(self, request):
        return None

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        self.calls += 1
        users = [content for content in request.contents if content.role == "user"]
        text = "".join(part.text or "" for part in users[-1].parts) if users else ""
        yield LlmResponse(
            content=Content(role="model", parts=[Part.from_text(text=f"echo:{text}; user_turns={len(users)}")]))


class OfflineRuntimeFactory:
    """Offline model with InMemory default or explicitly injected test storage."""

    def __init__(self, storage=None, artifact_store=None) -> None:
        self.storage = storage or StorageProviderFactory().create(StoragePolicy())
        self.artifact_store = artifact_store
        self.models: list[OfflineModel] = []

    async def create(self, tenant, app):
        model = OfflineModel(model_name="offline-echo")
        instrument_model_call_accounting(model)
        self.models.append(model)
        agent = LlmAgent(name="offline_assistant",
                         model=model,
                         instruction=app.instruction,
                         filters=[TenantBoundaryAgentFilter(tenant.tenant_id, app.app_id)])
        runner = Runner(app_name=sdk_app_name(tenant.tenant_id, app.app_id),
                        agent=agent,
                        session_service=self.storage.session_service,
                        memory_service=self.storage.memory_service,
                        enable_post_turn_processing=False,
                        defer_post_turn_processing=False)
        configure_summary(runner, app.runtime, OfflineModel(model_name="offline-summary"))
        return TenantRuntime(tenant,
                             app,
                             runner,
                             asyncio.Semaphore(20),
                             artifact_store=self.artifact_store,
                             write_guards=self.storage.write_guards)


class DemoRuntimeFactory:
    """Select the offline SDK model only for explicitly named demo apps."""

    def __init__(self, configured_factory, offline_factory, offline_app_ids=("assistant-offline", )):
        self.configured_factory = configured_factory
        self.offline_factory = offline_factory
        self.offline_app_ids = set(offline_app_ids)

    async def create(self, tenant, app):
        factory = self.offline_factory if app.app_id in self.offline_app_ids else self.configured_factory
        return await factory.create(tenant, app)

    async def validate(self, tenant):
        for app in tenant.apps.values():
            if app.app_id not in self.offline_app_ids:
                single = tenant.model_copy(deep=True)
                single.apps = {app.app_id: app}
                single.channels = [binding for binding in single.channels if binding.app_id == app.app_id]
                await self.configured_factory.validate(single)
