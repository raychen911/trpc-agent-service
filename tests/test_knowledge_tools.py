import json
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from trpc_service.agent.contracts import (
    AgentExecutionClaim,
    AgentExecutionContext,
    AgentExecutionRequest,
    AgentRuntimeConfig,
    AgentToolCall,
    AgentToolKind,
    AgentToolResult,
    PolicyAction,
    PolicyDecision,
)
from trpc_service.agent.models import AgentApp
from trpc_service.channels.contracts import ChannelBindingConfig, IncomingMessage, MessageKind
from trpc_service.channels.media import ChannelMediaStore
from trpc_service.storage.adapters.inmemory import build_inmemory_backend
from trpc_service.storage.database import build_session_factory
from trpc_service.storage.knowledge import TenantKnowledgeService
from trpc_service.storage.orm import Base
from trpc_service.storage.registry import StorageBackendRegistry
from trpc_service.storage.router import StorageRouter
from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import Tenant
from trpc_service.tool import CompositeToolInvoker, KnowledgeToolInvoker


async def _content(payload: bytes) -> AsyncIterator[bytes]:
    """Yield one upload body through the public streaming storage contract."""

    yield payload


@pytest.mark.anyio
async def test_im_upload_uses_the_selected_config_version_artifact_backend() -> None:
    """Ingress and later RAG ingestion resolve the same tenant Backend Profile."""

    default_backend = build_inmemory_backend("default")
    selected_backend = build_inmemory_backend("selected")
    assert default_backend.artifact is not None
    assert selected_backend.artifact is not None
    registry = StorageBackendRegistry()
    registry.register(default_backend)
    registry.register(selected_backend)

    class SelectedProfile:

        async def load_backends(self, context):  # type: ignore[no-untyped-def]
            assert context.config_version == 7
            return {
                "session": "selected",
                "knowledge": "selected",
                "artifact": "selected",
            }

    tenant = TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=7,
        request_id="routed-upload",
        trace_id="routed-upload",
    )
    binding = ChannelBindingConfig(
        binding_id=uuid4(),
        tenant_id=tenant.tenant_id,
        agent_app_id=tenant.agent_app_id,
        channel_type="feishu",
    )
    media = ChannelMediaStore(
        default_backend.artifact,
        storage_router=StorageRouter(registry),
        backend_profiles=SelectedProfile(),
    )
    artifact_id = await media.put(
        binding,
        principal_id="employee-1",
        message_id="routed-message",
        filename="policy.md",
        media_type="text/markdown",
        content=b"selected profile",
        context=tenant,
    )

    selected_content = b"".join(
        [block async for block in selected_backend.artifact.open(tenant, artifact_id)])
    assert selected_content == b"selected profile"
    with pytest.raises(LookupError):
        _ = [block async for block in default_backend.artifact.open(tenant, artifact_id)]


@pytest.mark.anyio
async def test_knowledge_tools_execute_the_tenant_scoped_public_workflow(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tools.db'}")
    sessions = build_session_factory(engine)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    tenant_id = uuid4()
    agent_id = uuid4()
    async with sessions.begin() as database:
        database.add(Tenant(tenant_id=tenant_id, name="Knowledge Tool Tenant"))
        await database.flush()
        database.add(AgentApp(agent_app_id=agent_id, tenant_id=tenant_id, name="Knowledge Agent"))
    backend = build_inmemory_backend()
    assert backend.knowledge is not None
    assert backend.artifact is not None
    service = TenantKnowledgeService(
        sessions,
        knowledge=backend.knowledge,
        artifacts=backend.artifact,
    )
    tenant = TenantContext(
        tenant_id=tenant_id,
        agent_app_id=agent_id,
        config_version=1,
        request_id="knowledge-tool-request",
        trace_id="knowledge-tool-trace",
    )
    binding = ChannelBindingConfig(
        binding_id=uuid4(),
        tenant_id=tenant_id,
        agent_app_id=agent_id,
        channel_type="feishu",
    )
    upload = await service.upload(
        tenant,
        principal_id="employee-1",
        filename="benefits.md",
        media_type="text/markdown",
        content=_content("餐补标准为每天二十元。".encode()),
    )
    artifact_id = upload.artifact_id
    context = AgentExecutionContext(
        request=AgentExecutionRequest(
            tenant=tenant,
            session_id="knowledge-session",
            incoming=IncomingMessage(
                external_message_id="knowledge-message",
                principal_id="employee-1",
                conversation_id="knowledge-conversation",
                kind=MessageKind.TEXT,
                occurred_at=datetime.now(timezone.utc),
                text="把附件加入 handbook",
                artifact_refs=(artifact_id, ),
            ),
            channel=binding,
        ),
        config=AgentRuntimeConfig(
            config_version=1,
            runner_name="trpc_agent",
            knowledge={"knowledge_base_names": ["handbook"]},
        ),
        policy=PolicyDecision(action=PolicyAction.ALLOW),
        claim=AgentExecutionClaim(claim_id="knowledge-claim"),
    )
    knowledge_tools = KnowledgeToolInvoker(service)
    invoker = CompositeToolInvoker(
        {name: knowledge_tools
         for name in KnowledgeToolInvoker.TOOL_NAMES})

    async def invoke(name: str, arguments: dict[str, object]) -> AgentToolResult:
        return await invoker.invoke(
            context,
            AgentToolCall(
                call_id=f"knowledge-tool-request:{name}",
                name=name,
                kind=AgentToolKind.TOOL,
                logical_call_index=0,
                resource="handbook",
                arguments={
                    "knowledge_base_name": "handbook",
                    **arguments,
                },
            ),
        )

    added = await service.ingest(
        tenant,
        context.config.knowledge,
        knowledge_base_name="handbook",
        artifact_ids=(artifact_id, ),
    )
    listed = json.loads((await invoke("knowledge.list", {})).content or "null")
    found = json.loads((await invoke("knowledge.search", {"query": "餐补"})).content or "null")
    assert added[0].status == "READY"
    assert listed[0]["filename"] == "benefits.md"
    assert found[0]["source"]["filename"] == "benefits.md"
    for mutation in ("knowledge.add", "knowledge.update", "knowledge.delete"):
        with pytest.raises(PermissionError, match="not registered"):
            await invoke(mutation, {})
    with pytest.raises(PermissionError, match="not registered"):
        await invoke("knowledge.unknown", {})
    await engine.dispose()
