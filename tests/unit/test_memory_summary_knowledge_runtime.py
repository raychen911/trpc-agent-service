from __future__ import annotations

from datetime import timedelta
from typing import ClassVar

import pytest
from trpc_agent_sdk.agents import BaseAgent
from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part

from tests.conftest import envelope, repository, tenant_config
from trpc_service.agent.registry import RevisionRegistry
from trpc_service.agent.runner import TenantRunner
from trpc_service.agent.worker import AgentWorker, ProcessStatus
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import SummarySnapshot
from trpc_service.storage.services import (
    PostgresKnowledgeStore,
    PostgresSessionStore,
    TenantDataServices,
)
from trpc_service.tenant.models import TenantContext


class RecordingAgent(BaseAgent):
    seen: ClassVar[list[str]] = []

    async def _run_async_impl(self, ctx: InvocationContext):
        user_content = ctx.user_content
        text = "\n".join(
            part.text or ""
            for part in (user_content.parts if user_content and user_content.parts else [])
        )
        type(self).seen.append(text)
        yield Event(
            invocation_id=ctx.invocation_id,
            author=self.name,
            content=Content(parts=[Part(text="recorded response")]),
        )


class Memory:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []
        self.put_calls: list[dict[str, object]] = []
        self.fail = False

    async def list_recent(self, tenant_id: str, principal_id: str, *, limit: int = 100):
        assert tenant_id == "tenant-a"
        assert principal_id
        return tuple(self.rows[-limit:][::-1])

    async def put(self, tenant_id: str, principal_id: str, value, **kwargs):
        if self.fail:
            raise RuntimeError("memory unavailable")
        self.put_calls.append(
            {"tenant_id": tenant_id, "principal_id": principal_id, "value": dict(value), **kwargs}
        )
        self.rows.append(
            {
                "memory_id": kwargs["memory_id"],
                "memory": dict(value),
                "source_sequence": kwargs["source_sequence"],
            }
        )
        return str(kwargs["memory_id"])


class Summary:
    def __init__(self) -> None:
        self.snapshot: SummarySnapshot | None = None
        self.put_calls: list[dict[str, object]] = []
        self.fail = False

    async def get(self, tenant_id: str, session_id: str):
        assert tenant_id == "tenant-a"
        return self.snapshot

    async def put(self, tenant_id: str, session_id: str, **kwargs):
        if self.fail:
            raise RuntimeError("summary unavailable")
        self.put_calls.append({"tenant_id": tenant_id, "session_id": session_id, **kwargs})
        self.snapshot = SummarySnapshot(
            tenant_id=tenant_id,
            session_id=session_id,
            up_to_sequence=kwargs["up_to_sequence"],
            summary=dict(kwargs["summary"]),
            version=(self.snapshot.version + 1 if self.snapshot else 1),
        )
        return True


class Knowledge:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[float], int]] = []

    async def search(self, tenant_id: str, embedding: list[float], *, limit: int = 5):
        self.calls.append((tenant_id, embedding, limit))
        return (
            {
                "item_id": "item-a",
                "chunk_id": "chunk-a",
                "score": 0.99,
                "metadata": {"text": "tenant-a knowledge"},
            },
        )


class Objects:
    async def stage(self, *_args, **_kwargs):
        raise AssertionError("media should not be staged")


class Audit:
    async def append(self, *_args, **_kwargs):
        return "audit"


def _services(repo, memory, summary, knowledge) -> TenantDataServices:
    return TenantDataServices(
        session=PostgresSessionStore(repo),
        memory=memory,
        summary=summary,
        artifact=Objects(),
        knowledge=knowledge,
        audit=Audit(),
    )


class Factory:
    def __init__(self, services: TenantDataServices) -> None:
        self.services = services

    async def for_context(self, context: TenantContext, config):
        assert context.tenant_id == config.tenant_id
        return self.services


@pytest.mark.asyncio
async def test_second_turn_injects_memory_summary_and_knowledge_with_tenant_scope() -> None:
    RecordingAgent.seen.clear()
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=b"runtime-context" * 3)
    memory = Memory()
    summary = Summary()
    knowledge = Knowledge()
    services = _services(repo, memory, summary, knowledge)

    async def load(_config):
        return RecordingAgent(name="recording-agent")

    worker = AgentWorker(
        repo,
        worker_id="context-worker",
        agent_loader=load,
        service_factory=Factory(services),
        query_embedding_provider=lambda text: [0.25] * 1536,
    )
    first = await runtime.accept("binding-unpredictable-a", envelope(text="first question"))
    assert (await worker.process(first)).status == ProcessStatus.COMMITTED
    second = await runtime.accept(
        "binding-unpredictable-a",
        envelope(message_id="message-2", text="second question"),
    )
    assert (await worker.process(second)).status == ProcessStatus.COMMITTED

    assert len(RecordingAgent.seen) == 2
    second_input = RecordingAgent.seen[-1]
    assert "first question" in second_input
    assert "recorded response" in second_input
    assert "tenant-a knowledge" in second_input
    assert "second question" in second_input
    assert knowledge.calls and knowledge.calls[-1][0] == "tenant-a"
    assert len(knowledge.calls[-1][1]) == 1536
    assert memory.put_calls[-1]["source_sequence"] == summary.put_calls[-1]["up_to_sequence"]


@pytest.mark.asyncio
async def test_post_commit_context_failure_does_not_undo_main_turn() -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=b"post-commit-failure" * 2)
    memory = Memory()
    summary = Summary()
    memory.fail = True
    summary.fail = True
    services = _services(repo, memory, summary, Knowledge())

    async def load(_config):
        return RecordingAgent(name="recording-agent")

    worker = AgentWorker(
        repo,
        worker_id="failure-worker",
        agent_loader=load,
        service_factory=Factory(services),
    )
    accepted = await runtime.accept("binding-unpredictable-a", envelope())
    result = await worker.process(accepted)

    assert result.status == ProcessStatus.COMMITTED
    snapshot = await repo.get_session_snapshot(
        accepted.context.tenant_id,
        accepted.context.session_id,
    )
    assert snapshot is not None and snapshot.events


@pytest.mark.asyncio
async def test_runner_rejects_cross_tenant_context_from_store() -> None:
    """Store calls receive the authenticated tenant, never a client-supplied one."""

    repo = repository()
    runtime = TenantRuntime(repo, routing_key=b"tenant-scope" * 3)
    accepted = await runtime.accept("binding-unpredictable-a", envelope())
    lease = await repo.acquire(
        acceptance=accepted,
        worker_id="runner-worker",
        lease_for=timedelta(seconds=30),
    )
    assert lease is not None
    memory = Memory()
    summary = Summary()
    knowledge = Knowledge()
    services = _services(repo, memory, summary, knowledge)
    calls: list[str] = []

    async def query_memory(tenant_id: str, principal_id: str, *, limit: int = 100):
        calls.append(tenant_id)
        return ()

    memory.list_recent = query_memory  # type: ignore[method-assign]

    async def load(_config):
        return RecordingAgent(name="recording-agent")

    runner = TenantRunner(
        config=tenant_config(),
        lease=lease,
        registry=RevisionRegistry(),
        agent_loader=load,
        services=services,
        query_embedding_provider=lambda _text: [0.1] * 1536,
    )
    [event async for event in runner.run(accepted.context, accepted.envelope)]
    assert calls == [accepted.context.tenant_id]
    assert knowledge.calls[-1][0] == accepted.context.tenant_id


class _Connection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def execute(self, query: str, *args: object):
        self.calls.append((query, args))
        return "OK"

    async def fetch(self, query: str, *args: object):
        self.calls.append((query, args))
        return [
            {
                "item_id": "item-a",
                "chunk_id": "chunk-a",
                "score": 0.75,
                "metadata_json": '{"text":"tenant-a"}',
            }
        ]


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def acquire(self):
        class _Acquire:
            async def __aenter__(_self):
                return self.connection

            async def __aexit__(_self, *_args):
                return None

        return _Acquire()


@pytest.mark.asyncio
async def test_postgres_knowledge_search_is_tenant_scoped_and_ranked() -> None:
    connection = _Connection()
    store = PostgresKnowledgeStore(_Pool(connection), profile_id="profile")
    results = await store.search("tenant-a", [0.0] * 1536, limit=3)

    assert results[0]["item_id"] == "item-a"
    query, args = connection.calls[-1]
    assert "WHERE embedding.tenant_id=$1" in query
    assert "AND item.profile_id=$2" in query
    assert "ORDER BY embedding.embedding <=> $3::vector" in query
    assert args[0] == "tenant-a"
    assert args[1] == "profile"
    assert args[3] == 3
