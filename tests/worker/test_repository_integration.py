# mypy: disable-error-code="import-untyped"
"""SQLite contract integration between Worker session fencing and SQL authority."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.models import LLMModel, LlmRequest, LlmResponse
from trpc_agent_sdk.types import Content, EventActions, Part

from trpc_service.agent import AgentFactory
from trpc_service.reliability import (
    AuditData,
    InboxEnvelope,
    ReliabilityRepository,
    ReplyPart,
    canonical_json_hash,
)
from trpc_service.security import EnvelopeCipher
from trpc_service.storage.database import Database
from trpc_service.storage.models import AgentApp, ChannelBinding, Tenant, TenantConfigRevision
from trpc_service.tenant.context import ConversationScope, TenantContext
from trpc_service.tenant.models import AgentAppSpec, ModelRoute
from trpc_service.worker import (
    EnvelopeEventCodec,
    FencedSessionService,
    ResolvedTenantTurn,
    RuntimeSessionView,
    TenantAgentExecutorFactory,
    WorkerOrchestrator,
    WorkerOutcome,
)

from .helpers import StaticResolver


class MemoryObjectStore:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def put_if_absent(
        self,
        object_key: str,
        ciphertext: str,
        *,
        tenant_id: str,
        ciphertext_sha256: str,
    ) -> None:
        del tenant_id, ciphertext_sha256
        existing = self.values.setdefault(object_key, ciphertext)
        if existing != ciphertext:
            raise ValueError("content address collision")

    async def get(self, object_key: str, *, tenant_id: str) -> str:
        del tenant_id
        return self.values[object_key]


class DeterministicModel(LLMModel):
    @classmethod
    def supported_models(cls) -> list[str]:
        return [r"fake-.*"]

    async def _generate_async_impl(
        self,
        request: LlmRequest,
        stream: bool = False,
        ctx: InvocationContext | None = None,
    ) -> AsyncGenerator[LlmResponse, None]:
        del stream, ctx
        self.validate_request(request)
        yield LlmResponse(
            content=Content(role="model", parts=[Part.from_text(text="runner answer")]),
            partial=False,
        )


@pytest_asyncio.fixture
async def repository(tmp_path: Path) -> AsyncIterator[ReliabilityRepository]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'worker.db').as_posix()}")
    await database.create_schema()
    async with database.session_factory.begin() as session:
        session.add(
            Tenant(
                tenant_id="tenant-a",
                display_name="Tenant A",
                status="active",
                active_config_revision=1,
                audit_policy={},
                budget_policy={},
            )
        )
        await session.flush()
        session.add(
            TenantConfigRevision(
                tenant_id="tenant-a",
                revision=1,
                schema_version=1,
                status="published",
                spec={"tenant_id": "tenant-a", "revision": 1},
                content_hash="a" * 64,
                created_by="test",
            )
        )
        session.add(
            AgentApp(
                tenant_id="tenant-a",
                app_id="support",
                revision=1,
                status="published",
                agent_name="support_agent",
                prompt="Answer clearly.",
                model_config={},
                tool_policy={},
                storage_config={},
            )
        )
        await session.flush()
        session.add(
            ChannelBinding(
                binding_id="binding-a",
                tenant_id="tenant-a",
                app_id="support",
                app_revision=1,
                config_revision=1,
                channel_type="telegram",
                external_account_id="bot-a",
                callback_path="/v1/channels/telegram/public-a/callback",
                public_callback_id="public-a",
                route_rule={},
                secret_refs={},
                identity_policy={},
                status="active",
            )
        )
    value = ReliabilityRepository(database.session_factory)
    try:
        yield value
    finally:
        await database.dispose()


def envelope(delivery_id: str) -> InboxEnvelope:
    payload = {"delivery_id": delivery_id, "text": "hello"}
    return InboxEnvelope(
        tenant_id="tenant-a",
        binding_id="binding-a",
        session_id="session-a",
        app_id="support",
        app_revision=1,
        config_revision=1,
        scope="private",
        principal_id="principal-a",
        external_delivery_id=delivery_id,
        payload=payload,
        payload_hash=canonical_json_hash(payload),
        request_id=f"request-{delivery_id}",
        trace_id=f"trace-{delivery_id}",
    )


def audit() -> AuditData:
    return AuditData(
        channel="telegram",
        user_id="principal-a",
        agent_name="support_agent",
        decision="allow",
        action="agent.turn.finalize",
        resource="agent:support@1",
        config_revision=1,
        policy_revision=1,
    )


@pytest.mark.asyncio
async def test_real_repository_commits_then_replays_only_encrypted_events(
    repository: ReliabilityRepository,
) -> None:
    await repository.accept_inbox(envelope("delivery-1"))
    claim = await repository.claim_next(
        "tenant-a",
        "worker-a",
        lease_ttl=timedelta(seconds=30),
    )
    assert claim is not None
    view = await repository.load_committed_session(claim)
    store = MemoryObjectStore()
    codec = EnvelopeEventCodec(cipher=EnvelopeCipher(b"w" * 32), store=store)
    service = FencedSessionService(
        port=repository,
        codec=codec,
        claim=claim,
        view=RuntimeSessionView.from_committed(
            view,
            app_name="tenant-app-a",
            user_id="principal-a",
        ),
    )
    session = await service.get_session(
        app_name="tenant-app-a",
        user_id="principal-a",
        session_id="session-a",
    )
    await service.append_event(
        session,
        Event(
            id="sdk-user",
            author="user",
            content=Content(role="user", parts=[Part.from_text(text="private prompt")]),
        ),
    )
    await service.append_event(
        session,
        Event(
            id="sdk-model",
            author="support_agent",
            content=Content(role="model", parts=[Part.from_text(text="private answer")]),
            actions=EventActions(state_delta={"topic": "done"}),
            turn_complete=True,
        ),
    )
    await repository.finalize_run(
        claim,
        final_state=service.final_session_state,
        final_event_id=service.last_event_id,
        reply_parts=(
            ReplyPart(
                "reply-1",
                0,
                {"schema_version": 1, "kind": "text", "text": "private answer"},
            ),
        ),
        audit=audit(),
    )

    assert all("private" not in key for key in store.values)
    assert all("private" not in value for value in store.values.values())
    await repository.accept_inbox(envelope("delivery-2"))
    next_claim = await repository.claim_next("tenant-a", "worker-b")
    assert next_claim is not None
    committed = await repository.load_committed_session(next_claim)
    assert committed.state == {"topic": "done"}
    assert all(
        event.content_ref and event.content_ref.startswith("evt+enc://")
        for event in committed.events
    )

    restored_service = FencedSessionService(
        port=repository,
        codec=codec,
        claim=next_claim,
        view=RuntimeSessionView.from_committed(
            committed,
            app_name="tenant-app-a",
            user_id="principal-a",
        ),
    )
    restored = await restored_service.get_session(
        app_name="tenant-app-a",
        user_id="principal-a",
        session_id="session-a",
    )
    assert [event.get_text() for event in restored.events] == [
        "private prompt",
        "private answer",
    ]


@pytest.mark.asyncio
async def test_real_runner_and_repository_complete_the_worker_main_chain(
    repository: ReliabilityRepository,
) -> None:
    await repository.accept_inbox(envelope("runner-delivery"))
    store = MemoryObjectStore()
    codec = EnvelopeEventCodec(cipher=EnvelopeCipher(b"r" * 32), store=store)
    app = AgentAppSpec(
        app_id="support",
        revision=1,
        name="support_agent",
        prompt="Answer clearly.",
        model=ModelRoute(
            provider="fake",
            model="fake-deterministic",
            timeout_seconds=2,
            token_ceiling=512,
        ),
    )
    context = TenantContext(
        tenant_id="tenant-a",
        app_id="support",
        app_revision=1,
        binding_id="binding-a",
        binding_revision=1,
        principal_id="principal-a",
        session_id="session-a",
        scope=ConversationScope.PRIVATE,
        request_id="request-runner-delivery",
        trace_id="trace-runner-delivery",
    )
    resolved = ResolvedTenantTurn(
        tenant_context=context,
        app=app,
        channel="telegram",
        config_revision=1,
        policy_revision=1,
    )
    orchestrator = WorkerOrchestrator(
        port=repository,
        event_codec=codec,
        tenant_resolver=StaticResolver(resolved),
        executor_factory=TenantAgentExecutorFactory(
            agent_factory=AgentFactory(
                model_resolver=lambda tenant_context, app_spec: DeterministicModel(
                    model_name="fake-deterministic"
                )
            )
        ),
        lease_ttl=timedelta(seconds=10),
        heartbeat_interval=timedelta(seconds=1),
    )
    result = await orchestrator.run_once(tenant_id="tenant-a", worker_id="worker-a")
    assert result.outcome is WorkerOutcome.SUCCEEDED

    await repository.accept_inbox(envelope("runner-delivery-2"))
    replay_claim = await repository.claim_next("tenant-a", "worker-b")
    assert replay_claim is not None
    committed = await repository.load_committed_session(replay_claim)
    assert [event.event_type for event in committed.events] == ["user", "assistant"]
    assert all(
        event.content_ref and event.content_ref.startswith("evt+enc://")
        for event in committed.events
    )
