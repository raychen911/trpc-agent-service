import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from prometheus_client import generate_latest

from trpc_service.gateway import (
    AgentMessageService,
    AgentReply,
    CrossNodeRouter,
    GatewayResponse,
    InMemoryNodeDirectory,
    NodeRecord,
    NormalizedMessage,
    TrpcAgentExecutor,
)
from trpc_service.gateway.router import NoHealthyNodeError
from trpc_service.metrics import PlatformMetrics
from trpc_service.storage.coordinator import TurnCoordinator
from trpc_service.storage.exceptions import DuplicateMessageError
from trpc_service.storage.inmemory import (
    InMemoryConversationStore,
    InMemoryCoordinationStore,
)


def message(external_id: str = "message-1") -> NormalizedMessage:
    return NormalizedMessage(
        tenant_id="tenant-1",
        agent_app_id="app-1",
        channel="telegram",
        account_id="bot-1",
        external_message_id=external_id,
        sender_user_id="user-1",
        conversation_id="chat-1",
        conversation_type="direct",
        text="hello",
        trace_id="trace-1",
    )


class FakeExecutor:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, request: NormalizedMessage) -> AgentReply:
        self.calls += 1
        return AgentReply(
            f"reply: {request.text}",
            "short summary",
            {"count": self.calls},
            input_tokens=7,
            output_tokens=3,
            cost=0.25,
        )


class FakeHandler:
    async def handle(self, request: NormalizedMessage, node_id: str) -> GatewayResponse:
        return GatewayResponse("processed", node_id, request.session_id, request.trace_id, "ok", 1)


def test_message_service_commits_turn_and_rejects_duplicate() -> None:
    async def scenario() -> None:
        data_plane = InMemoryConversationStore()
        coordination = InMemoryCoordinationStore()
        executor = FakeExecutor()
        service = AgentMessageService(executor, TurnCoordinator(data_plane, coordination))

        first = await service.handle(message(), "node-a")
        assert first.reply_text == "reply: hello"
        assert first.session_version == 1
        assert executor.calls == 1

        with pytest.raises(DuplicateMessageError):
            await service.handle(message(), "node-a")
        assert executor.calls == 1

    asyncio.run(scenario())


def test_group_session_is_shared_without_using_sender_as_owner() -> None:
    group_one = replace(
        message(),
        conversation_type="group",
        conversation_id="group-7",
        sender_user_id="member-1",
    )
    group_two = replace(group_one, sender_user_id="member-2")
    assert group_one.session_id == group_two.session_id
    assert group_one.session_user_id == group_two.session_user_id == "group:group-7"


def test_agent_metrics_include_tenant_tokens_and_cost() -> None:
    async def scenario() -> bytes:
        data_plane = InMemoryConversationStore()
        metrics = PlatformMetrics()
        service = AgentMessageService(
            FakeExecutor(),
            TurnCoordinator(data_plane, InMemoryCoordinationStore(), metrics=metrics),
            metrics=metrics,
        )
        await service.handle(message("metrics-1"), "node-a")
        return generate_latest(metrics.registry)

    exported = asyncio.run(scenario())
    assert b"trpc_model_calls_total" in exported
    assert b"trpc_model_tokens_total" in exported
    assert b"trpc_tenant_cost_total" in exported
    assert b'tenant_id="tenant-1"' in exported


def test_trpc_executor_timeout_stops_automatic_replay_boundary() -> None:
    class SlowRunner:
        async def run_async(self, **_kwargs):
            await asyncio.sleep(0.05)
            if False:
                yield None

    class SlowFactory:
        async def build(self, _tenant_id: str, _agent_app_id: str):
            return type(
                "Runtime",
                (),
                {
                    "runner": SlowRunner(),
                    "input_cost_per_million": 0,
                    "output_cost_per_million": 0,
                },
            )()

    async def scenario() -> None:
        executor = TrpcAgentExecutor(SlowFactory(), execution_timeout_seconds=0.01)
        with pytest.raises(RuntimeError, match="recovery review required"):
            await executor.execute(message("timeout-1"))

    asyncio.run(scenario())


def test_node_capacity_limits_concurrent_session_execution() -> None:
    class CountingExecutor:
        def __init__(self) -> None:
            self.active = 0
            self.maximum = 0

        async def execute(self, _request: NormalizedMessage) -> AgentReply:
            self.active += 1
            self.maximum = max(self.maximum, self.active)
            await asyncio.sleep(0.01)
            self.active -= 1
            return AgentReply("ok")

    async def scenario() -> None:
        executor = CountingExecutor()
        metrics = PlatformMetrics()
        service = AgentMessageService(
            executor,
            TurnCoordinator(InMemoryConversationStore(), InMemoryCoordinationStore()),
            metrics=metrics,
            max_concurrent_sessions=1,
        )
        second = replace(
            message("capacity-2"),
            sender_user_id="user-2",
            conversation_id="chat-2",
        )
        await asyncio.gather(
            service.handle(message("capacity-1"), "node-a"),
            service.handle(second, "node-a"),
        )
        assert executor.maximum == 1
        exported = generate_latest(metrics.registry)
        assert b"trpc_active_session_executions 0.0" in exported

    asyncio.run(scenario())


def test_rendezvous_route_is_stable_and_fails_over() -> None:
    now = datetime.now(timezone.utc) + timedelta(minutes=1)
    nodes = (
        NodeRecord("node-a", "http://a", 100, now),
        NodeRecord("node-b", "http://b", 100, now),
        NodeRecord("node-c", "http://c", 100, now),
    )
    selected = CrossNodeRouter.select_node("tenant:app:session", nodes)
    assert CrossNodeRouter.select_node("tenant:app:session", nodes) == selected
    remaining = tuple(node for node in nodes if node != selected)
    assert CrossNodeRouter.select_node("tenant:app:session", remaining) in remaining
    with pytest.raises(NoHealthyNodeError):
        CrossNodeRouter.select_node("key", ())


def test_router_forwards_to_selected_remote_node() -> None:
    async def scenario() -> None:
        directory = InMemoryNodeDirectory()
        await directory.heartbeat("remote", "http://remote", capacity=100, ttl_seconds=30)

        def responder(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/internal/v1/messages"
            assert request.headers["x-internal-token"] == "shared-secret"
            return httpx.Response(
                200,
                json={
                    "status": "processed",
                    "node_id": "remote",
                    "session_id": message().session_id,
                    "trace_id": "trace-1",
                    "reply_text": "remote reply",
                    "session_version": 2,
                },
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(responder))
        router = CrossNodeRouter("local", directory, FakeHandler(), "shared-secret", client=client)
        result = await router.dispatch(message())
        assert result.node_id == "remote"
        assert result.reply_text == "remote reply"
        assert router.verify_internal_token("shared-secret")
        assert not router.verify_internal_token("wrong")
        await client.aclose()

    asyncio.run(scenario())
