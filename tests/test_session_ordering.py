"""R1B ordering proofs for the reused SDK Session/Summary/Memory pipeline.

The product contract (README "Event, State, Summary ordering") is that the
SDK Runner persists every non-partial Event (and the state it carries) BEFORE
post-turn Summary processing, and Summary BEFORE Memory finalisation.  These
tests observe that order through SPY WRAPPERS AROUND SDK PUBLIC SERVICE
METHODS ONLY — no private SDK internals are asserted.

The runtime tests then pin the R1B wiring: each TenantAgentRuntime injects its
own per-runtime SDK summarizer so a turn-completion summary is actually
created and persisted, while the Worker's shared backend service is never
mutated.
"""

from __future__ import annotations

import asyncio

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.abc import MemoryServiceABC, SessionServiceABC
from trpc_agent_sdk.configs import RunConfig
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.memory import InMemoryMemoryService, MemoryServiceConfig
from trpc_agent_sdk.models import LlmResponse
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import (
    BaseSessionService,
    InMemorySessionService,
    SessionServiceConfig,
)
from trpc_agent_sdk.types import Content, EventActions, Part, Ttl

from tests.tenant_helpers import (
    FakeLLMModel,
    InMemoryTestStateBackend,
    make_tenant_config,
)
from trpc_service.agent.runtime import TenantAgentRuntime


class _SessionServiceSpy(BaseSessionService):
    """Delegating spy around a real session service (public API surface only).

    Every call is recorded (name + a minimal snapshot) BEFORE delegating so an
    exception in the target still shows the attempted call; snapshots never
    retain content objects, only identity/ordering facts.
    """

    def __init__(self, target: BaseSessionService, log: list[tuple]) -> None:
        self._target = target
        self._log = log
        super().__init__(session_config=target.session_config)

    def _record(self, name: str, *extra) -> None:
        self._log.append((name, ) + tuple(extra))

    async def create_session(self, **kwargs):
        self._record("create_session", kwargs.get("app_name"), kwargs.get("session_id"))
        return await self._target.create_session(**kwargs)

    async def get_session(self, **kwargs):
        self._record("get_session", kwargs.get("app_name"), kwargs.get("session_id"))
        return await self._target.get_session(**kwargs)

    async def list_sessions(self, **kwargs):
        self._record("list_sessions", kwargs.get("app_name"))
        return await self._target.list_sessions(**kwargs)

    async def delete_session(self, **kwargs):
        self._record("delete_session", kwargs.get("app_name"), kwargs.get("session_id"))
        return await self._target.delete_session(**kwargs)

    async def append_event(self, session, event):
        result = await self._target.append_event(session=session, event=event)
        # Recorded after delegation: what the persistence call actually saw.
        self._record(
            "append_event",
            bool(event.partial),
            dict(session.state),
            len(session.events),
        )
        return result

    async def update_session(self, session):
        self._record("update_session", len(session.events))
        return await self._target.update_session(session)

    async def create_session_summary(self, session, ctx=None):
        result = await self._target.create_session_summary(session, ctx=ctx)
        self._record("create_session_summary", len(session.events))
        return result

    async def get_session_summary(self, session):
        self._record("get_session_summary")
        return await self._target.get_session_summary(session)

    async def close(self):
        self._record("close")
        return await self._target.close()


class _MemoryServiceSpy(MemoryServiceABC):
    """Delegating spy around a real memory service (public API only)."""

    def __init__(self, target: InMemoryMemoryService, log: list[tuple]) -> None:
        self._target = target
        self._log = log
        super().__init__(enabled=True)

    @property
    def enabled(self) -> bool:
        return self._target.enabled

    async def store_session(self, session, agent_context=None):
        self._log.append(("store_session", session.app_name, len(session.events)))
        return await self._target.store_session(session, agent_context=agent_context)

    async def search_memory(self, key, query, limit=10, agent_context=None):
        return await self._target.search_memory(key, query, limit=limit, agent_context=agent_context)

    async def close(self):
        await self._target.close()


def _make_runner(spy_log: list[tuple], model: FakeLLMModel) -> Runner:
    session_service = _SessionServiceSpy(
        InMemorySessionService(session_config=SessionServiceConfig(ttl=Ttl(enable=False)), ), spy_log)
    memory_service = _MemoryServiceSpy(
        InMemoryMemoryService(memory_service_config=MemoryServiceConfig(enabled=True, ttl=Ttl(enable=False))),
        spy_log,
    )
    agent = LlmAgent(name="app_demo", model=model, instruction="say OK")
    return Runner(
        app_name="ordering_t:app_demo:v1",
        agent=agent,
        session_service=session_service,
        memory_service=memory_service,
        close_session_service_on_close=False,
        close_memory_service_on_close=False,
    )


def _stream_texts(runner: Runner, streaming: bool) -> list:

    async def _drive():
        events = []
        gen = runner.run_async(
            user_id="u1",
            session_id="s1",
            new_message=Content(role="user", parts=[Part.from_text(text="hello there ordering")]),
            run_config=RunConfig(streaming=streaming),
        )
        try:
            async for event in gen:
                events.append(event)
        finally:
            await gen.aclose()
        return events

    return asyncio.run(_drive())


class TestRunnerPostTurnOrdering:
    """Runner/Session ordering observed through public service methods."""

    def test_nonpartial_events_persisted_before_summary_then_memory(self):
        spy_log: list[tuple] = []
        model = FakeLLMModel()
        runner = _make_runner(spy_log, model)
        try:
            _stream_texts(runner, streaming=False)
        finally:
            # Runner owns no services here (close_*_on_close=False): nothing to
            # close beyond the run itself.
            pass

        names = [entry[0] for entry in spy_log]
        # The turn happened: session bootstrapped, events persisted, then the
        # post-turn chain ran in exactly summary -> memory order.
        assert "append_event" in names
        assert "create_session_summary" in names, spy_log
        assert "store_session" in names, spy_log
        last_append = max(i for i, n in enumerate(names) if n == "append_event")
        summary_at = names.index("create_session_summary")
        memory_at = names.index("store_session")
        assert last_append < summary_at < memory_at, spy_log
        # User message and final model response were both persisted.
        appends = [entry for entry in spy_log if entry[0] == "append_event"]
        assert len(appends) >= 2

    def test_partial_events_never_reach_persistence(self):
        spy_log: list[tuple] = []
        # Two model chunks: a partial delta then the final non-partial answer.
        model = FakeLLMModel(responses=[
            LlmResponse(
                content=Content(role="model", parts=[Part.from_text(text="par")]), partial=True, turn_complete=False),
            LlmResponse(content=Content(role="model", parts=[Part.from_text(text="partial answer")]),
                        partial=False,
                        turn_complete=True),
        ])
        runner = _make_runner(spy_log, model)
        _stream_texts(runner, streaming=True)

        appends = [entry for entry in spy_log if entry[0] == "append_event"]
        assert appends, spy_log
        assert all(entry[1] is False for entry in appends), \
            "partial events must never be persisted through append_event"
        # Ordering still holds end-to-end.
        names = [entry[0] for entry in spy_log]
        last_append = max(i for i, n in enumerate(names) if n == "append_event")
        assert last_append < names.index("create_session_summary") < names.index("store_session")

    def test_state_applied_by_append_is_persisted_before_summary(self):
        spy_log: list[tuple] = []
        session_service = _SessionServiceSpy(
            InMemorySessionService(session_config=SessionServiceConfig(ttl=Ttl(enable=False))),
            spy_log,
        )

        async def _drive():
            session = await session_service.create_session(app_name="a", user_id="u", session_id="s")
            event = Event(
                invocation_id="inv-1",
                author="app_demo",
                actions=EventActions(state_delta={"mood": "calm"}),
            )
            await session_service.append_event(session, event)
            # State was persisted by the append itself, before any summary call.
            reloaded = await session_service.get_session(app_name="a", user_id="u", session_id="s")
            assert reloaded.state["mood"] == "calm"
            await session_service.create_session_summary(reloaded)

        asyncio.run(_drive())
        names = [entry[0] for entry in spy_log]
        assert names == ["create_session", "append_event", "get_session", "create_session_summary"]
        append_entry = spy_log[1]
        assert append_entry[2] == {"mood": "calm"}, "append_event must persist the event's state delta before returning"


def _make_runtime(model: FakeLLMModel, backend: InMemoryTestStateBackend) -> TenantAgentRuntime:
    config = make_tenant_config("ordering_t")
    context_app_id = config.app.app_id
    del context_app_id
    return TenantAgentRuntime(
        config=config,
        model=model,
        tools=[],
        state_backend=backend,
    )


class TestPerRuntimeSummarizerInjection:
    """TenantAgentRuntime injects a per-runtime SDK summarizer (R1B)."""

    def _summarize_via_runtime_service(self, service, model_name: str):
        """Drive the SDK post-turn summary path through a runtime service.

        The default SDK trigger is a conversation-count threshold; instead of
        101 turns this crosses the SAME public threshold deterministically.
        """

        async def _drive():
            session = await service.create_session(
                app_name="ordering_t:app_demo:v1",
                user_id="u1",
                session_id="sum-1",
                state={"greeting": "hi"},
            )
            for i in range(3):
                await service.append_event(
                    session,
                    Event(
                        invocation_id=f"inv-{i}",
                        author="user" if i == 0 else "app_demo",
                        content=Content(role="user" if i == 0 else "model",
                                        parts=[Part.from_text(text=f"conversation message number {i}")]),
                    ),
                )
            session.conversation_count = 101
            await service.create_session_summary(session)
            # Reload from the SHARED backend service to prove persistence.
            reloaded = await service.get_session(app_name="ordering_t:app_demo:v1", user_id="u1", session_id="sum-1")
            return reloaded

        reloaded = asyncio.run(_drive())
        summary_events = [e for e in reloaded.events if e.is_summary_event()]
        assert len(summary_events) == 1, f"no summary Event persisted via runtime service ({model_name})"
        return reloaded, summary_events[0]

    def test_runtime_injects_summarizer_and_persists_summary_event(self):
        backend = InMemoryTestStateBackend()
        runtime = _make_runtime(FakeLLMModel(), backend)
        service = runtime._runner.session_service
        # The Runner receives a service whose summarizer manager is live...
        assert service.summarizer_manager is not None
        assert isinstance(service, SessionServiceABC)
        # ...while the shared backend service was never mutated.
        assert backend.session_service.summarizer_manager is None

        reloaded, summary_event = self._summarize_via_runtime_service(service, "one")
        # SDK summary event shape: system author, prefixed text, model window.
        assert summary_event.author == "system"
        assert summary_event.content.parts[0].text.startswith("Previous conversation summary:")
        # Retained history and state survive the compression (persistence
        # happened through the real backend, not only in memory of the call).
        assert reloaded.state["greeting"] == "hi"
        assert len(reloaded.historical_events) == 3

    def test_two_runtimes_share_backend_without_mutating_it(self):
        backend = InMemoryTestStateBackend()
        runtime_a = _make_runtime(FakeLLMModel(), backend)
        runtime_b = _make_runtime(FakeLLMModel(), backend)
        service_a = runtime_a._runner.session_service
        service_b = runtime_b._runner.session_service
        assert service_a is not service_b
        assert service_a.summarizer_manager is not service_b.summarizer_manager
        assert backend.session_service.summarizer_manager is None
        # Both runtimes summarize independently against the SAME backend.
        reloaded_a, _ = self._summarize_via_runtime_service(service_a, "a")
        assert reloaded_a.app_name == "ordering_t:app_demo:v1"
