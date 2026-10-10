from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.models import LLMModel, LlmRequest, LlmResponse
from trpc_agent_sdk.tools import FunctionTool
from trpc_agent_sdk.types import Blob, Content, Part

import trpc_service.agent.wecom_manager as wecom_manager_module
import trpc_service.agent.worker as worker_module
from tests.conftest import binding, envelope, repository, tenant_config
from trpc_service.agent.factory import DevelopmentAgentLoader, FallbackModel, ProductionAgentLoader
from trpc_service.agent.fake import DeterministicAgent
from trpc_service.agent.registry import RevisionRegistry
from trpc_service.agent.runner import PreparedMedia, TenantRunner, _media_prompt, _message_content
from trpc_service.agent.session import TurnBufferSessionService
from trpc_service.agent.wecom_manager import WeComConnectionManager
from trpc_service.agent.worker import AgentWorker, ProcessStatus, _record_usage, _target_id
from trpc_service.channels.envelopes import MediaReference, PayloadKind
from trpc_service.channels.wecom import WeComBindingLeaseUnavailable
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.artifacts import InMemoryArtifactStore
from trpc_service.storage.models import BindingRoute, SequencedEvent, SessionSnapshot
from trpc_service.storage.protocols import FencingConflict
from trpc_service.storage.services import PostgresSessionStore, TenantDataServices
from trpc_service.tenant.models import (
    Channel,
    ChannelBinding,
    ConversationKind,
    MediaPolicy,
    ModelPolicy,
    ToolPolicy,
)
from trpc_service.tool.confirmation import ConfirmationTokenService, InMemoryConfirmationLedger
from trpc_service.tool.execution import InMemoryExecutionLedger, ToolExecutor
from trpc_service.tool.governance import (
    GovernancePipeline,
    InMemoryBudgetLedger,
    SdkToolSafetyScanner,
)
from trpc_service.tool.integration import GovernedTool


class Model(LLMModel):
    def __init__(self, values=(), *, failure=None) -> None:
        super().__init__("fake")
        self.values = values
        self.failure = failure
        self.validated = False

    @classmethod
    def supported_models(cls):
        return [".*"]

    def validate_request(self, request):
        self.validated = True

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        if self.failure:
            raise self.failure
        for value in self.values:
            yield value


async def collect(model, request=None):
    return [item async for item in model.generate_async(request or LlmRequest())]


@pytest.mark.asyncio
async def test_fallback_model_before_output_only() -> None:
    fallback_response = LlmResponse(responseId="fallback")
    primary = Model([LlmResponse(errorCode="timeout")])
    fallback = Model([fallback_response])
    model = FallbackModel(primary, fallback)
    model.validate_request(LlmRequest())
    assert primary.validated
    assert await collect(model) == [fallback_response]

    primary = Model(failure=TimeoutError())
    assert await collect(FallbackModel(primary, fallback)) == [fallback_response]
    emitted = LlmResponse(responseId="primary")
    primary = Model([emitted])
    assert await collect(FallbackModel(primary, fallback)) == [emitted]

    class PartialThenFail(Model):
        async def _generate_async_impl(self, request, stream=False, ctx=None):
            yield emitted
            raise RuntimeError("after output")

    raw = PartialThenFail()

    async def raw_generate(request, stream=False, ctx=None):
        yield emitted
        raise RuntimeError("after output")

    raw.generate_async = raw_generate

    async def collect_impl(model):
        return [item async for item in model._generate_async_impl(LlmRequest())]

    with pytest.raises(RuntimeError, match="after output"):
        await collect_impl(FallbackModel(raw, fallback))
    assert FallbackModel.supported_models() == [".*"]


@pytest.mark.asyncio
async def test_production_loader_provider_selection_and_tools(monkeypatch) -> None:
    created = []

    def factory(*, model_name, **kwargs):
        created.append((model_name, kwargs))
        return Model()

    for name in ("OpenAIModel", "AnthropicModel", "LiteLLMModel"):
        monkeypatch.setattr(f"trpc_service.agent.factory.{name}", factory)
    monkeypatch.setattr(
        "trpc_service.agent.factory.LlmAgent", lambda **kwargs: SimpleNamespace(**kwargs)
    )
    secrets = LocalSecretProvider(allow_literal=True)

    async def tool_a(value: int) -> int:
        """Return a value."""

        return value

    pipeline = GovernancePipeline(
        InMemoryBudgetLedger(),
        SdkToolSafetyScanner(),
        ConfirmationTokenService(b"c" * 32, InMemoryConfirmationLedger()),
    )
    loader = ProductionAgentLoader(
        secrets,
        tools={"a": FunctionTool(tool_a)},
        governance=pipeline,
        tool_executor=ToolExecutor(b"e" * 32, InMemoryExecutionLedger()),
    )
    config = tenant_config().model_copy(
        update={
            "instructions": "answer safely",
            "tools": ToolPolicy(allow=frozenset({"a", "missing"})),
            "model": ModelPolicy(
                provider="openai",
                model="primary",
                base_url="https://model",
                api_key_ref=SecretRef(uri="literal://key"),
                fallback_model="secondary",
            ),
        }
    )
    agent = await loader(config)
    assert agent.name.startswith("tenant_agent_")
    assert len(agent.tools) == 1
    assert isinstance(agent.tools[0], GovernedTool)
    assert agent.generate_content_config.max_output_tokens == config.budget.max_tokens_per_turn
    assert agent.generate_content_config.http_options.timeout == 60_000
    assert created[0] == ("primary", {"api_key": "key", "base_url": "https://model"})
    assert created[1][0] == "secondary"
    assert isinstance(loader._model(config.model), FallbackModel)
    loader._model(ModelPolicy(provider="openai", model="reasoning", reasoning_effort="medium"))
    assert created[-1] == (
        "reasoning",
        {
            "api_key": "",
            "use_responses_api": True,
            "responses_api_params": {"reasoning": {"effort": "medium"}},
        },
    )
    loader._model(ModelPolicy(provider="anthropic", model="a"))
    loader._model(ModelPolicy(provider="litellm", model="l"))
    with pytest.raises(ValueError, match="unsupported"):
        loader._model(ModelPolicy(provider="unknown", model="x"))

    unsafe_loader = ProductionAgentLoader(secrets, tools={"a": FunctionTool(tool_a)})
    with pytest.raises(ValueError, match="governance"):
        await unsafe_loader(config)


def test_model_generation_controls_are_validated_and_applied(monkeypatch) -> None:
    monkeypatch.setattr(
        "trpc_service.agent.factory.LlmAgent", lambda **kwargs: SimpleNamespace(**kwargs)
    )
    loader = ProductionAgentLoader(LocalSecretProvider(allow_literal=True))
    config = tenant_config().model_copy(
        update={
            "model": ModelPolicy(
                provider="anthropic",
                model="claude-test",
                timeout_seconds=12.5,
                temperature=0.25,
                top_p=0.8,
                stop_sequences=("STOP",),
                thinking_budget_tokens=2048,
            )
        }
    )

    agent = loader._build_agent(config, Model())

    assert agent.generate_content_config.temperature == 0.25
    assert agent.generate_content_config.top_p == 0.8
    assert agent.generate_content_config.stop_sequences == ["STOP"]
    assert agent.generate_content_config.http_options.timeout == 12_500
    assert agent.planner.thinking_config.include_thoughts is True
    assert agent.planner.thinking_config.thinking_budget == 2048

    with pytest.raises(ValueError, match="at least 1024"):
        ModelPolicy(provider="anthropic", model="claude-test", thinking_budget_tokens=512)
    with pytest.raises(ValueError, match="requires the openai provider"):
        ModelPolicy(provider="anthropic", model="claude-test", reasoning_effort="high")
    with pytest.raises(ValueError, match="mutually exclusive"):
        ModelPolicy(
            provider="openai",
            model="gpt-test",
            reasoning_effort="low",
            thinking_budget_tokens=2048,
        )
    with pytest.raises(ValueError, match="1 to 256"):
        ModelPolicy(provider="openai", model="gpt-test", stop_sequences=("",))
    with pytest.raises(ValueError, match="smaller than max_tokens_per_turn"):
        value = tenant_config()
        payload = value.model_dump()
        payload["model"] = {
            "provider": "anthropic",
            "model": "claude-test",
            "thinking_budget_tokens": 2048,
        }
        payload["budget"]["max_tokens_per_turn"] = 2048
        value.__class__.model_validate(payload)


@pytest.mark.asyncio
async def test_development_loader_is_offline_only() -> None:
    class Production:
        calls = 0

        async def __call__(self, config):
            self.calls += 1
            return DeterministicAgent(name="delegated")

    production = Production()
    loader = DevelopmentAgentLoader(production, delay_seconds=0)
    offline = tenant_config().model_copy(
        update={"model": ModelPolicy(provider="offline", model="deterministic")}
    )
    agent = await loader(offline)
    assert isinstance(agent, DeterministicAgent)
    assert agent.name.startswith("offline_agent_")
    assert production.calls == 0

    await loader(tenant_config())
    assert production.calls == 1


@pytest.mark.asyncio
async def test_revision_registry_retirement_lifecycle() -> None:
    registry = RevisionRegistry[str]()
    loads = 0

    async def loader():
        nonlocal loads
        loads += 1
        return "value"

    await registry.retire(("missing", "app", 1))
    async with registry.use(("tenant", "app", 1), loader) as value:
        assert value == "value"
        async with registry.use(("tenant", "app", 1), loader):
            assert loads == 1 and await registry.size() == 1
        await registry.retire(("tenant", "app", 1))
        with pytest.raises(LookupError, match="retired"):
            async with registry.use(("tenant", "app", 1), loader):
                pass
    assert await registry.size() == 0


def snapshot_with_event() -> SessionSnapshot:
    event = Event(id="old", author="user", timestamp=10)
    return SessionSnapshot(
        tenant_id="tenant",
        app_id="app",
        session_id="session",
        principal_id="user",
        state={"persisted": 1, "temp:hidden": True},
        events=(
            SequencedEvent(
                event_id="old",
                author="user",
                timestamp=10,
                event=event.model_dump(mode="json", by_alias=True),
                sequence=1,
            ),
        ),
        next_sequence=2,
    )


@pytest.mark.asyncio
async def test_turn_buffer_full_session_contract() -> None:
    service = TurnBufferSessionService(snapshot_with_event())
    assert service.state == {"persisted": 1}
    assert await service.get_session(app_name="app", user_id="u", session_id="other") is None
    session = await service.get_session(app_name="app", user_id="u", session_id="session")
    assert session is not None and session.events[0].id == "old"
    listed = await service.list_sessions(app_name="app", user_id="u")
    assert len(listed.sessions) == 1 and listed.sessions[0].events == []
    assert not (await service.list_sessions(app_name="app", user_id="other")).sessions

    partial = Event(id="partial", author="agent", partial=True)
    await service.append_event(session, partial)
    assert not service.buffered_events
    complete = Event(id="new", author="agent", actions={"state_delta": {"answer": 2}})
    await service.append_event(session, complete)
    assert service.buffered_events[0].state_delta == {"answer": 2}
    await service.update_session(session)
    await service.delete_session(app_name="app", user_id="u", session_id="other")
    await service.delete_session(app_name="app", user_id="u", session_id="session")
    assert not service.buffered_events
    assert not (await service.list_sessions(app_name="app")).sessions

    with pytest.raises(ValueError, match="unexpected"):
        await service.create_session(app_name="app", user_id="u", session_id="wrong", state={})
    created = await service.create_session(app_name="app", user_id="u", state={"x": 1})
    assert created.id == "session"


@pytest.mark.asyncio
async def test_turn_buffer_never_persists_inline_media_bytes() -> None:
    service = TurnBufferSessionService(snapshot_with_event())
    session = await service.get_session(app_name="app", user_id="u", session_id="session")
    assert session is not None
    secret_bytes = b"private-image-payload"
    event = Event(
        id="media",
        author="user",
        content=Content(
            role="user",
            parts=[Part(inline_data=Blob(data=secret_bytes, mime_type="image/png"))],
        ),
    )

    await service.append_event(session, event)

    stored = service.buffered_events[-1].event
    rendered = str(stored)
    assert "private-image-payload" not in rendered
    assert stored["content"]["parts"][0]["text"] == (
        "[image/png media persisted as a tenant artifact]"
    )
    assert session.events[-1].content.parts[0].inline_data.data == secret_bytes


def test_media_prompts_and_group_targets() -> None:
    assert "empty" in _media_prompt(envelope(text="").model_copy(update={"text": None}))
    assert "IM event" in _media_prompt(
        envelope().model_copy(update={"text": None, "event_type": "recall"})
    )
    assert "1 item" in _media_prompt(
        envelope().model_copy(
            update={
                "text": None,
                "payload_kind": PayloadKind.IMAGE,
                "media": (MediaReference(provider_media_id="id"),),
            }
        )
    )

    content = _message_content(
        envelope(text="look at this"),
        (
            PreparedMedia(
                filename="photo.png",
                content_type="image/png",
                inline_data=b"image-bytes",
            ),
            PreparedMedia(
                filename="notes.txt",
                content_type="text/plain",
                text="bounded extracted text",
            ),
        ),
    )
    assert content.parts[0].text == "look at this"
    assert content.parts[1].inline_data.data == b"image-bytes"
    assert "bounded extracted text" in content.parts[2].text


def test_worker_usage_metrics_and_valid_group_target(monkeypatch) -> None:
    class Metric:
        def __init__(self) -> None:
            self.values = []

        def labels(self, **labels):
            self.values.append(("labels", labels))
            return self

        def inc(self, value=1):
            self.values.append(("inc", value))

    tokens = Metric()
    cost = Metric()
    monkeypatch.setattr(worker_module, "TOKENS", tokens)
    monkeypatch.setattr(worker_module, "TENANT_COST", cost)

    _record_usage(
        SimpleNamespace(
            usage_metadata=SimpleNamespace(
                prompt_token_count=2,
                tool_use_prompt_token_count=3,
                candidates_token_count=4,
                thoughts_token_count=1,
                total_token_count=12,
            )
        ),
        "tenant-a",
    )
    _record_usage(SimpleNamespace(usage_metadata=SimpleNamespace()), "tenant-a")
    _record_usage(SimpleNamespace(), "tenant-a")

    assert ("inc", 5) in tokens.values
    assert ("inc", 12) in cost.values

    accepted = SimpleNamespace(
        envelope=envelope().model_copy(
            update={
                "conversation_kind": ConversationKind.GROUP,
                "external_conversation_id": "group-1",
            }
        )
    )
    assert _target_id(accepted) == "group-1"


@pytest.mark.asyncio
async def test_runner_rejects_unpinned_config() -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=b"r" * 32)
    accepted = await runtime.accept("binding-unpredictable-a", envelope())
    lease = await repo.acquire(acceptance=accepted, worker_id="w", lease_for=timedelta(seconds=30))
    assert lease is not None
    runner = TenantRunner(
        config=tenant_config(),
        lease=lease,
        registry=RevisionRegistry(),
        agent_loader=lambda config: None,
    )
    with pytest.raises(ValueError, match="pinned"):
        async for _ in runner.run(
            accepted.context.model_copy(update={"config_version": 2}), accepted.envelope
        ):
            pass


@pytest.mark.asyncio
async def test_worker_duplicate_failure_and_invalid_group(monkeypatch) -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=b"w" * 32)
    accepted = await runtime.accept("binding-unpredictable-a", envelope())

    class NoLease:
        async def acquire(self, **kwargs):
            return None

        async def get_acceptance(self, *args):
            return None

    worker = AgentWorker(NoLease(), worker_id="w", agent_loader=lambda config: None)
    assert (await worker.process(accepted)).status == ProcessStatus.BUSY
    assert (
        await worker.process(accepted.model_copy(update={"duplicate": True}))
    ).status == ProcessStatus.DUPLICATE

    class BrokenRunner:
        def __init__(self, **kwargs):
            pass

        async def run(self, *args):
            raise RuntimeError("agent failed")
            yield

    monkeypatch.setattr("trpc_service.agent.worker.TenantRunner", BrokenRunner)
    failing = AgentWorker(repo, worker_id="fail", agent_loader=lambda config: None)
    with pytest.raises(RuntimeError, match="agent failed"):
        await failing.process(accepted)
    assert repo._leases == {}

    group = accepted.model_copy(
        update={
            "envelope": accepted.envelope.model_copy(
                update={
                    "conversation_kind": ConversationKind.GROUP,
                    "external_conversation_id": None,
                }
            )
        }
    )
    with pytest.raises(ValueError, match="target"):
        _target_id(group)
    assert _target_id(accepted) == accepted.envelope.external_user_id


@pytest.mark.asyncio
async def test_worker_rechecks_acceptance_after_another_worker_commits() -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=b"race" * 8)
    accepted = await runtime.accept("binding-unpredictable-a", envelope())

    async def load_agent(_config):
        return DeterministicAgent(name="race-agent", response="committed")

    committer = AgentWorker(repo, worker_id="committer", agent_loader=load_agent)
    committed = await committer.process(accepted)
    assert committed.status == ProcessStatus.COMMITTED
    assert not accepted.duplicate

    contender = AgentWorker(repo, worker_id="contender", agent_loader=load_agent)
    result = await contender.process(accepted)
    assert result.status == ProcessStatus.DUPLICATE


@pytest.mark.asyncio
async def test_worker_heartbeat_failure_is_fenced(monkeypatch) -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=b"h" * 32).accept(
        "binding-unpredictable-a", envelope()
    )

    async def renew(*_args, **_kwargs):
        raise FencingConflict("lease lost")

    repo.renew = renew

    class SlowRunner:
        def __init__(self, **_kwargs) -> None:
            self.state = {}
            self.buffered_events = ()

        async def run(self, *_args):
            try:
                await asyncio.sleep(0.15)
            except asyncio.CancelledError:
                cancelled.set()
                raise
            yield SimpleNamespace(
                usage_metadata=None,
                visible=False,
                is_final_response=lambda: False,
                get_text=lambda: "",
            )

    monkeypatch.setattr(worker_module, "TenantRunner", SlowRunner)
    cancelled = asyncio.Event()
    worker = AgentWorker(
        repo,
        worker_id="heartbeat-worker",
        agent_loader=lambda config: None,
        lease_for=timedelta(milliseconds=30),
    )

    with pytest.raises(FencingConflict, match="heartbeat failed"):
        await worker.process(accepted)
    assert cancelled.is_set()
    assert repo._leases == {}


@pytest.mark.asyncio
async def test_worker_turn_error_wins_over_simultaneous_heartbeat_failure(monkeypatch) -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=b"hb" * 16).accept(
        "binding-unpredictable-a", envelope()
    )
    renew_started = asyncio.Event()
    release_renew = asyncio.Event()

    async def renew(*_args, **_kwargs):
        renew_started.set()
        await release_renew.wait()
        await asyncio.sleep(0)
        raise FencingConflict("lease lost")

    repo.renew = renew

    class FailingRunner:
        def __init__(self, **_kwargs) -> None:
            self.state = {}
            self.buffered_events = ()

        async def run(self, *_args):
            await renew_started.wait()
            release_renew.set()
            raise RuntimeError("turn failed")
            yield

    monkeypatch.setattr(worker_module, "TenantRunner", FailingRunner)
    worker = AgentWorker(
        repo,
        worker_id="heartbeat-body-race",
        agent_loader=lambda config: None,
        lease_for=timedelta(milliseconds=30),
    )

    with pytest.raises(RuntimeError, match="turn failed"):
        await worker.process(accepted)
    assert repo._leases == {}


@pytest.mark.asyncio
async def test_worker_heartbeat_shutdown_cancels_inflight_renew(monkeypatch) -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=b"hc" * 16).accept(
        "binding-unpredictable-a", envelope()
    )
    renew_started = asyncio.Event()
    renew_cancelled = asyncio.Event()

    async def renew(*_args, **_kwargs):
        renew_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            renew_cancelled.set()
            raise

    repo.renew = renew

    class FiniteRunner:
        def __init__(self, **_kwargs) -> None:
            self.state = {}
            self.buffered_events = ()

        async def run(self, *_args):
            await renew_started.wait()
            yield SimpleNamespace(
                usage_metadata=None,
                visible=False,
                is_final_response=lambda: False,
                get_text=lambda: "",
            )

    monkeypatch.setattr(worker_module, "TenantRunner", FiniteRunner)
    worker = AgentWorker(
        repo,
        worker_id="heartbeat-shutdown-worker",
        agent_loader=lambda config: None,
        lease_for=timedelta(milliseconds=30),
    )

    with pytest.raises(FencingConflict, match="shutdown timed out"):
        await worker.process(accepted)
    assert renew_cancelled.is_set()
    assert repo._leases == {}


@pytest.mark.asyncio
async def test_worker_downloads_persists_and_sanitizes_feishu_image() -> None:
    repo = repository()
    inbound = envelope(text="").model_copy(
        update={
            "text": None,
            "payload_kind": PayloadKind.IMAGE,
            "media": (MediaReference(provider_media_id="img_key", filename="photo.png"),),
        }
    )
    accepted = await TenantRuntime(repo, routing_key=b"m" * 32).accept(
        "binding-unpredictable-a", inbound
    )
    objects = InMemoryArtifactStore()

    class Audit:
        def __init__(self) -> None:
            self.entries = []

        async def append(self, _tenant_id, **entry):
            self.entries.append(entry)
            return "audit"

    audit = Audit()

    class Factory:
        async def for_context(self, _context, _config):
            return TenantDataServices(
                session=PostgresSessionStore(repo),
                memory=object(),
                summary=object(),
                artifact=objects,
                knowledge=object(),
                audit=audit,
            )

    @dataclass(frozen=True)
    class Download:
        data: bytes = b"private-image-bytes"
        content_type: str = "image/png"
        filename: str | None = "photo.png"

    class Downloader:
        async def download_media(self, *_args, **_kwargs):
            return Download()

    async def load_media_agent(_config):
        return DeterministicAgent(name="media-agent")

    worker = AgentWorker(
        repo,
        worker_id="media-worker",
        agent_loader=load_media_agent,
        service_factory=Factory(),
        media_downloaders={Channel.FEISHU: Downloader()},
    )
    result = await worker.process(accepted)

    assert result.status == ProcessStatus.COMMITTED
    assert any("/artifacts/" in key for key in objects.objects)
    snapshot = repo.snapshot(accepted.context.tenant_id, accepted.context.session_id)
    assert snapshot is not None
    rendered = str([item.event for item in snapshot.events])
    assert "private-image-bytes" not in rendered
    assert "persisted as a tenant artifact" in rendered
    assert audit.entries[-1]["decision"] == "media_ingested"
    assert audit.entries[-1]["metadata"]["kind"] == "image"


@dataclass(frozen=True)
class _PreparedDownload:
    data: bytes
    content_type: str = "text/plain"
    filename: str | None = "notes.txt"


class _MediaDownloadFake:
    def __init__(self, result: _PreparedDownload | None = None, error: Exception | None = None):
        self.result = result or _PreparedDownload(b"offline media")
        self.error = error
        self.calls = []

    async def download_media(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.error is not None:
            raise self.error
        return self.result


class _MediaAuditFake:
    def __init__(self) -> None:
        self.entries = []

    async def append(self, _tenant_id, **entry):
        self.entries.append(entry)
        return "audit"


class _CommitFailingArtifact:
    def __init__(self) -> None:
        self.staged = []
        self.discarded = []

    async def stage(self, _tenant_id, _artifact_id, _content, *, checksum):
        key = f"staging/{len(self.staged) + 1}"
        self.staged.append((key, checksum))
        return key

    async def commit(self, _tenant_id, _artifact_id, _staged_key):
        raise RuntimeError("storage backend unavailable")

    async def discard(self, staged_key):
        self.discarded.append(staged_key)


def _media_services(artifact, audit=None):
    return SimpleNamespace(artifact=artifact, audit=audit or _MediaAuditFake())


def _media_inbound(*references: MediaReference, message_id: str = "media-message"):
    return envelope(message_id, text="").model_copy(
        update={
            "text": None,
            "payload_kind": PayloadKind.FILE,
            "media": references,
        }
    )


@pytest.mark.asyncio
async def test_worker_media_download_failure_degrades_without_leaking_error() -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=b"d" * 32).accept(
        "binding-unpredictable-a",
        _media_inbound(MediaReference(provider_media_id="file-key", filename="secret.txt")),
    )
    audit = _MediaAuditFake()
    downloader = _MediaDownloadFake(error=RuntimeError("secret provider response"))
    worker = AgentWorker(
        repo,
        worker_id="download-failure-worker",
        agent_loader=lambda config: None,
        media_downloaders={Channel.FEISHU: downloader},
    )
    config = await repo.get_config(
        accepted.context.tenant_id,
        accepted.context.app_id,
        accepted.context.config_version,
    )

    prepared = await worker._prepare_media(
        accepted, config, _media_services(InMemoryArtifactStore(), audit)
    )

    assert len(prepared) == 1
    assert prepared[0].inline_data is None
    assert prepared[0].text == "[media content unavailable: download failed]"
    assert len(downloader.calls) == 1
    assert audit.entries[0]["decision"] == "media_download_failed"
    assert audit.entries[0]["error_type"] == "RuntimeError"
    assert "secret provider response" not in str(audit.entries)

    class ProviderFailure(RuntimeError):
        provider_code = "provider_unavailable"

    assert worker_module._safe_error_type(ProviderFailure("ignored")) == "provider_unavailable"


@pytest.mark.asyncio
async def test_worker_media_missing_provider_id_degrades_without_download() -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=b"i" * 32).accept(
        "binding-unpredictable-a",
        _media_inbound(MediaReference(filename="missing-key.txt")),
    )
    downloader = _MediaDownloadFake()
    worker = AgentWorker(
        repo,
        worker_id="missing-key-worker",
        agent_loader=lambda config: None,
        media_downloaders={Channel.FEISHU: downloader},
    )
    config = await repo.get_config(
        accepted.context.tenant_id,
        accepted.context.app_id,
        accepted.context.config_version,
    )

    prepared = await worker._prepare_media(
        accepted, config, _media_services(InMemoryArtifactStore())
    )

    assert prepared[0].text == "[media content unavailable: provider media id is unavailable]"
    assert downloader.calls == []


@pytest.mark.asyncio
async def test_worker_media_enforces_item_byte_and_total_limits() -> None:
    repo = repository()
    config = tenant_config().model_copy(
        update={
            "media": MediaPolicy(
                max_items_per_turn=1,
                max_bytes_per_item=1024,
                max_total_bytes=1024,
                max_extracted_chars=512,
            )
        }
    )
    repo.add_config(config)
    accepted = await TenantRuntime(repo, routing_key=b"l" * 32).accept(
        "binding-unpredictable-a",
        _media_inbound(
            MediaReference(provider_media_id="first", filename="first.txt"),
            MediaReference(provider_media_id="second", filename="second.txt"),
        ),
    )
    downloader = _MediaDownloadFake(_PreparedDownload(b"x" * 1025))
    audit = _MediaAuditFake()
    worker = AgentWorker(
        repo,
        worker_id="limit-worker",
        agent_loader=lambda config: None,
        media_downloaders={Channel.FEISHU: downloader},
    )

    prepared = await worker._prepare_media(
        accepted, config, _media_services(InMemoryArtifactStore(), audit)
    )

    assert len(prepared) == 2
    assert prepared[0].text == "[media content unavailable: size limit exceeded]"
    assert prepared[1].text == "[additional media rejected: item limit exceeded]"
    assert len(downloader.calls) == 1
    assert [entry["error_type"] for entry in audit.entries] == ["media_too_large"]

    total_config = config.model_copy(
        update={
            "media": MediaPolicy(
                max_items_per_turn=4,
                max_bytes_per_item=2048,
                max_total_bytes=1024,
                max_extracted_chars=1024,
            )
        }
    )
    total_accepted = await TenantRuntime(repo, routing_key=b"t" * 32).accept(
        "binding-unpredictable-a",
        _media_inbound(
            MediaReference(provider_media_id="one", filename="one.txt"),
            MediaReference(provider_media_id="two", filename="two.txt"),
            message_id="total-message",
        ),
    )
    total_downloader = _MediaDownloadFake(_PreparedDownload(b"y" * 600))
    total_audit = _MediaAuditFake()
    total_worker = AgentWorker(
        repo,
        worker_id="total-limit-worker",
        agent_loader=lambda config: None,
        media_downloaders={Channel.FEISHU: total_downloader},
    )
    total_prepared = await total_worker._prepare_media(
        total_accepted,
        total_config,
        _media_services(InMemoryArtifactStore(), total_audit),
    )

    assert total_prepared[0].text == "y" * 600
    assert total_prepared[1].text == "[media content unavailable: size limit exceeded]"
    assert len(total_downloader.calls) == 2
    assert total_audit.entries[-1]["error_type"] == "media_too_large"


@pytest.mark.asyncio
async def test_worker_media_enforces_extraction_character_budget() -> None:
    repo = repository()
    config = tenant_config().model_copy(
        update={
            "media": MediaPolicy(
                max_items_per_turn=4,
                max_bytes_per_item=2048,
                max_total_bytes=4096,
                max_extracted_chars=256,
            )
        }
    )
    repo.add_config(config)
    accepted = await TenantRuntime(repo, routing_key=b"c" * 32).accept(
        "binding-unpredictable-a",
        _media_inbound(
            MediaReference(provider_media_id="first", filename="first.txt"),
            MediaReference(provider_media_id="second", filename="second.txt"),
            message_id="chars-message",
        ),
    )
    downloader = _MediaDownloadFake(_PreparedDownload(b"z" * 256))
    audit = _MediaAuditFake()
    objects = InMemoryArtifactStore()
    worker = AgentWorker(
        repo,
        worker_id="char-limit-worker",
        agent_loader=lambda config: None,
        media_downloaders={Channel.FEISHU: downloader},
    )

    prepared = await worker._prepare_media(accepted, config, _media_services(objects, audit))

    assert prepared[0].text == "z" * 256
    assert prepared[1].text == ("[media content unavailable: extraction character limit exceeded]")
    assert len(objects.objects) == 2
    assert audit.entries[-1]["error_type"] == "extraction_limit"


@pytest.mark.asyncio
async def test_worker_media_storage_failure_discards_staged_object() -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=b"s" * 32).accept(
        "binding-unpredictable-a",
        _media_inbound(MediaReference(provider_media_id="file-key", filename="file.txt")),
    )
    artifact = _CommitFailingArtifact()
    worker = AgentWorker(
        repo,
        worker_id="storage-failure-worker",
        agent_loader=lambda config: None,
        media_downloaders={Channel.FEISHU: _MediaDownloadFake()},
    )
    config = await repo.get_config(
        accepted.context.tenant_id,
        accepted.context.app_id,
        accepted.context.config_version,
    )

    with pytest.raises(RuntimeError, match="storage backend unavailable"):
        await worker._prepare_media(accepted, config, _media_services(artifact))

    assert len(artifact.staged) == 1
    assert artifact.discarded == [artifact.staged[0][0]]


@pytest.mark.asyncio
async def test_worker_media_rejects_binding_tenant_mismatch() -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=b"b" * 32).accept(
        "binding-unpredictable-a",
        _media_inbound(MediaReference(provider_media_id="file-key")),
    )
    repo.add_route(
        BindingRoute(
            binding=binding(tenant_id="another-tenant"),
            active_config_version=1,
        )
    )
    downloader = _MediaDownloadFake()
    worker = AgentWorker(
        repo,
        worker_id="binding-mismatch-worker",
        agent_loader=lambda config: None,
        media_downloaders={Channel.FEISHU: downloader},
    )
    config = await repo.get_config(
        accepted.context.tenant_id,
        accepted.context.app_id,
        accepted.context.config_version,
    )

    with pytest.raises(LookupError, match="binding is unavailable"):
        await worker._prepare_media(accepted, config, _media_services(InMemoryArtifactStore()))
    assert downloader.calls == []


@pytest.mark.asyncio
async def test_worker_media_disabled_or_without_downloader_is_noop() -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=b"n" * 32).accept(
        "binding-unpredictable-a",
        _media_inbound(MediaReference(provider_media_id="file-key")),
    )
    config = await repo.get_config(
        accepted.context.tenant_id,
        accepted.context.app_id,
        accepted.context.config_version,
    )
    services = _media_services(InMemoryArtifactStore())
    downloader = _MediaDownloadFake()
    worker_without_downloader = AgentWorker(
        repo,
        worker_id="no-downloader-worker",
        agent_loader=lambda config: None,
    )
    assert await worker_without_downloader._prepare_media(accepted, config, services) == ()

    disabled = config.model_copy(update={"media": MediaPolicy(enabled=False)})
    worker_disabled = AgentWorker(
        repo,
        worker_id="disabled-media-worker",
        agent_loader=lambda config: None,
        media_downloaders={Channel.FEISHU: downloader},
    )
    assert await worker_disabled._prepare_media(accepted, disabled, services) == ()
    assert downloader.calls == []


class Bindings:
    def __init__(self, values=()):
        self.values = values

    async def list_bindings(self, channel):
        return tuple(self.values)


class Connector:
    def __init__(self):
        self.block = asyncio.Event()

    async def run(self, binding, sink):
        await self.block.wait()


@pytest.mark.asyncio
async def test_wecom_manager_add_remove_failed_and_shutdown(monkeypatch) -> None:
    binding = ChannelBinding(
        binding_id="wecom",
        tenant_id="tenant",
        app_id="app",
        channel=Channel.WECOM_AI_BOT,
        account_id="bot",
    )
    repo = Bindings([binding])
    connector = Connector()

    async def sink(binding_id, inbound):
        return None

    manager = WeComConnectionManager(repo, connector, sink)
    await manager.reconcile_once()
    assert "wecom" in manager._tasks
    repo.values = ()
    await manager.reconcile_once()
    assert not manager._tasks

    async def fail(*args):
        raise RuntimeError("connector")

    connector.run = fail
    repo.values = [binding]
    await manager.reconcile_once()
    await asyncio.sleep(0)
    repo.values = ()
    await manager.reconcile_once()
    assert not manager._tasks

    repo.values = [binding]
    connector.run = Connector().run

    async def stop(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", stop)
    with pytest.raises(asyncio.CancelledError):
        await manager.run(refresh_seconds=0)
    assert not [task for task in manager._tasks.values() if not task.done()]


@pytest.mark.asyncio
async def test_wecom_standby_retries_lease_contention_without_exponential_backoff(
    monkeypatch,
) -> None:
    value = binding(channel="wecom_ai_bot")
    attempts = 0
    waits: list[float] = []

    class StandbyConnector:
        async def run(self, _binding, _sink) -> None:
            nonlocal attempts
            attempts += 1
            if attempts <= 3:
                raise WeComBindingLeaseUnavailable("another connector owns this channel binding")
            manager._stop_event.set()

    async def record_wait(awaitable, **kwargs):
        waits.append(kwargs["timeout"])
        awaitable.close()
        raise TimeoutError

    manager = WeComConnectionManager(
        Bindings((value,)),
        StandbyConnector(),
        object(),
        reconnect_jitter_ratio=0,
    )
    monkeypatch.setattr(wecom_manager_module.asyncio, "wait_for", record_wait)

    await manager._run_binding(value)

    assert attempts == 4
    assert waits == [0.5, 0.5, 0.5]


@pytest.mark.asyncio
async def test_wecom_connection_errors_keep_exponential_backoff(monkeypatch) -> None:
    value = binding(channel="wecom_ai_bot")
    attempts = 0
    waits: list[float] = []

    class FailingConnector:
        async def run(self, _binding, _sink) -> None:
            nonlocal attempts
            attempts += 1
            if attempts <= 3:
                raise ConnectionError("provider unavailable")
            manager._stop_event.set()

    async def record_wait(awaitable, **kwargs):
        waits.append(kwargs["timeout"])
        awaitable.close()
        raise TimeoutError

    manager = WeComConnectionManager(
        Bindings((value,)),
        FailingConnector(),
        object(),
        reconnect_jitter_ratio=0,
    )
    monkeypatch.setattr(wecom_manager_module.asyncio, "wait_for", record_wait)

    await manager._run_binding(value)

    assert attempts == 4
    assert waits == [0.5, 1.0, 2.0]
