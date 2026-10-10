from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]

import trpc_service._cli as cli
from trpc_service.config.secrets import LocalSecretProvider, SecretRef, SecretResolutionError
from trpc_service.config.settings import Environment, Role, ServiceSettings
from trpc_service.faults import FaultStageControlError


class Resource:
    def __init__(self) -> None:
        self.pool = object()
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def aclose(self) -> None:
        self.closed = True


def _settings(monkeypatch: pytest.MonkeyPatch, **updates: Any) -> ServiceSettings:
    monkeypatch.setenv("TRPC_SERVICE_DATABASE_DSN", "postgresql+asyncpg://trpc_runtime@db/service")
    monkeypatch.setenv(
        "TRPC_SERVICE_WORKER_DATABASE_DSN",
        "postgresql+asyncpg://trpc_worker@db/service",
    )
    monkeypatch.setenv(
        "TRPC_SERVICE_WORKER_DATABASE_DSN_REF",
        "env://TRPC_SERVICE_WORKER_DATABASE_DSN",
    )
    monkeypatch.setenv("TRPC_SERVICE_REDIS_URL", "redis://redis:6379/0")
    monkeypatch.setenv("TRPC_SERVICE_SESSION_HMAC_KEY", "s" * 32)
    monkeypatch.setenv("TRPC_SERVICE_EMERGENCY_QUEUE_KEY", "e" * 32)
    monkeypatch.setenv("TRPC_SERVICE_DEVELOPMENT_TOKEN", "dev-token")
    return ServiceSettings(**updates)


def test_only_enabled_worker_resolves_fault_token_and_wraps_same_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(
        monkeypatch,
        environment=Environment.TEST,
        fault_injection_enabled=True,
        fault_injection_run_id="run-1",
        fault_injection_run_token_ref=SecretRef(uri="literal://" + "r" * 32),
    )
    original = Resource()
    wrapped: list[Any] = []

    class WrappedRepository:
        def __init__(
            self,
            pool: object,
            *,
            fault_stages: object,
            ready_replay_cooldown_seconds: int,
        ) -> None:
            self.pool = pool
            self.fault_stages = fault_stages
            self.ready_replay_cooldown_seconds = ready_replay_cooldown_seconds
            wrapped.append(self)

    monkeypatch.setattr(
        "trpc_service.storage.postgres.PostgresRuntimeRepository", WrappedRepository
    )
    runtime, controller = cli._worker_fault_stage_runtime(
        settings,
        LocalSecretProvider(allow_literal=True),
        original,
    )

    assert wrapped and runtime is wrapped[0]
    assert runtime.pool is original.pool
    assert runtime.fault_stages is controller
    assert runtime.ready_replay_cooldown_seconds == settings.recovery_ready_replay_cooldown_seconds
    assert controller._pool is original.pool


@pytest.mark.asyncio
async def test_non_worker_does_not_resolve_fault_token(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(
        monkeypatch,
        environment=Environment.TEST,
        fault_injection_enabled=True,
        fault_injection_run_id="run-2",
        fault_injection_run_token_ref=SecretRef(uri="env://MISSING_FAULT_TOKEN"),
    )
    repository = Resource()
    redis = Resource()
    called: list[str] = []

    async def create(*_args: Any, **_kwargs: Any) -> Resource:
        return repository

    def from_url(*_args: Any, **_kwargs: Any) -> Resource:
        return redis

    async def admin(*_args: Any, **_kwargs: Any) -> None:
        called.append("admin")

    monkeypatch.setattr("trpc_service.storage.postgres.PostgresRuntimeRepository.create", create)
    monkeypatch.setattr("redis.asyncio.from_url", from_url)
    monkeypatch.setattr("trpc_service.metrics.setup.configure_tracing", lambda **_kwargs: None)
    monkeypatch.setattr(cli, "_serve_admin", admin)
    monkeypatch.delenv("MISSING_FAULT_TOKEN", raising=False)

    await cli._serve(Role.ADMIN, settings.model_copy(update={"worker_database_dsn_ref": None}))

    assert called == ["admin"]
    assert repository.closed
    # Admin has no Redis dependency, so the client factory is never used.
    assert not redis.closed


def test_enabled_worker_missing_or_invalid_token_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(
        monkeypatch,
        environment=Environment.TEST,
        fault_injection_enabled=True,
        fault_injection_run_id="run-3",
        fault_injection_run_token_ref=SecretRef(uri="env://MISSING_FAULT_TOKEN"),
    )
    with pytest.raises(SecretResolutionError):
        cli._worker_fault_stage_runtime(settings, LocalSecretProvider(), Resource())

    monkeypatch.setenv("MISSING_FAULT_TOKEN", "")
    with pytest.raises(FaultStageControlError):
        cli._worker_fault_stage_runtime(settings, LocalSecretProvider(), Resource())

    invalid = ServiceSettings.model_construct(
        fault_injection_enabled=True,
        fault_injection_run_id=None,
        fault_injection_run_token_ref=SecretRef(uri="env://MISSING_FAULT_TOKEN"),
    )

    with pytest.raises(ValueError, match="run_id"):
        cli._worker_fault_stage_runtime(invalid, LocalSecretProvider(), Resource())


@pytest.mark.asyncio
async def test_enabled_v2_worker_passes_controller_to_tool_and_claim_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(
        monkeypatch,
        environment=Environment.TEST,
        fault_injection_enabled=True,
        fault_injection_run_id="run-4",
        fault_injection_run_token_ref=SecretRef(uri="literal://" + "r" * 32),
    )
    runtime_repository = Resource()
    controller = object()
    monkeypatch.setenv("HOSTNAME", "wiring")
    monkeypatch.setattr(
        cli,
        "_worker_fault_stage_runtime",
        lambda *_args: (runtime_repository, controller),
    )

    class Capture:
        instance: Capture | None = None

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args = args
            self.kwargs = kwargs
            type(self).instance = self

        async def close(self) -> None:
            return None

        async def run(self, _stop_event: asyncio.Event | None = None) -> None:
            return None

    class ToolCapture(Capture):
        instance: ToolCapture | None = None

    class ConsumerCapture(Capture):
        instance: ConsumerCapture | None = None

    class AgentCapture(Capture):
        instance: AgentCapture | None = None

    class QueueCapture(Capture):
        instance: QueueCapture | None = None

        async def ensure_group(self) -> None:
            return None

    class ReclaimerCapture(Capture):
        instance: ReclaimerCapture | None = None

    class ClaimerCapture(Capture):
        instance: ClaimerCapture | None = None

    class ExecutorCapture(Capture):
        instance: ExecutorCapture | None = None

    for target in (
        "trpc_service.agent.factory.DevelopmentAgentLoader",
        "trpc_service.agent.factory.ProductionAgentLoader",
        "trpc_service.agent.worker.AgentWorker",
        "trpc_service.channels.feishu.FeishuAdapter",
        "trpc_service.channels.media_locator.WeComMediaLocatorCipher",
        "trpc_service.channels.wecom.WeComMediaDownloader",
        "trpc_service.queue.redis_streams.RedisStreamQueue",
        "trpc_service.storage.services.PostgresTenantServiceFactory",
        "trpc_service.tool.confirmation.ConfirmationTokenService",
        "trpc_service.tool.governance.GovernancePipeline",
        "trpc_service.tool.governance.SdkToolSafetyScanner",
        "trpc_service.tool.postgres.PostgresBudgetLedger",
        "trpc_service.tool.postgres.PostgresConfirmationLedger",
        "trpc_service.tool.postgres.PostgresExecutionLedger",
        "trpc_service.tool.postgres.PostgresGovernanceAuditSink",
    ):
        monkeypatch.setattr(target, Capture)
    monkeypatch.setattr("trpc_service.tool.execution.ToolExecutor", ToolCapture)
    monkeypatch.setattr("trpc_service.queue.session_ready.SessionReadyQueue", QueueCapture)
    monkeypatch.setattr("trpc_service.queue.session_ready.SessionReadyReclaimer", ReclaimerCapture)
    monkeypatch.setattr(
        "trpc_service.queue.session_worker_consumer.SessionWorkerConsumer", ConsumerCapture
    )
    monkeypatch.setattr("trpc_service.agent.mailbox_runtime.MailboxReadyClaimer", ClaimerCapture)
    monkeypatch.setattr("trpc_service.agent.mailbox_runtime.MailboxClaimExecutor", ExecutorCapture)
    monkeypatch.setattr("trpc_service.agent.worker.AgentWorker", AgentCapture)

    await cli._serve_worker(
        settings,
        LocalSecretProvider(allow_literal=True),
        Resource(),
        Resource(),
    )

    assert ToolCapture.instance is not None
    assert ConsumerCapture.instance is not None
    assert AgentCapture.instance is not None
    assert QueueCapture.instance is not None
    assert ReclaimerCapture.instance is not None
    assert ClaimerCapture.instance is not None
    assert ExecutorCapture.instance is not None
    assert ToolCapture.instance.kwargs["fault_stages"] is controller
    assert ToolCapture.instance.kwargs["worker_id"] == "worker-wiring"
    assert ConsumerCapture.instance.kwargs["consumer_id"] == "worker-wiring"
    assert AgentCapture.instance.kwargs["worker_id"] == "worker-wiring"
    assert AgentCapture.instance.args[0] is runtime_repository
    assert ClaimerCapture.instance.args[0] is runtime_repository
    assert ClaimerCapture.instance.kwargs["owner_id"] == "worker-wiring"
    assert ExecutorCapture.instance.args[0] is AgentCapture.instance
    assert ConsumerCapture.instance.args == (
        QueueCapture.instance,
        ReclaimerCapture.instance,
        ClaimerCapture.instance,
        ExecutorCapture.instance,
    )


def test_fault_stage_override_is_worker_only_and_secret_backed() -> None:
    path = Path(__file__).resolve().parents[2] / "deploy" / "fault-stage-runtime.override.yml"
    override = yaml.safe_load(path.read_text(encoding="utf-8"))

    assert set(override["services"]) == {"worker"}
    worker = override["services"]["worker"]
    environment = worker["environment"]
    assert environment["TRPC_SERVICE_ENVIRONMENT"] == "test"
    assert environment["TRPC_SERVICE_FAULT_INJECTION_ENABLED"] == "true"
    assert environment["TRPC_SERVICE_FAULT_INJECTION_RUN_ID"] == ("${TRPC_FAULT_RUN_ID:?required}")
    assert environment["TRPC_SERVICE_FAULT_INJECTION_RUN_TOKEN_REF"] == (
        "file:///run/secrets/fault_injection_run_token"
    )
    assert environment["TRPC_SERVICE_OFFLINE_AGENT_DELAY_SECONDS"] == (
        "${TRPC_FAULT_OFFLINE_AGENT_DELAY_SECONDS:?required}"
    )
    assert worker["secrets"] == ["fault_injection_run_token"]
    assert override["secrets"]["fault_injection_run_token"] == {
        "environment": "TRPC_FAULT_RUN_TOKEN"
    }
    rendered = path.read_text(encoding="utf-8")
    assert "run-token" not in rendered
    assert "TRPC_FAULT_RUN_TOKEN" in rendered
