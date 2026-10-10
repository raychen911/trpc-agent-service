from __future__ import annotations

import base64
import json

import pytest
from fastapi import APIRouter
from typer.testing import CliRunner

import trpc_service._cli as cli
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.config.settings import Environment, Role, ServiceSettings


def settings(monkeypatch, **updates) -> ServiceSettings:
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


def test_url_secret_and_backend_helpers(monkeypatch) -> None:
    monkeypatch.setenv("DB_PASSWORD", "p@ss")
    monkeypatch.setenv("REDIS_PASSWORD", "redis-pass")
    value = settings(
        monkeypatch,
        database_password_ref=SecretRef(uri="env://DB_PASSWORD"),
        redis_password_ref=SecretRef(uri="env://REDIS_PASSWORD"),
    )
    secrets = LocalSecretProvider()
    assert cli._database_dsn(value, secrets) == "postgresql://trpc_runtime:p%40ss@db/service"
    assert cli._migration_database_dsn(value, secrets) == (
        "postgresql+psycopg://trpc_runtime:p%40ss@db/service"
    )
    assert cli._redis_url(value, secrets) == "redis://:redis-pass@redis:6379/0"
    assert cli._url_password("redis://localhost/path?x=1", "p") == "redis://:p@localhost/path?x=1"
    assert cli._url_password("postgresql://u@host:5432/db", "p") == (
        "postgresql://u:p@host:5432/db"
    )

    encoded = base64.urlsafe_b64encode(b"x" * 32).decode().rstrip("=")
    assert cli._secret_bytes(encoded, exact=32) == b"x" * 32
    assert cli._secret_bytes(encoded) == b"x" * 32
    assert cli._secret_bytes("r" * 32, exact=32) == b"r" * 32
    assert cli._secret_bytes("z" * 40) == b"z" * 40
    with pytest.raises(ValueError, match="exactly"):
        cli._secret_bytes("short", exact=32)
    with pytest.raises(ValueError, match="at least"):
        cli._secret_bytes("short")
    assert cli._derive_key(b"k" * 32, b"one") != cli._derive_key(b"k" * 32, b"two")
    with pytest.raises(ValueError, match="root key"):
        cli._derive_key(b"short", b"purpose")

    non_postgres = ServiceSettings(
        database_dsn_ref=SecretRef(uri="literal://sqlite:///tmp/service.db")
    )
    assert (
        cli._migration_database_dsn(non_postgres, LocalSecretProvider(allow_literal=True))
        == "sqlite:///tmp/service.db"
    )


def test_s3_artifact_store_wiring_is_secret_backed(monkeypatch) -> None:
    import boto3

    calls = []

    def client(service, **kwargs):
        calls.append((service, kwargs))
        return object()

    monkeypatch.setattr(boto3, "client", client)
    assert cli._s3_artifact_store(ServiceSettings(), LocalSecretProvider()) is None
    with pytest.raises(ValueError, match="requires access key"):
        cli._s3_artifact_store(
            ServiceSettings(s3_endpoint="http://minio:9000"),
            LocalSecretProvider(),
        )

    value = ServiceSettings(
        s3_endpoint="http://minio:9000",
        s3_access_key="access-id",
        s3_secret_key_ref=SecretRef(uri="literal://secret-value"),
        s3_bucket="artifacts",
        media_download_max_bytes=4096,
    )
    store = cli._s3_artifact_store(value, LocalSecretProvider(allow_literal=True))

    assert store is not None and store._bucket == "artifacts"
    assert store._max_size_bytes == 4096
    assert calls[0][0] == "s3"
    assert calls[0][1]["endpoint_url"] == "http://minio:9000"
    assert calls[0][1]["aws_secret_access_key"] == "secret-value"


def test_migrate_restores_environment_and_serve_command(monkeypatch) -> None:
    settings(monkeypatch)
    upgrades = []
    monkeypatch.setattr(
        "alembic.command.upgrade", lambda config, revision: upgrades.append(revision)
    )
    runner = CliRunner()
    monkeypatch.delenv("TRPC_MIGRATION_DATABASE_DSN", raising=False)
    result = runner.invoke(cli.app, ["migrate", "--revision", "head"])
    assert result.exit_code == 0 and upgrades == ["head"]
    assert "TRPC_MIGRATION_DATABASE_DSN" not in __import__("os").environ
    monkeypatch.setenv("TRPC_MIGRATION_DATABASE_DSN", "previous")
    assert runner.invoke(cli.app, ["migrate", "--revision", "base"]).exit_code == 0
    assert __import__("os").environ["TRPC_MIGRATION_DATABASE_DSN"] == "previous"

    ran = []

    def run(coroutine):
        ran.append(coroutine)
        coroutine.close()

    monkeypatch.setattr(cli.asyncio, "run", run)
    monkeypatch.setattr(cli, "configure_logging", lambda level: None)
    assert runner.invoke(cli.app, ["serve", "--role", "worker"]).exit_code == 0
    assert ran


def test_alembic_config_resolution(monkeypatch, tmp_path) -> None:
    configured = tmp_path / "configured.ini"
    configured.write_text("[alembic]\n", encoding="utf-8")
    monkeypatch.setenv("TRPC_SERVICE_ALEMBIC_CONFIG", str(configured))
    assert cli._alembic_config_path() == configured

    monkeypatch.setenv("TRPC_SERVICE_ALEMBIC_CONFIG", str(tmp_path / "missing.ini"))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "__file__", str(tmp_path / "package" / "_cli.py"))
    with pytest.raises(RuntimeError, match=r"alembic\.ini"):
        cli._alembic_config_path()


def test_doctor_failure_report_is_machine_readable(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(cli, "version", lambda package: "1.1.17")
    output = tmp_path / "failed.json"
    result = CliRunner().invoke(cli.app, ["doctor", "--output", str(output)])
    assert result.exit_code == 1
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["gate"] == "fail" and "sdk_locked_1_1_19" in payload["rejection_reasons"]


def test_doctor_stdout_and_probe_command_paths(monkeypatch) -> None:
    runner = CliRunner()
    doctor = runner.invoke(cli.app, ["doctor"])
    assert doctor.exit_code == 0
    assert json.loads(doctor.stdout)["gate"] == "pass"

    outcomes = iter((True, False))

    def run(coroutine):
        coroutine.close()
        return next(outcomes)

    monkeypatch.setattr(cli.asyncio, "run", run)
    assert runner.invoke(cli.app, ["probe", "--role", "admin"]).exit_code == 0
    assert runner.invoke(cli.app, ["probe", "--role", "worker"]).exit_code == 1


class Resource:
    def __init__(self) -> None:
        self.closed = False
        self.pool = object()

    async def close(self):
        self.closed = True

    async def aclose(self):
        self.closed = True

    async def ready(self):
        return True

    async def xgroup_create(self, *_args, **_kwargs):
        return None


@pytest.mark.asyncio
async def test_lightweight_probe_dependency_lifecycle(monkeypatch) -> None:
    value = settings(monkeypatch)
    # Probes now fail closed while the role is draining.  This unit test is
    # about dependency lifecycle/cleanup, so provide the live-role marker
    # contract explicitly instead of relying on a machine-local state file.
    monkeypatch.setattr("trpc_service.lifecycle.is_process_ready", lambda *_args: True)
    repositories = []
    redis_values = []
    redis_options = []

    async def create(*_args, **_kwargs):
        resource = Resource()
        repositories.append(resource)
        return resource

    def from_url(*_args, **_kwargs):
        redis_options.append(_kwargs)
        resource = Resource()

        async def ping():
            return True

        resource.ping = ping
        redis_values.append(resource)
        return resource

    monkeypatch.setattr("trpc_service.storage.postgres.PostgresRuntimeRepository.create", create)
    monkeypatch.setattr("redis.asyncio.from_url", from_url)

    assert await cli._probe_dependencies(Role.WORKER, value)
    assert await cli._probe_dependencies(
        Role.ADMIN, value.model_copy(update={"worker_database_dsn_ref": None})
    )
    assert redis_options == [
        {
            "decode_responses": False,
            "socket_connect_timeout": value.redis_socket_connect_timeout_seconds,
            "socket_timeout": value.redis_socket_timeout_seconds,
            "retry_on_timeout": False,
        }
    ]
    assert all(item.closed for item in [*repositories, *redis_values])

    async def fail_create(*_args, **_kwargs):
        raise OSError("database unavailable")

    monkeypatch.setattr(
        "trpc_service.storage.postgres.PostgresRuntimeRepository.create", fail_create
    )
    assert not await cli._probe_dependencies(Role.ADMIN, value)


@pytest.mark.asyncio
async def test_top_level_role_dispatch_and_cleanup(monkeypatch) -> None:
    value = settings(monkeypatch, database_pool_min_size=3, database_pool_max_size=7)
    repositories = []
    redis_values = []
    create_options = []

    async def create(*args, **kwargs):
        resource = Resource()
        repositories.append(resource)
        create_options.append(kwargs)
        return resource

    def from_url(*args, **kwargs):
        resource = Resource()
        redis_values.append(resource)
        return resource

    monkeypatch.setattr("trpc_service.storage.postgres.PostgresRuntimeRepository.create", create)
    monkeypatch.setattr("redis.asyncio.from_url", from_url)
    monkeypatch.setattr("trpc_service.metrics.setup.configure_tracing", lambda **kwargs: None)
    called = []

    def handler(name):
        async def run(*args):
            called.append(name)

        return run

    mapping = {
        Role.GATEWAY: "_serve_gateway",
        Role.ADMIN: "_serve_admin",
        Role.WORKER: "_serve_worker",
        Role.OUTBOX_DISPATCHER: "_serve_outbox",
        Role.CHANNEL_DISPATCHER: "_serve_channel_dispatcher",
        Role.POST_TURN_PROJECTOR: "_serve_projector",
        Role.WECOM_CONNECTOR: "_serve_wecom",
        Role.SESSION_RECOVERY: "_serve_session_recovery",
        Role.ARTIFACT_GC: "_serve_artifact_gc",
    }
    for name in mapping.values():
        monkeypatch.setattr(cli, name, handler(name))
    for role in Role:
        role_settings = value
        if role not in cli._WORKER_DATABASE_ROLES:
            role_settings = value.model_copy(update={"worker_database_dsn_ref": None})
        await cli._serve(role, role_settings)
    assert called == list(mapping.values())
    assert all(item.closed for item in [*repositories, *redis_values])
    assert create_options == [
        {
            "min_size": 3,
            "max_size": 7,
            "ready_replay_cooldown_seconds": value.recovery_ready_replay_cooldown_seconds,
        }
    ] * len(Role)


class RunObject:
    runs = 0
    closes = 0

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.pool = object()

    async def run(self, *args, **kwargs):
        type(self).runs += 1

    async def serve(self):
        type(self).runs += 1

    async def ensure_group(self):
        return None

    async def close(self):
        type(self).closes += 1

    async def accept(self, *args):
        return None

    def ready_for_delivery(self, _binding):
        return True


@pytest.mark.asyncio
async def test_individual_role_wiring(monkeypatch) -> None:
    value = settings(
        monkeypatch,
        worker_concurrency=7,
        redis_stream="isolated-stream",
        redis_consumer_group="isolated-group",
        redis_reclaim_after_ms=12_345,
    )
    secrets = LocalSecretProvider()
    repo = Resource()
    redis = Resource()
    RunObject.runs = 0
    RunObject.closes = 0

    monkeypatch.setattr("uvicorn.Config", lambda *args, **kwargs: object())
    monkeypatch.setattr("uvicorn.Server", RunObject)
    monkeypatch.setattr("trpc_service.channels.feishu.FeishuAdapter", RunObject)
    monkeypatch.setattr("trpc_service.queue.emergency.EmergencyQueue", RunObject)
    monkeypatch.setattr("trpc_service.runtime.TenantRuntime", RunObject)
    monkeypatch.setattr("trpc_service.web.feishu_gateway.FeishuGatewayService", RunObject)
    monkeypatch.setattr(
        "trpc_service.web.feishu_gateway.create_feishu_gateway_router", lambda service: APIRouter()
    )
    await cli._serve_gateway(value, secrets, repo, redis)

    monkeypatch.setattr("trpc_service.tenant.auth.DevelopmentAuthorizer", RunObject)
    monkeypatch.setattr("trpc_service.tenant.auth.OidcAuthorizer", RunObject)
    monkeypatch.setattr("trpc_service.tenant.control.PostgresControlPlaneRepository", RunObject)
    monkeypatch.setattr("trpc_service.web.admin.create_admin_router", lambda *args: APIRouter())
    await cli._serve_admin(value, secrets, repo)
    production = ServiceSettings(
        environment=Environment.PRODUCTION,
        allow_development_token=False,
        oidc_issuer="https://issuer",
        oidc_audience="audience",
        port=9000,
    )
    await cli._serve_admin(production, secrets, repo)

    for target in (
        "trpc_service.agent.factory.ProductionAgentLoader",
        "trpc_service.agent.worker.AgentWorker",
        "trpc_service.queue.redis_streams.RedisStreamQueue",
        "trpc_service.queue.worker_consumer.WorkerConsumer",
        "trpc_service.queue.session_ready.SessionReadyQueue",
        "trpc_service.queue.session_ready.SessionReadyReclaimer",
        "trpc_service.queue.session_worker_consumer.SessionWorkerConsumer",
        "trpc_service.agent.mailbox_runtime.MailboxReadyClaimer",
        "trpc_service.agent.mailbox_runtime.MailboxClaimExecutor",
        "trpc_service.queue.dispatcher.OutboxDispatcher",
        "trpc_service.queue.emergency.EmergencyQueueDrainer",
        "trpc_service.channels.dispatcher.ChannelDispatcher",
        "trpc_service.storage.projector.PostTurnProjector",
        "trpc_service.storage.redis_projection.RedisProjectionStore",
        "trpc_service.agent.wecom_manager.WeComConnectionManager",
        "trpc_service.channels.wecom.WeComConnector",
        "trpc_service.storage.postgres.PostgresBindingLease",
    ):
        monkeypatch.setattr(target, RunObject)
    queue_options = []
    consumer_options = []
    ready_queue_options = []
    v2_consumer_options = []

    class QueueObject(RunObject):
        def __init__(self, *args, **kwargs):
            queue_options.append(kwargs)
            super().__init__(*args, **kwargs)

    class ConsumerObject(RunObject):
        def __init__(self, *args, **kwargs):
            consumer_options.append(kwargs)
            super().__init__(*args, **kwargs)

    class ReadyQueueObject(RunObject):
        def __init__(self, *args, **kwargs):
            ready_queue_options.append(kwargs)
            super().__init__(*args, **kwargs)

    class V2ConsumerObject(RunObject):
        def __init__(self, *args, **kwargs):
            v2_consumer_options.append(kwargs)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr("trpc_service.queue.redis_streams.RedisStreamQueue", QueueObject)
    monkeypatch.setattr("trpc_service.queue.worker_consumer.WorkerConsumer", ConsumerObject)
    monkeypatch.setattr("trpc_service.queue.session_ready.SessionReadyQueue", ReadyQueueObject)
    monkeypatch.setattr(
        "trpc_service.queue.session_worker_consumer.SessionWorkerConsumer", V2ConsumerObject
    )
    await cli._serve_worker(value, secrets, repo, redis)
    await cli._serve_outbox(value, secrets, repo, redis)
    await cli._serve_channel_dispatcher(value, secrets, repo)
    await cli._serve_projector(value, repo, redis)
    await cli._serve_wecom(value, secrets, repo)
    assert RunObject.runs >= 9 and RunObject.closes >= 2
    assert queue_options == []
    assert consumer_options == []
    assert ready_queue_options == [
        {
            "stream": "isolated-stream",
            "group": "isolated-group",
        },
        {
            "stream": "isolated-stream",
            "group": "isolated-group",
        },
    ]
    assert v2_consumer_options[0]["concurrency"] == 7
