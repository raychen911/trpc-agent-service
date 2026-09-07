from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
import yaml
from typer.testing import CliRunner

from tenant_agent import cli
from tenant_agent.models import ChannelType
from tenant_agent.services.native_session import (
    ProvisionedSqlSessionService,
    provision_native_sql_schema,
    validate_native_sql_schema,
)
from tenant_agent.storage.sql import SqlPlane
from tests.helpers import make_tenant

runner = CliRunner()


def test_validate_doctor_and_capacity_commands(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tenant = make_tenant()
    config = tmp_path / "tenant.yaml"
    config.write_text(
        yaml.safe_dump({"tenants": [tenant.model_dump(mode="json")]}, sort_keys=False),
        encoding="utf-8",
    )
    validated = runner.invoke(cli.app, ["validate-config", str(config)])
    assert validated.exit_code == 0
    assert '"valid": true' in validated.stdout
    assert tenant.tenant_id in validated.stdout

    invalid = tmp_path / "invalid-tenant.yaml"
    leaked_value = "accidentally-pasted-super-secret"
    invalid.write_text(
        yaml.safe_dump(
            {
                "tenants": [
                    {
                        **tenant.model_dump(mode="json"),
                        "models": {
                            "offline": {
                                "provider": "openai-compatible",
                                "model_name": "remote",
                                "api_key_ref": {"uri": leaked_value},
                            }
                        },
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    invalid_result = runner.invoke(cli.app, ["validate-config", str(invalid)])
    assert invalid_result.exit_code == 2
    assert '"valid": false' in invalid_result.output
    assert leaked_value not in invalid_result.output

    monkeypatch.setenv("TAP_CONTROL_DATABASE_URL", "inmemory://")
    doctor = runner.invoke(cli.app, ["doctor"])
    assert doctor.exit_code == 0
    assert '"trpc_agent_py": "1.1.19"' in doctor.stdout
    assert '"control_backend": "inmemory"' in doctor.stdout
    monkeypatch.setenv("TENANT_DEMO_WEBHOOK_TOKEN", "development-webhook-token")
    monkeypatch.setenv(
        "TAP_BOOTSTRAP_CONFIG_PATH",
        str(Path("config/tenants.example.yaml").resolve()),
    )
    probe = runner.invoke(
        cli.app,
        [
            "probe-channel",
            "--channel",
            "web",
            "--binding-id",
            "web-demo-001",
        ],
    )
    assert probe.exit_code == 0, probe.stdout
    assert '"ok": true' in probe.stdout

    capacity = runner.invoke(
        cli.app,
        [
            "capacity",
            "--peak-rps",
            "10",
            "--p95-latency",
            "2",
            "--concurrency-per-worker",
            "5",
        ],
    )
    assert capacity.exit_code == 0
    assert '"recommended_worker_replicas": 6' in capacity.stdout


def test_db_init_inmemory_and_sqlite_migration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TAP_CONTROL_DATABASE_URL", "inmemory://")
    memory = runner.invoke(cli.app, ["db-init"])
    assert memory.exit_code == 0
    assert "does not require" in memory.stdout

    database = tmp_path / "control.db"
    monkeypatch.setenv(
        "TAP_CONTROL_DATABASE_URL",
        f"sqlite+aiosqlite:///{database.as_posix()}",
    )
    migrated = runner.invoke(cli.app, ["db-init"])
    assert migrated.exit_code == 0
    assert "migrations applied" in migrated.stdout
    import sqlite3

    connection = sqlite3.connect(database)
    tables = {
        row[0] for row in connection.execute("select name from sqlite_master where type = ?", ("table",))
    }
    connection.close()
    assert {
        "alembic_version",
        "tenants",
        "session_events",
        "audit_logs",
        "usage_reservations",
    } <= tables
    from io import StringIO

    from alembic import command
    from alembic.config import Config

    alembic_config = Config(str(Path(cli.__file__).resolve().parents[2] / "alembic.ini"))
    alembic_config.stdout = StringIO()
    alembic_config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database.as_posix()}")
    command.check(alembic_config)

    upgrade_database = tmp_path / "upgrade-path.db"
    upgrade_config = Config(str(Path(cli.__file__).resolve().parents[2] / "alembic.ini"))
    upgrade_config.stdout = StringIO()
    upgrade_config.set_main_option(
        "sqlalchemy.url",
        f"sqlite+aiosqlite:///{upgrade_database.as_posix()}",
    )
    monkeypatch.setenv(
        "TAP_CONTROL_DATABASE_URL",
        f"sqlite+aiosqlite:///{upgrade_database.as_posix()}",
    )
    command.upgrade(upgrade_config, "1dc9c6ba78b8")
    connection = sqlite3.connect(upgrade_database)
    before_indexes = {row[1] for row in connection.execute("pragma index_list('inbound_receipts')")}
    connection.close()
    assert "ix_receipts_status_updated" not in before_indexes
    command.upgrade(upgrade_config, "head")
    connection = sqlite3.connect(upgrade_database)
    after_indexes = {row[1] for row in connection.execute("pragma index_list('inbound_receipts')")}
    connection.close()
    assert "ix_receipts_status_updated" in after_indexes

    migrate_help = runner.invoke(cli.app, ["migrate-data", "--help"])
    assert migrate_help.exit_code == 0
    assert "--embedding-map" in migrate_help.stdout
    assert "--golden-queries" in migrate_help.stdout
    assert "Explicitly allow" in migrate_help.stdout
    assert "native" in migrate_help.stdout

    resource_database = tmp_path / "summary-resource.db"
    monkeypatch.setenv(
        "TENANT_ALPHA_SUMMARY_SQL",
        f"sqlite+aiosqlite:///{resource_database.as_posix()}",
    )
    resource_migration = runner.invoke(
        cli.app,
        ["db-init", "--database-url-env", "TENANT_ALPHA_SUMMARY_SQL"],
    )
    assert resource_migration.exit_code == 0
    connection = sqlite3.connect(resource_database)
    assert (
        connection.execute(
            "select count(*) from sqlite_master where type='table' and name='summaries'"
        ).fetchone()[0]
        == 1
    )
    connection.close()

    native_database = tmp_path / "native-session.db"
    monkeypatch.setenv(
        "TENANT_ALPHA_NATIVE_SQL",
        f"sqlite+aiosqlite:///{native_database.as_posix()}",
    )
    native_init = runner.invoke(
        cli.app,
        ["native-session-init", "--database-url-env", "TENANT_ALPHA_NATIVE_SQL"],
    )
    assert native_init.exit_code == 0
    asyncio.run(validate_native_sql_schema(f"sqlite+aiosqlite:///{native_database.as_posix()}"))


def test_migrate_data_command_hash_verifies_sql_to_sql(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    source_url = f"sqlite+aiosqlite:///{source_path.as_posix()}"
    target_url = f"sqlite+aiosqlite:///{target_path.as_posix()}"

    async def seed() -> None:
        source = SqlPlane(source_url)
        target = SqlPlane(target_url)
        await source.initialize()
        await target.initialize()
        snapshot = await source.get_or_create_session(
            tenant_id="alpha",
            app_id="assistant",
            session_id="session",
            user_id="user",
            channel="web",
        )
        await source.append_event(
            snapshot=snapshot,
            event_id="event",
            kind="user_message",
            actor_id="user",
            payload={"text": "hello"},
            state_delta={},
            trace_id="0" * 32,
        )
        await source.close()
        await target.close()

    asyncio.run(seed())
    monkeypatch.setenv("SOURCE_TEST_DSN", source_url)
    monkeypatch.setenv("TARGET_TEST_DSN", target_url)
    result = runner.invoke(
        cli.app,
        [
            "migrate-data",
            "--tenant-id",
            "alpha",
            "--source-kind",
            "sql",
            "--source-dsn-env",
            "SOURCE_TEST_DSN",
            "--target-kind",
            "sql",
            "--target-dsn-env",
            "TARGET_TEST_DSN",
            "--resources",
            "sessions",
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert '"verified": true' in result.stdout
    assert '"sessions": 1' in result.stdout


def test_migrate_data_resumes_native_sql_history_with_isolated_dsns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hashlib
    import time

    from trpc_agent_sdk.events import Event
    from trpc_agent_sdk.sessions import SessionServiceConfig
    from trpc_agent_sdk.types import EventActions

    source_platform_url = f"sqlite+aiosqlite:///{(tmp_path / 'source-platform.db').as_posix()}"
    target_platform_url = f"sqlite+aiosqlite:///{(tmp_path / 'target-platform.db').as_posix()}"
    source_native_url = f"sqlite+aiosqlite:///{(tmp_path / 'source-native.db').as_posix()}"
    target_native_url = f"sqlite+aiosqlite:///{(tmp_path / 'target-native.db').as_posix()}"
    tenant_scope = hashlib.sha256(b"alpha").hexdigest()[:16]
    app_name = f"tap:{tenant_scope}:assistant"

    async def provision_and_seed() -> None:
        source_platform = SqlPlane(source_platform_url)
        target_platform = SqlPlane(target_platform_url)
        await source_platform.initialize()
        await target_platform.initialize()
        await source_platform.get_or_create_session(
            tenant_id="alpha",
            app_id="assistant",
            session_id="session",
            user_id="user",
            channel="web",
        )
        await source_platform.close()
        await target_platform.close()
        await provision_native_sql_schema(source_native_url)
        await provision_native_sql_schema(target_native_url)
        config = SessionServiceConfig(max_events=200, store_historical_events=True)
        source_native = ProvisionedSqlSessionService(
            db_url=source_native_url,
            is_async=True,
            session_config=config,
            expire_on_commit=False,
        )
        target_native = ProvisionedSqlSessionService(
            db_url=target_native_url,
            is_async=True,
            session_config=config,
            expire_on_commit=False,
        )
        source_session = await source_native.create_session(
            app_name=app_name,
            user_id="user",
            session_id="session",
        )
        target_session = await target_native.create_session(
            app_name=app_name,
            user_id="user",
            session_id="session",
        )
        base_timestamp = time.time()
        events = (
            Event(
                id="native-event-1",
                timestamp=base_timestamp,
                invocation_id="invocation-1",
                author="assistant",
                actions=EventActions(state_delta={"step": 1}),
            ),
            Event(
                id="native-event-2",
                timestamp=base_timestamp + 0.001,
                invocation_id="invocation-2",
                author="assistant",
                actions=EventActions(state_delta={"step": 2}),
            ),
        )
        await source_native.append_event(session=source_session, event=events[0].model_copy(deep=True))
        await source_native.append_event(session=source_session, event=events[1].model_copy(deep=True))
        # Simulate a prior migration process that committed only the first event.
        await target_native.append_event(session=target_session, event=events[0].model_copy(deep=True))
        await source_native.close()
        await target_native.close()

    asyncio.run(provision_and_seed())
    monkeypatch.setenv("SOURCE_PLATFORM_DSN", source_platform_url)
    monkeypatch.setenv("TARGET_PLATFORM_DSN", target_platform_url)
    monkeypatch.setenv("SOURCE_NATIVE_DSN", source_native_url)
    monkeypatch.setenv("TARGET_NATIVE_DSN", target_native_url)
    base_arguments = [
        "migrate-data",
        "--tenant-id",
        "alpha",
        "--source-kind",
        "sql",
        "--source-dsn-env",
        "SOURCE_PLATFORM_DSN",
        "--target-kind",
        "sql",
        "--target-dsn-env",
        "TARGET_PLATFORM_DSN",
        "--resources",
        "sessions",
        "--include-native-session-history",
    ]
    missing_isolated_dsns = runner.invoke(cli.app, base_arguments)
    assert missing_isolated_dsns.exit_code == 2

    arguments = [
        *base_arguments,
        "--source-native-dsn-env",
        "SOURCE_NATIVE_DSN",
        "--target-native-dsn-env",
        "TARGET_NATIVE_DSN",
    ]
    monkeypatch.setenv("TARGET_NATIVE_DSN", target_platform_url)
    shared_native_database = runner.invoke(cli.app, arguments)
    assert shared_native_database.exit_code == 2
    monkeypatch.setenv("TARGET_NATIVE_DSN", target_native_url)
    resumed = runner.invoke(cli.app, arguments)
    assert resumed.exit_code == 0, f"{resumed.output}\n{resumed.exception!r}"
    assert '"native_trpc_events": 1' in resumed.stdout
    idempotent = runner.invoke(cli.app, arguments)
    assert idempotent.exit_code == 0, idempotent.stdout
    assert '"native_trpc_events": 0' in idempotent.stdout

    async def verify_target() -> None:
        target_native = ProvisionedSqlSessionService(
            db_url=target_native_url,
            is_async=True,
            session_config=SessionServiceConfig(max_events=200, store_historical_events=True),
            expire_on_commit=False,
        )
        session = await target_native.get_session(
            app_name=app_name,
            user_id="user",
            session_id="session",
        )
        assert session is not None
        assert session.state == {"step": 2}
        assert [event.id for event in (*session.historical_events, *session.events)] == [
            "native-event-1",
            "native-event-2",
        ]
        await target_native.close()

    asyncio.run(verify_target())


def test_wecom_bot_probe_uses_env_without_printing_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, str] = {}

    class FakeConnection:
        def __init__(self) -> None:
            self.closed = asyncio.Event()

        @asynccontextmanager
        async def connected(self, bot_id: str, secret: str, handler: object):
            del handler
            observed.update(bot_id=bot_id, secret=secret)
            yield self

        async def request(self, command: str) -> None:
            assert command == "ping"

    monkeypatch.setenv("BOT_ID_FOR_TEST", "aibot_12345678")
    monkeypatch.setenv("BOT_SECRET_FOR_TEST", "secret-value-123456789")
    monkeypatch.setattr(cli, "WeComBotConnection", FakeConnection)
    result = runner.invoke(
        cli.app,
        [
            "wecom-bot",
            "--probe-only",
            "--bot-id-env",
            "BOT_ID_FOR_TEST",
            "--bot-secret-env",
            "BOT_SECRET_FOR_TEST",
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert observed == {"bot_id": "aibot_12345678", "secret": "secret-value-123456789"}
    assert observed["secret"] not in result.stdout


def test_wecom_bot_can_use_a_configured_model_without_replacing_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    class FakeConnection:
        @asynccontextmanager
        async def connected(self, bot_id: str, secret: str, handler: object):
            del bot_id, secret, handler
            yield self

        async def request(self, command: str) -> None:
            del command

    def fake_app(settings: object) -> object:
        captured["settings"] = settings
        return object()

    monkeypatch.setenv("BOT_ID_FOR_TEST", "aibot_12345678")
    monkeypatch.setenv("BOT_SECRET_FOR_TEST", "secret-value-123456789")
    monkeypatch.setattr(cli, "WeComBotConnection", FakeConnection)
    monkeypatch.setattr("tenant_agent.main.create_app", fake_app)
    monkeypatch.setattr(cli.uvicorn, "run", lambda app, **kwargs: captured.update(uvicorn=(app, kwargs)))
    config = tmp_path / "configured.yaml"
    config.write_text("tenants: []\n", encoding="utf-8")
    result = runner.invoke(
        cli.app,
        [
            "wecom-bot",
            "--config",
            str(config),
            "--bot-id-env",
            "BOT_ID_FOR_TEST",
            "--bot-secret-env",
            "BOT_SECRET_FOR_TEST",
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert captured["settings"].bootstrap_config_path == config  # type: ignore[union-attr]
    assert "secret-value-123456789" not in result.stdout


def test_probe_channel_supports_wecom_bot_without_printing_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant = make_tenant(channel=ChannelType.WECOM_BOT, binding_id="wecom-bot-001")

    class FakeSecrets:
        async def resolve(self, reference: object) -> str:
            uri = str(reference.uri)  # type: ignore[attr-defined]
            return "aibot_12345678" if uri.endswith("BOT_ID") else "secret-value-123456789"

    class FakeContainer:
        secrets = FakeSecrets()

        class Configs:
            async def resolve_binding(self, channel: str, binding_id: str) -> object:
                assert channel == "wecom_bot" and binding_id == "wecom-bot-001"
                return tenant

        configs = Configs()

        class Channels:
            def get(self, channel: ChannelType) -> None:
                del channel
                return None

        channels = Channels()

        async def initialize(self) -> None:
            return None

        async def close(self) -> None:
            return None

    class FakeConnection:
        @asynccontextmanager
        async def connected(self, bot_id: str, secret: str, handler: object):
            del bot_id, secret, handler
            yield self

        async def request(self, command: str) -> None:
            assert command == "ping"

    monkeypatch.setattr("tenant_agent.container.ApplicationContainer.build", lambda settings: FakeContainer())
    monkeypatch.setattr(cli, "WeComBotConnection", FakeConnection)
    monkeypatch.setenv("TENANT_ALPHA_WECOM_BOT_ID", "aibot_12345678")
    monkeypatch.setenv("TENANT_ALPHA_WECOM_BOT_SECRET", "secret-value-123456789")
    result = runner.invoke(
        cli.app,
        ["probe-channel", "--channel", "wecom_bot", "--binding-id", "wecom-bot-001"],
    )
    assert result.exit_code == 0, result.stdout
    assert "authenticated" in result.stdout
    assert "secret-value-123456789" not in result.stdout


def test_local_vector_migration_is_read_only_and_empty_requires_opt_in(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_source = tmp_path / "misspelled-source.db"
    empty_source = tmp_path / "empty-source.db"
    empty_target = tmp_path / "empty-target.db"

    async def provision() -> None:
        for path in (empty_source, empty_target):
            plane = SqlPlane(f"sqlite+aiosqlite:///{path.as_posix()}")
            await plane.initialize()
            await plane.close()

    asyncio.run(provision())
    monkeypatch.setenv("SOURCE_VECTOR", str(missing_source))
    monkeypatch.setenv("TARGET_VECTOR", str(empty_target))
    missing = runner.invoke(
        cli.app,
        [
            "migrate-data",
            "--tenant-id",
            "alpha",
            "--source-kind",
            "local-vector",
            "--source-dsn-env",
            "SOURCE_VECTOR",
            "--target-kind",
            "local-vector",
            "--target-dsn-env",
            "TARGET_VECTOR",
            "--resources",
            "knowledge",
        ],
    )
    assert missing.exit_code == 2
    assert not missing_source.exists()

    monkeypatch.setenv("SOURCE_VECTOR", str(empty_source))
    rejected_empty = runner.invoke(
        cli.app,
        [
            "migrate-data",
            "--tenant-id",
            "alpha",
            "--source-kind",
            "local-vector",
            "--source-dsn-env",
            "SOURCE_VECTOR",
            "--target-kind",
            "local-vector",
            "--target-dsn-env",
            "TARGET_VECTOR",
            "--resources",
            "knowledge",
        ],
    )
    assert rejected_empty.exit_code == 2
    assert '"empty_source"' in rejected_empty.stdout

    allowed_empty = runner.invoke(
        cli.app,
        [
            "migrate-data",
            "--tenant-id",
            "alpha",
            "--source-kind",
            "local-vector",
            "--source-dsn-env",
            "SOURCE_VECTOR",
            "--target-kind",
            "local-vector",
            "--target-dsn-env",
            "TARGET_VECTOR",
            "--resources",
            "knowledge",
            "--allow-empty-source",
        ],
    )
    assert allowed_empty.exit_code == 0


def test_serve_builds_selected_role_without_starting_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_run(app: object, **kwargs: object) -> None:
        captured["app"] = app
        captured.update(kwargs)

    monkeypatch.setattr(cli.uvicorn, "run", fake_run)
    result = runner.invoke(
        cli.app,
        ["serve", "--role", "gateway", "--host", "127.0.0.1", "--port", "9090"],
    )
    assert result.exit_code == 0
    assert captured["port"] == 9090
    assert captured["host"] == "127.0.0.1"


def test_live_channel_probe_covers_telegram_and_wecom_without_sending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    telegram = make_tenant(
        "alpha",
        channel=ChannelType.TELEGRAM,
        binding_id="telegram-live-001",
    )
    wecom = make_tenant(
        "beta",
        channel=ChannelType.WECOM,
        binding_id="wecom-live-001",
    )
    config = tmp_path / "channels.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "tenants": [
                    telegram.model_dump(mode="json"),
                    wecom.model_dump(mode="json"),
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TAP_CONTROL_DATABASE_URL", "inmemory://")
    monkeypatch.setenv("TAP_BOOTSTRAP_CONFIG_PATH", str(config))
    monkeypatch.setenv("TENANT_ALPHA_TELEGRAM_SECRET", "webhook-secret")
    monkeypatch.setenv("TENANT_ALPHA_TELEGRAM_TOKEN", "123456:telegram-token-value")
    monkeypatch.setenv("TENANT_BETA_WECOM_CALLBACK_TOKEN", "callbacktoken")
    monkeypatch.setenv(
        "TENANT_BETA_WECOM_ENCODING_AES_KEY",
        base64.b64encode(b"k" * 32).decode().rstrip("="),
    )
    monkeypatch.setenv("TENANT_BETA_WECOM_CORP_ID", "wwcorp1234")
    monkeypatch.setenv("TENANT_BETA_WECOM_CORP_SECRET", "corpsecret123")
    monkeypatch.setenv("TENANT_BETA_WECOM_AGENT_ID", "100001")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/getMe"):
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "result": {"id": 42, "username": "probe_bot"},
                },
            )
        if request.url.path.endswith("/cgi-bin/gettoken"):
            return httpx.Response(
                200,
                json={"errcode": 0, "access_token": "discarded"},
            )
        return httpx.Response(404)

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        cli.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler)),
    )
    telegram_probe = runner.invoke(
        cli.app,
        [
            "probe-channel",
            "--channel",
            "telegram",
            "--binding-id",
            "telegram-live-001",
        ],
    )
    assert telegram_probe.exit_code == 0, telegram_probe.stdout
    assert "probe_bot" in telegram_probe.stdout
    assert "telegram-token-value" not in telegram_probe.stdout

    wecom_probe = runner.invoke(
        cli.app,
        [
            "probe-channel",
            "--channel",
            "wecom",
            "--binding-id",
            "wecom-live-001",
        ],
    )
    assert wecom_probe.exit_code == 0, wecom_probe.stdout
    assert "access token acquired and discarded" in wecom_probe.stdout
    assert "corp-secret" not in wecom_probe.stdout
