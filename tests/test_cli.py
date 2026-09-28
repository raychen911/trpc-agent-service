from pathlib import Path
from typing import Any

import pytest

from trpc_service import _cli
from trpc_service.config import Settings


def test_cli_runs_the_fastapi_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setenv("TRPC_SERVICE_PORT", "8123")

    def fake_run(app: str, **options: Any) -> None:
        captured["app"] = app
        captured.update(options)

    monkeypatch.setattr(_cli.uvicorn, "run", fake_run)

    _cli.main()

    assert captured["app"] == "trpc_service.web.app:create_default_app"
    assert captured["factory"] is True
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8123
    assert captured["log_level"] == "info"
    log_config = captured["log_config"]
    assert (log_config["formatters"]["default"]["()"] == "trpc_service.log.JsonLogFormatter")
    assert (log_config["formatters"]["access"]["()"] == "trpc_service.log.JsonLogFormatter")


def test_empty_database_password_file_is_rejected(tmp_path: Path) -> None:
    password_file = tmp_path / "empty_password"
    password_file.touch()
    settings = Settings(database_password_file=password_file)

    with pytest.raises(ValueError, match="database password file is empty"):
        settings.resolved_database_url


def test_safe_log_config_routes_all_loggers_to_rotating_json_file(tmp_path: Path) -> None:
    """A configured local log file creates its directory and becomes the sole sink."""

    log_file = tmp_path / "logs" / "service.jsonl"
    config = _cli._safe_log_config(
        Settings(
            _env_file=None,
            log_file=log_file,
            log_level="warning",
            log_max_bytes=1_048_576,
            log_backup_count=2,
        ))

    assert log_file.parent.is_dir()
    assert config["root"] == {"handlers": ["json_file"], "level": "WARNING"}
    handler = config["handlers"]["json_file"]  # type: ignore[index]
    assert handler["filename"] == str(log_file)
    assert handler["maxBytes"] == 1_048_576
    assert all(
        logger.get("handlers") == ["json_file"]
        for logger in config["loggers"].values()  # type: ignore[union-attr]
        if isinstance(logger, dict) and "handlers" in logger)


@pytest.mark.anyio
async def test_background_role_initializes_and_closes_every_resource(
    monkeypatch: pytest.MonkeyPatch, ) -> None:
    """A background process starts storage before services and unwinds in reverse order."""

    events: list[str] = []

    class Storage:

        async def initialize(self) -> None:
            events.append("storage.initialize")

        async def close(self) -> None:
            events.append("storage.close")

    class Container:
        storage_composition = Storage()

        async def start(self) -> None:
            events.append("container.start")

        async def close(self) -> None:
            events.append("container.close")

    class Engine:

        async def dispose(self) -> None:
            events.append("engine.dispose")

    class StopEvent:

        def set(self) -> None:
            events.append("signal")

        async def wait(self) -> None:
            events.append("wait")

    class Loop:

        def add_signal_handler(self, signum: object, callback: object) -> None:
            del signum, callback
            events.append("handler")

    settings = Settings(
        _env_file=None,
        runtime_role="worker",
        worker_concurrency=1,
    )
    monkeypatch.setattr(_cli, "get_settings", lambda: settings)
    monkeypatch.setattr(_cli.logging.config, "dictConfig", lambda config: events.append("logging"))
    monkeypatch.setattr(_cli, "build_engine", lambda configured: Engine())
    monkeypatch.setattr(_cli, "build_session_factory", lambda engine: object())
    monkeypatch.setattr(
        _cli,
        "build_application_container",
        lambda **kwargs: Container(),
    )
    monkeypatch.setattr(_cli.asyncio, "Event", StopEvent)
    monkeypatch.setattr(_cli.asyncio, "get_running_loop", lambda: Loop())

    await _cli._run_background_role()

    assert events == [
        "logging",
        "handler",
        "handler",
        "storage.initialize",
        "container.start",
        "wait",
        "container.close",
        "storage.close",
        "engine.dispose",
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("role", "concurrency", "message"),
    [
        ("worker", 0, "positive worker concurrency"),
        ("channel", 1, "must not run Agent Worker slots"),
    ],
)
async def test_background_role_rejects_incompatible_concurrency(
    monkeypatch: pytest.MonkeyPatch,
    role: str,
    concurrency: int,
    message: str,
) -> None:
    settings = Settings(
        _env_file=None,
        runtime_role=role,
        worker_concurrency=concurrency,
    )
    monkeypatch.setattr(_cli, "get_settings", lambda: settings)
    monkeypatch.setattr(_cli.logging.config, "dictConfig", lambda config: None)

    with pytest.raises(ValueError, match=message):
        await _cli._run_background_role()


@pytest.mark.anyio
@pytest.mark.parametrize("scaler_mode", ["local_process", "kubernetes"])
async def test_supervisor_reconciles_capacity_and_closes_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scaler_mode: str,
) -> None:
    """Supervisor owns capacity adapters while the Web process remains process-free."""

    events: list[str] = []
    namespace_file = tmp_path / "namespace"
    namespace_file.write_text("trpc-agent-service\n", encoding="utf-8")

    class Engine:

        async def dispose(self) -> None:
            events.append("engine.dispose")

    class Registry:

        def __init__(self, sessions: object, *, stale_after_seconds: int) -> None:
            del sessions, stale_after_seconds
            events.append("registry")

    class Capacity:

        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            events.append(f"capacity.{scaler_mode}")

    class Store:

        def __init__(self, sessions: object) -> None:
            del sessions
            events.append("store")

    class Controller:

        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            events.append("controller")

        async def run(self) -> None:
            events.append("controller.run")

        def stop_reconciling(self) -> None:
            events.append("controller.stop")

        async def close(self) -> None:
            events.append("controller.close")

    class Heartbeat:

        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            events.append("heartbeat")

        async def start(self) -> None:
            events.append("heartbeat.start")

        async def close(self) -> None:
            events.append("heartbeat.close")

    class StopEvent:

        def set(self) -> None:
            events.append("signal")

        async def wait(self) -> None:
            events.append("wait")

    class Loop:

        def add_signal_handler(self, signum: object, callback: object) -> None:
            del signum, callback
            events.append("handler")

    settings = Settings(
        _env_file=None,
        runtime_role="supervisor",
        worker_concurrency=0,
        worker_scaler_mode=scaler_mode,
        kubernetes_namespace_file=namespace_file,
    )
    monkeypatch.setattr(_cli, "get_settings", lambda: settings)
    monkeypatch.setattr(_cli.logging.config, "dictConfig", lambda config: events.append("logging"))
    monkeypatch.setattr(_cli, "build_engine", lambda configured: Engine())
    monkeypatch.setattr(_cli, "build_session_factory", lambda engine: object())
    monkeypatch.setattr(_cli, "PostgreSQLRuntimeNodeRegistry", Registry)
    monkeypatch.setattr(_cli, "LocalWorkerProcessLauncher", Capacity)
    monkeypatch.setattr(_cli, "LocalWorkerCapacity", Capacity)
    monkeypatch.setattr(_cli, "KubernetesWorkerCapacity", Capacity)
    monkeypatch.setattr(_cli, "PostgreSQLWorkerPoolStore", Store)
    monkeypatch.setattr(_cli, "WorkerPoolController", Controller)
    monkeypatch.setattr(_cli, "RuntimeNodeHeartbeatService", Heartbeat)
    monkeypatch.setattr(_cli.asyncio, "Event", StopEvent)
    monkeypatch.setattr(_cli.asyncio, "get_running_loop", lambda: Loop())

    await _cli._run_supervisor_role()

    assert events.count("handler") == 2
    assert f"capacity.{scaler_mode}" in events
    assert "heartbeat.start" in events
    assert "controller.run" in events
    assert events[-3:] == ["controller.close", "heartbeat.close", "engine.dispose"]


@pytest.mark.anyio
async def test_supervisor_rejects_agent_worker_slots(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(
        _env_file=None,
        runtime_role="supervisor",
        worker_concurrency=1,
    )
    monkeypatch.setattr(_cli, "get_settings", lambda: settings)
    monkeypatch.setattr(_cli.logging.config, "dictConfig", lambda config: None)

    with pytest.raises(ValueError, match="must not execute Agent tasks"):
        await _cli._run_supervisor_role()


def test_cli_delegates_background_roles_to_async_runtime(monkeypatch: pytest.MonkeyPatch, ) -> None:
    """Worker and Channel roles never open an HTTP listener."""

    sentinel = object()
    captured: list[object] = []
    monkeypatch.setattr(
        _cli,
        "get_settings",
        lambda: Settings(_env_file=None, runtime_role="channel", worker_concurrency=0),
    )
    monkeypatch.setattr(_cli, "_run_background_role", lambda: sentinel)
    monkeypatch.setattr(_cli.asyncio, "run", captured.append)
    monkeypatch.setattr(
        _cli.uvicorn,
        "run",
        lambda *args, **kwargs: pytest.fail("background role opened an HTTP listener"),
    )

    _cli.main()

    assert captured == [sentinel]


def test_cli_delegates_supervisor_without_opening_http(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = object()
    captured: list[object] = []
    monkeypatch.setattr(
        _cli,
        "get_settings",
        lambda: Settings(_env_file=None, runtime_role="supervisor", worker_concurrency=0),
    )
    monkeypatch.setattr(_cli, "_run_supervisor_role", lambda: sentinel)
    monkeypatch.setattr(_cli.asyncio, "run", captured.append)
    monkeypatch.setattr(
        _cli.uvicorn,
        "run",
        lambda *args, **kwargs: pytest.fail("supervisor opened an HTTP listener"),
    )

    _cli.main()

    assert captured == [sentinel]
