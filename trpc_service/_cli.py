"""Command-line entrypoint for serving, migrating, and diagnosing the platform."""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path

import uvicorn
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from trpc_service.config import Settings, get_settings
from trpc_service.runtime import (
    RuntimeConfigurationError,
    run_dispatcher_role,
    run_projector_role,
    run_worker_role,
)
from trpc_service.storage import Database
from trpc_service.version import __version__


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trpc-agent-service")
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve", help="run the FastAPI gateway")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", default=8000, type=int)
    serve.add_argument("--reload", action="store_true")

    commands.add_parser("migrate", help="upgrade the configured database to head")
    commands.add_parser("doctor", help="run non-mutating dependency checks")
    commands.add_parser("worker", help="run the durable Agent Worker polling role")
    commands.add_parser("dispatcher", help="run the durable IM Outbox delivery role")
    commands.add_parser("projector", help="run durable Summary/Memory projections")
    return parser


def _migrate() -> None:
    settings = get_settings()
    Path("data").mkdir(exist_ok=True)
    package_root = Path(__file__).resolve().parent
    bundled_config = package_root / "_alembic.ini"
    if bundled_config.is_file():
        config_path = bundled_config
        script_path = package_root / "_migrations"
    else:
        source_root = package_root.parent
        config_path = source_root / "alembic.ini"
        script_path = source_root / "migrations"
    if not config_path.is_file() or not script_path.is_dir():
        raise RuntimeConfigurationError("database migration resources are unavailable")
    config = Config(str(config_path))
    config.set_main_option("script_location", str(script_path))
    config.set_main_option("sqlalchemy.url", settings.database_url.replace("%", "%%"))
    command.upgrade(config, "head")


async def _doctor() -> int:
    settings = get_settings()
    database = Database(settings.database_url)
    checks: dict[str, str] = {
        "python_environment": settings.env.value,
        "database": "failed",
        "sdk_version": "unknown",
    }
    try:
        async with database.session_factory() as session:
            await session.execute(text("SELECT 1"))
        checks["database"] = "ok"
    finally:
        await database.dispose()
    try:
        from trpc_agent_sdk.version import __version__ as sdk_version

        checks["sdk_version"] = sdk_version
    except ImportError:
        checks["sdk_version"] = "missing"
    print(json.dumps(checks, ensure_ascii=False, sort_keys=True))
    return 0 if checks["database"] == "ok" and checks["sdk_version"] == "1.1.19" else 1


async def _run_supervised(
    runner: Callable[[Settings, asyncio.Event], Awaitable[None]],
) -> int:
    """Run one process role until SIGINT/SIGTERM requests a cooperative stop."""

    settings = get_settings()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for process_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(process_signal, stop.set)
        except NotImplementedError:  # Windows event loops do not expose this API.
            signal.signal(process_signal, lambda *_: stop.set())
    await runner(settings, stop)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run a CLI command and return a process exit code."""

    arguments = _parser().parse_args(argv)
    if arguments.command == "migrate":
        _migrate()
        return 0
    if arguments.command == "doctor":
        return asyncio.run(_doctor())
    if arguments.command in {"worker", "dispatcher", "projector"}:
        runners = {
            "worker": run_worker_role,
            "dispatcher": run_dispatcher_role,
            "projector": run_projector_role,
        }
        runner = runners[arguments.command]
        try:
            return asyncio.run(_run_supervised(runner))
        except RuntimeConfigurationError as error:
            print(f"{arguments.command} startup rejected: {error}", file=sys.stderr)
            return 2
        except KeyboardInterrupt:
            return 130
        except Exception as error:
            print(
                f"{arguments.command} stopped after {type(error).__name__}",
                file=sys.stderr,
            )
            return 1
    if arguments.command == "serve":
        settings = get_settings()
        if arguments.reload and settings.env.value == "production":
            raise SystemExit("--reload is forbidden in production")
        uvicorn.run(
            "trpc_service.web.app:create_app",
            factory=True,
            host=arguments.host,
            port=arguments.port,
            reload=arguments.reload,
            log_config=None,
        )
        return 0
    raise AssertionError("unreachable command")


if __name__ == "__main__":
    raise SystemExit(main())
