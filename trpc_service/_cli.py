"""Command-line entry point."""

import argparse
import asyncio
import os
import sys
from collections.abc import Awaitable
from collections.abc import Sequence
from typing import Callable
from typing import Mapping
from typing import TextIO

import uvicorn
from trpc_service.log import configure_logging
from trpc_service.version import __version__


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trpc-agent-service")
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    gateway_parser = subparsers.add_parser("gateway", help="start the Gateway (public HTTP + UI)")
    gateway_parser.add_argument("--host", default="127.0.0.1")
    gateway_parser.add_argument("--port", type=int, default=8000)

    worker_parser = subparsers.add_parser("worker", help="start the Worker (internal execution)")
    worker_parser.add_argument("--host", default="127.0.0.1")
    worker_parser.add_argument("--port", type=int, default=8001)

    web_parser = subparsers.add_parser("web", help="alias for gateway (backward compatibility)")
    web_parser.add_argument("--host", default="127.0.0.1")
    web_parser.add_argument("--port", type=int, default=8000)

    subparsers.add_parser(
        "model-config-check",
        help="validate real-model configuration without calling the provider",
    )
    subparsers.add_parser(
        "model-connection-check",
        help="send a minimal request to the configured real-model provider",
    )

    db_migrate_parser = subparsers.add_parser(
        "db-migrate",
        help="run Alembic migrations (alembic upgrade head)",
    )
    db_migrate_parser.add_argument(
        "--path",
        default=None,
        help="Alembic config path (default: alembic.ini in project root)",
    )

    import_parser = subparsers.add_parser(
        "tenant-config-import",
        help="import tenant configuration from JSON into SQL",
    )
    import_parser.add_argument(
        "--path",
        default=None,
        help="JSON tenant config path (default: data/tenants.json)",
    )

    admin_parser = subparsers.add_parser("admin", help="start the Admin API (tenant configuration management)")
    admin_parser.add_argument("--host", default="127.0.0.1")
    admin_parser.add_argument("--port", type=int, default=8003)

    migrate_parser = subparsers.add_parser(
        "state-backend-migrate",
        help="offline tenant state migration between Redis and SQL (stop Gateway/Workers first)",
    )
    migrate_parser.add_argument("--tenant-id", required=True)
    migrate_parser.add_argument("--to", required=True, choices=["redis", "sql"])
    migrate_parser.add_argument("--expected-version", required=True, type=int)
    migrate_parser.add_argument(
        "--offline",
        action="store_true",
        help="confirm Gateway/Workers are stopped for this tenant before migrating",
    )
    subparsers.add_parser("backend-init", help="validate shared Redis/PostgreSQL/MinIO backends")
    return parser


def _run_server(app_import: str, *, host: str, port: int, factory: bool) -> None:
    """Start uvicorn without letting it reconfigure logging.

    ``uvicorn.run(log_config=None)`` silently substitutes uvicorn's default
    dictConfig (uvicorn/main.py), which would replace the safe root handlers
    installed by :func:`configure_logging` with raw StreamHandlers and re-arm
    the unfiltered ``uvicorn.access`` logger.  Building the ``Config``
    directly honours ``log_config=None`` (no dictConfig at all), and
    ``access_log=False`` disarms the access logger; fixed safe access events
    come from the ASGI middleware in a later stage.
    """
    config = uvicorn.Config(app_import, host=host, port=port, factory=factory, log_config=None, access_log=False)
    uvicorn.Server(config).run()


def main(
    argv: Sequence[str] | None = None,
    *,
    server_runner: Callable[..., object] | None = None,
    connection_checker: Callable[..., Awaitable[str]] | None = None,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    args = create_parser().parse_args(argv)
    output = sys.stdout if stdout is None else stdout
    errors = sys.stderr if stderr is None else stderr
    environment = os.environ if environ is None else environ

    if args.command == "gateway" or args.command == "web":
        configure_logging("gateway", environment)
        from trpc_service.channels.feishu.sdk import preload_feishu_sdk

        preload_feishu_sdk()
        runner = _run_server if server_runner is None else server_runner
        runner(
            "trpc_service.gateway.app:create_gateway_app",
            host=args.host,
            port=args.port,
            factory=True,
        )
        return 0

    if args.command == "worker":
        configure_logging("worker", environment)
        runner = _run_server if server_runner is None else server_runner
        runner(
            "trpc_service.worker.app:create_worker_app",
            host=args.host,
            port=args.port,
            factory=True,
        )
        return 0

    if args.command in {"model-config-check", "model-connection-check"}:
        from trpc_service.config import ModelConfigurationError
        from trpc_service.config import ModelSettings
        from trpc_service.config import build_model

        try:
            settings = ModelSettings.from_env(environment)
            model = build_model(settings, environment)
        except ModelConfigurationError as exc:
            print(f"model configuration error: {exc}", file=errors)
            return 2
        if args.command == "model-config-check":
            print(
                f"model configuration valid: provider={settings.provider} model={model.name}",
                file=output,
            )
            return 0

        from trpc_service.config import close_model_http_clients

        checker = check_model_connection if connection_checker is None else connection_checker

        async def probe_then_close() -> str:
            try:
                return await checker(model)
            finally:
                # Release shared keep-alive clients inside the probe's own
                # event loop; bounded and never raises, on both paths.
                await close_model_http_clients()

        try:
            asyncio.run(probe_then_close())
        except Exception as exc:  # Provider errors must not expose request credentials.
            print(f"model connection failed: {type(exc).__name__}", file=errors)
            return 2
        print(
            f"model connection valid: provider={settings.provider} model={model.name}",
            file=output,
        )
        return 0

    if args.command == "admin":
        configure_logging("admin", environment)
        runner = _run_server if server_runner is None else server_runner
        runner(
            "trpc_service.admin.app:create_admin_app",
            host=args.host,
            port=args.port,
            factory=True,
        )
        return 0

    if args.command == "db-migrate":
        import subprocess

        alembic_args = [sys.executable, "-m", "alembic"]
        if args.path:
            alembic_args.extend(["-c", args.path])
        alembic_args.extend(["upgrade", "head"])
        result = subprocess.run(alembic_args, capture_output=True, text=True)
        if result.returncode != 0:
            print("database migration failed", file=errors)
            return 1
        print("database migrated to head", file=output)
        return 0

    if args.command == "tenant-config-import":
        from pathlib import Path

        from trpc_service.config.tenant import TenantConfigError
        from trpc_service.config.tenant_repository import (
            TenantRepositoryConfigurationError,
            TenantRepositoryDataError,
            TenantRepositoryUnavailableError,
        )
        from trpc_service.storage.tenant_import import import_tenant_configs

        if args.path:
            json_path = Path(args.path)
        else:
            project_root = Path(__file__).resolve().parents[1]
            json_path = project_root / "data" / "tenants.json"

        try:
            inserted, skipped = asyncio.run(import_tenant_configs(json_path, environment))
        except TenantConfigError:
            print("import failed: invalid tenant configuration", file=errors)
            return 2
        except TenantRepositoryConfigurationError:
            print("import configuration error", file=errors)
            return 2
        except TenantRepositoryDataError:
            print("import data conflict", file=errors)
            return 1
        except TenantRepositoryUnavailableError:
            print("import failed: database is not available", file=errors)
            return 1

        print(f"import complete: {inserted} inserted, {skipped} skipped", file=output)
        return 0

    if args.command == "backend-init":
        from trpc_service.storage.backend_capabilities import BackendConfigurationError, initialize_data_backends

        try:
            asyncio.run(initialize_data_backends(environment))
        except BackendConfigurationError:
            print("data backend initialization failed", file=errors)
            return 1
        except Exception:
            print("data backend initialization failed", file=errors)
            return 1
        print("data backends initialized", file=output)
        return 0

    if args.command == "state-backend-migrate":
        # R1B offline tenant state migration between Redis and SQL.  Every
        # output line below is fixed safe text: tenant configuration, DSNs,
        # content, digests and upstream exception strings never print, and
        # exit codes are 0 success / 1 unavailable / 2 refused-or-misconfigured.
        from trpc_service.storage.state_backend import StateBackendConfigurationError
        from trpc_service.config.tenant_repository import TenantRepositoryConfigurationError
        from trpc_service.storage.message_repository import (
            MessageReceiptRepositoryConfigurationError,
            SqlMessageReceiptRepository,
        )
        from trpc_service.storage.state_migration import (
            StateMigrationPreconditionError,
            StateMigrationUnavailableError,
        )
        from trpc_service.storage.tenant_repository import SqlTenantConfigRepository

        if not args.offline:
            print("state migration refused: migration must be explicitly marked offline", file=errors)
            return 2

        async def _run_state_migration() -> int:
            from trpc_service.storage.backend_resolver import TenantStateBackendResolver
            from trpc_service.storage.state_migration import migrate_tenant_state

            tenant_repository = None
            receipt_repository = None
            resolver = None
            try:
                # Constructed INSIDE the running loop: the SDK SQL services
                # start their background cleanup task on the current loop.
                tenant_repository = SqlTenantConfigRepository.from_env(environment)
                receipt_repository = SqlMessageReceiptRepository.from_env(environment)
                resolver = TenantStateBackendResolver.from_env(environment)
                result = await migrate_tenant_state(
                    tenant_id=args.tenant_id,
                    target_backend=args.to,
                    expected_version=args.expected_version,
                    offline=True,
                    tenant_repository=tenant_repository,
                    receipt_repository=receipt_repository,
                    resolver=resolver,
                )
            finally:
                if resolver is not None:
                    await resolver.close()
                if tenant_repository is not None:
                    await tenant_repository.close()
                if receipt_repository is not None:
                    await receipt_repository.close()
            print(
                f"state migration complete: sessions={result.session_count} events={result.event_count}",
                file=output,
            )
            return 0

        try:
            return asyncio.run(_run_state_migration())
        except StateMigrationPreconditionError as exc:
            print(f"state migration refused: {exc}", file=errors)
            return 2
        except StateMigrationUnavailableError:
            print("state migration failed: state backends are not available", file=errors)
            return 1
        except (
                StateBackendConfigurationError,
                TenantRepositoryConfigurationError,
                MessageReceiptRepositoryConfigurationError,
        ):
            print("state migration configuration error", file=errors)
            return 2
        except Exception:
            # Never surface upstream exception text (DSNs and the like).
            print("state migration failed", file=errors)
            return 1

    return 2


async def check_model_connection(model: object) -> str:
    """Send a minimal request through the SDK and require visible model text."""
    from trpc_agent_sdk.models import LlmRequest
    from trpc_agent_sdk.types import Content
    from trpc_agent_sdk.types import Part

    request = LlmRequest(contents=[Content(role="user", parts=[Part.from_text(text="Reply with OK.")])])
    text_parts: list[str] = []
    async for response in model.generate_async(request, stream=False):
        if response.error_code:
            raise RuntimeError(response.error_code)
        if response.content and response.content.parts:
            text_parts.extend(part.text for part in response.content.parts if part.text)

    response_text = "".join(text_parts).strip()
    if not response_text:
        raise RuntimeError("model returned no text")
    return response_text


def entrypoint() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    entrypoint()
