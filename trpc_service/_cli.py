"""Command line entry point for the service."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import typer
import uvicorn

from trpc_service.config import load_settings
from trpc_service.config import load_environment_file
from trpc_service.config import load_tenant_configs
from trpc_service.log import configure_logging
from trpc_service.log import mask_sensitive_text
from trpc_service.web import build_container
from trpc_service.web import build_production_container
from trpc_service.web import create_app
from trpc_service.demos import LIVE_SCENARIOS
from trpc_service.demos import OFFLINE_SCENARIOS
from trpc_service.demos import run_demo
from trpc_service.channels.simulator import build_im_demo_container

cli = typer.Typer(no_args_is_help=True, help="Run and validate the tRPC Agent multi-tenant service.")
demo_cli = typer.Typer(no_args_is_help=True, help="Run deterministic module demonstrations.")
cli.add_typer(demo_cli, name="demo")


@demo_cli.command("list")
def demo_list() -> None:
    """List offline and explicitly-live scenarios."""
    typer.echo("offline: " + ", ".join(OFFLINE_SCENARIOS))
    typer.echo("live: " + ", ".join(LIVE_SCENARIOS))
    typer.echo("live suite: im-live")


@demo_cli.command("all")
def demo_all(as_json: bool = typer.Option(False, "--json")) -> None:
    """Run all no-network scenarios; this never invokes a real model or IM."""
    results = {name: asyncio.run(run_demo(name)) for name in OFFLINE_SCENARIOS}
    if as_json:
        typer.echo(json.dumps(results, ensure_ascii=False, indent=2))
        return
    for name, result in results.items():
        typer.echo(f"[PASS] {name}: {json.dumps(result, ensure_ascii=False)}")


@demo_cli.command("run")
def demo_run(scenario: str = typer.Argument(...), as_json: bool = typer.Option(False, "--json")) -> None:
    """Run one named scenario, including explicit live scenario placeholders."""
    result = asyncio.run(run_demo(scenario))
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2 if as_json else None))


def _make_scenario_command(scenario: str):

    def command(as_json: bool = typer.Option(False, "--json")) -> None:
        result = asyncio.run(run_demo(scenario))
        typer.echo(json.dumps(result, ensure_ascii=False, indent=2 if as_json else None))

    command.__name__ = f"demo_{scenario.replace('-', '_')}"
    command.__doc__ = f"Run the {scenario} demonstration."
    return command


for _scenario in (*OFFLINE_SCENARIOS,
                  *(item for item in LIVE_SCENARIOS if item not in {"migration-live", "migration-reverse-live"})):
    demo_cli.command(_scenario)(_make_scenario_command(_scenario))

IM_LIVE_CHANNELS = {
    "wecom": "wecom-live",
    "wecom-kf": "wecom-kf-live",
    "telegram": "telegram-live",
}


def _selected_im_live_channels(value: str) -> list[str]:
    requested = {item.strip().lower().replace("_", "-") for item in value.split(",") if item.strip()}
    if requested == {"all"}:
        return list(IM_LIVE_CHANNELS)
    invalid = requested - set(IM_LIVE_CHANNELS)
    if not requested or invalid:
        choices = "all, " + ", ".join(IM_LIVE_CHANNELS)
        raise typer.BadParameter(f"--channels accepts {choices}")
    return [channel for channel in IM_LIVE_CHANNELS if channel in requested]


async def _run_im_live_suite(channels: list[str]) -> dict[str, object]:
    results = []
    for channel in channels:
        scenario = IM_LIVE_CHANNELS[channel]
        try:
            detail = await run_demo(scenario)
            passed = bool(detail.get("authenticated")) if channel == "wecom" else bool(detail.get("delivered"))
            results.append({"channel": channel, "status": "passed" if passed else "failed", "detail": detail})
        except Exception as error:  # each channel must report independently in the unified live check
            results.append({
                "channel": channel,
                "status": "failed",
                "error_type": type(error).__name__,
                "message": mask_sensitive_text(str(error)),
            })
    passed_count = sum(item["status"] == "passed" for item in results)
    return {
        "scenario": "im-live",
        "passed": passed_count,
        "failed": len(results) - passed_count,
        "results": results,
    }


@demo_cli.command("im-live")
def im_live(env_file: Path = typer.Option(Path(".env"), help="Dotenv file containing real IM credentials."),
            channels: str = typer.Option("all", "--channels", help="all or comma-separated IM channel names."),
            confirm: bool = typer.Option(False, "--confirm", help="Allow real network calls and test messages."),
            as_json: bool = typer.Option(False, "--json")) -> None:
    """Validate real WeCom, WeChat Customer Service and Telegram credentials from one entry point."""
    if not confirm:
        raise typer.BadParameter("im-live connects to real IM services and may send test messages; pass --confirm")
    load_environment_file(env_file)
    selected = _selected_im_live_channels(channels)
    result = asyncio.run(_run_im_live_suite(selected))
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2 if as_json else None))
    if result["failed"]:
        raise typer.Exit(code=1)


@demo_cli.command("migration-live")
def migration_live(confirm: bool = typer.Option(False, "--confirm"), as_json: bool = typer.Option(False,
                                                                                                  "--json")) -> None:
    """Run a real Redis-to-PostgreSQL migration only after explicit confirmation."""
    if not confirm:
        raise typer.BadParameter("migration-live changes the configured test PostgreSQL; pass --confirm")
    import os
    previous = os.environ.get("TRPC_MIGRATION_CONFIRM")
    os.environ["TRPC_MIGRATION_CONFIRM"] = "YES"
    try:
        result = asyncio.run(run_demo("migration-live"))
    finally:
        if previous is None:
            os.environ.pop("TRPC_MIGRATION_CONFIRM", None)
        else:
            os.environ["TRPC_MIGRATION_CONFIRM"] = previous
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2 if as_json else None))


@demo_cli.command("migration-reverse-live")
def migration_reverse_live(confirm: bool = typer.Option(False, "--confirm"),
                           as_json: bool = typer.Option(False, "--json")) -> None:
    """Run a real PostgreSQL-to-Redis migration after explicit confirmation."""
    if not confirm:
        raise typer.BadParameter("migration-reverse-live changes the test Redis; pass --confirm")
    import os
    previous = os.environ.get("TRPC_MIGRATION_CONFIRM")
    os.environ["TRPC_MIGRATION_CONFIRM"] = "YES"
    try:
        result = asyncio.run(run_demo("migration-reverse-live"))
    finally:
        if previous is None:
            os.environ.pop("TRPC_MIGRATION_CONFIRM", None)
        else:
            os.environ["TRPC_MIGRATION_CONFIRM"] = previous
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2 if as_json else None))


@cli.command()
def serve(config: Path = typer.Option(Path("examples/config/tenants.yaml"), exists=True, readable=True),
          env_file: Path = typer.Option(Path(".env"), help="Model and service variables in dotenv format."),
          host: str = typer.Option("127.0.0.1"),
          port: int = typer.Option(8080, min=1, max=65535),
          log_level: str = typer.Option("INFO")) -> None:
    """Run Gateway, Worker and Admin roles in one development process."""
    load_environment_file(env_file)
    settings = load_settings()
    settings.config_file = str(config)
    settings.host = host
    settings.port = port
    settings.log_level = log_level
    configure_logging(log_level)
    configs = load_tenant_configs(config)
    if settings.environment == "development":
        app = create_app(build_container(settings, configs))
        uvicorn.run(app, host=host, port=port, log_level=log_level.lower())
        return

    async def run_production() -> None:
        container = await build_production_container(settings, configs)
        server = uvicorn.Server(uvicorn.Config(create_app(container), host=host, port=port,
                                               log_level=log_level.lower()))
        await server.serve()

    asyncio.run(run_production())


@cli.command("im-demo")
def im_demo(config: Path = typer.Option(Path("examples/config/im-demo.yaml"), exists=True, readable=True),
            env_file: Path = typer.Option(Path(".env"), help="Optional configured-model variables."),
            host: str = typer.Option("127.0.0.1"),
            port: int = typer.Option(8080, min=1, max=65535),
            log_level: str = typer.Option("INFO")) -> None:
    """Run the development-only visual simulator for WeCom, WeChat KF and Telegram."""
    load_environment_file(env_file)
    settings = load_settings()
    if settings.environment != "development":
        raise typer.BadParameter("im-demo requires TRPC_SERVICE_ENV=development")
    settings.config_file = str(config)
    settings.host = host
    settings.port = port
    settings.log_level = log_level
    configure_logging(log_level)
    configs = load_tenant_configs(config)

    async def run() -> None:
        container = await build_im_demo_container(settings, configs)
        server = uvicorn.Server(uvicorn.Config(create_app(container), host=host, port=port,
                                               log_level=log_level.lower()))
        await server.serve()

    asyncio.run(run())


@cli.command("check-config")
def check_config(config: Path = typer.Argument(..., exists=True, readable=True),
                 env_file: Path = typer.Option(Path(".env"), help="Optional dotenv model configuration.")) -> None:
    """Validate a tenant YAML file without resolving secrets or calling a model."""
    load_environment_file(env_file)
    configs = load_tenant_configs(config)
    typer.echo(f"valid: {len(configs)} tenant configuration(s)")


@cli.command("show-config")
def show_config(config: Path = typer.Argument(..., exists=True, readable=True),
                env_file: Path = typer.Option(Path(".env"), help="Optional dotenv model configuration.")) -> None:
    """Print validated configuration; inline SecretStr values remain excluded."""
    load_environment_file(env_file)
    data = [item.model_dump(mode="json") for item in load_tenant_configs(config)]
    typer.echo(json.dumps(data, ensure_ascii=False, indent=2))


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
