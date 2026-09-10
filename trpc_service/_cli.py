"""Command-line entry points."""

from __future__ import annotations

import asyncio
import json

import typer
import uvicorn

from trpc_service.channels.pull_runtime import run_pull_channels
from trpc_service.config.settings import get_settings
from trpc_service.storage.database import Database
from trpc_service.worker.runtime import run_worker

app = typer.Typer(no_args_is_help=True)


@app.command()
def serve() -> None:
    """Start the HTTP service."""
    settings = get_settings()
    uvicorn.run(
        "trpc_service.web.app:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.casefold(),
    )


@app.command("init-db")
def init_db() -> None:
    """Create the SQLite schema."""
    settings = get_settings()

    async def initialize() -> None:
        database = Database(settings.database_url)
        try:
            await database.initialize()
        finally:
            await database.dispose()

    asyncio.run(initialize())
    typer.echo("database initialized")


@app.command("run-channels")
def run_channels() -> None:
    """Run Telegram polling and WeCom AIBot WebSocket workers."""
    asyncio.run(run_pull_channels())


@app.command("run-worker")
def run_agent_worker() -> None:
    """Run a Redis Stream Agent worker."""
    asyncio.run(run_worker(get_settings()))


@app.command("show-config")
def show_config() -> None:
    """Show non-secret runtime configuration."""
    settings = get_settings()
    safe = {
        "app_env": settings.app_env.value,
        "host": settings.host,
        "port": settings.port,
        "log_level": settings.log_level,
        "database_url": settings.database_url,
        "model_provider": settings.model_provider,
        "model_name": settings.model_name,
        "model_base_url": settings.model_base_url,
        "has_model_api_key_ref": settings.model_api_key_ref is not None,
        "has_admin_api_key_ref": settings.admin_api_key_ref is not None,
        "has_session_hmac_key_ref": settings.session_hmac_key_ref is not None,
    }
    typer.echo(json.dumps(safe, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    app()
