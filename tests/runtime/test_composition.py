"""Runtime role fail-fast boundaries."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import SecretStr

from trpc_service.config import Environment, Settings
from trpc_service.runtime import (
    RuntimeConfigurationError,
    run_projector_role,
    run_worker_role,
)
from trpc_service.runtime.composition import run_dispatcher_role
from trpc_service.storage import Database


def settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "_env_file": None,
        "env": Environment.TEST,
        "secret_key": SecretStr("s" * 40),
        "admin_api_key": SecretStr("admin-test"),
    }
    values.update(overrides)
    return Settings(**values)


@pytest.mark.asyncio
async def test_worker_rejects_mock_before_opening_runtime_dependencies() -> None:
    with pytest.raises(RuntimeConfigurationError, match="real model provider"):
        await run_worker_role(settings(model_provider="mock"), asyncio.Event())


@pytest.mark.asyncio
async def test_compatible_provider_requires_platform_https_endpoint() -> None:
    with pytest.raises(RuntimeConfigurationError, match="model_base_url"):
        await run_worker_role(
            settings(model_provider="openai-compatible"),
            asyncio.Event(),
        )


@pytest.mark.asyncio
async def test_production_rejects_single_host_event_store_before_database_io() -> None:
    production = settings(
        env=Environment.PRODUCTION,
        database_url="postgresql+asyncpg://runtime:password@db/agent",
        public_base_url="https://agent.example.test",
        admin_api_key=SecretStr("a" * 40),
        model_provider="openai",
        event_store_backend="local",
    )

    with pytest.raises(RuntimeConfigurationError, match="authoritative SQL"):
        await run_worker_role(production, asyncio.Event())


async def schema_url(tmp_path) -> str:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'runtime.db'}"
    database = Database(database_url)
    await database.create_schema()
    await database.dispose()
    return database_url


@pytest.mark.asyncio
async def test_real_worker_composition_starts_and_stops_without_mocking_dependencies(
    tmp_path,
) -> None:
    stop = asyncio.Event()
    stop.set()

    await run_worker_role(
        settings(
            database_url=await schema_url(tmp_path),
            model_provider="openai",
            model_api_key=SecretStr("real-provider-key-shape"),
            event_store_backend="local",
            event_store_path=tmp_path / "events",
        ),
        stop,
    )


@pytest.mark.asyncio
async def test_real_dispatcher_composition_starts_and_stops_without_sending(
    tmp_path,
) -> None:
    stop = asyncio.Event()
    stop.set()

    await run_dispatcher_role(
        settings(database_url=await schema_url(tmp_path)),
        stop,
    )


@pytest.mark.asyncio
async def test_real_projector_composition_starts_and_stops_without_jobs(tmp_path) -> None:
    stop = asyncio.Event()
    stop.set()

    await run_projector_role(
        settings(
            database_url=await schema_url(tmp_path),
            event_store_backend="local",
            event_store_path=tmp_path / "events",
        ),
        stop,
    )
