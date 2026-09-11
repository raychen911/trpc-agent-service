"""Alembic env for async PostgreSQL migrations."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from alembic import context
from sqlalchemy.ext.asyncio import async_engine_from_config

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from trpc_service.storage.schema import metadata

config = context.config

url = os.environ.get("TRPC_DATABASE_URL", "").strip()
if url:
    config.set_main_option("sqlalchemy.url", url)

target_metadata = metadata


def run_migrations_offline() -> None:
    context.run_migrations(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )


def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        pool_pre_ping=True,
    )
    try:
        async with connectable.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
