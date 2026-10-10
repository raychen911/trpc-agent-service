"""Alembic environment; migration credentials are separate from runtime credentials."""

from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)


def _migration_url() -> str:
    value = os.getenv("TRPC_MIGRATION_DATABASE_DSN")
    if not value:
        raise RuntimeError("TRPC_MIGRATION_DATABASE_DSN is required for schema migration")
    return value.replace("postgresql+asyncpg://", "postgresql+psycopg://")


def _timeout_milliseconds(name: str, default_seconds: str, maximum_seconds: int) -> str:
    """Parse an operator timeout without interpolating untrusted text.

    PostgreSQL accepts a duration string for these settings.  We only return
    an integer millisecond value generated from a finite positive Decimal, so
    the value is safe to include in the ``SET`` statement below and cannot
    become SQL syntax.
    """

    raw = os.getenv(name, default_seconds)
    try:
        seconds = Decimal(raw)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise RuntimeError(f"{name} must be a finite positive number of seconds") from exc
    if not seconds.is_finite() or seconds <= 0 or seconds > maximum_seconds:
        raise RuntimeError(f"{name} must be greater than 0 and at most {maximum_seconds} seconds")
    milliseconds = int(seconds * 1000)
    if milliseconds < 1:
        raise RuntimeError(f"{name} is below the 1 millisecond PostgreSQL minimum")
    return f"{milliseconds}ms"


def _migration_timeouts() -> tuple[str, str]:
    lock_timeout = _timeout_milliseconds("TRPC_MIGRATION_LOCK_TIMEOUT_SECONDS", "30", 300)
    statement_timeout = _timeout_milliseconds(
        "TRPC_MIGRATION_STATEMENT_TIMEOUT_SECONDS", "900", 86_400
    )
    lock_ms = int(lock_timeout.removesuffix("ms"))
    statement_ms = int(statement_timeout.removesuffix("ms"))
    if lock_ms > statement_ms:
        raise RuntimeError(
            "TRPC_MIGRATION_LOCK_TIMEOUT_SECONDS cannot exceed "
            "TRPC_MIGRATION_STATEMENT_TIMEOUT_SECONDS"
        )
    return lock_timeout, statement_timeout


def run_migrations_offline() -> None:
    context.configure(
        url=_migration_url(),
        target_metadata=None,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        transactional_ddl=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = _migration_url()
    lock_timeout, statement_timeout = _migration_timeouts()
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=None, transactional_ddl=True)
        with context.begin_transaction():
            # Execute SET only after Alembic has opened its managed
            # transaction.  Running either statement first triggers
            # SQLAlchemy 2.x autobegin; the surrounding connection context
            # would then roll back otherwise-successful migration DDL when it
            # closes.
            #
            # Values are generated as bounded integer milliseconds by
            # ``_migration_timeouts``; they are not copied from the
            # environment into SQL verbatim.
            connection.exec_driver_sql(f"SET lock_timeout = '{lock_timeout}'")
            connection.exec_driver_sql(f"SET statement_timeout = '{statement_timeout}'")
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
