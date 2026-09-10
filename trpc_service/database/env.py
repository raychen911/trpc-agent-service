"""Alembic environment configured by the service migration runner."""

from __future__ import annotations

from alembic import context


def run_migrations(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=None,
        compare_type=True,
        transaction_per_migration=True,
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    raise RuntimeError("offline migrations are not supported; use trpc-agent-migrate db upgrade")

provided_connection = context.config.attributes.get("connection")
if provided_connection is None:
    from trpc_service.database.runner import create_database_engine, resolve_schema_dsn

    provided_engine = create_database_engine(resolve_schema_dsn())
    with provided_engine.connect() as provided_connection:
        run_migrations(provided_connection)
    provided_engine.dispose()
else:
    run_migrations(provided_connection)
