"""Programmatic Alembic runner for the service PostgreSQL schema."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.resources import files

from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import NullPool

MIGRATION_LOCK = "trpc-agent-alembic-v1"

EXPECTED_SCHEMA: dict[str, frozenset[str]] = {
    "tenant_config": frozenset({"tenant_id", "version", "config_json", "created_at", "updated_at"}),
    "tenant_active": frozenset({"tenant_id", "active_version"}),
    "session_state": frozenset(
        {"tenant_id", "session_id", "state_version", "latest_event_seq", "state_json"}
    ),
    "message_event": frozenset(
        {
            "tenant_id",
            "event_id",
            "session_id",
            "seq",
            "idempotency_key",
            "event_type",
            "payload_json",
            "trace_id",
            "created_at",
        }
    ),
    "memory": frozenset(
        {"tenant_id", "memory_id", "scope_key", "content", "version", "metadata_json", "created_at"}
    ),
    "summary": frozenset(
        {"tenant_id", "session_id", "summary_version", "source_event_seq", "content", "created_at"}
    ),
    "audit_log": frozenset(
        {
            "audit_id",
            "tenant_id",
            "channel",
            "user_id",
            "session_id",
            "agent_name",
            "tool_name",
            "decision",
            "latency_ms",
            "error_type",
            "token_usage",
            "cost",
            "trace_id",
            "metadata_json",
            "created_at",
        }
    ),
    "idempotency": frozenset(
        {
            "tenant_id",
            "key",
            "status",
            "response_ref",
            "result_json",
            "trace_id",
            "attempt",
            "created_at",
            "updated_at",
        }
    ),
    "compensation_task": frozenset(
        {
            "task_id",
            "tenant_id",
            "operation",
            "payload_json",
            "status",
            "attempt",
            "available_at",
            "last_error",
            "created_at",
            "updated_at",
        }
    ),
    "session_fence": frozenset({"tenant_id", "session_id", "fencing_token"}),
    "session_lease": frozenset({"tenant_id", "session_id", "owner", "fencing_token", "expires_at"}),
    "inbox_message": frozenset(
        {
            "message_id",
            "tenant_id",
            "dedupe_key",
            "session_id",
            "payload_json",
            "status",
            "attempts",
            "owner",
            "lease_until",
            "result_json",
            "created_at",
            "updated_at",
        }
    ),
    "outbox_message": frozenset(
        {
            "event_id",
            "tenant_id",
            "topic",
            "aggregate_id",
            "payload_json",
            "status",
            "attempts",
            "available_at",
            "locked_by",
            "locked_until",
            "last_error",
            "created_at",
            "updated_at",
        }
    ),
    "mailbox_message": frozenset(
        {
            "tenant_id",
            "session_id",
            "sequence",
            "message_id",
            "dedupe_key",
            "payload_json",
            "status",
            "attempts",
            "owner",
            "fencing_token",
            "lease_until",
            "available_at",
            "last_error",
            "created_at",
            "updated_at",
        }
    ),
    "mailbox_fence": frozenset({"tenant_id", "session_id", "fencing_token"}),
    "session_mailbox": frozenset(
        {
            "tenant_id",
            "session_id",
            "status",
            "accepted_sequence",
            "resolved_sequence",
            "processing_sequence",
            "processing_message_id",
            "queue_generation",
            "lease_owner",
            "lease_epoch",
            "lease_until",
            "retry_count",
            "attempt",
            "priority",
            "retry_at",
            "updated_at",
        }
    ),
    "session_mailbox_item": frozenset(
        {
            "tenant_id",
            "session_id",
            "sequence",
            "message_id",
            "trace_id",
            "priority",
            "retry_count",
            "attempt",
            "retry_at",
            "accepted_at",
            "resolved_at",
        }
    ),
    "tool_approval": frozenset(
        {
            "tenant_id",
            "approval_id",
            "session_id",
            "request_id",
            "tool_name",
            "arguments_hash",
            "status",
            "expires_at",
            "approved_by",
            "consumed_at",
            "created_at",
            "updated_at",
        }
    ),
    "tool_budget": frozenset(
        {
            "tenant_id",
            "request_id",
            "total_calls",
            "side_effect_calls",
            "max_calls",
            "max_side_effect_calls",
            "call_keys_json",
            "created_at",
            "updated_at",
        }
    ),
    "tool_executions": frozenset(
        {
            "tenant_id",
            "execution_id",
            "request_id",
            "session_id",
            "tool_name",
            "call_key",
            "arguments_hash",
            "side_effect",
            "status",
            "attempt",
            "fencing_token",
            "result_json",
            "error_type",
            "error_message",
            "started_at",
            "completed_at",
            "created_at",
            "updated_at",
        }
    ),
    "knowledge_chunk": frozenset({"tenant_id", "collection", "chunk_id", "text", "metadata_json"}),
    "migration_lease": frozenset(
        {
            "tenant_id",
            "migration_id",
            "owner_id",
            "owner_instance",
            "lease_epoch",
            "expires_at",
            "updated_at",
        }
    ),
    "migration_write_barrier": frozenset(
        {"tenant_id", "migration_id", "owner_instance", "lease_epoch", "mode", "updated_at"}
    ),
    "migration_checkpoint": frozenset(
        {
            "tenant_id",
            "migration_id",
            "phase",
            "batch_key",
            "cursor_json",
            "source_count",
            "target_count",
            "status",
            "checksum",
            "updated_at",
        }
    ),
}


class DatabaseMigrationError(RuntimeError):
    """Raised when a database migration command is unsafe or incomplete."""


def resolve_schema_dsn(dsn: str = "") -> str:
    value = dsn.strip() or os.getenv("POSTGRES_SCHEMA_DSN", "").strip()
    value = value or os.getenv("POSTGRES_DSN", "").strip()
    if not value:
        raise DatabaseMigrationError(
            "database migration requires --sql-dsn, POSTGRES_SCHEMA_DSN, or POSTGRES_DSN"
        )
    return value


def alembic_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(files("trpc_service.database")))
    return config


def create_database_engine(dsn: str) -> Engine:
    import psycopg

    return create_engine(
        "postgresql+psycopg://",
        creator=lambda: psycopg.connect(dsn),
        poolclass=NullPool,
    )


@contextmanager
def migration_connection(dsn: str) -> Iterator[Connection]:
    engine = create_database_engine(resolve_schema_dsn(dsn))
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql(
                f"SELECT pg_advisory_lock(hashtext('{MIGRATION_LOCK}'))"
            )
            connection.commit()
            try:
                yield connection
            except Exception:
                if connection.in_transaction():
                    connection.rollback()
                raise
            finally:
                if connection.in_transaction():
                    connection.rollback()
                connection.exec_driver_sql(
                    f"SELECT pg_advisory_unlock(hashtext('{MIGRATION_LOCK}'))"
                )
                connection.commit()
    finally:
        engine.dispose()


def upgrade_database(dsn: str = "", revision: str = "head") -> dict[str, object]:
    with migration_connection(dsn) as connection:
        config = alembic_config()
        config.attributes["connection"] = connection
        command.upgrade(config, revision)
    return database_status(dsn)


def downgrade_database(
    dsn: str = "",
    revision: str = "-1",
    *,
    allow_destructive: bool = False,
) -> dict[str, object]:
    if not allow_destructive:
        raise DatabaseMigrationError(
            "database downgrade is destructive; pass --allow-destructive after backup verification"
        )
    with migration_connection(dsn) as connection:
        config = alembic_config()
        config.attributes["connection"] = connection
        command.downgrade(config, revision)
    return database_status(dsn)


def database_status(dsn: str = "") -> dict[str, object]:
    engine = create_database_engine(resolve_schema_dsn(dsn))
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            available = set(inspector.get_table_names(schema="public"))
            missing_tables = sorted(set(EXPECTED_SCHEMA) - available)
            missing_columns: dict[str, list[str]] = {}
            for table in sorted(set(EXPECTED_SCHEMA) & available):
                actual = {column["name"] for column in inspector.get_columns(table, schema="public")}
                missing = sorted(EXPECTED_SCHEMA[table] - actual)
                if missing:
                    missing_columns[table] = missing
            current = MigrationContext.configure(connection).get_current_revision()
            heads = ScriptDirectory.from_config(alembic_config()).get_heads()
    finally:
        engine.dispose()

    passed = current in heads and not missing_tables and not missing_columns
    return {
        "status": "pass" if passed else "fail",
        "current_revision": current,
        "head_revisions": list(heads),
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }
