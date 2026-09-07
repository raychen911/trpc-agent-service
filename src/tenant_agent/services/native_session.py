"""Provision and validate the pinned tRPC native SQL Session schema."""

from __future__ import annotations

from typing import Any, cast

from sqlalchemy import Column, MetaData, String, Table, delete, insert, inspect, select, text
from sqlalchemy.ext.asyncio import create_async_engine
from trpc_agent_sdk.sessions import SqlSessionService, StorageSession
from trpc_agent_sdk.storage import SqlSession
from typing_extensions import override

NATIVE_SESSION_SCHEMA_VERSION = "trpc-agent-py-1.1.19-v1"
_version_metadata = MetaData()
_version_table = Table(
    "tenant_agent_native_schema_version",
    _version_metadata,
    Column("component", String(64), primary_key=True),
    Column("version", String(128), nullable=False),
)


class ProvisionedSqlSessionService(SqlSessionService):  # type: ignore[misc]
    """Pinned SQL Session service with async-safe server-default refreshes.

    tRPC-Agent-Python 1.1.19 commits a ``func.now()`` update while loading a
    session and then reads that expired ORM attribute synchronously. Refreshing
    inside the greenlet-aware storage boundary prevents ``MissingGreenlet`` and
    keeps runtime schema ownership with the explicit provisioner below.
    """

    @override
    async def _get_session(
        self,
        sql_session: SqlSession,
        app_name: str,
        user_id: str,
        session_id: str,
    ) -> StorageSession | None:
        storage_session = await super()._get_session(
            sql_session,
            app_name,
            user_id,
            session_id,
        )
        if storage_session is not None:
            await self._sql_storage.refresh(sql_session, storage_session)
        return storage_session


def _session_metadata() -> MetaData:
    from trpc_agent_sdk.sessions._sql_session_service import SessionStorageBase

    return cast(MetaData, SessionStorageBase.metadata)


async def provision_native_sql_schema(dsn: str) -> None:
    """Create the isolated native schema in an explicit one-shot operation."""

    engine = create_async_engine(dsn)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(_session_metadata().create_all)
            await connection.run_sync(_version_metadata.create_all)
            await connection.execute(
                delete(_version_table).where(_version_table.c.component == "trpc-session")
            )
            await connection.execute(
                insert(_version_table).values(
                    component="trpc-session",
                    version=NATIVE_SESSION_SCHEMA_VERSION,
                )
            )
    finally:
        await engine.dispose()


async def validate_native_sql_schema(dsn: str) -> None:
    """Fail read-only when the pinned native schema is absent or stale."""

    engine = create_async_engine(dsn)
    try:
        async with engine.connect() as connection:

            def inspect_schema(sync_connection: Any) -> list[str]:
                inspector = inspect(sync_connection)
                existing_tables = set(inspector.get_table_names())
                missing: list[str] = []
                for table_name, table in _session_metadata().tables.items():
                    if table_name not in existing_tables:
                        missing.append(table_name)
                        continue
                    existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
                    missing.extend(
                        f"{table_name}.{column.name}"
                        for column in table.columns
                        if column.name not in existing_columns
                    )
                if _version_table.name not in existing_tables:
                    missing.append(_version_table.name)
                return missing

            missing = await connection.run_sync(inspect_schema)
            if missing:
                raise RuntimeError("native tRPC SQL Session schema is not provisioned")
            version = (
                await connection.execute(
                    select(_version_table.c.version).where(_version_table.c.component == "trpc-session")
                )
            ).scalar_one_or_none()
            if version != NATIVE_SESSION_SCHEMA_VERSION:
                raise RuntimeError("native tRPC SQL Session schema version is incompatible")
    finally:
        await engine.dispose()


async def assert_native_sql_runtime_role_unprivileged(dsn: str) -> None:
    """Reject superuser/BYPASSRLS identities for native PostgreSQL runtime I/O."""

    engine = create_async_engine(dsn)
    try:
        if engine.dialect.name != "postgresql":
            return
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user")
                )
            ).one()
            owned_tables = {
                str(owned[0])
                for owned in (
                    await connection.execute(
                        text(
                            "SELECT c.relname FROM pg_class c "
                            "JOIN pg_namespace n ON n.oid = c.relnamespace "
                            "WHERE c.relkind IN ('r', 'p') "
                            "AND n.nspname = ANY(current_schemas(false)) "
                            "AND c.relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user)"
                        )
                    )
                ).all()
            }
        native_tables = set(_session_metadata().tables) | {_version_table.name}
        if bool(row[0]) or bool(row[1]) or owned_tables & native_tables:
            raise RuntimeError("production native Session role must be non-owner and non-bypass")
    finally:
        await engine.dispose()
