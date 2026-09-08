import sqlite3
from collections.abc import Iterator
from pathlib import Path

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from trpc_service.storage.models import Base
from trpc_service.storage.tenant_context import current_database_tenant


class Database:
    """Own the SQLAlchemy engine and request-scoped session factory."""

    def __init__(self, url: str) -> None:
        self.url = make_url(url)
        engine_options: dict[str, object] = {"pool_pre_ping": True}

        if self.url.get_backend_name() == "sqlite":
            engine_options["connect_args"] = {"check_same_thread": False}
            if self.url.database in (None, "", ":memory:"):
                engine_options["poolclass"] = StaticPool

        self.engine = create_engine(self.url, **engine_options)
        self._enable_sqlite_foreign_keys(self.engine)
        self.session_factory = sessionmaker(
            bind=self.engine,
            class_=Session,
            expire_on_commit=False,
            autoflush=False,
        )
        self._configure_postgresql_rls_context(self.session_factory)

    @staticmethod
    def _enable_sqlite_foreign_keys(engine: Engine) -> None:
        if engine.dialect.name != "sqlite":
            return

        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(
            dbapi_connection: sqlite3.Connection,
            _connection_record: object,
        ) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    def create_schema(self) -> None:
        if self.url.get_backend_name() == "sqlite" and self.url.database not in (
            None,
            "",
            ":memory:",
        ):
            Path(self.url.database).parent.mkdir(parents=True, exist_ok=True)
        Base.metadata.create_all(self.engine)

    @staticmethod
    def _configure_postgresql_rls_context(factory: sessionmaker[Session]) -> None:
        @event.listens_for(factory, "after_begin")
        def set_rls_context(
            session: Session,
            _transaction: object,
            connection: Connection,
        ) -> None:
            if connection.dialect.name != "postgresql":
                return
            tenant_id = session.info.get("tenant_id") or current_database_tenant()
            connection.exec_driver_sql(
                "SELECT set_config('trpc.tenant_id', %s, true)",
                (tenant_id or "",),
            )
            connection.exec_driver_sql(
                "SELECT set_config('trpc.rls_bypass', %s, true)",
                ("off" if tenant_id else "on",),
            )

    def session(self) -> Iterator[Session]:
        with self.session_factory() as session:
            yield session

    def dispose(self) -> None:
        self.engine.dispose()
