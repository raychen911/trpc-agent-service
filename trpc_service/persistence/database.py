"""Database lifecycle shared by repositories and migration jobs."""

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import event
import time
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from .models import Base


class Database:
    def __init__(self, url: str, **engine_options) -> None:
        self.url = url
        self.engine = create_engine(url, future=True, pool_pre_ping=True, hide_parameters=True, **engine_options)
        if self.engine.dialect.name == "sqlite":
            # Python's legacy SQLite transaction mode otherwise commits a first SAVEPOINT
            # independently of the enclosing receipt/inbox transaction.
            @event.listens_for(self.engine, "connect")
            def sqlite_connect(connection, record):
                connection.isolation_level = None

            @event.listens_for(self.engine, "begin")
            def sqlite_begin(connection):
                connection.exec_driver_sql("BEGIN IMMEDIATE")
        from trpc_service.telemetry.runtime import observe, count

        @event.listens_for(self.engine, "before_cursor_execute")
        def before(connection, cursor, statement, parameters, context, many):
            context.trpc_started = time.perf_counter()

        @event.listens_for(self.engine, "after_cursor_execute")
        def after(connection, cursor, statement, parameters, context, many):
            observe("sql.command_latency_ms", (time.perf_counter() - context.trpc_started) * 1000)

        @event.listens_for(self.engine, "handle_error")
        def failed(context):
            count("sql.command_errors")

        self.sessions: sessionmaker[Session] = sessionmaker(self.engine, expire_on_commit=False)

    def create_schema(self) -> None:
        from .schema import initialize_schema
        initialize_schema(self)

    def table_names(self) -> set[str]:
        return set(inspect(self.engine).get_table_names())

    def check_schema(self) -> None:
        from .schema import check_version
        check_version(self)
        inspector = inspect(self.engine)
        actual = set(inspector.get_table_names())
        for table in Base.metadata.sorted_tables:
            if table.name not in actual:
                raise RuntimeError("database schema is not initialized; run the migrate job")
            columns = {column["name"] for column in inspector.get_columns(table.name)}
            if not set(table.columns.keys()) <= columns:
                raise RuntimeError("database schema is incompatible with this service")

    def close(self) -> None:
        self.engine.dispose()
