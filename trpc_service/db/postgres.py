"""Small synchronous PostgreSQL transaction helpers used by long-lived processes.

The runtime port is intentionally synchronous so a worker can make a complete
fenced state transition in one database transaction.  psycopg is used here
instead of an async ORM session so the same implementation can be called by
FastAPI's worker threads and by the dispatcher/worker CLI processes.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager

from trpc_service.runtime.models import TenantContext


def psycopg_dsn(database_url: str) -> str:
    """Translate SQLAlchemy-style PostgreSQL URLs to a psycopg-compatible DSN."""

    return database_url.replace("postgresql+asyncpg://", "postgresql://", 1).replace(
        "postgresql+psycopg://", "postgresql://", 1
    )


class PostgresConnections:
    """Open short transactions with an un-leakable tenant GUC scope."""

    _ROLE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,62}$")

    def __init__(self, database_url: str, database_role: str | None = None) -> None:
        self.dsn = psycopg_dsn(database_url)
        if database_role is not None and not self._ROLE_PATTERN.fullmatch(database_role):
            raise ValueError("database role must be a lower-case PostgreSQL role identifier")
        self.database_role = database_role

    def _set_restricted_role(self, connection: object) -> None:
        """Use the workload role and reject a superuser/bypass-RLS login.

        Supplying a role makes this an executable deployment invariant instead
        of trusting a Compose or Helm URL to have been configured correctly.
        The session identity is checked as well: a superuser could reset a
        role after a compromised process obtains its credentials.
        """

        if self.database_role is None:
            return
        connection.execute(f"SET LOCAL ROLE {self.database_role}")  # type: ignore[attr-defined]
        row = connection.execute(  # type: ignore[attr-defined]
            """
            SELECT
              EXISTS(
                SELECT 1 FROM pg_roles
                WHERE rolname = session_user AND (rolsuper OR rolbypassrls)
              ) AS session_is_privileged,
              EXISTS(
                SELECT 1 FROM pg_roles
                WHERE rolname = current_user AND (rolsuper OR rolbypassrls)
              ) AS effective_is_privileged
            """
        ).fetchone()
        if row["session_is_privileged"] or row["effective_is_privileged"]:
            raise RuntimeError("PostgreSQL workload login must not be superuser or BYPASSRLS")

    @contextmanager
    def tenant(self, context: TenantContext) -> Iterator[object]:
        from psycopg import connect
        from psycopg.rows import dict_row

        connection = connect(self.dsn, row_factory=dict_row)
        try:
            with connection.transaction():
                self._set_restricted_role(connection)
                connection.execute(
                    "SELECT pg_catalog.set_config('app.tenant_id', %s, true)",
                    (context.tenant_id,),
                )
                yield connection
        finally:
            connection.close()

    def check(self) -> None:
        """Prove that the configured workload login and role can run a query."""

        with self.bootstrap() as connection:
            connection.execute("SELECT 1").fetchone()

    @contextmanager
    def bootstrap(self) -> Iterator[object]:
        """Use only for the protected callback locator before tenant resolution."""

        from psycopg import connect
        from psycopg.rows import dict_row

        connection = connect(self.dsn, row_factory=dict_row)
        try:
            with connection.transaction():
                self._set_restricted_role(connection)
                yield connection
        finally:
            connection.close()
