"""Optional PostgreSQL row-level security support.

RLS is deliberately opt-in.  The application still passes tenant_id through
every storage interface; PostgreSQL adds a second boundary for shared tables.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import wraps
from typing import Any


class PostgresRLSError(RuntimeError):
    """Raised when the optional PostgreSQL RLS contract is incomplete."""


RLS_TABLES = (
    "tenant_config",
    "tenant_active",
    "session_state",
    "message_event",
    "memory",
    "summary",
    "audit_log",
    "idempotency",
    "compensation_task",
    "session_fence",
    "session_lease",
    "inbox_message",
    "outbox_message",
    "mailbox_message",
    "mailbox_fence",
    "session_mailbox",
    "session_mailbox_item",
    "tool_approval",
    "tool_budget",
    "tool_executions",
    "knowledge_chunk",
    "migration_lease",
    "migration_write_barrier",
    "migration_checkpoint",
)
RLS_RUNTIME_TABLES = tuple(
    table for table in RLS_TABLES if table not in {"tenant_config", "tenant_active"}
)

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def _truthy(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def rls_enabled() -> bool:
    """Return whether runtime tenant context enforcement is enabled."""

    return _truthy("POSTGRES_RLS_ENABLED")


def postgres_schema_auto_create() -> bool:
    """Return whether application processes may create PostgreSQL schemas."""

    return _truthy("POSTGRES_AUTO_CREATE_SCHEMA", "1")


def validate_runtime_role(owner: object, *, expected_role_env: str | None = None) -> None:
    """Reject privileged or unexpected roles in an RLS runtime connection."""

    if not rls_enabled():
        return
    connection = _connection(owner)
    with connection.cursor() as cur:
        cur.execute(
            "SELECT current_user, rolsuper, rolbypassrls "
            "FROM pg_roles WHERE rolname = current_user"
        )
        row = cur.fetchone()
    if row is None:
        raise PostgresRLSError("RLS runtime connection role could not be inspected")
    current_user, is_superuser, bypasses_rls = row
    expected_role = os.getenv(expected_role_env, "").strip() if expected_role_env else ""
    if expected_role and str(current_user) != expected_role:
        raise PostgresRLSError(
            f"RLS runtime connection uses role {current_user!r}; expected {expected_role!r}"
        )
    if bool(is_superuser) or bool(bypasses_rls):
        raise PostgresRLSError(
            f"RLS runtime connection role {current_user!r} must not be SUPERUSER or BYPASSRLS"
        )


def validate_worker_role(owner: object, *, expected_role_env: str | None = None) -> None:
    """Reject privileged or non-worker roles on cross-tenant worker connections."""

    if not rls_enabled():
        return
    connection = _connection(owner)
    with connection.cursor() as cur:
        cur.execute(
            "SELECT current_user, rolsuper, rolbypassrls, rolcanlogin "
            "FROM pg_roles WHERE rolname = current_user"
        )
        row = cur.fetchone()
    if row is None:
        raise PostgresRLSError("RLS worker connection role could not be inspected")
    current_user, is_superuser, bypasses_rls, can_login = row
    expected_role = os.getenv(expected_role_env, "").strip() if expected_role_env else ""
    if expected_role and str(current_user) != expected_role:
        raise PostgresRLSError(
            f"RLS worker connection uses role {current_user!r}; expected {expected_role!r}"
        )
    if bool(is_superuser) or not bool(bypasses_rls) or not bool(can_login):
        raise PostgresRLSError(
            f"RLS worker role {current_user!r} must be LOGIN, NOSUPERUSER, and BYPASSRLS"
        )


def _tenant_from_first_argument(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str | None:
    return str(args[0]) if args else kwargs.get("tenant_id")


def _tenant_from_event(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str | None:
    if args:
        return getattr(args[0], "tenant_id", None)
    event = kwargs.get("event")
    return getattr(event, "tenant_id", None)


def _tenant_from_value(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str | None:
    if args:
        return getattr(args[0], "tenant_id", None)
    value = kwargs.get("value")
    return getattr(value, "tenant_id", None)


def _tenant_from_optional_keyword(index: int) -> Callable[[tuple[Any, ...], dict[str, Any]], str | None]:
    def getter(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str | None:
        if "tenant_id" in kwargs:
            return kwargs["tenant_id"]
        return args[index] if len(args) > index else None

    return getter


def _connection(owner: object):
    return getattr(owner, "_conn", owner)


@contextmanager
def postgres_tenant_context(owner: object, tenant_id: str | None) -> Iterator[None]:
    """Set a transaction-local tenant context and clear it on commit/rollback.

    The owner may be a storage adapter exposing ``_conn`` or a raw psycopg
    connection.  ``SET LOCAL`` is intentionally used instead of a session
    setting so a pooled or reused connection cannot retain another tenant.
    """

    if not rls_enabled():
        yield
        return
    if not tenant_id or not str(tenant_id).strip():
        raise PostgresRLSError("POSTGRES_RLS_ENABLED requires a non-empty tenant_id")

    lock = getattr(owner, "_lock", None)
    if lock is not None:
        lock.acquire()
    try:
        connection = _connection(owner)
        with connection.transaction():
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT set_config('app.tenant_id', %s, true)",
                    (str(tenant_id),),
                )
            yield
    finally:
        if lock is not None:
            lock.release()


def rls_tenant_method(
    tenant_getter: Callable[[tuple[Any, ...], dict[str, Any]], str | None],
    *,
    require_tenant: bool = True,
):
    """Decorate a PostgreSQL adapter method with transaction-local RLS context."""

    def decorator(method):
        @wraps(method)
        def wrapped(self, *args, **kwargs):
            tenant_id = tenant_getter(args, kwargs)
            if rls_enabled() and require_tenant and not tenant_id:
                raise PostgresRLSError(f"{method.__qualname__} requires tenant-scoped access")
            with postgres_tenant_context(self, tenant_id):
                return method(self, *args, **kwargs)

        return wrapped

    return decorator


def _identifier(value: str, label: str) -> str:
    value = str(value).strip()
    if not _IDENTIFIER.fullmatch(value):
        raise PostgresRLSError(f"invalid PostgreSQL {label}: {value!r}")
    return value


def _role_exists(cur, role: str) -> tuple[bool, bool, bool, bool, bool, bool] | None:
    cur.execute(
        (
            "SELECT rolsuper, rolbypassrls, rolcanlogin, rolcreaterole, "
            "rolcreatedb, rolreplication FROM pg_roles WHERE rolname = %s"
        ),
        (role,),
    )
    row = cur.fetchone()
    return tuple(bool(value) for value in row) if row else None


def _ensure_login_role(cur, role: str, password: str | None) -> None:
    state = _role_exists(cur, role)
    if state is None:
        if not password:
            raise PostgresRLSError(
                f"role {role!r} does not exist; provide its password through the RLS migration environment"
            )
        from psycopg import sql

        cur.execute(
            sql.SQL(
                "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD {}"
            ).format(
                sql.Identifier(role),
                sql.Literal(password),
            )
        )
        return
    if state[0] or state[1] or state[3] or state[4] or state[5]:
        raise PostgresRLSError(
            f"role {role!r} must be a least-privilege login role "
            "without SUPERUSER, BYPASSRLS, CREATEDB, CREATEROLE, or REPLICATION"
        )
    if not state[2]:
        raise PostgresRLSError(f"role {role!r} must have LOGIN enabled")


def _ensure_worker_role(cur, role: str, password: str | None) -> None:
    state = _role_exists(cur, role)
    if state is None:
        if not password:
            raise PostgresRLSError(
                f"worker role {role!r} does not exist; provide its password through the migration environment"
            )
        from psycopg import sql

        cur.execute(
            sql.SQL(
                "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOINHERIT NOREPLICATION BYPASSRLS PASSWORD {}"
            ).format(sql.Identifier(role), sql.Literal(password))
        )
        return
    if state[0] or state[3] or state[4] or state[5]:
        raise PostgresRLSError(
            f"worker role {role!r} must be a least-privilege login role "
            "without SUPERUSER, CREATEDB, CREATEROLE, or REPLICATION"
        )
    if not state[1]:
        from psycopg import sql

        cur.execute(sql.SQL("ALTER ROLE {} BYPASSRLS").format(sql.Identifier(role)))
    if not state[2]:
        raise PostgresRLSError(f"worker role {role!r} must have LOGIN enabled")


def _grant_runtime_privileges(cur, role: str, tables: tuple[str, ...]) -> None:
    from psycopg import sql

    role_id = sql.Identifier(role)
    table_ids = sql.SQL(", ").join(sql.Identifier("public", table) for table in tables)
    cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role_id))
    cur.execute(
        sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {} TO {}").format(table_ids, role_id)
    )
    cur.execute(
        sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {}").format(role_id)
    )


def _policy_sql(cur, table: str, app_role: str, admin_role: str) -> None:
    from psycopg import sql

    table_id = sql.Identifier(table)
    tenant_policy = sql.Identifier("trpc_agent_tenant_isolation")
    admin_policy = sql.Identifier("trpc_agent_admin_access")
    app_role_id = sql.Identifier(app_role)
    admin_role_id = sql.Identifier(admin_role)
    tenant_predicate = sql.SQL("tenant_id = current_setting('app.tenant_id', true)")

    for policy in (tenant_policy, admin_policy):
        cur.execute(
            sql.SQL("DROP POLICY IF EXISTS {} ON public.{}").format(policy, table_id)
        )

    cur.execute(
        sql.SQL(
            "CREATE POLICY {} ON public.{} TO {} "
            "USING ({}) WITH CHECK ({})"
        ).format(
            tenant_policy,
            table_id,
            app_role_id,
            tenant_predicate,
            tenant_predicate,
        )
    )
    cur.execute(
        sql.SQL(
            "CREATE POLICY {} ON public.{} TO {} "
            "USING (true) WITH CHECK (true)"
        ).format(admin_policy, table_id, admin_role_id)
    )
    cur.execute(sql.SQL("ALTER TABLE public.{} ENABLE ROW LEVEL SECURITY").format(table_id))
    cur.execute(sql.SQL("ALTER TABLE public.{} FORCE ROW LEVEL SECURITY").format(table_id))


def apply_rls_migration(
    connection,
    *,
    app_role: str | None = None,
    admin_role: str | None = None,
    app_password: str | None = None,
    admin_password: str | None = None,
    worker_role: str | None = None,
    worker_password: str | None = None,
) -> None:
    """Create least-privilege roles and install tenant policies.

    The connection must be owned by a schema/migration role.  Runtime
    connections should use ``app_role``; control-plane code uses ``admin_role``.
    """

    app_role = _identifier(
        app_role or os.getenv("POSTGRES_RLS_APP_ROLE", "trpc_agent_app"),
        "application role",
    )
    admin_role = _identifier(
        admin_role or os.getenv("POSTGRES_RLS_ADMIN_ROLE", "trpc_agent_admin"),
        "admin role",
    )
    if app_role == admin_role:
        raise PostgresRLSError("application and admin RLS roles must be different")
    worker_role = _identifier(
        worker_role or os.getenv("POSTGRES_RLS_WORKER_ROLE", "trpc_worker"),
        "worker role",
    )
    if worker_role in {app_role, admin_role}:
        raise PostgresRLSError("worker, application, and admin RLS roles must be different")

    with connection.transaction(), connection.cursor() as cur:
        _ensure_login_role(cur, app_role, app_password or os.getenv("POSTGRES_RLS_APP_PASSWORD"))
        _ensure_login_role(cur, admin_role, admin_password or os.getenv("POSTGRES_RLS_ADMIN_PASSWORD"))
        _ensure_worker_role(cur, worker_role, worker_password or os.getenv("POSTGRES_RLS_WORKER_PASSWORD"))
        _grant_runtime_privileges(cur, app_role, RLS_RUNTIME_TABLES)
        _grant_runtime_privileges(cur, admin_role, RLS_TABLES)
        _grant_runtime_privileges(cur, worker_role, RLS_TABLES)
        for table in RLS_TABLES:
            cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
            if cur.fetchone()[0] is None:
                raise PostgresRLSError(
                    f"cannot apply RLS: required table public.{table} does not exist; run schema migration first"
                )
            _policy_sql(cur, table, app_role, admin_role)


def apply_rls_migration_dsn(dsn: str, **kwargs: Any) -> None:
    """Open a migration connection, apply RLS, and close it."""

    if not dsn.strip():
        raise PostgresRLSError("RLS migration requires a PostgreSQL schema DSN")
    import psycopg

    with psycopg.connect(dsn) as connection:
        connection.autocommit = True
        apply_rls_migration(connection, **kwargs)


__all__ = [
    "RLS_RUNTIME_TABLES",
    "RLS_TABLES",
    "PostgresRLSError",
    "_tenant_from_event",
    "_tenant_from_first_argument",
    "_tenant_from_optional_keyword",
    "_tenant_from_value",
    "apply_rls_migration",
    "apply_rls_migration_dsn",
    "postgres_schema_auto_create",
    "postgres_tenant_context",
    "rls_enabled",
    "rls_tenant_method",
    "validate_runtime_role",
    "validate_worker_role",
]
