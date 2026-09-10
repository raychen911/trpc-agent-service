from __future__ import annotations

import os
import unittest
from contextlib import contextmanager
from threading import RLock
from unittest.mock import patch
from uuid import uuid4

from trpc_service.storage.postgres_rls import (
    RLS_TABLES,
    PostgresRLSError,
    _ensure_login_role,
    _ensure_worker_role,
    _identifier,
    apply_rls_migration,
    postgres_tenant_context,
    rls_tenant_method,
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        rendered = str(query)
        self.connection.executed.append((rendered, params))
        if "FROM pg_roles" in rendered:
            self._result = self.connection.role_result
        elif "to_regclass" in rendered:
            self._result = (self.connection.regclass,)
        else:
            self._result = None

    def fetchone(self):
        return self._result


class FakeConnection:
    def __init__(self):
        self.executed = []
        self.transactions = []
        self.role_result = (False, False, True, False, False, False)
        self.regclass = "public.table"

    @contextmanager
    def transaction(self):
        self.transactions.append("begin")
        try:
            yield
        except Exception:
            self.transactions.append("rollback")
            raise
        else:
            self.transactions.append("commit")

    def cursor(self):
        return FakeCursor(self)


class RLSUnitTests(unittest.TestCase):
    def test_tenant_context_sets_transaction_local_setting_and_releases_lock(self):
        connection = FakeConnection()
        owner = type("Owner", (), {"_conn": connection, "_lock": RLock()})()

        with patch.dict(os.environ, {"POSTGRES_RLS_ENABLED": "1"}, clear=False):
            with postgres_tenant_context(owner, "tenant-a"):
                self.assertEqual(connection.transactions, ["begin"])
                self.assertIn(
                    ("SELECT set_config('app.tenant_id', %s, true)", ("tenant-a",)),
                    connection.executed,
                )
            self.assertEqual(connection.transactions, ["begin", "commit"])

    def test_tenant_context_fails_closed_without_a_tenant(self):
        connection = FakeConnection()
        owner = type("Owner", (), {"_conn": connection, "_lock": RLock()})()

        with (
            patch.dict(os.environ, {"POSTGRES_RLS_ENABLED": "1"}, clear=False),
            self.assertRaises(PostgresRLSError),
            postgres_tenant_context(owner, None),
        ):
            pass
        self.assertEqual(connection.executed, [])

    def test_disabled_rls_does_not_touch_connection(self):
        connection = FakeConnection()
        owner = type("Owner", (), {"_conn": connection, "_lock": RLock()})()

        with patch.dict(os.environ, {"POSTGRES_RLS_ENABLED": "0"}, clear=False), postgres_tenant_context(
            owner, None
        ):
            pass
        self.assertEqual(connection.executed, [])
        self.assertEqual(connection.transactions, [])

    def test_tenant_method_decorator_extracts_context_and_requires_scope(self):
        connection = FakeConnection()

        class Store:
            _conn = connection
            _lock = RLock()

            @rls_tenant_method(lambda args, kwargs: args[0] if args else kwargs.get("tenant_id"))
            def read(self, tenant_id):
                return tenant_id

        store = Store()
        with patch.dict(os.environ, {"POSTGRES_RLS_ENABLED": "1"}, clear=False):
            self.assertEqual(store.read("tenant-a"), "tenant-a")
            with self.assertRaises(PostgresRLSError):
                store.read("")
        self.assertEqual(connection.transactions, ["begin", "commit"])

    def test_migration_grants_only_runtime_tables_to_app_role_and_forces_rls(self):
        connection = FakeConnection()

        with apply_rls_environment():
            apply_rls_migration(
                connection,
                app_role="app_role",
                admin_role="admin_role",
                app_password="app-password",
                admin_password="admin-password",
                worker_role="worker_role",
                worker_password="worker-password",
            )

        grant_statements = [
            query
            for query, _ in connection.executed
            if "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE" in query
        ]
        self.assertEqual(len(grant_statements), 3)
        self.assertNotIn("tenant_config", grant_statements[0])
        self.assertNotIn("tenant_active", grant_statements[0])
        self.assertIn("tenant_config", grant_statements[1])
        self.assertIn("tenant_config", grant_statements[2])
        self.assertTrue(
            any("FORCE ROW LEVEL SECURITY" in query for query, _ in connection.executed)
        )
        self.assertEqual(connection.transactions[-1], "commit")

    def test_rls_role_validation_and_identifiers_fail_closed(self):
        with self.assertRaises(PostgresRLSError):
            _identifier("bad-role", "application role")
        with self.assertRaises(PostgresRLSError):
            _identifier("", "application role")

        connection = FakeConnection()
        connection.role_result = None
        with connection.cursor() as cursor:
            with self.assertRaises(PostgresRLSError):
                _ensure_login_role(cursor, "app", None)
            with self.assertRaises(PostgresRLSError):
                _ensure_worker_role(cursor, "worker", None)

        connection.role_result = (True, False, True, False, False, False)
        with connection.cursor() as cursor, self.assertRaises(PostgresRLSError):
            _ensure_login_role(cursor, "app", "password")
        connection.role_result = (False, False, True, True, False, False)
        with connection.cursor() as cursor, self.assertRaises(PostgresRLSError):
            _ensure_worker_role(cursor, "worker", "password")
        connection.role_result = (False, False, False, False, False, False)
        with connection.cursor() as cursor:
            with self.assertRaises(PostgresRLSError):
                _ensure_login_role(cursor, "app", "password")
            with self.assertRaises(PostgresRLSError):
                _ensure_worker_role(cursor, "worker", "password")

    def test_apply_rls_rejects_role_collisions_and_missing_tables(self):
        connection = FakeConnection()
        with self.assertRaises(PostgresRLSError):
            apply_rls_migration(connection, app_role="same", admin_role="same")
        with self.assertRaises(PostgresRLSError):
            apply_rls_migration(connection, app_role="app", admin_role="admin", worker_role="app")
        connection.regclass = None
        with self.assertRaises(PostgresRLSError):
            apply_rls_migration(
                connection,
                app_role="app",
                admin_role="admin",
                worker_role="worker",
                app_password="a",
                admin_password="b",
                worker_password="c",
            )


@contextmanager
def apply_rls_environment():
    with patch.dict(os.environ, {"POSTGRES_RLS_ENABLED": "0"}, clear=False):
        yield


def _integration_enabled() -> bool:
    return bool(
        os.getenv("POSTGRES_RLS_TEST_DSN", "").strip()
        and os.getenv("POSTGRES_RLS_TEST_ALLOW_DESTRUCTIVE", "").strip().lower()
        in {"1", "true", "yes", "on"}
    )


@unittest.skipUnless(
    _integration_enabled(),
    "set POSTGRES_RLS_TEST_DSN and POSTGRES_RLS_TEST_ALLOW_DESTRUCTIVE=1 for disposable PostgreSQL",
)
class PostgresRLSIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import conninfo

        cls.psycopg = psycopg
        cls.base_dsn = os.environ["POSTGRES_RLS_TEST_DSN"]
        cls.suffix = uuid4().hex[:12]
        cls.app_role = f"trpc_rls_app_{cls.suffix}"
        cls.admin_role = f"trpc_rls_admin_{cls.suffix}"
        cls.worker_role = f"trpc_rls_worker_{cls.suffix}"
        cls.app_password = f"app-{uuid4().hex}"
        cls.admin_password = f"admin-{uuid4().hex}"
        cls.worker_password = f"worker-{uuid4().hex}"

        from trpc_service.migrate import ensure_postgres_schema

        with patch.dict(
            os.environ,
            {"POSTGRES_AUTO_CREATE_SCHEMA": "1", "POSTGRES_RLS_ENABLED": "0"},
            clear=False,
        ):
            ensure_postgres_schema(cls.base_dsn)

        from trpc_service.storage.postgres_rls import apply_rls_migration_dsn

        apply_rls_migration_dsn(
            cls.base_dsn,
            app_role=cls.app_role,
            admin_role=cls.admin_role,
            app_password=cls.app_password,
            admin_password=cls.admin_password,
            worker_role=cls.worker_role,
            worker_password=cls.worker_password,
        )

        base_info = conninfo.conninfo_to_dict(cls.base_dsn)
        cls.app_dsn = conninfo.make_conninfo(
            **{**base_info, "user": cls.app_role, "password": cls.app_password}
        )
        cls.admin_dsn = conninfo.make_conninfo(
            **{**base_info, "user": cls.admin_role, "password": cls.admin_password}
        )
        cls.worker_dsn = conninfo.make_conninfo(
            **{**base_info, "user": cls.worker_role, "password": cls.worker_password}
        )

    @classmethod
    def tearDownClass(cls):
        with cls.psycopg.connect(cls.base_dsn) as connection:
            connection.autocommit = True
            with connection.cursor() as cur:
                cur.execute("DELETE FROM memory WHERE memory_id LIKE %s", (f"{cls.suffix}-%",))
                for table in RLS_TABLES:
                    cur.execute(
                        f"DROP POLICY IF EXISTS trpc_agent_tenant_isolation ON public.{table}"
                    )
                    cur.execute(
                        f"DROP POLICY IF EXISTS trpc_agent_admin_access ON public.{table}"
                    )
                    cur.execute(f"ALTER TABLE public.{table} NO FORCE ROW LEVEL SECURITY")
                    cur.execute(f"ALTER TABLE public.{table} DISABLE ROW LEVEL SECURITY")
                cur.execute(f'DROP OWNED BY "{cls.app_role}"')
                cur.execute(f'DROP OWNED BY "{cls.admin_role}"')
                cur.execute(f'DROP OWNED BY "{cls.worker_role}"')
                cur.execute(f'DROP ROLE IF EXISTS "{cls.app_role}"')
                cur.execute(f'DROP ROLE IF EXISTS "{cls.admin_role}"')
                cur.execute(f'DROP ROLE IF EXISTS "{cls.worker_role}"')

    def test_app_role_isolation_and_admin_visibility(self):
        from trpc_service.storage.base import MemoryItem
        from trpc_service.storage.postgres_store import PostgresStorage

        with patch.dict(
            os.environ,
            {
                "POSTGRES_RLS_ENABLED": "1",
                "POSTGRES_AUTO_CREATE_SCHEMA": "0",
                "POSTGRES_RLS_APP_ROLE": self.app_role,
                "POSTGRES_RLS_WORKER_ROLE": self.worker_role,
            },
            clear=False,
        ):
            storage = PostgresStorage(self.app_dsn)
            try:
                storage.memory.put(
                    MemoryItem(
                        "tenant-a",
                        f"{self.suffix}-adapter",
                        "session",
                        "adapter-content",
                    )
                )
                self.assertTrue(storage.memory.search("tenant-a", "adapter-content"))
                self.assertEqual(storage.memory.search("tenant-b", "adapter-content"), [])
                with self.assertRaises(PostgresRLSError):
                    storage.memory.search("", "adapter-content")
            finally:
                storage.close()

        with patch.dict(
            os.environ,
            {
                "POSTGRES_RLS_ENABLED": "1",
                "POSTGRES_AUTO_CREATE_SCHEMA": "0",
                "POSTGRES_PROCESS_ROLE": "worker",
                "POSTGRES_RLS_WORKER_ROLE": self.worker_role,
            },
            clear=False,
        ):
            from trpc_service.storage.postgres_rls import validate_worker_role
            from trpc_service.storage.postgres_store import PostgresStorage

            worker_storage = PostgresStorage(self.worker_dsn)
            try:
                validate_worker_role(worker_storage, expected_role_env="POSTGRES_RLS_WORKER_ROLE")
            finally:
                worker_storage.close()

        with self.psycopg.connect(self.app_dsn) as app:
            app.autocommit = True
            with app.cursor() as cur:
                cur.execute("SELECT count(*) FROM memory")
                self.assertEqual(cur.fetchone()[0], 0)

            with app.transaction(), app.cursor() as cur:
                cur.execute("SELECT set_config('app.tenant_id', %s, true)", ("tenant-a",))
                cur.execute(
                    """
                    INSERT INTO memory (
                      tenant_id, memory_id, scope_key, content, version, metadata_json, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """,
                    (
                        "tenant-a",
                        f"{self.suffix}-a",
                        "session",
                        "tenant-a-content",
                        1,
                        "{}",
                    ),
                )
                cur.execute("SELECT count(*) FROM memory")
                self.assertEqual(cur.fetchone()[0], 2)

            with app.transaction(), app.cursor() as cur:
                cur.execute("SELECT set_config('app.tenant_id', %s, true)", ("tenant-b",))
                cur.execute("SELECT count(*) FROM memory")
                self.assertEqual(cur.fetchone()[0], 0)

            with self.assertRaises(self.psycopg.errors.InsufficientPrivilege):
                with app.transaction(), app.cursor() as cur:
                    cur.execute("SELECT set_config('app.tenant_id', %s, true)", ("tenant-b",))
                    cur.execute(
                        """
                        INSERT INTO memory (
                          tenant_id, memory_id, scope_key, content, version, metadata_json, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        """,
                        (
                            "tenant-a",
                            f"{self.suffix}-b",
                            "session",
                            "cross-tenant",
                            1,
                            "{}",
                        ),
                    )

        with self.psycopg.connect(self.admin_dsn) as admin:
            admin.autocommit = True
            with admin.cursor() as cur:
                cur.execute("SELECT count(*) FROM memory WHERE memory_id LIKE %s", (f"{self.suffix}-%",))
                self.assertEqual(cur.fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
