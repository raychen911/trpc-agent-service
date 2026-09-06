from __future__ import annotations

from types import SimpleNamespace

import pytest

from trpc_service.storage.postgres_rls import (
    PostgresRLSError,
    _ensure_login_role,
    _ensure_worker_role,
    _tenant_from_event,
    _tenant_from_value,
    validate_runtime_role,
    validate_worker_role,
)


class RoleCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=()):
        self.connection.executed.append((str(query), params))

    def fetchone(self):
        return self.connection.result


class RoleConnection:
    def __init__(self, result):
        self.result = result
        self.executed = []

    def cursor(self):
        return RoleCursor(self)


def owner(result):
    connection = RoleConnection(result)
    return SimpleNamespace(_conn=connection), connection


def test_runtime_and_worker_role_validation_accepts_least_privilege_roles(monkeypatch):
    runtime, runtime_connection = owner(("app-role", False, False))
    monkeypatch.setenv("POSTGRES_RLS_ENABLED", "1")
    monkeypatch.setenv("EXPECTED_APP_ROLE", "app-role")
    validate_runtime_role(runtime, expected_role_env="EXPECTED_APP_ROLE")
    assert runtime_connection.executed

    worker, worker_connection = owner(("worker-role", False, True, True))
    monkeypatch.setenv("EXPECTED_WORKER_ROLE", "worker-role")
    validate_worker_role(worker, expected_role_env="EXPECTED_WORKER_ROLE")
    assert worker_connection.executed


@pytest.mark.parametrize(
    "validator,result,expected",
    [
        (validate_runtime_role, None, "could not be inspected"),
        (validate_runtime_role, ("wrong", False, False), "expected"),
        (validate_runtime_role, ("app", True, False), "SUPERUSER"),
        (validate_worker_role, None, "could not be inspected"),
        (validate_worker_role, ("wrong", False, True, True), "expected"),
        (validate_worker_role, ("worker", True, True, True), "must be LOGIN"),
        (validate_worker_role, ("worker", False, False, True), "must be LOGIN"),
        (validate_worker_role, ("worker", False, True, False), "must be LOGIN"),
    ],
)
def test_role_validation_rejects_incomplete_or_privileged_roles(monkeypatch, validator, result, expected):
    target, _connection = owner(result)
    monkeypatch.setenv("POSTGRES_RLS_ENABLED", "1")
    monkeypatch.setenv("EXPECTED_ROLE", "app" if validator is validate_runtime_role else "worker")
    with pytest.raises(PostgresRLSError, match=expected):
        validator(target, expected_role_env="EXPECTED_ROLE")


def test_rls_role_creation_paths_and_tenant_extractors(monkeypatch):
    connection = RoleConnection(None)
    with connection.cursor() as cursor:
        _ensure_login_role(cursor, "app", "password")
        _ensure_worker_role(cursor, "worker", "password")
    assert len(connection.executed) == 4

    event = SimpleNamespace(tenant_id="tenant-event")
    value = SimpleNamespace(tenant_id="tenant-value")
    assert _tenant_from_event((event,), {}) == "tenant-event"
    assert _tenant_from_event((), {"event": event}) == "tenant-event"
    assert _tenant_from_value((value,), {}) == "tenant-value"
    assert _tenant_from_value((), {"value": value}) == "tenant-value"
    assert _tenant_from_event((), {}) is None
    assert _tenant_from_value((), {}) is None
