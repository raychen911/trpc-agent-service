from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from scripts.session_ready_backlog_exporter import (
    BACKLOG_QUERY,
    DATABASE_DSN_ENV,
    NAMESPACE_ENV,
    BacklogExporter,
    ExporterConfig,
    _configuration,
    create_app,
)


class _Connection:
    def __init__(self, value: Any = 0, error: BaseException | None = None) -> None:
        self.value = value
        self.error = error
        self.queries: list[str] = []

    async def fetchval(self, query: str) -> Any:
        self.queries.append(query)
        if self.error is not None:
            raise self.error
        return self.value


class _Acquire:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _Connection:
        return self._connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection
        self.closed = False

    def acquire(self) -> _Acquire:
        return _Acquire(self.connection)

    async def close(self) -> None:
        self.closed = True


def _exporter(connection: _Connection, namespace: str = "trpc-service") -> BacklogExporter:
    return BacklogExporter(
        ExporterConfig(
            database_dsn="postgresql://metrics@example.invalid/service",
            namespace=namespace,
        ),
        pool=_Pool(connection),
    )


def test_configuration_reads_only_dsn_and_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(DATABASE_DSN_ENV, "postgresql+asyncpg://metrics@db/service")
    monkeypatch.setenv(NAMESPACE_ENV, "Trpc-Service")

    config = _configuration()

    assert config.database_dsn == "postgresql://metrics@db/service"
    assert config.namespace == "trpc-service"


@pytest.mark.parametrize(
    ("dsn", "namespace"),
    (
        ("", "trpc-service"),
        ("postgresql://db/service", ""),
        ("postgresql://db/service", "bad_namespace"),
        ("postgresql://db/service", "a" * 64),
    ),
)
def test_configuration_rejects_missing_or_invalid_values(
    monkeypatch: pytest.MonkeyPatch, dsn: str, namespace: str
) -> None:
    monkeypatch.setenv(DATABASE_DSN_ENV, dsn)
    monkeypatch.setenv(NAMESPACE_ENV, namespace)

    with pytest.raises(ValueError):
        _configuration()


@pytest.mark.asyncio
async def test_read_backlog_uses_fixed_function_and_accepts_nonnegative_int() -> None:
    connection = _Connection(17)
    exporter = _exporter(connection)

    assert await exporter.read_backlog() == 17
    assert connection.queries == [BACKLOG_QUERY]


@pytest.mark.asyncio
async def test_read_backlog_rejects_invalid_function_result() -> None:
    for value in (True, -1, 1.5, "17"):
        exporter = _exporter(_Connection(value))
        with pytest.raises(RuntimeError, match="unavailable"):
            await exporter.read_backlog()


def test_metrics_and_health_are_healthy_without_tenant_labels() -> None:
    connection = _Connection(3)
    exporter = _exporter(connection)
    app = create_app(exporter)

    with TestClient(app) as client:
        metrics = client.get("/metrics")
        ready = client.get("/health/ready")
        live = client.get("/health/live")

    assert metrics.status_code == 200
    body = metrics.text
    assert 'trpc_session_ready_backlog{namespace="trpc-service"} 3.0' in body
    assert "tenant_id" not in body
    assert "session_id" not in body
    assert ready.status_code == 200
    assert ready.json() == {"status": "ok"}
    assert live.status_code == 200
    assert live.json() == {"status": "ok"}


def test_database_failure_is_503_and_never_a_zero_backlog() -> None:
    connection = _Connection(error=OSError("database unavailable"))
    exporter = _exporter(connection)
    app = create_app(exporter)

    with TestClient(app) as client:
        metrics = client.get("/metrics")
        ready = client.get("/health/ready")
        live = client.get("/health/live")

    assert metrics.status_code == 503
    assert "trpc_session_ready_backlog" not in metrics.text
    assert "0" not in metrics.text
    assert ready.status_code == 503
    assert ready.json() == {"status": "unavailable"}
    assert live.status_code == 200


def test_missing_configuration_keeps_process_live_but_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DATABASE_DSN_ENV, raising=False)
    monkeypatch.setenv(NAMESPACE_ENV, "trpc-service")
    app = create_app()

    with TestClient(app) as client:
        metrics = client.get("/metrics")
        ready = client.get("/health/ready")
        live = client.get("/health/live")

    assert metrics.status_code == 503
    assert "trpc_session_ready_backlog" not in metrics.text
    assert ready.status_code == 503
    assert live.status_code == 200


@pytest.mark.asyncio
async def test_close_is_idempotent_for_uncreated_pool() -> None:
    exporter = BacklogExporter(None)
    await exporter.close()
    await exporter.close()


@pytest.mark.asyncio
async def test_pool_is_created_from_environment_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    connection = _Connection(4)
    pool = _Pool(connection)

    async def fake_create_pool(dsn: str, **kwargs: Any) -> _Pool:
        calls.append((dsn, kwargs))
        return pool

    monkeypatch.setattr(
        "scripts.session_ready_backlog_exporter.asyncpg.create_pool", fake_create_pool
    )
    monkeypatch.setenv(DATABASE_DSN_ENV, "postgresql+asyncpg://metrics@db/service")
    monkeypatch.setenv(NAMESPACE_ENV, "trpc-service")
    exporter = BacklogExporter.from_environment()

    assert await exporter.read_backlog() == 4
    assert calls[0][0] == "postgresql://metrics@db/service"
    assert calls[0][1]["min_size"] == 1
    assert calls[0][1]["max_size"] == 2
    await exporter.close()
    assert pool.closed
