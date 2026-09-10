"""Cross-suite fixtures that never contain committed credentials."""

from __future__ import annotations

import secrets
import socket
import os
from collections.abc import Iterator
from datetime import datetime

import pytest

from tests.support import FIXED_UTC


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "data_shared_backend: Phase 7 test requiring configured shared Redis/PostgreSQL",
    )


@pytest.fixture
def fixed_utc() -> datetime:
    return FIXED_UTC


@pytest.fixture
def runtime_secret_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    values = {
        "TRPC_DEMO_ALPHA_SECRET": secrets.token_urlsafe(32),
        "TRPC_DEMO_BETA_SECRET": secrets.token_urlsafe(32),
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    return values


@pytest.fixture
def block_external_network(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    original_connect = socket.socket.connect

    def guarded_connect(sock: socket.socket, address: object) -> object:
        host = address[0] if isinstance(address, tuple) and address else ""
        if host not in {"127.0.0.1", "::1", "localhost"}:
            raise AssertionError("external network access is forbidden in offline tests")
        return original_connect(sock, address)  # type: ignore[arg-type]

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    yield


@pytest.fixture
def shared_redis_url() -> str:
    value = os.getenv("TRPC_SHARED_REDIS_URL")
    if not value:
        pytest.skip("TRPC_SHARED_REDIS_URL is required for shared backend tests")
    return value


@pytest.fixture
def shared_database_url() -> str:
    value = os.getenv("TRPC_SHARED_DATABASE_URL")
    if not value:
        pytest.skip("TRPC_SHARED_DATABASE_URL is required for shared backend tests")
    return value


@pytest.fixture
def shared_namespace() -> str:
    from uuid import uuid4

    return f"pytest-{uuid4().hex}"


@pytest.fixture
def data_namespace() -> str:
    """Unique, non-sensitive namespace for Phase 7 shared-backend tests."""
    from uuid import uuid4

    return f"phase7-{uuid4().hex}"


@pytest.fixture
def ops_namespace() -> str:
    """Unique, non-sensitive namespace for Phase 8 shared-backend tests.

    Reuses the existing ``shared_backend`` marker declared in
    ``pyproject.toml`` so quickstart commands stay unchanged; this fixture only
    isolates state created by Phase 8 runs.
    """
    from uuid import uuid4

    return f"phase8-{uuid4().hex}"


@pytest.fixture
def data_shared_redis_url() -> str:
    value = os.getenv("TRPC_SHARED_REDIS_URL")
    if not value:
        pytest.skip("TRPC_SHARED_REDIS_URL is required for Phase 7 shared-backend tests")
    return value


@pytest.fixture
def data_shared_database_url() -> str:
    value = os.getenv("TRPC_SHARED_DATABASE_URL")
    if not value:
        pytest.skip("TRPC_SHARED_DATABASE_URL is required for Phase 7 shared-backend tests")
    return value
