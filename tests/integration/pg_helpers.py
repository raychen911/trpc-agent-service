"""Shared helpers for PostgreSQL integration tests.

This module provides utilities that can be imported by test files.
Fixtures remain in conftest.py for pytest auto-discovery.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
import uuid

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def docker_is_available() -> bool:
    try:
        result = subprocess.run(["docker", "ps"], capture_output=True, timeout=5)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


requires_docker = pytest.mark.skipif(
    not docker_is_available(),
    reason="Docker not available",
)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _ping_and_dispose(db_url: str) -> None:
    engine = create_async_engine(db_url, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            await conn.execute(sa.text("SELECT 1"))
    finally:
        await engine.dispose()


def wait_for_postgres(db_url: str, timeout: float = 30.0) -> bool:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            asyncio.run(_ping_and_dispose(db_url))
            return True
        except Exception:
            time.sleep(0.5)
    return False


class SQLResult:
    """Result from running SQL in a PostgreSQL container."""

    def __init__(self, stdout: str, stderr: str, returncode: int):
        self.stdout = stdout.strip()
        self.stderr = stderr.strip()
        self.returncode = returncode

    @property
    def success(self) -> bool:
        return self.returncode == 0

    @property
    def output(self) -> str:
        """Combined output for error checking."""
        return f"{self.stdout} {self.stderr}".strip()


class PostgreSQLContainer:
    """Manages a temporary PostgreSQL 16 container for integration tests."""

    def __init__(self, name_prefix: str = "trpc-test-pg"):
        self.container = f"{name_prefix}-{uuid.uuid4().hex[:8]}"
        self.port = free_port()
        self.user = "trpc_test"
        self.password = "testpass"
        self.db = "trpc_test"
        self.url = f"postgresql+asyncpg://{self.user}:{self.password}@127.0.0.1:{self.port}/{self.db}"

    def start(self) -> None:
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self.container,
                "-p",
                f"{self.port}:5432",
                "-e",
                f"POSTGRES_USER={self.user}",
                "-e",
                f"POSTGRES_PASSWORD={self.password}",
                "-e",
                f"POSTGRES_DB={self.db}",
                "postgres:16",
            ],
            capture_output=True,
            check=True,
            timeout=120,
        )
        if not wait_for_postgres(self.url):
            self.stop()
            raise RuntimeError(f"PostgreSQL container {self.container} did not become ready")

    def stop(self) -> None:
        subprocess.run(
            ["docker", "rm", "-f", self.container],
            capture_output=True,
            timeout=10,
        )

    def run_sql(self, sql: str) -> SQLResult:
        """Execute SQL in the container and return structured result."""
        result = subprocess.run(
            [
                "docker",
                "exec",
                self.container,
                "psql",
                "-U",
                self.user,
                "-d",
                self.db,
                "-t",
                "-A",
                "-c",
                sql,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return SQLResult(result.stdout, result.stderr, result.returncode)


def run_alembic(postgres_url: str, *args: str, check: bool = False) -> subprocess.CompletedProcess[str]:
    """Run an Alembic command against the given database URL."""
    env = {**os.environ, "TRPC_DATABASE_URL": postgres_url}
    result = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=PROJECT_ROOT,
        timeout=30,
    )
    if check:
        assert result.returncode == 0, f"alembic {args} failed: {result.stderr}"
    return result
