"""Shared fixtures for PostgreSQL integration tests.

Fixtures are auto-discovered by pytest. Shared utilities are in pg_helpers.py.
"""

from __future__ import annotations

import pytest

from .pg_helpers import PostgreSQLContainer, docker_is_available, requires_docker

__all__ = ["requires_docker"]


@pytest.fixture(scope="module")
def postgres_url():
    """Start a temporary PostgreSQL 16 container and return its async URL.

    Always creates an isolated container — never uses TRPC_DATABASE_URL.
    """
    if not docker_is_available():
        pytest.skip("Docker not available")

    pg = PostgreSQLContainer(name_prefix="trpc-test-pg")
    pg.start()
    try:
        yield pg.url
    finally:
        pg.stop()


@pytest.fixture
def pg_container():
    """Provide a PostgreSQLContainer instance for tests that need run_sql()."""
    if not docker_is_available():
        pytest.skip("Docker not available")

    pg = PostgreSQLContainer(name_prefix="trpc-4a-int-pg")
    pg.start()
    try:
        yield pg
    finally:
        pg.stop()
