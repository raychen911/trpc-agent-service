from __future__ import annotations

import os
from uuid import uuid4

import pytest


@pytest.fixture
def shared_namespace() -> str:
    """Return a unique, test-only namespace for backend keys and rows."""

    return f"pytest-{uuid4().hex}"


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
