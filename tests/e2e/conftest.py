"""Real service fixtures; local mode is explicit and never a network fallback."""

import os
import uuid
from contextlib import AsyncExitStack
from dataclasses import dataclass

import pytest
import pytest_asyncio
from qdrant_client import QdrantClient
from qdrant_client.http import models
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema
from sqlalchemy.schema import DropSchema
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.sessions import RedisSessionService
from trpc_agent_sdk.sessions import SqlSessionService

from trpc_service.persistence import Database
from trpc_service.storage import QdrantKnowledgeStore


def required_env(name):
    value = os.environ.get(name)
    if not value:
        pytest.fail(f"--backend-mode=real requires {name}; no local fallback is allowed")
    return value


@dataclass
class SessionBackends:
    source: object
    target: object
    reopen_target: object
    suffix: str
    sql_url: str


@pytest_asyncio.fixture
async def session_backends(request, tmp_path):
    real = request.config.getoption("--backend-mode") == "real"
    suffix = uuid.uuid4().hex[:12]
    sql_url = required_env("TRPC_SESSION_DATABASE_URL") if real else f"sqlite:///{tmp_path / 'runtime.db'}"
    async with AsyncExitStack() as stack:
        source = RedisSessionService(db_url=required_env("TRPC_REDIS_URL")) if real else InMemorySessionService()
        stack.push_async_callback(source.close)
        target = SqlSessionService(db_url=sql_url)
        stack.push_async_callback(target.close)

        def reopen():
            reader = SqlSessionService(db_url=sql_url)
            stack.push_async_callback(reader.close)
            return reader

        yield SessionBackends(source, target, reopen, suffix, sql_url)


@pytest.fixture
def platform_database(request, tmp_path):
    real = request.config.getoption("--backend-mode") == "real"
    url = required_env("TRPC_DATABASE_URL") if real else f"sqlite+pysqlite:///{tmp_path / 'platform.db'}"
    admin = create_engine(url) if real else None
    schema = f"e2e_{uuid.uuid4().hex}" if real else None
    if real:
        with admin.begin() as connection:
            connection.execute(CreateSchema(schema))
    database = Database(url, **({"connect_args": {"options": f"-csearch_path={schema}"}} if real else {}))
    database.create_schema()
    try:
        yield database
    finally:
        database.close()
        if admin is not None:
            try:
                with admin.begin() as connection:
                    connection.execute(DropSchema(schema, cascade=True))
            finally:
                admin.dispose()


@pytest.fixture
def vector_backend(request, tmp_path):
    real = request.config.getoption("--backend-mode") == "real"
    client = QdrantClient(url=required_env("TRPC_QDRANT_URL"), timeout=15) if real else QdrantClient(
        path=str(tmp_path / "qdrant"))
    collection = f"e2e_{uuid.uuid4().hex}"
    client.create_collection(collection, vectors_config=models.VectorParams(size=2, distance=models.Distance.COSINE))
    try:
        yield QdrantKnowledgeStore(client, collection)
    finally:
        # Only this fixture's unique collection is removed.
        try:
            client.delete_collection(collection)
        finally:
            client.close()
