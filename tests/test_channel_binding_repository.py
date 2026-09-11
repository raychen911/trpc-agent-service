"""Focused unit contracts for the R2A channel binding repository."""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace

import pytest

from trpc_service.channels.binding import ChannelBinding
from trpc_service.storage.channel_binding_repository import (
    ChannelBindingRepositoryDataError,
    ChannelBindingRepositoryUnavailableError,
    SqlChannelBindingRepository,
    _normalized_binding,
    _normalize_lookup,
    _row_to_binding,
)
from trpc_service.storage.schema import channel_binding_versions, channel_bindings


def _binding(**overrides) -> ChannelBinding:
    values = {
        "binding_id": uuid.uuid4(),
        "tenant_id": "tenant_a",
        "app_id": "app_demo",
        "channel": "wecom",
        "external_account_id": "robot-1",
        "secret_ref": "env:TRPC_WECOM_ROBOT",
        "enabled": True,
        "version": 1,
    }
    values.update(overrides)
    return ChannelBinding(**values)


class _Result:

    def __init__(self, row=None):
        self._row = row

    def first(self):
        return self._row


class _Connection:

    def __init__(self, results):
        self.results = list(results)
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return self.results.pop(0) if self.results else _Result()


class _Begin:

    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_):
        return None


class _Engine:

    def __init__(self, connection):
        self.connection = connection
        self.disposed = 0

    def begin(self):
        return _Begin(self.connection)

    async def dispose(self):
        self.disposed += 1


def test_repository_preserves_case_sensitive_external_account_id():
    normalized = _normalized_binding(_binding(external_account_id="Robot-1"), version=3)
    assert normalized.external_account_id == "Robot-1"
    assert normalized.version == 3
    assert _normalize_lookup("wecom", " Robot-1 ") == ("wecom", "Robot-1")


def test_schema_permits_case_sensitive_but_trimmed_external_account_ids():
    for table, constraint_name in (
        (channel_bindings, "channel_bindings_account_normalized"),
        (channel_binding_versions, "channel_binding_versions_account_normalized"),
    ):
        constraint = next(constraint for constraint in table.constraints if constraint.name == constraint_name)
        assert str(constraint.sqltext) == (
            "external_account_id = btrim(external_account_id) AND external_account_id <> ''")


def test_schema_keeps_optional_wecom_webhook_secret_references():
    for table in (channel_bindings, channel_binding_versions):
        assert table.c.webhook_token_ref.nullable is True
        assert table.c.webhook_aes_key_ref.nullable is True


def test_create_checks_current_tenant_app_and_writes_head_and_snapshot():
    current_app = SimpleNamespace(_mapping={"app_id": "app_demo"})
    connection = _Connection([_Result(current_app), _Result(), _Result()])
    repository = SqlChannelBindingRepository(_Engine(connection), owns_engine=False)

    created = asyncio.run(repository.create(_binding(external_account_id="Robot-1")))

    assert created.version == 1
    assert created.external_account_id == "Robot-1"
    assert len(connection.statements) == 3


def test_create_rejects_binding_for_a_different_current_app():
    current_app = SimpleNamespace(_mapping={"app_id": "other_app"})
    repository = SqlChannelBindingRepository(_Engine(_Connection([_Result(current_app)])), owns_engine=False)

    with pytest.raises(ChannelBindingRepositoryDataError, match="app does not match"):
        asyncio.run(repository.create(_binding()))


def test_corrupt_database_row_and_closed_repository_fail_closed():
    with pytest.raises(ChannelBindingRepositoryDataError, match="row is corrupt"):
        _row_to_binding(SimpleNamespace(_mapping={"tenant_id": "tenant_a"}))

    engine = _Engine(_Connection([]))
    repository = SqlChannelBindingRepository(engine)
    asyncio.run(repository.close())
    asyncio.run(repository.close())
    assert engine.disposed == 1
    with pytest.raises(ChannelBindingRepositoryUnavailableError, match="closed"):
        asyncio.run(repository.list_for_tenant("tenant_a"))
