# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Storage migration full-copy, isolation, checksum and dual-write tests."""

from __future__ import annotations

import pytest

from trpc_service.workspace import DualWriteBackend
from trpc_service.workspace import LocalMigrationBackend
from trpc_service.workspace import StorageMigrator
from trpc_service.workspace import StorageRecord


def _record(tenant: str, kind: str, record_id: str, value: str, version: int = 1) -> StorageRecord:
    return StorageRecord(
        tenant_id=tenant,
        kind=kind,
        record_id=record_id,
        version=version,
        payload={"value": value},
    )


async def test_migrator_copies_only_selected_tenant_and_verifies_checksums():
    source = LocalMigrationBackend([
        _record("tenant_a", "session", "s1", "一"),
        _record("tenant_a", "session", "s2", "two"),
        _record("tenant_a", "memory", "m1", "memory"),
        _record("tenant_b", "session", "s1", "must-not-copy"),
    ])
    target = LocalMigrationBackend()
    report = await StorageMigrator(source, target, batch_size=1).migrate("tenant_a", ["session", "memory"])

    assert report.verified is True
    assert report.copied_by_kind == {"session": 2, "memory": 1}
    foreign, _ = await target.scan("tenant_b", "session", None, 10)
    assert foreign == []


async def test_dual_write_updates_both_backends_and_ignores_older_versions():
    source = LocalMigrationBackend()
    target = LocalMigrationBackend()
    dual = DualWriteBackend(source, target)

    await dual.upsert(_record("tenant_a", "session", "s1", "new", version=2))
    await dual.upsert(_record("tenant_a", "session", "s1", "old", version=1))
    for backend in (source, target):
        records, _ = await backend.scan("tenant_a", "session", None, 10)
        assert records[0].payload["value"] == "new"


def test_migrator_rejects_invalid_batch_size():
    with pytest.raises(ValueError, match="positive"):
        StorageMigrator(LocalMigrationBackend(), LocalMigrationBackend(), batch_size=0)
