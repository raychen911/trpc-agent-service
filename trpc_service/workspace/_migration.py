# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Backend-neutral migration primitives with checksummed verification."""

from __future__ import annotations

import hashlib
import json
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict


class StorageRecord(BaseModel):
    """Canonical record exchanged by migration adapters."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    kind: str
    record_id: str
    version: int = 0
    payload: dict[str, Any]

    def checksum(self) -> str:
        canonical = self.model_dump(mode="json")
        encoded = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


class MigrationBackend(ABC):
    """Minimal scan/upsert interface implemented by source and target adapters."""

    @abstractmethod
    async def scan(
        self,
        tenant_id: str,
        kind: str,
        cursor: Optional[str],
        limit: int,
    ) -> tuple[list[StorageRecord], Optional[str]]:
        raise NotImplementedError

    @abstractmethod
    async def upsert(self, record: StorageRecord) -> None:
        raise NotImplementedError


class MigrationReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    copied_by_kind: dict[str, int]
    source_checksums: dict[str, str]
    target_checksums: dict[str, str]
    verified: bool


class StorageMigrator:
    """Perform online-friendly full copy and deterministic verification."""

    def __init__(self, source: MigrationBackend, target: MigrationBackend, batch_size: int = 500) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self._source = source
        self._target = target
        self._batch_size = batch_size

    async def copy_kind(self, tenant_id: str, kind: str) -> int:
        cursor: Optional[str] = None
        copied = 0
        while True:
            records, cursor = await self._source.scan(tenant_id, kind, cursor, self._batch_size)
            for record in records:
                if record.tenant_id != tenant_id or record.kind != kind:
                    raise ValueError("migration backend returned an out-of-scope record")
                await self._target.upsert(record)
                copied += 1
            if cursor is None:
                return copied

    async def _digest(self, backend: MigrationBackend, tenant_id: str, kind: str) -> str:
        cursor: Optional[str] = None
        checksums: list[str] = []
        while True:
            records, cursor = await backend.scan(tenant_id, kind, cursor, self._batch_size)
            checksums.extend(record.checksum() for record in records)
            if cursor is None:
                break
        joined = "".join(sorted(checksums)).encode()
        return hashlib.sha256(joined).hexdigest()

    async def migrate(self, tenant_id: str, kinds: list[str]) -> MigrationReport:
        copied: dict[str, int] = {}
        source_checksums: dict[str, str] = {}
        target_checksums: dict[str, str] = {}
        for kind in kinds:
            copied[kind] = await self.copy_kind(tenant_id, kind)
            source_checksums[kind] = await self._digest(self._source, tenant_id, kind)
            target_checksums[kind] = await self._digest(self._target, tenant_id, kind)
        return MigrationReport(
            tenant_id=tenant_id,
            copied_by_kind=copied,
            source_checksums=source_checksums,
            target_checksums=target_checksums,
            verified=source_checksums == target_checksums,
        )


class DualWriteBackend(MigrationBackend):
    """Write to source and migration target while reads remain on source."""

    def __init__(self, source: MigrationBackend, target: MigrationBackend) -> None:
        self._source = source
        self._target = target

    async def scan(self, tenant_id: str, kind: str, cursor: Optional[str],
                   limit: int) -> tuple[list[StorageRecord], Optional[str]]:
        return await self._source.scan(tenant_id, kind, cursor, limit)

    async def upsert(self, record: StorageRecord) -> None:
        await self._source.upsert(record)
        await self._target.upsert(record)


class LocalMigrationBackend(MigrationBackend):
    """Deterministic adapter used by migration tests and local dry-runs."""

    def __init__(self, records: Optional[list[StorageRecord]] = None) -> None:
        self._records: dict[tuple[str, str, str], StorageRecord] = {}
        for record in records or []:
            self._records[(record.tenant_id, record.kind, record.record_id)] = record

    async def scan(self, tenant_id: str, kind: str, cursor: Optional[str],
                   limit: int) -> tuple[list[StorageRecord], Optional[str]]:
        records = sorted(
            (record
             for (tid, record_kind, _), record in self._records.items() if tid == tenant_id and record_kind == kind),
            key=lambda record: record.record_id,
        )
        offset = int(cursor or 0)
        batch = records[offset:offset + limit]
        next_offset = offset + len(batch)
        next_cursor = str(next_offset) if next_offset < len(records) else None
        return [record.model_copy(deep=True) for record in batch], next_cursor

    async def upsert(self, record: StorageRecord) -> None:
        key = (record.tenant_id, record.kind, record.record_id)
        current = self._records.get(key)
        if current is None or record.version >= current.version:
            self._records[key] = record.model_copy(deep=True)
