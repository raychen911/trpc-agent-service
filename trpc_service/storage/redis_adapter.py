"""Redis-backed tenant Memory adapter."""

from __future__ import annotations

import hashlib

from redis.asyncio import Redis

from trpc_service.config.models import MemoryRecord
from trpc_service.storage.contracts import StorageConflictError


class RedisMemoryStore:
    def __init__(self, client: Redis, namespace: str = "trpc-service") -> None:
        self._client = client
        self._namespace = namespace

    async def create(self, record: MemoryRecord) -> MemoryRecord:
        records_key, index_key = self._keys(record.tenant_id, record.principal_id)
        source_key: str | None = None
        if record.source_event_id:
            source_key = (
                f"{self._namespace}:tenant:{record.tenant_id}:memory-source:"
                f"{record.source_event_id}"
            )
            reserved = await self._client.set(source_key, record.memory_id, nx=True)
            if not reserved:
                raise StorageConflictError("memory source event already exists")
        try:
            async with self._client.pipeline(transaction=True) as pipeline:
                pipeline.hset(records_key, record.memory_id, record.model_dump_json())
                pipeline.zadd(index_key, {record.memory_id: record.created_at.timestamp()})
                await pipeline.execute()
        except Exception:
            if source_key:
                await self._client.delete(source_key)
            raise
        return record

    async def list_for_principal(
        self, tenant_id: str, principal_id: str, limit: int = 20
    ) -> list[MemoryRecord]:
        if limit <= 0:
            return []
        records_key, index_key = self._keys(tenant_id, principal_id)
        memory_ids = await self._client.zrevrange(index_key, 0, limit - 1)
        if not memory_ids:
            return []
        raw_records = await self._client.hmget(records_key, memory_ids)
        return [MemoryRecord.model_validate_json(raw) for raw in raw_records if raw is not None]

    def _keys(self, tenant_id: str, principal_id: str) -> tuple[str, str]:
        principal_hash = hashlib.sha256(principal_id.encode("utf-8")).hexdigest()
        prefix = f"{self._namespace}:tenant:{tenant_id}:principal:{principal_hash}:memory"
        return f"{prefix}:records", f"{prefix}:index"


__all__ = ["RedisMemoryStore"]
