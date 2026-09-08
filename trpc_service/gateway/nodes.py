import asyncio
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from redis.asyncio import Redis

from trpc_service.gateway.contracts import NodeRecord


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class InMemoryNodeDirectory:
    def __init__(self) -> None:
        self._nodes: dict[str, NodeRecord] = {}
        self._guard = asyncio.Lock()

    async def heartbeat(
        self,
        node_id: str,
        base_url: str,
        *,
        capacity: int,
        ttl_seconds: float,
        metadata: Mapping[str, Any] | None = None,
    ) -> NodeRecord:
        record = NodeRecord(
            node_id=node_id,
            base_url=base_url.rstrip("/"),
            capacity=capacity,
            expires_at=_utcnow() + timedelta(seconds=ttl_seconds),
            metadata=dict(metadata or {}),
        )
        async with self._guard:
            self._nodes[node_id] = record
        return record

    async def list_healthy(self) -> Sequence[NodeRecord]:
        now = _utcnow()
        async with self._guard:
            expired = [key for key, value in self._nodes.items() if value.expires_at <= now]
            for key in expired:
                self._nodes.pop(key, None)
            return tuple(sorted(self._nodes.values(), key=lambda item: item.node_id))

    async def unregister(self, node_id: str) -> None:
        async with self._guard:
            self._nodes.pop(node_id, None)


class RedisNodeDirectory:
    def __init__(self, client: Redis, prefix: str = "trpc:nodes") -> None:
        self._redis = client
        self._prefix = prefix

    @classmethod
    def from_url(cls, url: str, prefix: str = "trpc:nodes") -> "RedisNodeDirectory":
        return cls(Redis.from_url(url, decode_responses=True), prefix)

    def _key(self, node_id: str) -> str:
        return f"{self._prefix}:{node_id}"

    async def heartbeat(
        self,
        node_id: str,
        base_url: str,
        *,
        capacity: int,
        ttl_seconds: float,
        metadata: Mapping[str, Any] | None = None,
    ) -> NodeRecord:
        expires_at = _utcnow() + timedelta(seconds=ttl_seconds)
        record = NodeRecord(
            node_id,
            base_url.rstrip("/"),
            capacity,
            expires_at,
            dict(metadata or {}),
        )
        payload = json.dumps(
            {
                "node_id": node_id,
                "base_url": record.base_url,
                "capacity": capacity,
                "expires_at": expires_at.isoformat(),
                "metadata": dict(record.metadata),
            }
        )
        await self._redis.set(self._key(node_id), payload, px=max(1, int(ttl_seconds * 1000)))
        await self._redis.sadd(self._prefix, node_id)
        return record

    async def list_healthy(self) -> Sequence[NodeRecord]:
        node_ids = sorted(await self._redis.smembers(self._prefix))
        if not node_ids:
            return ()
        payloads = await self._redis.mget([self._key(node_id) for node_id in node_ids])
        records = []
        expired = []
        for node_id, payload in zip(node_ids, payloads, strict=True):
            if payload is None:
                expired.append(node_id)
                continue
            value = json.loads(payload)
            records.append(
                NodeRecord(
                    node_id=value["node_id"],
                    base_url=value["base_url"],
                    capacity=int(value["capacity"]),
                    expires_at=datetime.fromisoformat(value["expires_at"]),
                    metadata=value.get("metadata", {}),
                )
            )
        if expired:
            await self._redis.srem(self._prefix, *expired)
        return tuple(records)

    async def unregister(self, node_id: str) -> None:
        await self._redis.delete(self._key(node_id))
        await self._redis.srem(self._prefix, node_id)

    async def close(self) -> None:
        await self._redis.aclose()
