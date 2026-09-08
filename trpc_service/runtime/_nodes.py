"""TTL node directory and stable, load-aware rendezvous routing."""

from __future__ import annotations

import hashlib
import json
import time
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Callable
from typing import Literal
from typing import Optional

import redis.asyncio as aioredis
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field


class NodeInfo(BaseModel):
    """One live service node and its advertised capacity."""

    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(min_length=1, max_length=255)
    role: Literal["gateway", "worker", "outbox"]
    capacity: int = Field(default=1, ge=1)
    active_sessions: int = Field(default=0, ge=0)
    metadata: dict[str, str] = Field(default_factory=dict)
    heartbeat_at: float = Field(default_factory=time.time)


class NodeDirectoryABC(ABC):
    """Shared port for node liveness registration."""

    @abstractmethod
    async def heartbeat(self, node: NodeInfo, ttl_seconds: float) -> None:
        """Create or refresh a node lease."""

    @abstractmethod
    async def healthy(self, role: Optional[str] = None) -> list[NodeInfo]:
        """List non-expired nodes in deterministic order."""

    @abstractmethod
    async def remove(self, node_id: str) -> None:
        """Remove a node during graceful shutdown."""


class InMemoryNodeDirectory(NodeDirectoryABC):
    """Process-local implementation with an injectable clock."""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._nodes: dict[str, tuple[NodeInfo, float]] = {}

    async def heartbeat(self, node: NodeInfo, ttl_seconds: float) -> None:
        now = self._clock()
        stored = node.model_copy(deep=True, update={"heartbeat_at": now})
        self._nodes[node.node_id] = (stored, now + ttl_seconds)

    async def healthy(self, role: Optional[str] = None) -> list[NodeInfo]:
        now = self._clock()
        expired = [node_id for node_id, (_node, deadline) in self._nodes.items() if deadline <= now]
        for node_id in expired:
            self._nodes.pop(node_id, None)
        nodes = [node for node, _deadline in self._nodes.values() if role is None or node.role == role]
        return [node.model_copy(deep=True) for node in sorted(nodes, key=lambda item: item.node_id)]

    async def remove(self, node_id: str) -> None:
        self._nodes.pop(node_id, None)


class RedisNodeDirectory(NodeDirectoryABC):
    """Redis-backed TTL directory shared by all runtime roles."""

    def __init__(
        self,
        *,
        redis_url: Optional[str] = None,
        client: Any = None,
        prefix: str = "trpc-service:nodes",
    ) -> None:
        if client is not None:
            self._redis = client
        elif redis_url:
            self._redis = aioredis.from_url(redis_url, decode_responses=True)
        else:
            raise ValueError("RedisNodeDirectory requires redis_url or client")
        self._prefix = prefix
        self._index = f"{prefix}:index"

    def _key(self, node_id: str) -> str:
        return f"{self._prefix}:node:{node_id}"

    async def heartbeat(self, node: NodeInfo, ttl_seconds: float) -> None:
        now = time.time()
        ttl = max(1, int(ttl_seconds))
        stored = node.model_copy(update={"heartbeat_at": now})
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.set(self._key(node.node_id), stored.model_dump_json(), ex=ttl)
            pipe.zadd(self._index, {node.node_id: now + ttl_seconds})
            pipe.expire(self._index, ttl * 2)
            await pipe.execute()

    async def healthy(self, role: Optional[str] = None) -> list[NodeInfo]:
        now = time.time()
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.zremrangebyscore(self._index, "-inf", now)
            pipe.zrangebyscore(self._index, now, "+inf")
            results = await pipe.execute()
        node_ids = results[-1]
        if not node_ids:
            return []
        payloads = await self._redis.mget([self._key(node_id) for node_id in node_ids])
        missing = []
        nodes = []
        for node_id, payload in zip(node_ids, payloads):
            if payload is None:
                missing.append(node_id)
                continue
            node = NodeInfo.model_validate(json.loads(payload))
            if role is None or node.role == role:
                nodes.append(node)
        if missing:
            await self._redis.zrem(self._index, *missing)
        return sorted(nodes, key=lambda item: item.node_id)

    async def remove(self, node_id: str) -> None:
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.delete(self._key(node_id))
            pipe.zrem(self._index, node_id)
            await pipe.execute()

    async def close(self) -> None:
        await self._redis.aclose()


class RendezvousRouter:
    """Map a session key stably while accounting for node capacity/load."""

    @staticmethod
    def choose(routing_key: str, nodes: list[NodeInfo]) -> Optional[NodeInfo]:
        if not nodes:
            return None

        def score(node: NodeInfo) -> float:
            digest = hashlib.sha256(f"{routing_key}\0{node.node_id}".encode("utf-8")).digest()
            uniform = (int.from_bytes(digest, "big") + 1) / (2**256)
            available_weight = node.capacity / (node.active_sessions + 1)
            return uniform**(1 / available_weight)

        return max(nodes, key=lambda node: (score(node), node.node_id)).model_copy(deep=True)
