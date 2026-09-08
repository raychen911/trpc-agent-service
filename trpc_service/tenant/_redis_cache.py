# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
"""Redis tenant config cache and cross-node invalidation notifications."""

from __future__ import annotations

import json
import threading
from typing import Callable
from typing import Optional

import redis

from ._models import Tenant
from ._persistence import MySqlTenantRepository
from ._persistence import TenantConfigCodec


class RedisTenantConfigCache:
    """Encrypted L2 config cache with a small Pub/Sub invalidation channel."""

    CHANNEL = "tenant-config-events"

    def __init__(self, redis_url: str, codec: TenantConfigCodec, ttl_seconds: int = 600) -> None:
        self._client = redis.Redis.from_url(redis_url, decode_responses=True)
        self._codec = codec
        self._ttl = ttl_seconds
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    @staticmethod
    def _key(tenant_id: str) -> str:
        return f"tenant:{tenant_id}:config"

    @staticmethod
    def _version_key(tenant_id: str) -> str:
        return f"tenant:{tenant_id}:version"

    def get(self, tenant_id: str) -> Optional[tuple[Tenant, int]]:
        raw = self._client.get(self._key(tenant_id))
        if not raw:
            return None
        envelope = json.loads(raw)
        return self._codec.decode(envelope["config"], envelope.get("encrypted_secrets")), int(envelope["version"])

    def set(self, tenant: Tenant, version: int) -> None:
        public, encrypted = self._codec.encode(tenant)
        envelope = json.dumps({
            "version": version,
            "config": public,
            "encrypted_secrets": encrypted,
        },
                              ensure_ascii=False,
                              separators=(",", ":"))
        pipeline = self._client.pipeline(transaction=True)
        pipeline.set(self._key(tenant.tenant_id), envelope, ex=self._ttl)
        pipeline.set(self._version_key(tenant.tenant_id), version, ex=self._ttl)
        pipeline.execute()

    def invalidate(self, tenant_id: str) -> None:
        self._client.delete(self._key(tenant_id), self._version_key(tenant_id))

    def publish(self, tenant_id: str, version: int, event_type: str) -> None:
        self._client.publish(
            self.CHANNEL,
            json.dumps({
                "tenant_id": tenant_id,
                "version": version,
                "event_type": event_type,
            }, separators=(",", ":")))

    def start_listener(self, callback: Callable[[str, int, str], None]) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()

        def listen() -> None:
            pubsub = self._client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(self.CHANNEL)
            try:
                while not self._stop.is_set():
                    message = pubsub.get_message(timeout=1.0)
                    if message and message.get("type") == "message":
                        payload = json.loads(message["data"])
                        callback(payload["tenant_id"], int(payload["version"]), payload["event_type"])
            finally:
                pubsub.close()

        self._thread = threading.Thread(target=listen, name="tenant-config-listener", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._client.close()


class ConfigOutboxPublisher:
    """Replay committed MySQL outbox events into Redis notifications."""

    def __init__(self, repository: MySqlTenantRepository, cache: RedisTenantConfigCache) -> None:
        self._repository = repository
        self._cache = cache

    def publish_pending(self, limit: int = 100) -> int:
        published = 0
        for event in self._repository.pending_outbox(limit):
            self._cache.publish(event["tenant_id"], int(event["config_version"]), event["event_type"])
            self._repository.mark_outbox_published(event["event_id"])
            published += 1
        return published
