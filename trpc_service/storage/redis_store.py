"""Redis-backed storage for shared state, idempotency, and session events."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime
from typing import Any

from trpc_service.storage.base import (
    AuditRecord,
    IdempotencyRecord,
    IdempotencyStatus,
    MemoryItem,
    SessionEvent,
    SessionState,
    Summary,
    now_utc,
)
from trpc_service.storage.compensation import RedisCompensationStore
from trpc_service.storage.locking import RedisSessionLockMixin, SessionLeaseLost


def _encode(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _decode(value: bytes | str | None) -> Any:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return json.loads(value)


def _dt(value: datetime) -> str:
    return value.isoformat()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


class RedisStorage(RedisSessionLockMixin):
    backend_name = "redis"

    def __init__(self, url: str | None = None, prefix: str = "trpc-agent") -> None:
        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("Redis backend requires the 'redis' package") from exc
        self.url = url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.prefix = prefix
        self.client = redis.Redis.from_url(self.url, decode_responses=False)
        self.client.ping()
        self.session = self
        self.memory = self
        self.summary = self
        self.audit = self
        self.idempotency = self
        self.compensation = RedisCompensationStore(
            self.client,
            self.prefix,
            visibility_timeout=int(os.getenv("COMPENSATION_VISIBILITY_TIMEOUT_SECONDS", "180")),
            max_attempts=int(os.getenv("COMPENSATION_MAX_ATTEMPTS", "10")),
        )
        # 幂等记录 TTL（秒），默认 7 天
        self.idempotency_ttl = int(os.getenv("IDEMPOTENCY_TTL_SECONDS", str(7 * 24 * 3600)))

    def _key(self, kind: str, tenant_id: str, suffix: str = "") -> str:
        value = f"{self.prefix}:{kind}:{tenant_id}"
        return f"{value}:{suffix}" if suffix else value

    def append_event(self, event: SessionEvent, fencing_token: int | None = None) -> int:
        duplicate_key = (
            self._key(
                "event-idempotency",
                event.tenant_id,
                f"{event.session_id}:{event.idempotency_key}:{event.event_type}",
            )
            if event.idempotency_key
            else self._key(
                "event-no-idempotency",
                event.tenant_id,
                event.event_id,
            )
        )
        seq_key = self._key("session-seq", event.tenant_id, event.session_id)
        events_key = self._key("session-events", event.tenant_id, event.session_id)
        state_key = self._key("session-state", event.tenant_id, event.session_id)
        lease_key = self._key("session-lease", event.tenant_id, event.session_id)
        payload = {
            "tenant_id": event.tenant_id,
            "session_id": event.session_id,
            "event_id": event.event_id,
            "event_type": event.event_type,
            "payload": event.payload,
            "trace_id": event.trace_id,
            "idempotency_key": event.idempotency_key,
            "created_at": _dt(event.created_at),
        }
        script = """
        local fencing = tonumber(ARGV[3] or '0')
        if fencing > 0 then
          local lease_raw = redis.call('GET', KEYS[5])
          if not lease_raw then return -1 end
          local lease = cjson.decode(lease_raw)
          if tonumber(lease['fencing_token'] or '0') ~= fencing then return -1 end
        end
        if ARGV[2] ~= '' then
          local existing = redis.call('GET', KEYS[3])
          if existing then
            return tonumber(existing)
          end
        end
        local seq = redis.call('INCR', KEYS[1])
        local payload = cjson.decode(ARGV[1])
        payload['seq'] = seq
        redis.call('RPUSH', KEYS[2], cjson.encode(payload))
        if ARGV[2] ~= '' then
          local ttl = tonumber(ARGV[4] or '604800')
          redis.call('SET', KEYS[3], tostring(seq), 'EX', ttl)
        end
        if redis.call('EXISTS', KEYS[4]) == 0 then
          redis.call('SET', KEYS[4], cjson.encode({state_version = 0, state = {}}))
        end
        return seq
        """
        seq = self.client.eval(
            script,
            5,
            seq_key,
            events_key,
            duplicate_key,
            state_key,
            lease_key,
            _encode(payload),
            event.idempotency_key or "",
            str(int(fencing_token or 0)),
            str(self.idempotency_ttl),
        )
        if int(seq) == -1:
            raise SessionLeaseLost(f"session fencing token rejected: {event.tenant_id}/{event.session_id}")
        return int(seq)

    def load_events(self, tenant_id: str, session_id: str, after_seq: int = 0) -> list[SessionEvent]:
        values = self.client.lrange(self._key("session-events", tenant_id, session_id), after_seq, -1)
        result = []
        for value in values:
            item = _decode(value)
            created_at = _parse_dt(item.pop("created_at"))
            result.append(SessionEvent(**item, created_at=created_at))
        return result

    def load_state(self, tenant_id: str, session_id: str) -> SessionState:
        raw = _decode(self.client.get(self._key("session-state", tenant_id, session_id)))
        latest = int(self.client.get(self._key("session-seq", tenant_id, session_id)) or 0)
        if not raw:
            return SessionState(tenant_id=tenant_id, session_id=session_id, latest_event_seq=latest)
        return SessionState(
            tenant_id=tenant_id,
            session_id=session_id,
            state=dict(raw.get("state", {})),
            state_version=int(raw.get("state_version", 0)),
            latest_event_seq=latest,
        )

    def compare_and_set_state(
        self,
        tenant_id: str,
        session_id: str,
        expected_version: int,
        state: dict[str, Any],
        fencing_token: int | None = None,
    ) -> bool:
        key = self._key("session-state", tenant_id, session_id)
        lease_key = self._key("session-lease", tenant_id, session_id)
        script = """
        local fencing = tonumber(ARGV[3] or '0')
        if fencing > 0 then
          local lease_raw = redis.call('GET', KEYS[2])
          if not lease_raw then return -1 end
          local lease = cjson.decode(lease_raw)
          if tonumber(lease['fencing_token'] or '0') ~= fencing then return -1 end
        end
        local raw = redis.call('GET', KEYS[1])
        local current = 0
        if raw then
          current = tonumber(cjson.decode(raw)['state_version'] or '0')
        end
        if current ~= tonumber(ARGV[1]) then return 0 end
        redis.call('SET', KEYS[1], ARGV[2])
        return 1
        """
        updated = self.client.eval(
            script,
            2,
            key,
            lease_key,
            str(expected_version),
            _encode({"state_version": expected_version + 1, "state": state}),
            str(int(fencing_token or 0)),
        )
        if int(updated) == -1:
            raise SessionLeaseLost(f"session fencing token rejected: {tenant_id}/{session_id}")
        return int(updated) == 1

    def restore_state(
        self,
        tenant_id: str,
        session_id: str,
        state: dict[str, Any],
        state_version: int,
    ) -> None:
        key = self._key("session-state", tenant_id, session_id)
        self.client.set(
            key,
            _encode(
                {
                    "state_version": int(state_version),
                    "state": state,
                }
            ),
        )

    def put(self, value: MemoryItem | Summary) -> None:
        if isinstance(value, Summary):
            key = self._key("summary", value.tenant_id, value.session_id)
            script = """
            local current = redis.call('GET', KEYS[1])
            if current then
              local item = cjson.decode(current)
              local source = tonumber(item['source_event_seq'] or '0')
              if source > tonumber(ARGV[2]) then return 0 end
              local version = tonumber(item['summary_version'] or '0') + 1
              local next_item = cjson.decode(ARGV[1])
              next_item['summary_version'] = version
              redis.call('SET', KEYS[1], cjson.encode(next_item))
              return version
            end
            local next_item = cjson.decode(ARGV[1])
            next_item['summary_version'] = 1
            redis.call('SET', KEYS[1], cjson.encode(next_item))
            return 1
            """
            encoded = _encode({**asdict(value), "created_at": _dt(value.created_at)})
            version = self.client.eval(
                script,
                1,
                key,
                encoded,
                int(value.source_event_seq),
            )
            value.summary_version = int(version)
            return
        key = self._key("memory", value.tenant_id, value.memory_id)
        self.client.set(key, _encode({**asdict(value), "created_at": _dt(value.created_at)}))
        self.client.sadd(self._key("memory-index", value.tenant_id), value.memory_id)

    def search(
        self,
        tenant_id: str,
        query: str,
        limit: int = 5,
        scope_keys: tuple[str, ...] | None = None,
    ) -> list[MemoryItem]:
        words = {word.lower() for word in query.split() if word}
        allowed_scopes = set(scope_keys) if scope_keys is not None else None
        items = []
        for memory_id in self.client.smembers(self._key("memory-index", tenant_id)):
            item = _decode(
                self.client.get(
                    self._key("memory", tenant_id, memory_id.decode() if isinstance(memory_id, bytes) else memory_id)
                )
            )
            if item and (allowed_scopes is None or item.get("scope_key") in allowed_scopes):
                score = sum(1 for word in words if word in item["content"].lower())
                if score or not words:
                    items.append((score, item))
        items.sort(key=lambda pair: -pair[0])
        result = []
        for _, item in items[:limit]:
            created_at = _parse_dt(item.pop("created_at"))
            result.append(MemoryItem(**item, created_at=created_at))
        return result

    def latest(self, tenant_id: str, session_id: str) -> Summary | None:
        item = _decode(self.client.get(self._key("summary", tenant_id, session_id)))
        if not item:
            return None
        created_at = _parse_dt(item.pop("created_at"))
        return Summary(**item, created_at=created_at)

    def append(self, record: AuditRecord) -> None:
        key = self._key("audit", record.tenant_id)
        self.client.lpush(key, _encode({**asdict(record), "created_at": _dt(record.created_at)}))

    def list_by_tenant(self, tenant_id: str, limit: int = 100) -> list[AuditRecord]:
        return [
            self._audit_record(_decode(value))
            for value in self.client.lrange(self._key("audit", tenant_id), 0, limit - 1)
        ]

    @staticmethod
    def _audit_record(item: dict) -> AuditRecord:
        created_at = _parse_dt(item.pop("created_at"))
        return AuditRecord(**item, created_at=created_at)

    def start(
        self,
        tenant_id: str,
        key: str,
        trace_id: str,
        lease_seconds: int = 180,
    ) -> IdempotencyRecord:
        redis_key = self._key("idempotency", tenant_id, key)
        now = now_utc()
        record = IdempotencyRecord(
            tenant_id, key, IdempotencyStatus.PROCESSING, trace_id=trace_id, created_at=now, updated_at=now
        )
        payload = _encode(
            {**asdict(record), "status": record.status.value, "created_at": _dt(now), "updated_at": _dt(now)}
        )
        import redis

        with self.client.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(redis_key)
                    current = self.get(tenant_id, key)
                    if current is None:
                        pipe.multi()
                        pipe.set(redis_key, payload, ex=self.idempotency_ttl)
                        pipe.execute()
                        return record
                    if current.status == IdempotencyStatus.FAILED or (
                        current.status == IdempotencyStatus.PROCESSING
                        and (now - current.updated_at).total_seconds() >= lease_seconds
                    ):
                        current.status = IdempotencyStatus.PROCESSING
                        current.trace_id = trace_id
                        current.response_ref = None
                        current.result = None
                        current.attempt += 1
                        current.updated_at = now
                        pipe.multi()
                        pipe.set(
                            redis_key,
                            _encode(
                                {
                                    **asdict(current),
                                    "status": current.status.value,
                                    "created_at": _dt(current.created_at),
                                    "updated_at": _dt(current.updated_at),
                                }
                            ),
                            ex=self.idempotency_ttl,
                        )
                        pipe.execute()
                        return current
                    pipe.unwatch()
                    return current
                except redis.WatchError:
                    continue

    def complete(self, tenant_id: str, key: str, response_ref: str, result: dict) -> IdempotencyRecord:
        record = self.get(tenant_id, key)
        if record is None:
            raise KeyError(f"idempotency record missing: {tenant_id}/{key}")
        record.status = IdempotencyStatus.COMPLETED
        record.response_ref = response_ref
        record.result = result
        record.updated_at = now_utc()
        self._save_idempotency(record)
        return record

    def fail(self, tenant_id: str, key: str, error_type: str) -> IdempotencyRecord:
        record = self.get(tenant_id, key)
        if record is None:
            raise KeyError(f"idempotency record missing: {tenant_id}/{key}")
        record.status = IdempotencyStatus.FAILED
        record.result = {"error_type": error_type}
        record.updated_at = now_utc()
        self._save_idempotency(record)
        return record

    def get(self, tenant_id: str, key: str) -> IdempotencyRecord | None:
        item = _decode(self.client.get(self._key("idempotency", tenant_id, key)))
        if not item:
            return None
        item["status"] = IdempotencyStatus(item["status"])
        item["attempt"] = int(item.get("attempt", 1))
        item["created_at"] = _parse_dt(item["created_at"])
        item["updated_at"] = _parse_dt(item["updated_at"])
        return IdempotencyRecord(**item)

    def _save_idempotency(self, record: IdempotencyRecord) -> None:
        self.client.set(
            self._key("idempotency", record.tenant_id, record.key),
            _encode(
                {
                    **asdict(record),
                    "status": record.status.value,
                    "created_at": _dt(record.created_at),
                    "updated_at": _dt(record.updated_at),
                }
            ),
            ex=self.idempotency_ttl,
        )

    def claim_delivery(self, tenant_id: str, key: str) -> bool:
        redis_key = self._key("idempotency", tenant_id, key)
        script = """
        local raw = redis.call('GET', KEYS[1])
        if not raw then return 0 end
        local item = cjson.decode(raw)
        if item['status'] ~= ARGV[1] then return 0 end
        local result = item['result'] or {}
        if result['delivered'] or result['delivery_claimed'] then return 0 end
        result['delivery_claimed'] = true
        item['result'] = result
        item['updated_at'] = ARGV[2]
        redis.call('SET', KEYS[1], cjson.encode(item))
        return 1
        """
        claimed = self.client.eval(
            script,
            1,
            redis_key,
            IdempotencyStatus.COMPLETED.value,
            _dt(now_utc()),
        )
        return int(claimed) == 1

    def release_delivery(self, tenant_id: str, key: str) -> None:
        redis_key = self._key("idempotency", tenant_id, key)
        script = """
        local raw = redis.call('GET', KEYS[1])
        if not raw then return 0 end
        local item = cjson.decode(raw)
        local result = item['result'] or {}
        result['delivery_claimed'] = nil
        item['result'] = result
        item['updated_at'] = ARGV[1]
        redis.call('SET', KEYS[1], cjson.encode(item))
        return 1
        """
        self.client.eval(script, 1, redis_key, _dt(now_utc()))

    def close(self) -> None:
        self.client.close()
