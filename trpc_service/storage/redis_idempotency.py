"""Redis-backed atomic message claim and terminal repository."""

from __future__ import annotations

import secrets
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from trpc_service.storage.contracts import ConditionalWriteFailed, NotFound, StateBackendUnavailable
from trpc_service.storage.models import (
    ClaimDisposition, ClaimResult, ExecutionPhase, ExecutionResult, ExecutionStatus,
    IdempotencyKey, IdempotencyRecord, IdempotencyState,
)
from trpc_service.storage.redis_codec import RedisKeyCodec
from trpc_service.storage.redis_scripts.loader import RedisScriptLoader


_DIRECTORY = Path(__file__).with_name("redis_scripts")
_CLAIM = (_DIRECTORY / "idempotency_claim.lua").read_text(encoding="utf-8")
_TRANSITION = (_DIRECTORY / "idempotency_complete.lua").read_text(encoding="utf-8")
_INSPECT = (_DIRECTORY / "idempotency_inspect.lua").read_text(encoding="utf-8")
_RECOVER = (_DIRECTORY / "idempotency_recover.lua").read_text(encoding="utf-8")


class RedisIdempotencyRepository:
    def __init__(self, redis: Any, *, namespace: str = "trpc:v1", node_id: str,
                 owner_lease_ms: int = 60_000) -> None:
        self.redis = redis
        self.codec = RedisKeyCodec(namespace)
        self.loader = RedisScriptLoader(redis)
        self.node_id = node_id
        self.owner_lease_ms = owner_lease_ms

    async def claim(self, key: IdempotencyKey, fingerprint: str, trace_id: UUID, now: datetime) -> ClaimResult:
        token = secrets.token_urlsafe(24)
        initial = IdempotencyRecord.pending(
            key=key, content_fingerprint=fingerprint, owner_token=token, trace_id=trace_id, now=now
        )
        try:
            result = await self.loader.execute_source(
                "idempotency_claim_v2", _CLAIM,
                [self.codec.idempotency(key), self.codec.message_lease(key)],
                [initial.model_dump_json(), fingerprint, token, str(trace_id), now.isoformat(), self.owner_lease_ms],
            )
        except Exception:
            raise StateBackendUnavailable() from None
        disposition = str(result[0])
        record = IdempotencyRecord.model_validate_json(result[1])
        if disposition == "acquired":
            return ClaimResult(disposition=ClaimDisposition.ACQUIRED, owner_token=record.owner_token, attempt=record.attempt)
        if disposition == "conflict":
            return ClaimResult(disposition=ClaimDisposition.CONFLICT)
        if disposition == "processing":
            return ClaimResult(disposition=ClaimDisposition.PROCESSING, original_trace_id=record.owner_trace_id)
        if disposition == "outcome_unknown":
            return ClaimResult(disposition=ClaimDisposition.OUTCOME_UNKNOWN, original_trace_id=record.execution_trace_id or record.owner_trace_id)
        return ClaimResult(disposition=ClaimDisposition.COMPLETED, original_trace_id=record.execution_trace_id, result=record.result)

    async def complete_from_recovery(self, marker: Any) -> str:
        redis_key = f"{self.codec.namespace}:msg:{marker.idempotency_key_digest}"
        try:
            raw = await self.redis.get(redis_key)
            if raw is None:
                raise ConditionalWriteFailed("Recovery target is missing.")
            current = IdempotencyRecord.model_validate_json(raw)
            result = marker.execution_result
            terminal = IdempotencyState(result.status.value)
            replacement = current.model_copy(update={
                "state": terminal, "execution_phase": ExecutionPhase.TERMINAL, "owner_token": None,
                "owner_trace_id": None, "result": result, "updated_at": marker.created_at,
            })
            response = await self.loader.execute_source(
                "idempotency_recover_v1", _RECOVER, [redis_key],
                [marker.message_generation, str(marker.execution_trace_id), replacement.model_dump_json()],
            )
        except ConditionalWriteFailed:
            raise
        except Exception:
            raise StateBackendUnavailable() from None
        disposition = str(response[0])
        if disposition not in {"reconciled", "already_terminal"}:
            raise ConditionalWriteFailed(f"Recovery CAS rejected: {disposition}.")
        return disposition

    async def _replace(self, key: IdempotencyKey, owner_token: str, expected: IdempotencyState,
                       replacement: IdempotencyRecord, *, release: bool = False) -> IdempotencyRecord:
        try:
            result = await self.loader.execute_source(
                "idempotency_transition_v2", _TRANSITION,
                [self.codec.idempotency(key), self.codec.message_lease(key)],
                [owner_token, expected.value, replacement.model_dump_json(), 1 if release else 0],
            )
        except Exception:
            raise StateBackendUnavailable() from None
        if result[0] != "updated":
            raise ConditionalWriteFailed("Idempotency transition was rejected.")
        return replacement

    async def mark_running(self, key: IdempotencyKey, owner_token: str, execution_trace_id: UUID, now: datetime) -> IdempotencyRecord:
        current = await self.get(key)
        try:
            updated = current.mark_running(owner_token, execution_trace_id, now)
        except ValueError:
            raise ConditionalWriteFailed("Idempotency transition was rejected.") from None
        return await self._replace(key, owner_token, IdempotencyState.PENDING, updated)

    async def mark_pre_start_failed(self, key: IdempotencyKey, owner_token: str, safe_error: str, now: datetime) -> IdempotencyRecord:
        current = await self.get(key)
        try:
            updated = current.mark_pre_start_failed(owner_token, safe_error, now)
        except ValueError:
            raise ConditionalWriteFailed("Idempotency transition was rejected.") from None
        return await self._replace(key, owner_token, IdempotencyState.PENDING, updated, release=True)

    async def complete(self, key: IdempotencyKey, owner_token: str, result: ExecutionResult, now: datetime) -> IdempotencyRecord:
        current = await self.get(key)
        try:
            updated = current.complete(owner_token, result, now)
        except ValueError:
            raise ConditionalWriteFailed("Idempotency transition was rejected.") from None
        return await self._replace(key, owner_token, IdempotencyState.RUNNING, updated, release=True)

    async def mark_post_start_failed(self, key: IdempotencyKey, owner_token: str, result: ExecutionResult, now: datetime) -> IdempotencyRecord:
        if result.status != ExecutionStatus.FAILED_POST_START:
            raise ValueError("post-start failure requires a failed result")
        return await self.complete(key, owner_token, result, now)

    async def mark_outcome_unknown(self, key: IdempotencyKey, owner_token: str, result: ExecutionResult, now: datetime) -> IdempotencyRecord:
        if result.status != ExecutionStatus.OUTCOME_UNKNOWN:
            raise ValueError("outcome-unknown requires an unknown result")
        return await self.complete(key, owner_token, result, now)

    async def get(self, key: IdempotencyKey) -> IdempotencyRecord:
        try:
            raw = await self.loader.execute_source("idempotency_inspect_v1", _INSPECT, [self.codec.idempotency(key)], [])
        except Exception:
            raise StateBackendUnavailable() from None
        if raw is None:
            raise NotFound("Idempotency record was not found.")
        return IdempotencyRecord.model_validate_json(raw)

    async def reset(self) -> None:
        # Deliberately scoped to this adapter namespace; production code has no global reset.
        cursor = 0
        pattern = f"{self.codec.namespace}:msg:*"
        while True:
            cursor, keys = await self.redis.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                await self.redis.delete(*keys)
            if cursor == 0:
                break
