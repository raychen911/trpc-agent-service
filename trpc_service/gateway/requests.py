# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Durable request lifecycle stores."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Protocol

from trpc_service.gateway.models import AgentRequest
from trpc_service.gateway.models import ChatResult
from trpc_service.gateway.models import RequestRecord
from trpc_service.gateway.models import RequestState
from trpc_service.gateway.idempotency import IdempotencyConflictError


class RequestNotFoundError(KeyError):
    """Raised when an asynchronous request ID is unknown."""


class RequestStore(Protocol):

    async def save_payload(self, request: AgentRequest) -> None:
        """Persist trusted approval/budget metadata before admission to execution."""

    async def create(self, record: RequestRecord) -> RequestRecord:
        """Create a request exactly once."""

    async def reserve_and_create_request(self, record: RequestRecord) -> tuple[RequestRecord, bool]:
        """One transaction owns both the idempotency key and durable payload."""

    async def get(self, tenant_id: str, request_id: str) -> RequestRecord:
        """Return one tenant-scoped request."""

    async def transition(self,
                         tenant_id: str,
                         request_id: str,
                         state: RequestState,
                         *,
                         result: ChatResult | None = None,
                         error_code: str = "",
                         retryable: bool = False,
                         increment_attempts: bool = False) -> RequestRecord:
        """Update state and optional terminal result."""

    async def list_stale(self, *, older_than_seconds: int, limit: int = 100) -> list[RequestRecord]:
        """Return non-terminal records eligible for bounded repair."""

    async def increment_model_attempts(self, tenant_id: str, request_id: str) -> RequestRecord:
        """Record one model call immediately before it starts."""

    async def increment_successful_model_calls(self, tenant_id: str, request_id: str) -> RequestRecord:
        """Record one complete, error-free model response."""

    async def increment_recovery_count(self, tenant_id: str, request_id: str) -> RequestRecord:
        """Record one retry, reclaim or durable admission repair."""

    async def claim_stale(self, *, older_than_seconds: int, limit: int = 100) -> list[RequestRecord]:
        """Claim stale reserved admissions that may never have reached the queue."""


class InMemoryRequestStore:
    """Concurrency-safe local request store used by tests and development."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str], RequestRecord] = {}
        self._lock = asyncio.Lock()
        self._keys: dict[tuple[str, str], str] = {}

    async def reserve_and_create_request(self, record: RequestRecord) -> tuple[RequestRecord, bool]:
        async with self._lock:
            key = (record.tenant_id, record.idempotency_key)
            owner = self._keys.get(key) if record.idempotency_key else None
            if owner:
                existing = self._records[(record.tenant_id, owner)]
                if existing.payload_hash != record.payload_hash:
                    raise IdempotencyConflictError("idempotency key was reused with a different payload")
                return existing.model_copy(deep=True), False
            self._records[(record.tenant_id, record.request_id)] = record.model_copy(deep=True)
            if record.idempotency_key:
                self._keys[key] = record.request_id
            return record.model_copy(deep=True), True

    async def create(self, record: RequestRecord) -> RequestRecord:
        key = (record.tenant_id, record.request_id)
        async with self._lock:
            existing = self._records.get(key)
            if existing:
                return existing.model_copy(deep=True)
            self._records[key] = record.model_copy(deep=True)
            return record.model_copy(deep=True)

    async def get(self, tenant_id: str, request_id: str) -> RequestRecord:
        async with self._lock:
            record = self._records.get((tenant_id, request_id))
            if record is None:
                raise RequestNotFoundError(f"request not found: {tenant_id}/{request_id}")
            return record.model_copy(deep=True)

    async def save_payload(self, request: AgentRequest) -> None:
        async with self._lock:
            self._records[(request.tenant_id, request.request_id)].request = request.model_copy(deep=True)

    async def transition(self,
                         tenant_id: str,
                         request_id: str,
                         state: RequestState,
                         *,
                         result: ChatResult | None = None,
                         error_code: str = "",
                         retryable: bool = False,
                         increment_attempts: bool = False) -> RequestRecord:
        async with self._lock:
            key = (tenant_id, request_id)
            record = self._records.get(key)
            if record is None:
                raise RequestNotFoundError(f"request not found: {tenant_id}/{request_id}")
            if record.state in {RequestState.SUCCEEDED, RequestState.FAILED}:
                return record.model_copy(deep=True)
            if state == RequestState.QUEUED and record.state == RequestState.RUNNING:
                return record.model_copy(deep=True)
            record.state = state
            record.result = result.model_copy(deep=True) if result else record.result
            record.error_code = error_code
            record.retryable = retryable
            if increment_attempts:
                record.attempts += 1
            record.updated_at = datetime.now(timezone.utc)
            return record.model_copy(deep=True)

    async def list_stale(self, *, older_than_seconds: int, limit: int = 100) -> list[RequestRecord]:
        deadline = datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)
        active = {RequestState.RESERVED, RequestState.QUEUED, RequestState.RUNNING, RequestState.RETRYABLE_FAILED}
        async with self._lock:
            records = [
                item.model_copy(deep=True) for item in self._records.values()
                if item.state in active and item.updated_at <= deadline
            ]
        return sorted(records, key=lambda item: item.updated_at)[:limit]

    async def increment_model_attempts(self, tenant_id: str, request_id: str) -> RequestRecord:
        async with self._lock:
            record = self._records[(tenant_id, request_id)]
            record.model_attempts += 1
            record.updated_at = datetime.now(timezone.utc)
            return record.model_copy(deep=True)

    async def increment_successful_model_calls(self, tenant_id: str, request_id: str) -> RequestRecord:
        async with self._lock:
            record = self._records[(tenant_id, request_id)]
            record.successful_model_calls += 1
            record.updated_at = datetime.now(timezone.utc)
            return record.model_copy(deep=True)

    async def increment_recovery_count(self, tenant_id: str, request_id: str) -> RequestRecord:
        async with self._lock:
            record = self._records[(tenant_id, request_id)]
            record.recovery_count += 1
            record.updated_at = datetime.now(timezone.utc)
            return record.model_copy(deep=True)

    async def claim_stale(self, *, older_than_seconds: int, limit: int = 100) -> list[RequestRecord]:
        deadline = datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)
        async with self._lock:
            records = sorted((item for item in self._records.values()
                              if item.state == RequestState.RESERVED and item.updated_at <= deadline),
                             key=lambda item: item.updated_at)[:limit]
            for item in records:
                item.updated_at = datetime.now(timezone.utc)
            return [item.model_copy(deep=True) for item in records]


class PostgresRequestStore:
    """PostgreSQL request store; accepts an asyncpg-compatible pool."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def reserve_and_create_request(self, record: RequestRecord) -> tuple[RequestRecord, bool]:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Serialize admission with storage-route transitions. A request
                # that read an older route before this transaction must retry;
                # it cannot be inserted after the cutover drain completed.
                await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                                   f"storage-route:{record.tenant_id}")
                route = await conn.fetchrow(
                    "SELECT route_version,config_version,admission_paused FROM storage_migration_route "
                    "WHERE tenant_id=$1 AND active FOR SHARE", record.tenant_id)
                if route:
                    from trpc_service.gateway.errors import MigrationTransitionError
                    admission_paused = route["admission_paused"]
                    config_changed = record.config_version != route["config_version"]
                    storage_route_changed = (record.storage_route_version
                                             and record.storage_route_version != route["route_version"])
                    if admission_paused or config_changed or storage_route_changed:
                        raise MigrationTransitionError("migration_transition")
                    record.storage_route_version = route["route_version"]
                    if record.request:
                        record.request.storage_route_version = route["route_version"]
                if record.idempotency_key:
                    await conn.execute(
                        "INSERT INTO idempotency_record "
                        "(tenant_id,idempotency_key,request_id,state,payload_hash,expires_at) "
                        "VALUES ($1,$2,$3,'reserved',$4,now()+interval '7 days') "
                        "ON CONFLICT (tenant_id,idempotency_key) DO NOTHING", record.tenant_id, record.idempotency_key,
                        record.request_id, record.payload_hash)
                    row = await conn.fetchrow(
                        "SELECT request_id,payload_hash FROM idempotency_record "
                        "WHERE tenant_id=$1 AND idempotency_key=$2 FOR UPDATE", record.tenant_id,
                        record.idempotency_key)
                    if row["payload_hash"] != record.payload_hash:
                        raise IdempotencyConflictError("idempotency key was reused with a different payload")
                    if row["request_id"] != record.request_id:
                        return await PostgresRequestStore(conn).get(record.tenant_id, row["request_id"]), False
                return await PostgresRequestStore(conn).create(record), True

    async def finalize(self, request, result, messages):
        """Result, Outbox and durable idempotency success are one transaction."""
        from trpc_service.gateway.outbox import PostgresOutboxStore
        from trpc_service.storage.fencing import verify_postgres_commit
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await verify_postgres_commit(conn)
                state = await conn.fetchval(
                    "SELECT state FROM request_execution WHERE tenant_id=$1 AND request_id=$2 FOR UPDATE",
                    request.tenant_id, request.request_id)
                if state is None or state == "failed":
                    raise RuntimeError("cannot_finalize_missing_or_failed_request")
                for message in messages:
                    await PostgresOutboxStore(conn).add(message)
                await PostgresRequestStore(conn).transition(request.tenant_id,
                                                            request.request_id,
                                                            RequestState.SUCCEEDED,
                                                            result=result)
                await conn.execute(
                    "UPDATE idempotency_record SET state='succeeded',result_json=$3::jsonb,updated_at=now() "
                    "WHERE tenant_id=$1 AND request_id=$2", request.tenant_id, request.request_id,
                    result.model_dump_json())

    async def save_payload(self, request: AgentRequest) -> None:
        await self._pool.execute(
            "UPDATE request_execution SET request_json=$3::jsonb,updated_at=now() "
            "WHERE tenant_id=$1 AND request_id=$2", request.tenant_id, request.request_id, request.model_dump_json())

    async def create(self, record: RequestRecord) -> RequestRecord:
        await self._pool.execute(
            """
            INSERT INTO request_execution
                (request_id, tenant_id, state, payload_hash, config_version, storage_route_version, attempts,
                 model_attempts, successful_model_calls, recovery_count,
                 result_json, error_code, retryable, request_json, idempotency_key,
                 created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14::jsonb,$15,$16,$17)
            ON CONFLICT (tenant_id, request_id) DO NOTHING
            """,
            record.request_id,
            record.tenant_id,
            record.state.value,
            record.payload_hash,
            record.config_version,
            record.storage_route_version,
            record.attempts,
            record.model_attempts,
            record.successful_model_calls,
            record.recovery_count,
            record.result.model_dump_json() if record.result else None,
            record.error_code,
            record.retryable,
            record.request.model_dump_json() if record.request else None,
            record.idempotency_key,
            record.created_at,
            record.updated_at,
        )
        return await self.get(record.tenant_id, record.request_id)

    async def get(self, tenant_id: str, request_id: str) -> RequestRecord:
        row = await self._pool.fetchrow(
            "SELECT * FROM request_execution WHERE tenant_id=$1 AND request_id=$2",
            tenant_id,
            request_id,
        )
        if row is None:
            raise RequestNotFoundError(f"request not found: {tenant_id}/{request_id}")
        result_value = row["result_json"]
        if isinstance(result_value, str):
            result_value = json.loads(result_value)
        request_value = row["request_json"]
        if isinstance(request_value, str):
            request_value = json.loads(request_value)
        return RequestRecord(
            request_id=row["request_id"],
            tenant_id=row["tenant_id"],
            state=row["state"],
            payload_hash=row["payload_hash"] or "",
            config_version=row["config_version"],
            storage_route_version=row.get("storage_route_version", 0),
            attempts=row["attempts"],
            model_attempts=row["model_attempts"],
            successful_model_calls=row["successful_model_calls"],
            recovery_count=row["recovery_count"],
            result=ChatResult.model_validate(result_value) if result_value else None,
            error_code=row["error_code"] or "",
            retryable=bool(row["retryable"]),
            request=AgentRequest.model_validate(request_value) if request_value else None,
            idempotency_key=row["idempotency_key"] or "",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def transition(self,
                         tenant_id: str,
                         request_id: str,
                         state: RequestState,
                         *,
                         result: ChatResult | None = None,
                         error_code: str = "",
                         retryable: bool = False,
                         increment_attempts: bool = False) -> RequestRecord:
        row = await self._pool.fetchrow(
            """
            UPDATE request_execution
               SET state=$3,
                   result_json=COALESCE($4::jsonb, result_json),
                   error_code=$5,
                   retryable=$6,
                   attempts=attempts + CASE WHEN $7 THEN 1 ELSE 0 END,
                   updated_at=now()
             WHERE tenant_id=$1 AND request_id=$2
               AND state NOT IN ('succeeded','failed')
               AND NOT (state='running' AND $3='queued')
         RETURNING *
            """,
            tenant_id,
            request_id,
            state.value,
            result.model_dump_json() if result else None,
            error_code,
            retryable,
            increment_attempts,
        )
        if row is None:
            return await self.get(tenant_id, request_id)
        return await self.get(tenant_id, request_id)

    async def list_stale(self, *, older_than_seconds: int, limit: int = 100) -> list[RequestRecord]:
        rows = await self._pool.fetch(
            """
            SELECT tenant_id,request_id FROM request_execution
             WHERE state IN ('reserved','queued','running','retryable_failed')
               AND updated_at < now() - make_interval(secs => $1)
             ORDER BY updated_at
             LIMIT $2
            """, older_than_seconds, limit)
        return [await self.get(row["tenant_id"], row["request_id"]) for row in rows]

    async def increment_model_attempts(self, tenant_id: str, request_id: str) -> RequestRecord:
        await self._pool.execute(
            "UPDATE request_execution SET model_attempts=model_attempts+1,updated_at=now() "
            "WHERE tenant_id=$1 AND request_id=$2", tenant_id, request_id)
        return await self.get(tenant_id, request_id)

    async def increment_successful_model_calls(self, tenant_id: str, request_id: str) -> RequestRecord:
        await self._pool.execute(
            "UPDATE request_execution SET successful_model_calls=successful_model_calls+1,updated_at=now() "
            "WHERE tenant_id=$1 AND request_id=$2", tenant_id, request_id)
        return await self.get(tenant_id, request_id)

    async def increment_recovery_count(self, tenant_id: str, request_id: str) -> RequestRecord:
        await self._pool.execute(
            "UPDATE request_execution SET recovery_count=recovery_count+1,updated_at=now() "
            "WHERE tenant_id=$1 AND request_id=$2", tenant_id, request_id)
        return await self.get(tenant_id, request_id)

    async def claim_stale(self, *, older_than_seconds: int, limit: int = 100) -> list[RequestRecord]:
        rows = await self._pool.fetch(
            """
            WITH due AS (
                SELECT tenant_id,request_id FROM request_execution
                 WHERE state='reserved'
                   AND updated_at < now() - make_interval(secs => $1)
                 ORDER BY updated_at FOR UPDATE SKIP LOCKED LIMIT $2
            )
            UPDATE request_execution AS r SET updated_at=now()
              FROM due WHERE r.tenant_id=due.tenant_id AND r.request_id=due.request_id
            RETURNING r.tenant_id,r.request_id
            """, older_than_seconds, limit)
        return [await self.get(row["tenant_id"], row["request_id"]) for row in rows]
