"""Precise, one-shot checkpoints for opt-in fault injection.

The controller is deliberately passive: callers must explicitly arm a control
record before a checkpoint can match.  The production runtime can therefore
keep using :class:`NoopFaultStageController` unless a separate test run opts
in.  No event body or tool content is accepted by this module.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol
from uuid import uuid4

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class FaultStage(StrEnum):
    """Supported process checkpoints."""

    ENQUEUE = "enqueue"
    TOOL = "tool"
    COMMIT_TXN_OPEN = "commit_txn_open"


_MAX_TENANT_ID_LENGTH = 256
_MAX_RUNTIME_ID_LENGTH = 256
_MAX_RUN_ID_LENGTH = 128
_MAX_CONTROL_ID_LENGTH = 64
_MAX_TOKEN_BYTES = 32
_MAX_WAIT_TIMEOUT_SECONDS = 300.0


class FaultStageEvent(BaseModel):
    """Content-free identity of a runtime checkpoint.

    For the v2 ``ENQUEUE`` checkpoint, ``inbound_id`` carries the
    ``SessionReady.event_id`` (the PostgreSQL outbox id) because the Redis
    notification intentionally contains no inbound-message body or id.  The
    authoritative claim resolves that event to the inbound row.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    stage: FaultStage
    tenant_id: str = Field(min_length=1, max_length=_MAX_TENANT_ID_LENGTH)
    worker_id: str = Field(min_length=1, max_length=_MAX_RUNTIME_ID_LENGTH)
    inbound_id: str | None = Field(default=None, max_length=_MAX_RUNTIME_ID_LENGTH)
    turn_id: str | None = Field(default=None, max_length=_MAX_RUNTIME_ID_LENGTH)
    execution_key: str | None = Field(default=None, max_length=_MAX_RUNTIME_ID_LENGTH)
    stream_id: str | None = Field(default=None, max_length=_MAX_RUNTIME_ID_LENGTH)
    fencing_token: int | None = Field(default=None, ge=0, le=2**63 - 1)

    @field_validator("tenant_id", "worker_id")
    @classmethod
    def _required_id_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("fault stage identity cannot be blank")
        return value

    @field_validator("inbound_id", "turn_id", "execution_key", "stream_id")
    @classmethod
    def _optional_id_must_not_be_blank(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("fault stage identity cannot be blank")
        return value

    @model_validator(mode="after")
    def _validate_stage_fields(self) -> FaultStageEvent:
        required_fields = {
            FaultStage.ENQUEUE: ("inbound_id", "stream_id"),
            FaultStage.TOOL: ("turn_id", "execution_key"),
            FaultStage.COMMIT_TXN_OPEN: ("inbound_id", "turn_id", "fencing_token"),
        }[self.stage]
        missing = [field for field in required_fields if getattr(self, field) is None]
        if missing:
            raise ValueError(f"{self.stage.value} fault stage requires {', '.join(missing)}")
        return self


class FaultStageController(Protocol):
    """Small interface used by runtime consumers at an exact checkpoint."""

    async def checkpoint(self, event: FaultStageEvent) -> bool:
        """Wait for a matching release and return whether it was released."""


class FaultStageControlError(ValueError):
    """Raised when a control cannot be safely armed or addressed."""


class NoopFaultStageController:
    """Disabled controller used by default in every runtime environment."""

    async def checkpoint(self, event: FaultStageEvent) -> bool:
        del event
        return False


_TARGET_FIELDS = (
    "worker_id",
    "inbound_id",
    "turn_id",
    "execution_key",
    "stream_id",
    "fencing_token",
)
_DEFAULT_TTL_SECONDS = 30.0
_MAX_TTL_SECONDS = 300.0


class PostgresFaultStageController:
    """Tenant-scoped one-shot checkpoint controller backed by PostgreSQL.

    The raw run token is hashed immediately and is never retained.  A matching
    checkpoint atomically changes ``armed`` to ``entered`` and records only
    content-free marker fields.  It then polls a bounded status transition to
    ``released`` or ``expired``.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        run_id: str,
        run_token: str,
        ttl_seconds: float = _DEFAULT_TTL_SECONDS,
        poll_interval_seconds: float = 0.1,
        wait_timeout_seconds: float | None = None,
    ) -> None:
        self._pool = pool
        self._run_id = _require_nonblank(
            run_id, "fault injection run_id", max_length=_MAX_RUN_ID_LENGTH
        )
        if (
            not isinstance(run_token, str)
            or not run_token.strip()
            or len(run_token.encode("utf-8")) < _MAX_TOKEN_BYTES
        ):
            raise FaultStageControlError("fault injection run token must be at least 32 bytes")
        self._run_token_hash = _hash_token(run_token)
        self._ttl_seconds = _validate_ttl(ttl_seconds)
        if poll_interval_seconds <= 0:
            raise ValueError("fault stage poll interval must be positive")
        self._poll_interval_seconds = poll_interval_seconds
        self._wait_timeout_seconds = (
            self._ttl_seconds if wait_timeout_seconds is None else wait_timeout_seconds
        )
        if (
            self._wait_timeout_seconds <= 0
            or self._wait_timeout_seconds > _MAX_WAIT_TIMEOUT_SECONDS
        ):
            raise ValueError("fault stage wait timeout must be greater than 0 and at most 300s")

    async def arm(self, event: FaultStageEvent, *, ttl_seconds: float | None = None) -> str:
        """Arm one exact target for this controller's run.

        The unique tenant/run/stage/target key makes arming a target twice
        fail closed, including after a previous release or expiry.
        """

        self._validate_event(event)
        ttl = self._ttl_seconds if ttl_seconds is None else _validate_ttl(ttl_seconds)
        control_id = str(uuid4())
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl)
        fingerprint = _target_fingerprint(event)
        async with self._tenant_transaction(event.tenant_id) as connection:
            try:
                await connection.execute(
                    """
                    INSERT INTO fault_stage_controls (
                        tenant_id, control_id, run_id, stage, target_fingerprint,
                        target_worker_id, target_inbound_id, target_turn_id,
                        target_execution_key, target_stream_id, target_fencing_token,
                        token_hash, status, expires_at
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'armed',$13
                    )
                    """,
                    event.tenant_id,
                    control_id,
                    self._run_id,
                    event.stage.value,
                    fingerprint,
                    event.worker_id,
                    event.inbound_id,
                    event.turn_id,
                    event.execution_key,
                    event.stream_id,
                    event.fencing_token,
                    self._run_token_hash,
                    expires_at,
                )
            except asyncpg.UniqueViolationError as error:
                raise FaultStageControlError(
                    "fault stage control already exists for this run target"
                ) from error
        return control_id

    async def checkpoint(self, event: FaultStageEvent) -> bool:
        """Enter and await one matching control, returning only on release/expiry."""

        self._validate_event(event)
        fingerprint = _target_fingerprint(event)
        async with self._tenant_transaction(event.tenant_id) as connection:
            row = await connection.fetchrow(
                """
                UPDATE fault_stage_controls
                   SET status='entered',
                       entered_at=clock_timestamp(),
                       marker_id=gen_random_uuid(),
                       marker_status='entered',
                       marker_worker_id=$6,
                       marker_inbound_id=$7,
                       marker_turn_id=$8,
                       marker_execution_key=$9,
                       marker_stream_id=$10,
                       marker_fencing_token=$11,
                       marker_at=clock_timestamp()
                 WHERE tenant_id=$1
                   AND control_id IS NOT NULL
                   AND run_id=$2
                   AND token_hash=$3
                   AND stage=$4
                   AND target_fingerprint=$5
                   AND status='armed'
                   AND expires_at>clock_timestamp()
                   AND target_worker_id=$6
                   AND target_inbound_id IS NOT DISTINCT FROM $7
                   AND target_turn_id IS NOT DISTINCT FROM $8
                   AND target_execution_key IS NOT DISTINCT FROM $9
                   AND target_stream_id IS NOT DISTINCT FROM $10
                   AND target_fencing_token IS NOT DISTINCT FROM $11
                RETURNING control_id, expires_at
                """,
                event.tenant_id,
                self._run_id,
                self._run_token_hash,
                event.stage.value,
                fingerprint,
                event.worker_id,
                event.inbound_id,
                event.turn_id,
                event.execution_key,
                event.stream_id,
                event.fencing_token,
            )
        if row is None:
            return False
        control_id = str(row["control_id"])
        raw_expires_at = row.get("expires_at") if hasattr(row, "get") else row["expires_at"]
        expires_at = _as_datetime(raw_expires_at)
        return await self._poll_until_released(
            event,
            control_id=control_id,
            fingerprint=fingerprint,
            expires_at=expires_at,
        )

    async def release(self, control_id: str, *, tenant_id: str) -> bool:
        """Release an entered control only when run, tenant and token match."""

        tenant_id = _require_nonblank(
            tenant_id, "fault stage tenant_id", max_length=_MAX_TENANT_ID_LENGTH
        )
        control_id = _require_nonblank(
            control_id, "fault stage control_id", max_length=_MAX_CONTROL_ID_LENGTH
        )
        async with self._tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                UPDATE fault_stage_controls
                   SET status='released', released_at=clock_timestamp()
                 WHERE tenant_id=$1
                   AND control_id=$2
                   AND run_id=$3
                   AND token_hash=$4
                   AND status='entered'
                   AND expires_at>clock_timestamp()
                RETURNING control_id
                """,
                tenant_id,
                control_id,
                self._run_id,
                self._run_token_hash,
            )
        return row is not None

    async def cleanup_expired(self, *, tenant_id: str, limit: int = 100) -> int:
        """Delete expired records; acceptance reports must read markers first.

        Marker evidence lives with the control row and is intentionally removed
        by this bounded cleanup operation.
        """

        tenant_id = _require_nonblank(
            tenant_id, "fault stage tenant_id", max_length=_MAX_TENANT_ID_LENGTH
        )
        if limit < 1 or limit > 1000:
            raise ValueError("fault stage cleanup limit must be between 1 and 1000")
        async with self._tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                """
                WITH candidates AS (
                    SELECT tenant_id, control_id
                      FROM fault_stage_controls
                     WHERE tenant_id=$1
                       AND expires_at<=clock_timestamp()
                       AND status IN ('armed','entered','released','expired')
                     ORDER BY expires_at, control_id
                     FOR UPDATE SKIP LOCKED
                     LIMIT $2
                )
                DELETE FROM fault_stage_controls AS controls
                 USING candidates
                 WHERE controls.tenant_id=candidates.tenant_id
                   AND controls.control_id=candidates.control_id
                RETURNING controls.control_id
                """,
                tenant_id,
                limit,
            )
        return len(rows)

    async def _poll_until_released(
        self,
        event: FaultStageEvent,
        *,
        control_id: str,
        fingerprint: str,
        expires_at: datetime | None,
    ) -> bool:
        started = time.monotonic()
        timeout_deadline = started + self._wait_timeout_seconds
        if expires_at is not None:
            ttl_deadline = started + max(0.0, (expires_at - datetime.now(UTC)).total_seconds())
            timeout_deadline = min(timeout_deadline, ttl_deadline)
        while True:
            async with self._tenant_transaction(event.tenant_id) as connection:
                row = await connection.fetchrow(
                    """
                    SELECT status, expires_at
                      FROM fault_stage_controls
                     WHERE tenant_id=$1
                       AND control_id=$2
                       AND run_id=$3
                       AND token_hash=$4
                       AND stage=$5
                       AND target_fingerprint=$6
                    """,
                    event.tenant_id,
                    control_id,
                    self._run_id,
                    self._run_token_hash,
                    event.stage.value,
                    fingerprint,
                )
            if row is None:
                return False
            status = str(row["status"])
            if status == "released":
                return True
            if status in {"expired", "armed"}:
                return False
            if status != "entered":
                return False
            current_expiry = _as_datetime(
                row.get("expires_at") if hasattr(row, "get") else row["expires_at"]
            )
            now = datetime.now(UTC)
            if current_expiry is not None and current_expiry <= now:
                await self._mark_expired(event, control_id, fingerprint)
                return False
            remaining = timeout_deadline - time.monotonic()
            if remaining <= 0:
                await self._mark_expired(event, control_id, fingerprint)
                return False
            await asyncio.sleep(min(self._poll_interval_seconds, remaining))

    async def _mark_expired(
        self, event: FaultStageEvent, control_id: str, fingerprint: str
    ) -> None:
        async with self._tenant_transaction(event.tenant_id) as connection:
            await connection.execute(
                """
                UPDATE fault_stage_controls
                   SET status='expired'
                 WHERE tenant_id=$1
                   AND control_id=$2
                   AND run_id=$3
                   AND token_hash=$4
                   AND stage=$5
                   AND target_fingerprint=$6
                   AND status='entered'
                """,
                event.tenant_id,
                control_id,
                self._run_id,
                self._run_token_hash,
                event.stage.value,
                fingerprint,
            )

    def _validate_event(self, event: FaultStageEvent) -> None:
        if not isinstance(event, FaultStageEvent):
            raise TypeError("fault stage checkpoint requires FaultStageEvent")

    @asynccontextmanager
    async def _tenant_transaction(self, tenant_id: str) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            yield connection


def _require_nonblank(value: str, label: str, *, max_length: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FaultStageControlError(f"{label} cannot be empty")
    if len(value) > max_length:
        raise FaultStageControlError(f"{label} exceeds the maximum length")
    return value


def _validate_ttl(value: float) -> float:
    if value <= 0 or value > _MAX_TTL_SECONDS:
        raise ValueError(
            f"fault stage TTL must be greater than 0 and at most {_MAX_TTL_SECONDS:g}s"
        )
    return float(value)


def _hash_token(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _target_fingerprint(event: FaultStageEvent) -> str:
    target = {field: getattr(event, field) for field in _TARGET_FIELDS}
    canonical = json.dumps(target, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _as_datetime(value: Any) -> datetime | None:
    if value is None or not isinstance(value, datetime):
        return None
    normalized: datetime = value
    if normalized.tzinfo is None:
        return normalized.replace(tzinfo=UTC)
    return normalized.astimezone(UTC)


__all__ = [
    "FaultStage",
    "FaultStageControlError",
    "FaultStageController",
    "FaultStageEvent",
    "NoopFaultStageController",
    "PostgresFaultStageController",
]
