"""PostgreSQL tenant usage aggregation repository (Stage 6C).

High-cardinality per-tenant usage facts live here (PostgreSQL is the
source of truth); metrics stay low-cardinality elsewhere.  ``add_usage``
is a single atomic ``INSERT ... ON CONFLICT DO UPDATE`` — no read-modify-
write race between Workers.  NULL token/cost columns propagate through the
``+`` arithmetic (NULL + x = NULL), so once a day has an unknown component
it stays unknown: unknown is never presented as zero, and two concurrent
increments can never clobber each other's totals.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date
from typing import Protocol

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.exc import TimeoutError as SATimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine

from trpc_service.storage.database import (
    DatabaseSettings,
    check_database_readiness,
    create_database_engine,
)
from trpc_service.storage.schema import message_receipts, request_usage_records, tenant_usage_daily
from trpc_service.usage.models import ProfileDailyUsage, RequestUsageRecord, TenantDailyUsage, UsageIncrement

_TENANT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_INTEGRITY_SQLSTATES = frozenset({"23000", "23001", "23502", "23503", "23505", "23514"})


class UsageRepositoryConfigurationError(ValueError):
    """Repository could not be configured."""


class UsageRepositoryUnavailableError(RuntimeError):
    """Repository backend is not reachable or not ready."""


class UsageRepositoryDataError(RuntimeError):
    """Usage row violates data rules or stored rows are corrupt."""


class UsageRepository(Protocol):
    """Read/atomic-accumulate protocol for daily tenant usage."""

    async def get_daily(self, tenant_id: str, day: date) -> TenantDailyUsage:
        ...

    async def add_usage(self, usage: UsageIncrement) -> TenantDailyUsage:
        ...

    async def get_for_request(self, tenant_id: str, request_id) -> RequestUsageRecord | None:
        ...

    async def check_ready(self) -> None:
        ...

    async def close(self) -> None:
        ...


def _sqlstate(exc: BaseException) -> str | None:
    orig = getattr(exc, "orig", exc)
    for attr in ("sqlstate", "pgcode"):
        code = getattr(orig, attr, None)
        if isinstance(code, str) and code:
            return code
    return None


def _raise_mapped(exc: DBAPIError | OSError) -> None:
    if isinstance(exc, IntegrityError) and _sqlstate(exc) in _INTEGRITY_SQLSTATES:
        raise UsageRepositoryDataError("usage row violates integrity rules") from None
    raise UsageRepositoryUnavailableError("usage database query failed") from None


class SqlUsageRepository:
    """PostgreSQL-backed daily usage aggregation."""

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "SqlUsageRepository":
        from trpc_service.storage.database import DatabaseConfigurationError

        try:
            settings = DatabaseSettings.from_env(environ)
        except DatabaseConfigurationError:
            # Fixed text, no cause: a DSN must never ride this boundary.
            raise UsageRepositoryConfigurationError("usage repository could not be configured") from None
        engine = create_database_engine(settings)
        return cls(engine)

    def __init__(self, engine: AsyncEngine, *, owns_engine: bool = True) -> None:
        self._engine = engine
        self._owns_engine = owns_engine
        self._closed = False

    async def add_usage(self, usage: UsageIncrement) -> TenantDailyUsage:
        """Atomically accumulate one request's usage; return the day after."""
        self._require_open()
        if not isinstance(usage, UsageIncrement):
            raise UsageRepositoryDataError("usage must be a UsageIncrement")
        stmt = (
            pg_insert(tenant_usage_daily).values(
                usage_date=usage.usage_date,
                tenant_id=usage.tenant_id,
                model_profile=usage.model_profile,
                requests=usage.requests,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cost_microunits=usage.cost_microunits,
            ).on_conflict_do_update(
                index_elements=["usage_date", "tenant_id", "model_profile"],
                # NULL + value = NULL: unknown days stay unknown (never a
                # fabricated zero), known days accumulate atomically.
                set_={
                    "requests": tenant_usage_daily.c.requests + usage.requests,
                    "input_tokens": tenant_usage_daily.c.input_tokens + usage.input_tokens,
                    "output_tokens": tenant_usage_daily.c.output_tokens + usage.output_tokens,
                    "cost_microunits": tenant_usage_daily.c.cost_microunits + usage.cost_microunits,
                    "updated_at": sa.text("now()"),
                },
            ))
        try:
            async with self._engine.begin() as conn:
                await conn.execute(stmt)
                if usage.request_id is not None:
                    receipt_id = usage.receipt_id
                    if receipt_id is None and usage.config_version is not None:
                        receipt_id = (await conn.execute(
                            sa.select(message_receipts.c.receipt_id).where(
                                message_receipts.c.tenant_id == usage.tenant_id,
                                message_receipts.c.request_id == usage.request_id,
                                message_receipts.c.config_version == usage.config_version,
                            ))).scalar_one_or_none()
                    record = pg_insert(request_usage_records).values(
                        tenant_id=usage.tenant_id,
                        request_id=usage.request_id,
                        receipt_id=receipt_id,
                        config_version=usage.config_version,
                        model_profile=usage.model_profile,
                        input_tokens=usage.input_tokens,
                        output_tokens=usage.output_tokens,
                        cost_microunits=usage.cost_microunits,
                        occurred_at=usage.occurred_at,
                    ).on_conflict_do_nothing(index_elements=["tenant_id", "request_id"])
                    await conn.execute(record)
                return await self._read_daily(conn, usage.tenant_id, usage.usage_date)
        except (UsageRepositoryDataError, UsageRepositoryUnavailableError):
            raise
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            _raise_mapped(exc)
            raise UsageRepositoryUnavailableError("usage database query failed") from None  # unreachable

    async def get_daily(self, tenant_id: str, day: date) -> TenantDailyUsage:
        self._require_open()
        self._validate_tenant_day(tenant_id, day)
        try:
            async with self._engine.connect() as conn:
                return await self._read_daily(conn, tenant_id, day)
        except (UsageRepositoryDataError, UsageRepositoryUnavailableError):
            raise
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            _raise_mapped(exc)
            raise UsageRepositoryUnavailableError("usage database query failed") from None  # unreachable

    async def get_for_request(self, tenant_id: str, request_id) -> RequestUsageRecord | None:
        self._require_open()
        self._validate_tenant_day(tenant_id, date.today())
        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(
                    sa.select(request_usage_records).where(
                        request_usage_records.c.tenant_id == tenant_id,
                        request_usage_records.c.request_id == request_id,
                    ))).first()
            if row is None:
                return None
            return RequestUsageRecord(**dict(row._mapping))
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise UsageRepositoryUnavailableError("usage database query failed") from None

    async def _read_daily(self, conn, tenant_id: str, day: date) -> TenantDailyUsage:
        rows = (await conn.execute(
            sa.select(tenant_usage_daily).where(
                tenant_usage_daily.c.tenant_id == tenant_id,
                tenant_usage_daily.c.usage_date == day,
            ).order_by(tenant_usage_daily.c.model_profile.asc()))).fetchall()
        profiles = []
        for row in rows:
            m = row._mapping
            profiles.append(
                ProfileDailyUsage(
                    model_profile=m["model_profile"],
                    requests=m["requests"],
                    input_tokens=m["input_tokens"],
                    output_tokens=m["output_tokens"],
                    cost_microunits=m["cost_microunits"],
                ))
        return TenantDailyUsage(usage_date=day, tenant_id=tenant_id, profiles=tuple(profiles))

    @staticmethod
    def _validate_tenant_day(tenant_id: str, day: date) -> None:
        if _TENANT_ID_PATTERN.fullmatch(str(tenant_id)) is None:
            raise UsageRepositoryDataError("invalid tenant ID")
        if not isinstance(day, date):
            raise UsageRepositoryDataError("invalid usage date")

    async def check_ready(self) -> None:
        if self._closed:
            raise UsageRepositoryUnavailableError("repository is closed")
        try:
            await check_database_readiness(self._engine)
            async with self._engine.connect() as conn:
                await conn.execute(sa.text("SELECT 1 FROM tenant_usage_daily LIMIT 0"))
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise UsageRepositoryUnavailableError("usage database is not reachable") from None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_engine and self._engine is not None:
            await self._engine.dispose()

    def _require_open(self) -> None:
        if self._closed:
            raise UsageRepositoryUnavailableError("repository is closed")


__all__ = [
    "SqlUsageRepository",
    "UsageRepository",
    "UsageRepositoryConfigurationError",
    "UsageRepositoryDataError",
    "UsageRepositoryUnavailableError",
]
