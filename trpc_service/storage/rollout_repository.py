"""Transactional persistence for one deterministic rollout per tenant."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, IntegrityError, TimeoutError as SATimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine

from trpc_service.config.rollout import TenantConfigRollout
from trpc_service.config.tenant import TenantConfig, TenantConfigDraft
from trpc_service.config.tenant_repository import (
    TenantConfigVersionConflictError,
    TenantNotFoundError,
    TenantRepositoryDataError,
    TenantRepositoryUnavailableError,
)
from trpc_service.storage.schema import message_receipts, tenant_config_rollouts, tenant_config_versions
from trpc_service.storage.tenant_repository import SqlTenantConfigRepository, _row_to_tenant_config, _snapshot_insert


class TenantRolloutNotFoundError(RuntimeError):
    """No running rollout exists for the tenant."""


class SqlTenantConfigRolloutRepository:

    def __init__(self, engine: AsyncEngine, *, owns_engine: bool = False) -> None:
        self._engine = engine
        self._owns_engine = owns_engine
        self._closed = False

    async def check_ready(self) -> None:
        if self._closed:
            raise TenantRepositoryUnavailableError("repository is closed")
        try:
            async with self._engine.connect() as conn:
                await conn.execute(sa.text("SELECT 1"))
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise TenantRepositoryUnavailableError("database is not reachable") from exc

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            if self._owns_engine:
                await self._engine.dispose()

    async def get_running(self, tenant_id: str) -> TenantConfigRollout | None:
        self._require_open()
        stmt = sa.select(tenant_config_rollouts).where(
            tenant_config_rollouts.c.tenant_id == tenant_id,
            tenant_config_rollouts.c.status == "running",
        )
        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(stmt)).first()
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise TenantRepositoryUnavailableError("database query failed") from exc
        return _row_to_rollout(row) if row is not None else None

    async def begin(
        self,
        tenant_id: str,
        expected_active_version: int,
        desired: TenantConfigDraft,
        candidate_percent: int,
    ) -> TenantConfigRollout:
        """Create desired head and running record atomically under head CAS."""
        self._require_open()
        if type(candidate_percent) is not int or not 1 <= candidate_percent <= 99:
            raise ValueError("candidate_percent must be between 1 and 99")
        candidate_version = expected_active_version + 1
        try:
            async with self._engine.begin() as conn:
                head = await SqlTenantConfigRepository._conditional_bump(conn, tenant_id, expected_active_version,
                                                                         candidate_version, desired.enabled,
                                                                         desired.app, desired.governance,
                                                                         desired.backend_profile, desired.audit_policy)
                if head is None:
                    await SqlTenantConfigRepository._raise_head_mismatch(conn, tenant_id)
                await conn.execute(
                    _snapshot_insert(tenant_id, candidate_version, desired.enabled, desired.app, desired.governance,
                                     desired.backend_profile, desired.audit_policy))
                now = datetime.now(timezone.utc)
                rollout_id = uuid4()
                await conn.execute(tenant_config_rollouts.insert().values(rollout_id=rollout_id,
                                                                          tenant_id=tenant_id,
                                                                          active_version=expected_active_version,
                                                                          candidate_version=candidate_version,
                                                                          candidate_percent=candidate_percent,
                                                                          status="running",
                                                                          started_at=now,
                                                                          finished_at=None))
                return TenantConfigRollout(tenant_id, expected_active_version, candidate_version, candidate_percent,
                                           now)
        except (TenantNotFoundError, TenantConfigVersionConflictError):
            raise
        except IntegrityError:
            # Partial unique index means another in-flight rollout won.
            raise TenantConfigVersionConflictError("rollout already running") from None
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise TenantRepositoryUnavailableError("database write failed") from exc

    async def promote(self, tenant_id: str, expected_candidate_version: int) -> TenantConfigRollout:
        return await self._finish(tenant_id, expected_candidate_version, "promoted")

    async def abort(self, tenant_id: str, expected_candidate_version: int) -> TenantConfig:
        """Forward-copy active snapshot into a newer head, then close rollout."""
        self._require_open()
        try:
            async with self._engine.begin() as conn:
                row = await self._running_for_update(conn, tenant_id)
                rollout = _row_to_rollout(row)
                if rollout.candidate_version != expected_candidate_version:
                    raise TenantConfigVersionConflictError("version conflict")
                snapshot = (await conn.execute(
                    sa.select(tenant_config_versions).where(tenant_config_versions.c.tenant_id == tenant_id,
                                                            tenant_config_versions.c.version == rollout.active_version)
                )).first()
                if snapshot is None:
                    raise TenantRepositoryDataError("active rollout snapshot missing")
                active = _row_to_tenant_config(snapshot)
                next_version = expected_candidate_version + 1
                head = await SqlTenantConfigRepository._conditional_bump(conn, tenant_id, expected_candidate_version,
                                                                         next_version, active.enabled, active.app,
                                                                         active.governance, active.backend_profile,
                                                                         active.audit_policy)
                if head is None:
                    await SqlTenantConfigRepository._raise_head_mismatch(conn, tenant_id)
                await conn.execute(
                    _snapshot_insert(tenant_id, next_version, active.enabled, active.app, active.governance,
                                     active.backend_profile, active.audit_policy))
                await conn.execute(tenant_config_rollouts.update().where(
                    tenant_config_rollouts.c.rollout_id == row._mapping["rollout_id"],
                    tenant_config_rollouts.c.status == "running").values(status="aborted",
                                                                         finished_at=datetime.now(timezone.utc)))
                return _row_to_tenant_config(head)
        except (TenantRolloutNotFoundError, TenantNotFoundError, TenantConfigVersionConflictError,
                TenantRepositoryDataError):
            raise
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise TenantRepositoryUnavailableError("database write failed") from exc

    async def status_counts(self, tenant_id: str) -> dict[str, int]:
        """Only low-cardinality terminal receipt counts since rollout began."""
        rollout = await self.get_running(tenant_id)
        if rollout is None:
            return {}
        stmt = sa.select(message_receipts.c.config_version, message_receipts.c.state, message_receipts.c.error_code,
                         sa.func.count()).where(
                             message_receipts.c.tenant_id == tenant_id,
                             message_receipts.c.config_version.in_([rollout.active_version, rollout.candidate_version]),
                             message_receipts.c.started_at
                             >= rollout.started_at).group_by(message_receipts.c.config_version,
                                                             message_receipts.c.state, message_receipts.c.error_code)
        try:
            async with self._engine.connect() as conn:
                rows = (await conn.execute(stmt)).all()
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise TenantRepositoryUnavailableError("database query failed") from exc
        return {f"v{row[0]}:{row[1]}:{row[2] or 'none'}": row[3] for row in rows}

    async def _finish(self, tenant_id: str, expected_candidate_version: int, status: str) -> TenantConfigRollout:
        self._require_open()
        try:
            async with self._engine.begin() as conn:
                row = await self._running_for_update(conn, tenant_id)
                rollout = _row_to_rollout(row)
                if rollout.candidate_version != expected_candidate_version:
                    raise TenantConfigVersionConflictError("version conflict")
                await conn.execute(tenant_config_rollouts.update().where(
                    tenant_config_rollouts.c.rollout_id == row._mapping["rollout_id"],
                    tenant_config_rollouts.c.status == "running").values(status=status,
                                                                         finished_at=datetime.now(timezone.utc)))
                return rollout
        except (TenantRolloutNotFoundError, TenantConfigVersionConflictError):
            raise
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise TenantRepositoryUnavailableError("database write failed") from exc

    async def _running_for_update(self, conn, tenant_id: str):
        row = (await conn.execute(
            sa.select(tenant_config_rollouts).where(tenant_config_rollouts.c.tenant_id == tenant_id,
                                                    tenant_config_rollouts.c.status == "running").with_for_update()
        )).first()
        if row is None:
            raise TenantRolloutNotFoundError("rollout not found")
        return row

    def _require_open(self) -> None:
        if self._closed:
            raise TenantRepositoryUnavailableError("repository is closed")


def _row_to_rollout(row) -> TenantConfigRollout:
    return TenantConfigRollout(row._mapping["tenant_id"], row._mapping["active_version"],
                               row._mapping["candidate_version"], row._mapping["candidate_percent"],
                               row._mapping["started_at"])


__all__ = ["SqlTenantConfigRolloutRepository", "TenantRolloutNotFoundError"]
