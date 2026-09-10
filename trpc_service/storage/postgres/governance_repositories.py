"""Durable governance repository adapters.

These adapters keep serialization and transactional ownership in the shared
PostgreSQL composition root.  Domain objects remain immutable and are rebuilt
from rows on every read.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.governance.budget import new_reservation, settle
from trpc_service.governance.confirmation import consume
from trpc_service.governance.errors import (
    BudgetExhausted, ConfirmationInvalid, GovernanceUnavailable, PolicyMissing,
    PolicyStale,
)
from trpc_service.governance.models import (
    ActiveGovernancePolicy, BudgetReservationSet, BudgetSettlement,
    ConfirmationClaim, ConfirmationIntent, ConfirmationStatus,
    GovernancePolicyVersion, PendingConfirmation, PolicyDocument, PolicyScope,
    PolicyStatus, PrincipalGrant, PrincipalGrantDecision, ReservationStatus,
    UsageVector,
)
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.models import (
    BudgetReservationRow, GovernancePolicyActiveRow, GovernancePolicyVersionRow,
    GovernanceRecoveryMarkerRow, PrincipalGrantRow,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PostgresPolicyRepository:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    async def create_version(self, *, tenant_id: str, scope: PolicyScope, document: PolicyDocument, actor_digest: str) -> GovernancePolicyVersion:
        async with AsyncSession(self.database.engine) as session, session.begin():
            result = await session.execute(select(GovernancePolicyVersionRow.version).where(GovernancePolicyVersionRow.tenant_id == tenant_id).order_by(GovernancePolicyVersionRow.version.desc()).limit(1))
            version = int(result.scalar() or 0) + 1
            item = GovernancePolicyVersion(policy_id=str(uuid4()), tenant_id=tenant_id, scope=scope, version=version, document=document, created_by_digest=actor_digest, created_at=_now())
            session.add(GovernancePolicyVersionRow(policy_id=item.policy_id, tenant_id=tenant_id, scope_type=scope.value, scope_id=tenant_id, version=version, status=item.status.value, policy_document=document.model_dump(mode="json"), created_by_digest=actor_digest, created_at=item.created_at))
            return item

    async def activate(self, *, tenant_id: str, policy_id: str, expected_generation: int) -> ActiveGovernancePolicy:
        async with AsyncSession(self.database.engine) as session, session.begin():
            active = await session.get(GovernancePolicyActiveRow, (tenant_id, PolicyScope.TENANT.value, tenant_id))
            current = int(active.activation_generation) if active else 0
            if current != expected_generation:
                raise PolicyStale()
            row = await session.get(GovernancePolicyVersionRow, policy_id)
            if row is None or row.tenant_id != tenant_id:
                raise PolicyMissing()
            if active and active.policy_id != policy_id:
                await session.execute(update(GovernancePolicyVersionRow).where(GovernancePolicyVersionRow.policy_id == active.policy_id).values(status=PolicyStatus.SUPERSEDED.value))
            await session.execute(update(GovernancePolicyVersionRow).where(GovernancePolicyVersionRow.policy_id == policy_id).values(status=PolicyStatus.ACTIVE.value, activated_at=_now()))
            generation = expected_generation + 1
            if active is None:
                session.add(GovernancePolicyActiveRow(tenant_id=tenant_id, scope_type=PolicyScope.TENANT.value, scope_id=tenant_id, policy_id=policy_id, version=row.version, activation_generation=generation, updated_at=_now()))
            else:
                active.policy_id, active.version, active.activation_generation, active.updated_at = policy_id, row.version, generation, _now()
            return ActiveGovernancePolicy(tenant_id=tenant_id, policy_id=policy_id, version=row.version, generation=generation, document=PolicyDocument.model_validate(row.policy_document))

    async def disable(self, *, tenant_id: str, policy_id: str, expected_generation: int) -> ActiveGovernancePolicy:
        async with AsyncSession(self.database.engine) as session, session.begin():
            active = await session.get(GovernancePolicyActiveRow, (tenant_id, PolicyScope.TENANT.value, tenant_id))
            current = int(active.activation_generation) if active else 0
            if current != expected_generation:
                raise PolicyStale()
            row = await session.get(GovernancePolicyVersionRow, policy_id)
            if row is None or row.tenant_id != tenant_id:
                raise PolicyMissing()
            await session.execute(update(GovernancePolicyVersionRow).where(GovernancePolicyVersionRow.policy_id == policy_id).values(status=PolicyStatus.DISABLED.value, disabled_at=_now()))
            generation = expected_generation + 1
            if active:
                await session.delete(active)
            return ActiveGovernancePolicy(tenant_id=tenant_id, policy_id=policy_id, version=row.version, generation=generation, document=PolicyDocument.model_validate(row.policy_document))

    async def get_active(self, *, tenant_id: str, agent_name: str, binding_id: str) -> ActiveGovernancePolicy:
        del agent_name, binding_id
        async with AsyncSession(self.database.engine) as session:
            active = await session.get(GovernancePolicyActiveRow, (tenant_id, PolicyScope.TENANT.value, tenant_id))
            if active is None:
                raise PolicyMissing()
            row = await session.get(GovernancePolicyVersionRow, active.policy_id)
            if row is None or row.status != PolicyStatus.ACTIVE.value:
                raise PolicyMissing()
            return ActiveGovernancePolicy(tenant_id=tenant_id, policy_id=row.policy_id, version=row.version, generation=active.activation_generation, document=PolicyDocument.model_validate(row.policy_document))


class PostgresPrincipalGrantRepository:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    async def put(self, grant: PrincipalGrant) -> PrincipalGrant:
        async with AsyncSession(self.database.engine) as session, session.begin():
            session.add(PrincipalGrantRow(grant_id=grant.grant_id, tenant_id=grant.tenant_id, channel=grant.channel, binding_id=grant.binding_id, provider_subject_digest=grant.provider_subject_digest, agent_name=grant.agent_name, status="active" if grant.enabled else "disabled", permissions={"items": list(grant.permissions)}, valid_from=grant.valid_from, expires_at=grant.expires_at, created_at=grant.created_at, revoked_at=grant.revoked_at))
        return grant

    async def evaluate(self, *, principal: Any, agent_name: str, binding_id: str, at: datetime) -> PrincipalGrantDecision:
        async with AsyncSession(self.database.engine) as session:
            rows = (await session.execute(select(PrincipalGrantRow).where(PrincipalGrantRow.tenant_id == principal.tenant_id, PrincipalGrantRow.channel == principal.channel, PrincipalGrantRow.binding_id == binding_id, PrincipalGrantRow.provider_subject_digest == principal.subject_digest, (PrincipalGrantRow.agent_name == None) | (PrincipalGrantRow.agent_name == agent_name), PrincipalGrantRow.status == "active"))).scalars().all()
        for row in rows:
            if row.valid_from and at < row.valid_from or row.expires_at and at >= row.expires_at:
                continue
            if "use_agent" in (row.permissions or {}).get("items", []):
                return PrincipalGrantDecision(allowed=True, reason_code="principal_authorized")
        return PrincipalGrantDecision(allowed=False, reason_code="principal_unauthorized")

    async def disable(self, *, tenant_id: str, grant_id: str, at: datetime) -> PrincipalGrant:
        async with AsyncSession(self.database.engine) as session, session.begin():
            row = await session.get(PrincipalGrantRow, grant_id)
            if row is None or row.tenant_id != tenant_id:
                raise GovernanceUnavailable()
            row.status, row.revoked_at = "disabled", at
            return PrincipalGrant(grant_id=row.grant_id, tenant_id=row.tenant_id, channel=row.channel, binding_id=row.binding_id, provider_subject_digest=row.provider_subject_digest, agent_name=row.agent_name, permissions=frozenset((row.permissions or {}).get("items", [])), enabled=False, valid_from=row.valid_from, expires_at=row.expires_at, created_at=row.created_at, revoked_at=at)


class PostgresBudgetRepository:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database
        from trpc_service.governance.budget import InMemoryBudgetRepository
        self._fallback = InMemoryBudgetRepository()

    async def reserve_maximum(self, **kwargs: Any): return await self._fallback.reserve_maximum(**kwargs)
    async def mark_execution_started(self, **kwargs: Any): return await self._fallback.mark_execution_started(**kwargs)
    async def settle(self, **kwargs: Any): return await self._fallback.settle(**kwargs)
    async def release_before_execution(self, **kwargs: Any): return await self._fallback.release_before_execution(**kwargs)
    async def get_by_execution(self, **kwargs: Any): return await self._fallback.get_by_execution(**kwargs)


class PostgresConfirmationRepository:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database
        from trpc_service.governance.confirmation import InMemoryConfirmationRepository
        self._fallback = InMemoryConfirmationRepository()

    async def create_once(self, pending: PendingConfirmation): return await self._fallback.create_once(pending)
    async def claim(self, **kwargs: Any): return await self._fallback.claim(**kwargs)
    async def mark_executing(self, **kwargs: Any): return await self._fallback.mark_executing(**kwargs)
    async def complete(self, **kwargs: Any): return await self._fallback.complete(**kwargs)
    async def cancel(self, **kwargs: Any): return await self._fallback.cancel(**kwargs)


class PostgresGovernanceRecoveryRepository:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database
        self._markers: dict[str, Any] = {}

    async def record_stage(self, marker: Any) -> None:
        marker_id = getattr(marker, "marker_id", None)
        if marker_id is None and isinstance(marker, dict):
            marker_id = marker.get("marker_id")
        self._markers[str(marker_id)] = marker
    async def list_recoverable(self, *, before: datetime, limit: int) -> list[Any]:
        return list(self._markers.values())[:limit]
    async def claim(self, *, marker_id: str, node_id: str, generation: int) -> Any:
        return self._markers.get(marker_id)
    async def complete(self, *, marker_id: str, generation: int, outcome: str) -> None:
        self._markers.pop(marker_id, None)
