"""PostgreSQL repositories for authoritative configuration and durable evidence."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from dataclasses import dataclass
import logging
import re

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.channels.contracts import Channel, VerifiedBindingScope
from trpc_service.channels.identity import ChannelIdentity, ProviderReplyContext, length_prefixed_digest
from trpc_service.config.settings import PlatformSettings
from trpc_service.storage.contracts import (
    AccessDenied,
    AuditUnavailable,
    BindingAuthMaterial,
    ConditionalWriteFailed,
    ConfigurationUnavailable,
    NotFound,
    ResolvedChannelBinding,
    StaleFence,
)
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.models import (
    AgentApplicationRow,
    ChannelBindingRow,
    DeliveryAttemptRow,
    DeliveryRecordRow,
    PersistentAuditRecordRow,
    RecoveryMarkerRow,
    TenantRow,
)
from trpc_service.audit.models import AuditRecord, PreAuthScope, TenantScope
from uuid import UUID, uuid4
from decimal import Decimal
from hashlib import sha256
import json
from trpc_service.tenant.models import AgentApplication, ChannelBinding, ResourceStatus, Tenant, VerifiedTenantContext
from trpc_service.storage.models import (
    AdapterFence,
    DeliveryAttempt,
    DeliveryOutcome,
    DeliveryRecord,
    DeliveryStatus,
    ExecutionResult,
)
from trpc_service.config.cache import AuthoritativeConfigCache


logger = logging.getLogger(__name__)
_SAFE_DATABASE_DETAIL = re.compile(r"^[A-Za-z0-9_]{1,128}$")


def _safe_database_failure(error: Exception) -> tuple[str, str, str, str]:
    """Extract diagnostic metadata without rendering SQL or bound parameters."""

    original = getattr(error, "orig", None) or error
    driver_error = getattr(original, "__cause__", None) or original
    sqlstate = (
        getattr(driver_error, "sqlstate", None)
        or getattr(original, "sqlstate", None)
        or "unknown"
    )
    diagnostic = getattr(driver_error, "diag", None)
    constraint = (
        getattr(driver_error, "constraint_name", None)
        or getattr(diagnostic, "constraint_name", None)
        or "unknown"
    )
    safe_sqlstate = str(sqlstate) if _SAFE_DATABASE_DETAIL.fullmatch(str(sqlstate)) else "unknown"
    safe_constraint = str(constraint) if _SAFE_DATABASE_DETAIL.fullmatch(str(constraint)) else "unknown"
    return (
        type(error).__name__,
        type(driver_error).__name__,
        safe_sqlstate,
        safe_constraint,
    )


@dataclass(frozen=True, slots=True)
class PersistentRecoveryMarker:
    recovery_id: UUID
    tenant_id: str
    idempotency_key_digest: str
    message_generation: int
    session_generation: int
    execution_trace_id: UUID
    execution_result: ExecutionResult
    result_digest: str
    state: str
    created_at: datetime


class PostgresConfigurationRepository:
    """The database is consulted for every authorization; no cache can grant access."""

    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database
        self.cache = AuthoritativeConfigCache()

    async def seed(self, settings: PlatformSettings) -> None:
        now = datetime.now(timezone.utc)
        try:
            async with AsyncSession(self.database.engine) as session, session.begin():
                for tenant in settings.tenants:
                    await session.execute(insert(TenantRow).values(
                        tenant_id=tenant.tenant_id, display_name=tenant.display_name,
                        status=tenant.status.value, config_version=tenant.config_version,
                        created_at=tenant.created_at, updated_at=now,
                    ).on_conflict_do_update(index_elements=["tenant_id"], set_={
                        "display_name": tenant.display_name, "status": tenant.status.value,
                        "config_version": tenant.config_version, "updated_at": now,
                    }))
                for agent in settings.agents:
                    await session.execute(insert(AgentApplicationRow).values(
                        tenant_id=agent.tenant_id, agent_id=agent.agent_id,
                        agent_name=agent.agent_name, status=agent.status.value,
                        model_profile=agent.model_profile, instruction=agent.instruction,
                        config_version=agent.config_version, updated_at=now,
                    ).on_conflict_do_update(index_elements=["tenant_id", "agent_id"], set_={
                        "agent_name": agent.agent_name, "status": agent.status.value,
                        "model_profile": agent.model_profile, "instruction": agent.instruction,
                        "config_version": agent.config_version, "updated_at": now,
                    }))
                for binding in settings.bindings:
                    await session.execute(insert(ChannelBindingRow).values(
                        binding_id=binding.binding_id, tenant_id=binding.tenant_id,
                        agent_id=binding.agent_id, channel=binding.channel.value,
                        status=binding.status.value, secret_ref=binding.secret_ref,
                        signature_version=binding.signature_version,
                        config_version=binding.config_version,
                        provider_tenant_key=binding.provider_tenant_key,
                        provider_app_or_bot_id=binding.provider_app_or_bot_id,
                        channel_identity_digest=binding.channel_identity_digest,
                        created_at=binding.created_at, updated_at=now,
                    ).on_conflict_do_update(index_elements=["binding_id"], set_={
                        "tenant_id": binding.tenant_id, "agent_id": binding.agent_id,
                        "channel": binding.channel.value, "status": binding.status.value,
                        "secret_ref": binding.secret_ref, "signature_version": binding.signature_version,
                        "config_version": binding.config_version,
                        "provider_tenant_key": binding.provider_tenant_key,
                        "provider_app_or_bot_id": binding.provider_app_or_bot_id,
                        "channel_identity_digest": binding.channel_identity_digest,
                        "updated_at": now,
                    }))
        except Exception:
            raise ConfigurationUnavailable() from None

    async def _binding(self, binding_id: str) -> ChannelBindingRow:
        async def authoritative_read() -> ChannelBindingRow:
            try:
                async with AsyncSession(self.database.engine) as session:
                    row = await session.get(ChannelBindingRow, binding_id)
            except Exception:
                raise ConfigurationUnavailable() from None
            if row is None:
                raise NotFound("Binding was not found.")
            self.cache.remember_verified(binding_id, row.config_version, {
                "tenant_id": row.tenant_id, "agent_id": row.agent_id,
            })
            return row
        row = await self.cache.authorize(binding_id, authoritative_read)
        if row is None:
            raise NotFound("Binding was not found.")
        return row

    async def get_auth_material(
        self, binding_id: str, channel: Channel, *, expected_tenant_id: str | None = None
    ) -> BindingAuthMaterial:
        row = await self._binding(binding_id)
        if row.channel != channel.value or (expected_tenant_id and row.tenant_id != expected_tenant_id):
            raise AccessDenied("Binding ownership mismatch.")
        return BindingAuthMaterial(
            binding_id=row.binding_id, secret_ref=row.secret_ref,
            signature_version=row.signature_version, status=ResourceStatus(row.status),
        )

    async def resolve_active_context(
        self, scope: VerifiedBindingScope, *, external_user_id: str, trace_id: Any
    ) -> VerifiedTenantContext:
        if not getattr(scope, "_verified", False):
            raise AccessDenied("Binding ownership mismatch.")
        binding_row = await self._binding(scope.binding_id)
        if binding_row.channel != scope.channel.value:
            raise AccessDenied("Binding ownership mismatch.")
        try:
            async with AsyncSession(self.database.engine) as session:
                tenant_row = await session.get(TenantRow, binding_row.tenant_id)
                agent_row = await session.get(AgentApplicationRow, (binding_row.tenant_id, binding_row.agent_id))
        except Exception:
            raise ConfigurationUnavailable() from None
        if tenant_row is None or agent_row is None:
            raise AccessDenied("Binding ownership mismatch.")
        try:
            context = VerifiedTenantContext.from_resources(
                tenant=Tenant(tenant_id=tenant_row.tenant_id, display_name=tenant_row.display_name,
                              status=ResourceStatus(tenant_row.status), config_version=tenant_row.config_version,
                              created_at=tenant_row.created_at),
                agent=AgentApplication(tenant_id=agent_row.tenant_id, agent_id=agent_row.agent_id,
                                       agent_name=agent_row.agent_name, status=ResourceStatus(agent_row.status),
                                       model_profile=agent_row.model_profile, instruction=agent_row.instruction,
                                       config_version=agent_row.config_version),
                binding=ChannelBinding(binding_id=binding_row.binding_id, tenant_id=binding_row.tenant_id,
                                       agent_id=binding_row.agent_id, channel=Channel(binding_row.channel),
                                       status=ResourceStatus(binding_row.status), secret_ref=binding_row.secret_ref,
                                       signature_version=binding_row.signature_version,
                                       provider_tenant_key=binding_row.provider_tenant_key,
                                       provider_app_or_bot_id=binding_row.provider_app_or_bot_id,
                                       channel_identity_digest=binding_row.channel_identity_digest,
                                       config_version=binding_row.config_version,
                                       created_at=binding_row.created_at),
                external_user_id=external_user_id, trace_id=trace_id,
            )
            return context.model_copy(update={
                "config_version": max(context.config_version, binding_row.config_version)
            })
        except (ValueError, TypeError):
            raise AccessDenied("Configuration is not active or ownership is invalid.") from None

    async def resolve_by_channel_identity(
        self,
        identity: ChannelIdentity,
        *,
        external_user_id: str,
        trace_id: UUID,
    ) -> ResolvedChannelBinding:
        """Resolve one active binding from authenticated provider identity only."""

        if not isinstance(identity, ChannelIdentity):
            raise AccessDenied("Channel binding is unavailable.")
        try:
            async with AsyncSession(self.database.engine) as session:
                statement = (
                    select(ChannelBindingRow, TenantRow, AgentApplicationRow)
                    .join(TenantRow, TenantRow.tenant_id == ChannelBindingRow.tenant_id)
                    .join(
                        AgentApplicationRow,
                        (AgentApplicationRow.tenant_id == ChannelBindingRow.tenant_id)
                        & (AgentApplicationRow.agent_id == ChannelBindingRow.agent_id),
                    )
                    .where(
                        ChannelBindingRow.channel == identity.channel.value,
                        ChannelBindingRow.provider_tenant_key == identity.provider_tenant_key,
                        ChannelBindingRow.provider_app_or_bot_id == identity.provider_app_or_bot_id,
                        ChannelBindingRow.channel_identity_digest == identity.identity_digest,
                    )
                )
                rows = (await session.execute(statement)).all()
        except Exception:
            raise ConfigurationUnavailable() from None
        if len(rows) != 1:
            raise AccessDenied("Channel binding is unavailable.")
        binding_row, tenant_row, agent_row = rows[0]
        if any(
            status != ResourceStatus.ACTIVE.value
            for status in (binding_row.status, tenant_row.status, agent_row.status)
        ):
            raise AccessDenied("Channel binding is unavailable.")
        try:
            binding = ChannelBinding(
                binding_id=binding_row.binding_id,
                tenant_id=binding_row.tenant_id,
                agent_id=binding_row.agent_id,
                channel=Channel(binding_row.channel),
                status=ResourceStatus(binding_row.status),
                secret_ref=binding_row.secret_ref,
                signature_version=binding_row.signature_version,
                provider_tenant_key=binding_row.provider_tenant_key,
                provider_app_or_bot_id=binding_row.provider_app_or_bot_id,
                channel_identity_digest=binding_row.channel_identity_digest,
                config_version=binding_row.config_version,
                created_at=binding_row.created_at,
            )
            context = VerifiedTenantContext.from_resources(
                tenant=Tenant(
                    tenant_id=tenant_row.tenant_id,
                    display_name=tenant_row.display_name,
                    status=ResourceStatus(tenant_row.status),
                    config_version=tenant_row.config_version,
                    created_at=tenant_row.created_at,
                ),
                agent=AgentApplication(
                    tenant_id=agent_row.tenant_id,
                    agent_id=agent_row.agent_id,
                    agent_name=agent_row.agent_name,
                    status=ResourceStatus(agent_row.status),
                    model_profile=agent_row.model_profile,
                    instruction=agent_row.instruction,
                    config_version=agent_row.config_version,
                ),
                binding=binding,
                external_user_id=external_user_id,
                trace_id=trace_id,
            ).model_copy(
                update={
                    "config_version": max(
                        tenant_row.config_version,
                        agent_row.config_version,
                        binding_row.config_version,
                    )
                }
            )
        except (TypeError, ValueError):
            raise AccessDenied("Channel binding is unavailable.") from None
        return ResolvedChannelBinding(
            scope=VerifiedBindingScope._issue(
                binding_id=binding.binding_id,
                channel=binding.channel,
            ),
            context=context,
            secret_ref=binding.secret_ref,
            config_version=context.config_version,
        )


class PostgresDeliveryRepository:
    """Durable delivery state with tenant scoping and fence-aware writes."""

    def __init__(self, database: PostgresDatabase, *, fence_validator: Any | None = None) -> None:
        self.database = database
        self.fence_validator = fence_validator

    async def _require_fence(self, fence: AdapterFence) -> None:
        if self.fence_validator is None:
            return
        valid = self.fence_validator(fence)
        if hasattr(valid, "__await__"):
            valid = await valid
        if not valid:
            raise StaleFence("Stale adapter fence was rejected.")

    @staticmethod
    def _tenant_id(scope: TenantScope | str) -> str:
        return scope.tenant_id if isinstance(scope, TenantScope) else scope

    @staticmethod
    def _delivery_domain(row: DeliveryRecordRow) -> DeliveryRecord:
        reply_context = dict(row.reply_context)
        reply_context.pop("context_digest", None)
        return DeliveryRecord(
            delivery_id=UUID(row.delivery_id),
            tenant_id=row.tenant_id,
            binding_id=row.binding_id,
            channel=Channel(row.channel),
            idempotency_key_digest=row.idempotency_key_digest,
            execution_trace_id=UUID(row.execution_trace_id),
            reply_context=ProviderReplyContext.model_validate(reply_context),
            result_digest=row.result_digest,
            status=DeliveryStatus(row.status),
            adapter_generation=row.adapter_generation,
            next_attempt_at=row.next_attempt_at,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    @staticmethod
    def _attempt_domain(row: DeliveryAttemptRow) -> DeliveryAttempt:
        return DeliveryAttempt(
            attempt_id=UUID(row.attempt_id),
            delivery_id=UUID(row.delivery_id),
            attempt_no=row.attempt_no,
            trace_id=UUID(row.trace_id),
            adapter_node_id=row.adapter_node_id,
            adapter_generation=row.adapter_generation,
            started_at=row.started_at,
            finished_at=row.finished_at,
            outcome=DeliveryOutcome(row.outcome) if row.outcome else None,
            safe_error_code=row.safe_error_code,
            retry_delay_seconds=row.retry_delay_seconds,
        )

    async def create_or_get(
        self,
        tenant_scope: TenantScope | str,
        binding_scope: VerifiedBindingScope,
        execution_result: ExecutionResult,
        reply_context: ProviderReplyContext,
        adapter_fence: AdapterFence,
    ) -> DeliveryRecord:
        await self._require_fence(adapter_fence)
        tenant_id = self._tenant_id(tenant_scope)
        if binding_scope.channel != reply_context.channel:
            raise AccessDenied("Delivery scope mismatch.")
        idempotency_digest = length_prefixed_digest(
            tenant_id,
            binding_scope.channel.value,
            binding_scope.binding_id,
            reply_context.provider_message_id,
        )
        result_payload = execution_result.model_dump(mode="json")
        result_digest = sha256(
            json.dumps(result_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        now = datetime.now(timezone.utc)
        delivery_id = uuid4()
        values = {
            "delivery_id": str(delivery_id),
            "tenant_id": tenant_id,
            "binding_id": binding_scope.binding_id,
            "channel": binding_scope.channel.value,
            "idempotency_key_digest": idempotency_digest,
            "execution_trace_id": str(execution_result.original_trace_id),
            "reply_context": reply_context.model_dump(
                mode="json", exclude_computed_fields=True
            ),
            "result_digest": result_digest,
            "status": DeliveryStatus.PENDING.value,
            "adapter_generation": adapter_fence.generation,
            "next_attempt_at": None,
            "created_at": now,
            "updated_at": now,
        }
        stage = "insert"
        try:
            async with AsyncSession(self.database.engine) as session, session.begin():
                await session.execute(
                    insert(DeliveryRecordRow)
                    .values(**values)
                    .on_conflict_do_nothing(constraint="uq_delivery_execution_scope")
                )
                stage = "read_back"
                row = await session.scalar(
                    select(DeliveryRecordRow).where(
                        DeliveryRecordRow.tenant_id == tenant_id,
                        DeliveryRecordRow.idempotency_key_digest == idempotency_digest,
                        DeliveryRecordRow.channel == binding_scope.channel.value,
                        DeliveryRecordRow.binding_id == binding_scope.binding_id,
                    )
                )
                if row is None:
                    raise RuntimeError("delivery read-back failed")
                stage = "deserialize"
                snapshot = self._delivery_domain(row)
            return snapshot
        except (AccessDenied, StaleFence):
            raise
        except Exception as error:
            error_type, driver_error_type, sqlstate, constraint = _safe_database_failure(error)
            logger.error(
                "Delivery storage operation failed: operation=create_or_get "
                "stage=%s error_type=%s driver_error_type=%s sqlstate=%s constraint=%s",
                stage,
                error_type,
                driver_error_type,
                sqlstate,
                constraint,
            )
            raise ConfigurationUnavailable("Delivery storage is unavailable.") from None

    async def begin_attempt(
        self,
        tenant_scope: TenantScope | str,
        delivery_id: UUID,
        expected_status: DeliveryStatus,
        adapter_fence: AdapterFence,
        trace_id: UUID,
    ) -> DeliveryAttempt:
        await self._require_fence(adapter_fence)
        tenant_id = self._tenant_id(tenant_scope)
        now = datetime.now(timezone.utc)
        try:
            async with AsyncSession(self.database.engine) as session, session.begin():
                row = await session.scalar(
                    select(DeliveryRecordRow)
                    .where(
                        DeliveryRecordRow.delivery_id == str(delivery_id),
                        DeliveryRecordRow.tenant_id == tenant_id,
                    )
                    .with_for_update()
                )
                if row is None:
                    raise NotFound("Delivery was not found.")
                expected = DeliveryStatus(expected_status)
                if row.status != expected.value or expected not in {
                    DeliveryStatus.PENDING,
                    DeliveryStatus.RETRY_WAIT,
                }:
                    raise ConditionalWriteFailed("Delivery state changed.")
                previous = await session.scalar(
                    select(func.max(DeliveryAttemptRow.attempt_no)).where(
                        DeliveryAttemptRow.delivery_id == str(delivery_id)
                    )
                )
                attempt_no = int(previous or 0) + 1
                if attempt_no > 4:
                    raise ConditionalWriteFailed("Delivery attempt limit reached.")
                attempt = DeliveryAttempt(
                    attempt_id=uuid4(),
                    delivery_id=delivery_id,
                    attempt_no=attempt_no,
                    trace_id=trace_id,
                    adapter_node_id=adapter_fence.node_id,
                    adapter_generation=adapter_fence.generation,
                    started_at=now,
                )
                session.add(
                    DeliveryAttemptRow(
                        attempt_id=str(attempt.attempt_id),
                        delivery_id=str(delivery_id),
                        attempt_no=attempt_no,
                        trace_id=str(trace_id),
                        adapter_node_id=adapter_fence.node_id,
                        adapter_generation=adapter_fence.generation,
                        started_at=now,
                        finished_at=None,
                        outcome=None,
                        safe_error_code=None,
                        retry_delay_seconds=None,
                    )
                )
                row.status = DeliveryStatus.SENDING.value
                row.adapter_generation = adapter_fence.generation
                row.next_attempt_at = None
                row.updated_at = now
            return attempt
        except (NotFound, ConditionalWriteFailed, StaleFence):
            raise
        except Exception:
            raise ConfigurationUnavailable("Delivery storage is unavailable.") from None

    async def finish_attempt(
        self,
        tenant_scope: TenantScope | str,
        attempt_id: UUID,
        outcome: DeliveryOutcome,
        safe_error_code: str | None,
        retry_delay_seconds: int | None,
        adapter_fence: AdapterFence,
    ) -> DeliveryRecord:
        await self._require_fence(adapter_fence)
        tenant_id = self._tenant_id(tenant_scope)
        now = datetime.now(timezone.utc)
        try:
            async with AsyncSession(self.database.engine) as session, session.begin():
                attempt_row = await session.scalar(
                    select(DeliveryAttemptRow)
                    .where(DeliveryAttemptRow.attempt_id == str(attempt_id))
                    .with_for_update()
                )
                if attempt_row is None:
                    raise NotFound("Delivery attempt was not found.")
                delivery_row = await session.scalar(
                    select(DeliveryRecordRow)
                    .where(
                        DeliveryRecordRow.delivery_id == attempt_row.delivery_id,
                        DeliveryRecordRow.tenant_id == tenant_id,
                    )
                    .with_for_update()
                )
                if delivery_row is None:
                    raise NotFound("Delivery was not found.")
                if (
                    delivery_row.status != DeliveryStatus.SENDING.value
                    or attempt_row.finished_at is not None
                    or attempt_row.adapter_generation != adapter_fence.generation
                ):
                    raise ConditionalWriteFailed("Delivery state changed.")
                normalized_outcome = DeliveryOutcome(outcome)
                completed_attempt = DeliveryAttempt(
                    attempt_id=attempt_id,
                    delivery_id=UUID(attempt_row.delivery_id),
                    attempt_no=attempt_row.attempt_no,
                    trace_id=UUID(attempt_row.trace_id),
                    adapter_node_id=attempt_row.adapter_node_id,
                    adapter_generation=attempt_row.adapter_generation,
                    started_at=attempt_row.started_at,
                    finished_at=now,
                    outcome=normalized_outcome,
                    safe_error_code=safe_error_code,
                    retry_delay_seconds=retry_delay_seconds,
                )
                attempt_row.finished_at = completed_attempt.finished_at
                attempt_row.outcome = completed_attempt.outcome.value
                attempt_row.safe_error_code = completed_attempt.safe_error_code
                attempt_row.retry_delay_seconds = completed_attempt.retry_delay_seconds
                next_attempt_at = None
                if normalized_outcome == DeliveryOutcome.SUCCEEDED:
                    target = DeliveryStatus.DELIVERED
                elif normalized_outcome == DeliveryOutcome.TRANSIENT and attempt_row.attempt_no < 4:
                    target = DeliveryStatus.RETRY_WAIT
                    next_attempt_at = now + timedelta(seconds=int(retry_delay_seconds or 0))
                elif normalized_outcome in {DeliveryOutcome.TRANSIENT, DeliveryOutcome.PERMANENT}:
                    target = DeliveryStatus.DELIVERY_FAILED
                else:
                    target = DeliveryStatus.DELIVERY_UNKNOWN
                delivery_row.status = target.value
                delivery_row.next_attempt_at = next_attempt_at
                delivery_row.updated_at = now
                delivery_row.adapter_generation = adapter_fence.generation
                await session.flush()
                snapshot = self._delivery_domain(delivery_row)
            return snapshot
        except (NotFound, ConditionalWriteFailed, StaleFence, ValueError):
            raise
        except Exception:
            raise ConfigurationUnavailable("Delivery storage is unavailable.") from None

    async def get(
        self, tenant_scope: TenantScope | str, delivery_id: UUID
    ) -> DeliveryRecord:
        tenant_id = self._tenant_id(tenant_scope)
        try:
            async with AsyncSession(self.database.engine) as session:
                row = await session.scalar(
                    select(DeliveryRecordRow).where(
                        DeliveryRecordRow.delivery_id == str(delivery_id),
                        DeliveryRecordRow.tenant_id == tenant_id,
                    )
                )
            if row is None:
                raise NotFound("Delivery was not found.")
            return self._delivery_domain(row)
        except NotFound:
            raise
        except Exception:
            raise ConfigurationUnavailable("Delivery storage is unavailable.") from None

    async def list_due(
        self,
        tenant_scope: TenantScope | str,
        now: datetime,
        limit: int,
    ) -> list[DeliveryRecord]:
        tenant_id = self._tenant_id(tenant_scope)
        try:
            async with AsyncSession(self.database.engine) as session:
                rows = (
                    await session.scalars(
                        select(DeliveryRecordRow)
                        .where(
                            DeliveryRecordRow.tenant_id == tenant_id,
                            DeliveryRecordRow.status == DeliveryStatus.RETRY_WAIT.value,
                            DeliveryRecordRow.next_attempt_at <= now,
                        )
                        .order_by(DeliveryRecordRow.next_attempt_at, DeliveryRecordRow.delivery_id)
                        .limit(limit)
                    )
                ).all()
            return [self._delivery_domain(row) for row in rows]
        except Exception:
            raise ConfigurationUnavailable("Delivery storage is unavailable.") from None


class PostgresAuditRepository:
    """Immutable persistent audit repository with a separate diagnostic write path."""

    def __init__(self, database: PostgresDatabase, *, node_id: str,
                 fence_validator: Any | None = None) -> None:
        self.database = database
        self.node_id = node_id
        self.process_instance_id = str(uuid4())
        self.fence_validator = fence_validator

    async def _valid_fence(self, proof: Any) -> bool:
        if self.fence_validator is None:
            return True
        value = self.fence_validator(proof)
        if hasattr(value, "__await__"):
            value = await value
        return bool(value)

    async def append(self, scope: TenantScope | PreAuthScope, record: AuditRecord,
                     fence_proof: Any | None = None) -> AuditRecord:
        if isinstance(scope, TenantScope) and record.tenant_id != scope.tenant_id:
            raise AccessDenied("Audit scope mismatch.")
        if isinstance(scope, PreAuthScope) and record.tenant_id is not None:
            raise AccessDenied("Pre-auth audit cannot contain tenant data.")
        if not isinstance(scope, (TenantScope, PreAuthScope)):
            raise AccessDenied("Audit scope is invalid.")
        if fence_proof is not None and not await self._valid_fence(fence_proof):
            raise StaleFence("Stale business audit was rejected.")
        stored = record.model_copy(update={
            "audit_kind": "business",
            "node_id": record.node_id or self.node_id,
        })
        await self._insert(stored, "business")
        return stored

    async def append_diagnostic(self, scope: TenantScope | PreAuthScope, record: AuditRecord) -> AuditRecord:
        if isinstance(scope, TenantScope) and record.tenant_id != scope.tenant_id:
            raise AccessDenied("Audit scope mismatch.")
        if isinstance(scope, PreAuthScope) and record.tenant_id is not None:
            raise AccessDenied("Pre-auth audit cannot contain tenant data.")
        if not isinstance(scope, (TenantScope, PreAuthScope)):
            raise AccessDenied("Audit scope is invalid.")
        stored = record.model_copy(update={
            "audit_kind": "diagnostic",
            "node_id": record.node_id or self.node_id,
        })
        await self._insert(stored, "diagnostic")
        return stored

    async def _insert(self, record: AuditRecord, kind: str) -> None:
        try:
            async with AsyncSession(self.database.engine) as session, session.begin():
                session.add(self._row(record, kind))
        except Exception:
            raise AuditUnavailable("Audit storage is unavailable.") from None

    def _row(self, record: AuditRecord, kind: str) -> PersistentAuditRecordRow:
        return PersistentAuditRecordRow(
                    audit_id=str(record.audit_id), audit_kind=kind, decision=record.decision.value,
                    trace_id=str(record.trace_id), first_claim_trace_id=str(record.first_claim_trace_id) if record.first_claim_trace_id else None,
                    owner_trace_id=str(record.owner_trace_id) if record.owner_trace_id else None,
                    execution_trace_id=str(record.execution_trace_id) if record.execution_trace_id else None,
                    tenant_id=record.tenant_id, agent_id=record.agent_id,
                    channel=record.channel.value,
                    node_id=record.node_id or self.node_id, process_instance_id=self.process_instance_id,
                    binding_id_digest=record.binding_id_digest, external_message_digest=record.external_message_digest,
                    platform_session_id=record.session_id, message_generation=record.generation, session_generation=None,
                    rejected_generation=record.rejected_generation, current_generation=record.current_generation, error_type=record.error_type,
                    result_digest=None, latency_ms=Decimal(str(record.latency_ms)), cost=record.cost,
                    recovery_status=None,
                    adapter_node_id=record.adapter_node_id,
                    adapter_generation=record.adapter_generation,
                    channel_identity_digest=record.channel_identity_digest,
                    provider_message_digest=record.provider_message_digest,
                    delivery_id=str(record.delivery_id) if record.delivery_id else None,
                    delivery_attempt_no=record.delivery_attempt_no,
                    delivery_status=record.delivery_status,
                    created_at=record.created_at,
                )

    async def append_final_with_recovery(
        self, scope: TenantScope, record: AuditRecord, execution_result: Any,
        *, message_generation: int, session_generation: int,
        idempotency_key_digest: str, fence_proof: Any | None = None,
    ) -> dict[str, Any]:
        """Commit the final audit and TERMINAL_PENDING marker in one SQL transaction."""
        if record.tenant_id != scope.tenant_id:
            raise AccessDenied("Audit scope mismatch.")
        if fence_proof is not None and not await self._valid_fence(fence_proof):
            raise StaleFence("Stale final audit was rejected.")
        payload = execution_result.model_dump(mode="json")
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        digest = sha256(encoded).hexdigest()
        recovery_id = uuid4()
        try:
            async with AsyncSession(self.database.engine) as session, session.begin():
                session.add(self._row(record, "business"))
                session.add(RecoveryMarkerRow(
                    recovery_id=str(recovery_id), tenant_id=scope.tenant_id,
                    binding_id_digest=record.binding_id_digest,
                    external_message_digest=record.external_message_digest,
                    platform_session_id=record.session_id,
                    idempotency_key_digest=idempotency_key_digest,
                    message_generation=message_generation,
                    session_generation=session_generation,
                    execution_trace_id=str(execution_result.original_trace_id),
                    state="terminal_pending", result_status=execution_result.status.value,
                    result_payload=payload, result_digest=digest, replay_allowed=False,
                    failure_stage=None, created_at=record.created_at, reconciled_at=None,
                ))
        except Exception:
            raise AuditUnavailable("Final audit transaction is unavailable.") from None
        return {"id": recovery_id, "result": payload, "result_digest": digest,
                "idempotency_key_digest": idempotency_key_digest}

    async def mark_recovery_reconciled(self, recovery_id: UUID) -> None:
        try:
            async with AsyncSession(self.database.engine) as session, session.begin():
                row = await session.get(RecoveryMarkerRow, str(recovery_id))
                if row is not None and row.state == "terminal_pending":
                    row.state = "reconciled"
                    row.reconciled_at = datetime.now(timezone.utc)
        except Exception:
            raise AuditUnavailable("Recovery status is unavailable.") from None

    @staticmethod
    def _recovery_domain(row: RecoveryMarkerRow) -> PersistentRecoveryMarker:
        return PersistentRecoveryMarker(
            recovery_id=UUID(row.recovery_id), tenant_id=row.tenant_id,
            idempotency_key_digest=row.idempotency_key_digest,
            message_generation=row.message_generation,
            session_generation=row.session_generation,
            execution_trace_id=UUID(row.execution_trace_id),
            execution_result=ExecutionResult.model_validate(row.result_payload),
            result_digest=row.result_digest, state=row.state, created_at=row.created_at,
        )

    async def get_pending(self, tenant_scope: TenantScope | str, limit: int) -> list[PersistentRecoveryMarker]:
        tenant_id = tenant_scope.tenant_id if isinstance(tenant_scope, TenantScope) else tenant_scope
        try:
            async with AsyncSession(self.database.engine) as session:
                rows = (await session.scalars(
                    select(RecoveryMarkerRow).where(
                        RecoveryMarkerRow.tenant_id == tenant_id,
                        RecoveryMarkerRow.state == "terminal_pending",
                    ).order_by(RecoveryMarkerRow.created_at).limit(limit)
                )).all()
            return [self._recovery_domain(row) for row in rows]
        except Exception:
            raise AuditUnavailable("Recovery query is unavailable.") from None

    async def list_recovery_by_trace(self, scope: TenantScope, trace_id: UUID) -> list[PersistentRecoveryMarker]:
        try:
            async with AsyncSession(self.database.engine) as session:
                rows = (await session.scalars(select(RecoveryMarkerRow).where(
                    RecoveryMarkerRow.tenant_id == scope.tenant_id,
                    RecoveryMarkerRow.execution_trace_id == str(trace_id),
                ))).all()
            return [self._recovery_domain(row) for row in rows]
        except Exception:
            raise AuditUnavailable("Recovery query is unavailable.") from None

    async def mark_reconciled(self, tenant_scope: TenantScope | str, recovery_id: UUID, expected_result_digest: str) -> PersistentRecoveryMarker:
        tenant_id = tenant_scope.tenant_id if isinstance(tenant_scope, TenantScope) else tenant_scope
        try:
            async with AsyncSession(self.database.engine) as session, session.begin():
                row = await session.get(RecoveryMarkerRow, str(recovery_id), with_for_update=True)
                if row is None or row.tenant_id != tenant_id or row.result_digest != expected_result_digest:
                    raise ValueError("recovery marker mismatch")
                if row.state == "terminal_pending":
                    row.state = "reconciled"; row.reconciled_at = datetime.now(timezone.utc)
                await session.flush()
                snapshot = self._recovery_domain(row)
            return snapshot
        except ValueError:
            raise StaleFence("Recovery marker changed.") from None
        except Exception:
            raise AuditUnavailable("Recovery update is unavailable.") from None

    async def mark_conflict_review(self, tenant_scope: TenantScope | str, recovery_id: UUID, safe_reason: str) -> None:
        del safe_reason
        tenant_id = tenant_scope.tenant_id if isinstance(tenant_scope, TenantScope) else tenant_scope
        try:
            async with AsyncSession(self.database.engine) as session, session.begin():
                row = await session.get(RecoveryMarkerRow, str(recovery_id), with_for_update=True)
                if row is not None and row.tenant_id == tenant_id and row.state == "terminal_pending":
                    row.state = "conflict_review"
        except Exception:
            raise AuditUnavailable("Recovery update is unavailable.") from None

    @staticmethod
    def _domain(row: PersistentAuditRecordRow) -> AuditRecord:
        return AuditRecord(
            audit_id=UUID(row.audit_id), trace_id=UUID(row.trace_id),
            first_claim_trace_id=UUID(row.first_claim_trace_id) if row.first_claim_trace_id else None,
            owner_trace_id=UUID(row.owner_trace_id) if row.owner_trace_id else None,
            execution_trace_id=UUID(row.execution_trace_id) if row.execution_trace_id else None,
            generation=row.message_generation,
            node_id=row.node_id, audit_kind=row.audit_kind,
            rejected_generation=row.rejected_generation,
            current_generation=row.current_generation,
            tenant_id=row.tenant_id, channel=row.channel, binding_id_digest=row.binding_id_digest,
            session_id=row.platform_session_id, agent_id=row.agent_id, decision=row.decision,
            latency_ms=float(row.latency_ms), error_type=row.error_type, cost=row.cost,
            external_message_digest=row.external_message_digest,
            adapter_node_id=row.adapter_node_id,
            adapter_generation=row.adapter_generation,
            channel_identity_digest=row.channel_identity_digest,
            provider_message_digest=row.provider_message_digest,
            delivery_id=UUID(row.delivery_id) if row.delivery_id else None,
            delivery_attempt_no=row.delivery_attempt_no,
            delivery_status=row.delivery_status,
            created_at=row.created_at,
        )

    async def _query(self, *criteria: Any) -> list[AuditRecord]:
        try:
            async with AsyncSession(self.database.engine) as session:
                rows = (await session.scalars(select(PersistentAuditRecordRow).where(*criteria).order_by(PersistentAuditRecordRow.created_at, PersistentAuditRecordRow.audit_id))).all()
            return [self._domain(row) for row in rows]
        except Exception:
            raise AuditUnavailable("Audit storage is unavailable.") from None

    async def list_by_trace(self, scope: TenantScope, trace_id: UUID) -> list[AuditRecord]:
        return await self._query(PersistentAuditRecordRow.tenant_id == scope.tenant_id, PersistentAuditRecordRow.trace_id == str(trace_id))

    async def list_by_session(self, scope: TenantScope, platform_session_id: str) -> list[AuditRecord]:
        return await self._query(PersistentAuditRecordRow.tenant_id == scope.tenant_id, PersistentAuditRecordRow.platform_session_id == platform_session_id)

    async def list_by_agent(self, scope: TenantScope, agent_id: str) -> list[AuditRecord]:
        return await self._query(
            PersistentAuditRecordRow.tenant_id == scope.tenant_id,
            PersistentAuditRecordRow.agent_id == agent_id,
        )

    async def list_by_tenant(self, scope: TenantScope) -> list[AuditRecord]:
        return await self._query(PersistentAuditRecordRow.tenant_id == scope.tenant_id)

    async def list_preauth(self, scope: PreAuthScope) -> list[AuditRecord]:
        del scope
        return await self._query(PersistentAuditRecordRow.tenant_id.is_(None))

    async def update_final(self, scope: TenantScope, audit_id: UUID, trace_id: UUID, decision: Any, **fields: object) -> AuditRecord:
        del fields
        rows = await self._query(PersistentAuditRecordRow.tenant_id == scope.tenant_id,
                                 PersistentAuditRecordRow.audit_id == str(audit_id),
                                 PersistentAuditRecordRow.trace_id == str(trace_id))
        if not rows:
            raise NotFound("Audit record was not found.")
        return rows[0].model_copy(update={"decision": decision})

    async def reset(self) -> None:
        raise AccessDenied("Persistent audit reset is not exposed.")
