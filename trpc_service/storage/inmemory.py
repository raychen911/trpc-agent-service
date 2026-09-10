"""Single-process repository adapters used by the local validation runtime."""

from __future__ import annotations

from collections.abc import Mapping
import secrets
from uuid import UUID

from trpc_service.audit.models import AuditRecord, PreAuthScope, TenantScope
from trpc_service.channels.contracts import Channel, VerifiedBindingScope
from trpc_service.channels.identity import ChannelIdentity
from trpc_service.config.settings import PlatformSettings
from trpc_service.storage.contracts import (
    AccessDenied,
    AuditUnavailable,
    BindingAuthMaterial,
    ConditionalWriteFailed,
    NotFound,
    ResolvedChannelBinding,
    SecretBytes,
    SecretUnavailable,
)
from trpc_service.tenant.models import VerifiedTenantContext
from trpc_service.storage.models import ClaimDisposition, ClaimResult, ExecutionResult, ExecutionStatus, IdempotencyKey, IdempotencyRecord, IdempotencyState


class _InMemoryGovernanceRecoveryRepository:
    def __init__(self) -> None:
        self._markers: dict[str, object] = {}

    async def record_stage(self, marker: object) -> None:
        marker_id = getattr(marker, "marker_id", None)
        if marker_id is None and isinstance(marker, dict):
            marker_id = marker.get("marker_id")
        self._markers[str(marker_id)] = marker

    async def list_recoverable(self, *, before: object, limit: int) -> list[object]:
        del before
        return list(self._markers.values())[:limit]

    async def claim(self, *, marker_id: str, node_id: str, generation: int) -> object:
        del node_id, generation
        return self._markers.get(marker_id)

    async def complete(self, *, marker_id: str, generation: int, outcome: str) -> None:
        del generation, outcome
        self._markers.pop(marker_id, None)


class InMemoryIdempotencyRepository:
    def __init__(self) -> None:
        self._records: dict[tuple[str, str, str, str], IdempotencyRecord] = {}
        self.fail_complete_once = False

    @staticmethod
    def _key(key: IdempotencyKey) -> tuple[str, str, str, str]:
        return (
            key.tenant_id,
            key.channel.value,
            key.binding_id,
            key.external_message_id,
        )

    async def claim(self, key: IdempotencyKey, fingerprint: str, trace_id: UUID, now: object) -> ClaimResult:
        storage_key = self._key(key)
        current = self._records.get(storage_key)
        if current is None:
            token = secrets.token_urlsafe(24)
            current = IdempotencyRecord.pending(key=key, content_fingerprint=fingerprint, owner_token=token, trace_id=trace_id, now=now)
            self._records[storage_key] = current
            return ClaimResult(disposition=ClaimDisposition.ACQUIRED, owner_token=token, attempt=1)
        if current.content_fingerprint != fingerprint:
            return ClaimResult(disposition=ClaimDisposition.CONFLICT)
        if current.state == IdempotencyState.FAILED_PRE_START:
            token = secrets.token_urlsafe(24)
            current = current.reclaim(token, trace_id, now)
            self._records[storage_key] = current
            return ClaimResult(disposition=ClaimDisposition.ACQUIRED, owner_token=token, attempt=current.attempt)
        if current.state in {IdempotencyState.PENDING, IdempotencyState.RUNNING}:
            return ClaimResult(disposition=ClaimDisposition.PROCESSING, original_trace_id=current.owner_trace_id)
        return ClaimResult(disposition=ClaimDisposition.COMPLETED, original_trace_id=current.execution_trace_id, result=current.result)

    async def mark_running(self, key: IdempotencyKey, owner_token: str, execution_trace_id: UUID, now: object) -> IdempotencyRecord:
        current = self._records[self._key(key)]
        if current.owner_trace_id != execution_trace_id:
            raise ConditionalWriteFailed("Idempotency transition was rejected.")
        try:
            updated = current.mark_running(owner_token, execution_trace_id, now)
        except ValueError:
            raise ConditionalWriteFailed("Idempotency transition was rejected.") from None
        self._records[self._key(key)] = updated
        return updated

    async def mark_pre_start_failed(self, key: IdempotencyKey, owner_token: str, safe_error: str, now: object) -> IdempotencyRecord:
        current = self._records[self._key(key)]
        try:
            updated = current.mark_pre_start_failed(owner_token, safe_error, now)
        except ValueError:
            raise ConditionalWriteFailed("Idempotency transition was rejected.") from None
        self._records[self._key(key)] = updated
        return updated

    async def complete(self, key: IdempotencyKey, owner_token: str, result: ExecutionResult, now: object) -> IdempotencyRecord:
        if self.fail_complete_once:
            self.fail_complete_once = False
            raise ConditionalWriteFailed("Terminal write outcome is uncertain.")
        current = self._records[self._key(key)]
        if current.execution_trace_id != result.original_trace_id:
            raise ConditionalWriteFailed("Idempotency transition was rejected.")
        try:
            updated = current.complete(owner_token, result, now)
        except ValueError:
            raise ConditionalWriteFailed("Idempotency transition was rejected.") from None
        self._records[self._key(key)] = updated
        return updated

    async def mark_outcome_unknown(self, key: IdempotencyKey, owner_token: str, result: ExecutionResult, now: object) -> IdempotencyRecord:
        if result.status != ExecutionStatus.OUTCOME_UNKNOWN:
            raise ValueError("outcome-unknown transition requires an outcome-unknown result")
        return await self.complete(key, owner_token, result, now)

    async def mark_post_start_failed(self, key: IdempotencyKey, owner_token: str, result: ExecutionResult, now: object) -> IdempotencyRecord:
        if result.status != ExecutionStatus.FAILED_POST_START:
            raise ValueError("post-start failure transition requires a failed result")
        return await self.complete(key, owner_token, result, now)

    async def get(self, key: IdempotencyKey) -> IdempotencyRecord:
        try:
            return self._records[self._key(key)]
        except KeyError:
            raise NotFound("Idempotency record was not found.") from None

    async def reset(self) -> None:
        self._records.clear()
        self.fail_complete_once = False


class EnvironmentSecretResolver:
    def __init__(self, environ: Mapping[str, str]) -> None:
        self._environ = environ

    def resolve(self, secret_ref: str) -> SecretBytes:
        value = self._environ.get(secret_ref, "")
        if not value:
            raise SecretUnavailable("Binding secret is unavailable.")
        return SecretBytes(value.encode("utf-8"))


class InMemoryAuditRepository:
    def __init__(self) -> None:
        self._tenant_records: dict[str, list[AuditRecord]] = {}
        self._preauth_records: list[AuditRecord] = []
        self.fail_append = False
        self.fail_update = False
        self.fail_on_append_number: int | None = None
        self.append_count = 0

    async def append(self, scope: TenantScope | PreAuthScope, record: AuditRecord, fence_proof: object | None = None) -> AuditRecord:
        self.append_count += 1
        if self.fail_append or self.append_count == self.fail_on_append_number:
            raise AuditUnavailable("Audit storage is unavailable.")
        if isinstance(scope, TenantScope):
            if record.tenant_id != scope.tenant_id:
                raise AccessDenied("Audit scope mismatch.")
            self._tenant_records.setdefault(scope.tenant_id, []).append(record)
        elif isinstance(scope, PreAuthScope):
            if record.tenant_id is not None:
                raise AccessDenied("Pre-auth audit cannot contain tenant data.")
            self._preauth_records.append(record)
        else:
            raise AccessDenied("Audit scope is invalid.")
        return record

    async def append_diagnostic(self, scope: TenantScope | PreAuthScope, record: AuditRecord) -> AuditRecord:
        return await self.append(scope, record)

    async def update_final(self, scope: TenantScope, audit_id: UUID, trace_id: UUID, decision: object, **fields: object) -> AuditRecord:
        if not isinstance(scope, TenantScope):
            raise AccessDenied("Audit scope is invalid.")
        if self.fail_update:
            raise AuditUnavailable("Audit storage is unavailable.")
        records = self._tenant_records.get(scope.tenant_id, [])
        for index, record in enumerate(records):
            if record.audit_id == audit_id and record.trace_id == trace_id:
                try:
                    updated = AuditRecord.model_validate({**record.model_dump(), "decision": decision, **fields})
                except Exception as exc:
                    raise ValueError("Audit update contains invalid fields.") from exc
                records[index] = updated
                return updated
        raise NotFound("Audit record was not found.")

    async def list_by_trace(self, scope: TenantScope, trace_id: UUID) -> list[AuditRecord]:
        if not isinstance(scope, TenantScope):
            raise AccessDenied("Audit scope is invalid.")
        return [record for record in self._tenant_records.get(scope.tenant_id, []) if record.trace_id == trace_id]

    async def list_by_session(self, scope: TenantScope, platform_session_id: str) -> list[AuditRecord]:
        if not isinstance(scope, TenantScope):
            raise AccessDenied("Audit scope is invalid.")
        return [record for record in self._tenant_records.get(scope.tenant_id, []) if record.session_id == platform_session_id]

    async def list_by_tenant(self, scope: TenantScope) -> list[AuditRecord]:
        if not isinstance(scope, TenantScope):
            raise AccessDenied("Audit scope is invalid.")
        return list(self._tenant_records.get(scope.tenant_id, []))

    async def list_preauth(self, scope: PreAuthScope) -> list[AuditRecord]:
        if not isinstance(scope, PreAuthScope):
            raise AccessDenied("Audit scope is invalid.")
        return list(self._preauth_records)

    async def reset(self) -> None:
        self._tenant_records.clear()
        self._preauth_records.clear()
        self.fail_append = False
        self.fail_update = False
        self.fail_on_append_number = None
        self.append_count = 0


class InMemoryPlatformAdapters:
    def __init__(self, settings: PlatformSettings) -> None:
        self.settings = settings
        self.audit = InMemoryAuditRepository()
        self.idempotency = InMemoryIdempotencyRepository()
        self._bindings = {item.binding_id: item for item in settings.bindings}
        self._tenants = {item.tenant_id: item for item in settings.tenants}
        self._agents = {(item.tenant_id, item.agent_id): item for item in settings.agents}
        from trpc_service.governance.policy import InMemoryGovernancePolicyRepository
        from trpc_service.governance.principal import InMemoryPrincipalGrantRepository
        from trpc_service.governance.budget import InMemoryBudgetRepository
        from trpc_service.governance.confirmation import InMemoryConfirmationRepository

        class _Governance:
            def __init__(self) -> None:
                self.policy = InMemoryGovernancePolicyRepository()
                self.principal = InMemoryPrincipalGrantRepository()
                self.budget = InMemoryBudgetRepository()
                self.confirmation = InMemoryConfirmationRepository()
                self.recovery = _InMemoryGovernanceRecoveryRepository()

            @property
            def callback(self):
                from trpc_service.tool.governance_callbacks import before_tool_callback
                return before_tool_callback

        self.governance = _Governance()

    async def get_auth_material(self, binding_id: str, channel: Channel = Channel.LOCAL_HTTP) -> BindingAuthMaterial:
        binding = self._bindings.get(binding_id)
        if binding is None or binding.channel != channel:
            raise NotFound("Binding was not found.")
        return BindingAuthMaterial(binding.binding_id, binding.secret_ref, binding.signature_version, binding.status)

    async def resolve_active_context(
        self,
        scope: VerifiedBindingScope,
        *,
        external_user_id: str,
        trace_id: UUID,
    ) -> VerifiedTenantContext:
        return self._resolve_active_context(
            scope,
            external_user_id=external_user_id,
            trace_id=trace_id,
        )

    async def resolve_by_channel_identity(
        self,
        identity: object,
        *,
        external_user_id: str,
        trace_id: UUID,
    ) -> ResolvedChannelBinding:
        if not isinstance(identity, ChannelIdentity):
            raise AccessDenied("Channel binding is unavailable.")
        matches = [
            binding
            for binding in self._bindings.values()
            if (
                binding.channel == identity.channel
                and binding.provider_tenant_key == identity.provider_tenant_key
                and binding.provider_app_or_bot_id == identity.provider_app_or_bot_id
                and binding.channel_identity_digest == identity.identity_digest
            )
        ]
        if len(matches) != 1:
            raise AccessDenied("Channel binding is unavailable.")
        binding = matches[0]
        scope = VerifiedBindingScope._issue(
            binding_id=binding.binding_id,
            channel=binding.channel,
        )
        try:
            context = self._resolve_active_context(
                scope,
                external_user_id=external_user_id,
                trace_id=trace_id,
            ).model_copy(
                update={
                    "config_version": max(
                        self._tenants[binding.tenant_id].config_version,
                        self._agents[(binding.tenant_id, binding.agent_id)].config_version,
                        binding.config_version,
                    )
                }
            )
        except (AccessDenied, KeyError, ValueError):
            raise AccessDenied("Channel binding is unavailable.") from None
        return ResolvedChannelBinding(
            scope=scope,
            context=context,
            secret_ref=binding.secret_ref,
            config_version=context.config_version,
        )

    def _resolve_active_context(
        self,
        scope: VerifiedBindingScope,
        *,
        external_user_id: str,
        trace_id: UUID,
    ) -> VerifiedTenantContext:
        if not getattr(scope, "_verified", False):
            raise AccessDenied("Binding access denied.")
        binding = self._bindings.get(scope.binding_id)
        if binding is None or binding.channel != scope.channel:
            raise AccessDenied("Binding access denied.")
        tenant = self._tenants.get(binding.tenant_id)
        agent = self._agents.get((binding.tenant_id, binding.agent_id))
        if tenant is None or agent is None:
            raise AccessDenied("Binding access denied.")
        try:
            return VerifiedTenantContext.from_resources(
                tenant=tenant,
                agent=agent,
                binding=binding,
                external_user_id=external_user_id,
                trace_id=trace_id,
            )
        except ValueError:
            raise AccessDenied("Binding access denied.") from None

    def context_for_test(self, binding_id: str, external_user_id: str, trace_id: UUID) -> VerifiedTenantContext:
        return self._resolve_active_context(
            VerifiedBindingScope._issue(binding_id=binding_id, channel=Channel.LOCAL_HTTP),
            external_user_id=external_user_id,
            trace_id=trace_id,
        )
