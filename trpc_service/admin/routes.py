"""Admin API routes with fixed-text error mapping."""

from __future__ import annotations

from uuid import UUID

from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from trpc_service.admin.auth import AdminToken
from trpc_service.admin.schemas import ExecutionAuditEventResponse
from trpc_service.admin.schemas import ExecutionAuditListResponse
from trpc_service.admin.schemas import MessageAuditEventResponse
from trpc_service.admin.schemas import MessageAuditListResponse
from trpc_service.admin.schemas import TenantConfigResponse
from trpc_service.admin.schemas import TenantCreateRequest
from trpc_service.admin.schemas import TenantRollbackRequest
from trpc_service.admin.schemas import TenantUpdateRequest
from trpc_service.admin.schemas import TenantVersionsResponse
from trpc_service.admin.schemas import (
    TenantRolloutBeginRequest,
    TenantRolloutMutationRequest,
    TenantRolloutResponse,
)
from trpc_service.admin.schemas import ChannelBindingCreateRequest, ChannelBindingListResponse
from trpc_service.admin.schemas import ChannelBindingRollbackRequest, ChannelBindingUpdateRequest
from trpc_service.channels.binding import ChannelBinding
from trpc_service.storage.channel_binding_repository import (
    ChannelBindingAlreadyExistsError,
    ChannelBindingNotFoundError,
    ChannelBindingRepository,
    ChannelBindingRepositoryDataError,
    ChannelBindingRepositoryUnavailableError,
    ChannelBindingTargetVersionNotFoundError,
    ChannelBindingVersionConflictError,
)
from trpc_service.config.tenant import TenantConfig
from trpc_service.config.tenant import TenantConfigDraft
from trpc_service.config.tenant_repository import (
    TenantAlreadyExistsError, )
from trpc_service.config.tenant_repository import TenantConfigAdminRepository
from trpc_service.config.tenant_repository import (
    TenantConfigTargetVersionNotFoundError, )
from trpc_service.config.tenant_repository import (
    TenantConfigVersionConflictError, )
from trpc_service.config.tenant_repository import TenantNotFoundError
from trpc_service.config.tenant_repository import (
    TenantRepositoryDataError, )
from trpc_service.config.tenant_repository import (
    TenantRepositoryUnavailableError, )
from trpc_service.storage.execution_audit_repository import ExecutionAuditRepository
from trpc_service.storage.execution_audit_repository import (
    ExecutionAuditRepositoryDataError, )
from trpc_service.storage.execution_audit_repository import (
    ExecutionAuditRepositoryUnavailableError, )
from trpc_service.storage.approval_repository import ToolApprovalRepository
from trpc_service.storage.approval_repository import (
    ToolApprovalRepositoryDataError,
    ToolApprovalRepositoryUnavailableError,
)
from trpc_service.storage.message_repository import MessageReceiptRepository
from trpc_service.storage.usage_repository import (
    UsageRepository,
    UsageRepositoryDataError,
    UsageRepositoryUnavailableError,
)
from trpc_service.admin.schemas import ApprovalTerminationResponse
from trpc_service.admin.schemas import OrphanedApprovalListResponse
from trpc_service.admin.schemas import OrphanedApprovalResponse
from trpc_service.admin.schemas import TenantUsageResponse
from trpc_service.admin.schemas import UsageProfileResponse
from trpc_service.admin.schemas import UnifiedAuditListResponse, UnifiedAuditRecordResponse
from trpc_service.storage.audit_query_repository import (
    AuditQueryRepository,
    AuditQueryRepositoryDataError,
    AuditQueryRepositoryUnavailableError,
)
from trpc_service.governance.approval import OrphanTerminationAction
from trpc_service.storage.message_repository import (
    MessageReceiptRepositoryDataError, )
from trpc_service.storage.message_repository import (
    MessageReceiptRepositoryUnavailableError, )
from trpc_service.tenant.context import InvalidTenantIdError
from trpc_service.tenant.context import validate_tenant_id
from trpc_service.storage.rollout_repository import TenantRolloutNotFoundError

_TOKEN_HEADER = "x-trpc-admin-token"

MSG_UNAUTHORIZED = "Admin authentication failed."
MSG_TENANT_NOT_FOUND = "Tenant not found."
MSG_TARGET_NOT_FOUND = "Target version not found."
MSG_ALREADY_EXISTS = "Tenant already exists."
MSG_VERSION_CONFLICT = "Configuration version conflict."
MSG_VALIDATION_FAILED = "Request validation failed."
MSG_OPERATION_FAILED = "Tenant configuration operation failed."
MSG_REPOSITORY_UNAVAILABLE = "Tenant repository is unavailable."
MSG_INVALID_TENANT_ID = "Invalid tenant ID format."
MSG_INTERNAL_ERROR = "Internal server error."
MSG_INVALID_MESSAGE_ID = "Invalid message ID."
MSG_AUDIT_QUERY_FAILED = "Audit query failed."
MSG_EXECUTION_AUDIT_QUERY_FAILED = "Execution audit query failed."
MSG_USAGE_QUERY_FAILED = "Usage query failed."
MSG_UNIFIED_AUDIT_QUERY_FAILED = "Audit query failed."
MSG_INVALID_USAGE_DAY = "Invalid day format; expected YYYY-MM-DD."
MSG_APPROVAL_NOT_FOUND = "Approval not found."
MSG_APPROVAL_DISPOSITION_REJECTED = "Approval disposition rejected."


def _domain_error_handlers(application: FastAPI) -> None:
    handlers: dict[type[Exception], tuple[int, str]] = {
        InvalidTenantIdError: (422, MSG_INVALID_TENANT_ID),
        TenantNotFoundError: (404, MSG_TENANT_NOT_FOUND),
        TenantConfigTargetVersionNotFoundError: (404, MSG_TARGET_NOT_FOUND),
        TenantAlreadyExistsError: (409, MSG_ALREADY_EXISTS),
        TenantConfigVersionConflictError: (409, MSG_VERSION_CONFLICT),
        TenantRepositoryDataError: (500, MSG_OPERATION_FAILED),
        TenantRepositoryUnavailableError: (503, MSG_REPOSITORY_UNAVAILABLE),
        MessageReceiptRepositoryDataError: (500, MSG_AUDIT_QUERY_FAILED),
        MessageReceiptRepositoryUnavailableError: (503, MSG_REPOSITORY_UNAVAILABLE),
        ExecutionAuditRepositoryDataError: (500, MSG_EXECUTION_AUDIT_QUERY_FAILED),
        ExecutionAuditRepositoryUnavailableError: (503, MSG_REPOSITORY_UNAVAILABLE),
        UsageRepositoryDataError: (500, MSG_USAGE_QUERY_FAILED),
        UsageRepositoryUnavailableError: (503, MSG_REPOSITORY_UNAVAILABLE),
        AuditQueryRepositoryDataError: (500, MSG_UNIFIED_AUDIT_QUERY_FAILED),
        AuditQueryRepositoryUnavailableError: (503, MSG_REPOSITORY_UNAVAILABLE),
        ToolApprovalRepositoryDataError: (500, MSG_APPROVAL_DISPOSITION_REJECTED),
        ToolApprovalRepositoryUnavailableError: (503, MSG_REPOSITORY_UNAVAILABLE),
        ChannelBindingNotFoundError: (404, "Channel binding not found."),
        ChannelBindingTargetVersionNotFoundError: (404, "Channel binding version not found."),
        ChannelBindingAlreadyExistsError: (409, "Channel account is already bound."),
        ChannelBindingVersionConflictError: (409, "Channel binding version conflict."),
        ChannelBindingRepositoryDataError: (500, "Channel binding operation failed."),
        ChannelBindingRepositoryUnavailableError: (503, "Channel binding repository is unavailable."),
        TenantRolloutNotFoundError: (404, "Tenant rollout not found."),
    }
    for exc_type, (status, message) in handlers.items():

        def _make_handler(status: int, message: str):

            def _handler(_request: Request, _exc: Exception) -> JSONResponse:
                return JSONResponse(status_code=status, content={"detail": message})

            return _handler

        application.add_exception_handler(exc_type, _make_handler(status, message))

    def _validation_handler(_request: Request, _exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": MSG_VALIDATION_FAILED})

    application.add_exception_handler(RequestValidationError, _validation_handler)

    async def _catch_all_handler(_request: Request, exc: Exception) -> JSONResponse:
        import logging
        logger = logging.getLogger(__name__)
        logger.error("Unhandled exception in Admin API: %s", type(exc).__name__)
        return JSONResponse(status_code=500, content={"detail": MSG_INTERNAL_ERROR})

    application.add_exception_handler(Exception, _catch_all_handler)


def register_admin_routes(
    application: FastAPI,
    repository: TenantConfigAdminRepository,
    token: AdminToken,
    message_repository: MessageReceiptRepository | None = None,
    execution_repository: ExecutionAuditRepository | None = None,
    usage_repository: UsageRepository | None = None,
    approval_repository: ToolApprovalRepository | None = None,
    channel_binding_repository: ChannelBindingRepository | None = None,
    audit_query_repository: AuditQueryRepository | None = None,
    rollout_repository=None,
) -> None:
    """Register the /admin/v1 tenant management routes."""

    async def _require_token(request: Request) -> None:
        supplied = request.headers.get(_TOKEN_HEADER)
        if not token.matches(supplied):
            raise HTTPException(status_code=401, detail=MSG_UNAUTHORIZED)

    auth = [Depends(_require_token)]

    if rollout_repository is not None:

        @application.post("/admin/v1/tenants/{tenant_id}/rollout",
                          response_model=TenantRolloutResponse,
                          status_code=201,
                          dependencies=auth)
        async def begin_rollout(tenant_id: str, body: TenantRolloutBeginRequest) -> TenantRolloutResponse:
            validate_tenant_id(tenant_id)
            rollout = await rollout_repository.begin(tenant_id, body.expected_active_version, body.desired,
                                                     body.candidate_percent)
            return TenantRolloutResponse.from_rollout(rollout)

        @application.get("/admin/v1/tenants/{tenant_id}/rollout",
                         response_model=TenantRolloutResponse,
                         dependencies=auth)
        async def rollout_status(tenant_id: str) -> TenantRolloutResponse:
            validate_tenant_id(tenant_id)
            rollout = await rollout_repository.get_running(tenant_id)
            if rollout is None:
                raise TenantRolloutNotFoundError("rollout not found")
            return TenantRolloutResponse.from_rollout(rollout, await rollout_repository.status_counts(tenant_id))

        @application.post("/admin/v1/tenants/{tenant_id}/rollout/promote",
                          response_model=TenantRolloutResponse,
                          dependencies=auth)
        async def promote_rollout(tenant_id: str, body: TenantRolloutMutationRequest) -> TenantRolloutResponse:
            validate_tenant_id(tenant_id)
            rollout = await rollout_repository.promote(tenant_id, body.expected_candidate_version)
            return TenantRolloutResponse.from_rollout(rollout)

        @application.post("/admin/v1/tenants/{tenant_id}/rollout/abort",
                          response_model=TenantConfigResponse,
                          dependencies=auth)
        async def abort_rollout(tenant_id: str, body: TenantRolloutMutationRequest) -> TenantConfigResponse:
            validate_tenant_id(tenant_id)
            return TenantConfigResponse.from_config(await rollout_repository.abort(tenant_id,
                                                                                   body.expected_candidate_version))

    if audit_query_repository is not None:

        @application.get(
            "/admin/v1/tenants/{tenant_id}/audit",
            response_model=UnifiedAuditListResponse,
            dependencies=auth,
        )
        async def list_unified_audit(
            tenant_id: str,
            request_id: UUID | None = Query(default=None),
            trace_id: str | None = Query(default=None),
            before: str | None = Query(default=None),
            limit: int = Query(default=50, ge=1, le=100)
        ) -> UnifiedAuditListResponse:
            from datetime import datetime as _datetime
            validate_tenant_id(tenant_id)
            parsed_before = None
            if before is not None:
                try:
                    parsed_before = _datetime.fromisoformat(before.replace("Z", "+00:00"))
                    if parsed_before.tzinfo is None:
                        raise ValueError
                except ValueError:
                    raise HTTPException(status_code=422, detail=MSG_VALIDATION_FAILED) from None
            if trace_id is not None and (len(trace_id) != 32 or any(c not in "0123456789abcdef" for c in trace_id)):
                raise HTTPException(status_code=422, detail=MSG_VALIDATION_FAILED)
            rows = await audit_query_repository.list_for_tenant(tenant_id,
                                                                request_id=request_id,
                                                                trace_id=trace_id,
                                                                before=parsed_before,
                                                                limit=limit)
            return UnifiedAuditListResponse(events=[UnifiedAuditRecordResponse.from_record(row) for row in rows])

    @application.post(
        "/admin/v1/tenants",
        status_code=201,
        response_model=TenantConfigResponse,
        dependencies=auth,
    )
    async def create_tenant(body: TenantCreateRequest) -> TenantConfigResponse:
        created = await repository.create(
            TenantConfig(
                tenant_id=body.tenant_id,
                enabled=body.enabled,
                version=1,
                app=body.app,
                governance=body.governance,
                backend_profile=body.backend_profile,
                audit_policy=body.audit_policy,
            ))
        return TenantConfigResponse.from_config(created)

    @application.get(
        "/admin/v1/tenants/{tenant_id}",
        response_model=TenantConfigResponse,
        dependencies=auth,
    )
    async def get_tenant(tenant_id: str) -> TenantConfigResponse:
        validate_tenant_id(tenant_id)
        current = await repository.get(tenant_id)
        if current is None:
            raise TenantNotFoundError("tenant not found")
        return TenantConfigResponse.from_config(current)

    @application.get(
        "/admin/v1/tenants/{tenant_id}/versions",
        response_model=TenantVersionsResponse,
        dependencies=auth,
    )
    async def list_tenant_versions(
            tenant_id: str,
            before_version: int | None = Query(default=None, ge=1),
            limit: int = Query(default=50, ge=1, le=100),
    ) -> TenantVersionsResponse:
        validate_tenant_id(tenant_id)
        current = await repository.get(tenant_id)
        if current is None:
            raise TenantNotFoundError("tenant not found")
        history = await repository.list_versions(tenant_id, before_version=before_version, limit=limit)
        return TenantVersionsResponse(versions=[TenantConfigResponse.from_config(c) for c in history])

    @application.put(
        "/admin/v1/tenants/{tenant_id}",
        response_model=TenantConfigResponse,
        dependencies=auth,
    )
    async def update_tenant(
        tenant_id: str,
        body: TenantUpdateRequest,
    ) -> TenantConfigResponse:
        validate_tenant_id(tenant_id)
        updated = await repository.update(
            tenant_id,
            body.expected_version,
            TenantConfigDraft(
                enabled=body.desired.enabled,
                app=body.desired.app,
                governance=body.desired.governance,
                backend_profile=body.desired.backend_profile,
                audit_policy=body.desired.audit_policy,
            ),
        )
        return TenantConfigResponse.from_config(updated)

    @application.post(
        "/admin/v1/tenants/{tenant_id}/rollback",
        response_model=TenantConfigResponse,
        dependencies=auth,
    )
    async def rollback_tenant(
        tenant_id: str,
        body: TenantRollbackRequest,
    ) -> TenantConfigResponse:
        validate_tenant_id(tenant_id)
        rolled = await repository.rollback(tenant_id, body.expected_version, body.target_version)
        return TenantConfigResponse.from_config(rolled)

    if channel_binding_repository is not None:

        @application.post("/admin/v1/tenants/{tenant_id}/channel-bindings",
                          response_model=ChannelBinding,
                          status_code=201,
                          dependencies=auth)
        async def create_channel_binding(tenant_id: str, body: ChannelBindingCreateRequest) -> ChannelBinding:
            validate_tenant_id(tenant_id)
            if body.binding.tenant_id != tenant_id:
                raise ChannelBindingNotFoundError("channel binding tenant mismatch")
            return await channel_binding_repository.create(body.binding)

        @application.get("/admin/v1/tenants/{tenant_id}/channel-bindings",
                         response_model=ChannelBindingListResponse,
                         dependencies=auth)
        async def list_channel_bindings(
                tenant_id: str,
                limit: int = Query(default=50, ge=1, le=100),
        ) -> ChannelBindingListResponse:
            validate_tenant_id(tenant_id)
            return ChannelBindingListResponse(
                bindings=list(await channel_binding_repository.list_for_tenant(tenant_id, limit=limit)))

        @application.get("/admin/v1/tenants/{tenant_id}/channel-bindings/{binding_id}",
                         response_model=ChannelBinding,
                         dependencies=auth)
        async def get_channel_binding(tenant_id: str, binding_id: UUID) -> ChannelBinding:
            validate_tenant_id(tenant_id)
            binding = await channel_binding_repository.get(tenant_id, binding_id)
            if binding is None:
                raise ChannelBindingNotFoundError("channel binding not found")
            return binding

        @application.put("/admin/v1/tenants/{tenant_id}/channel-bindings/{binding_id}",
                         response_model=ChannelBinding,
                         dependencies=auth)
        async def update_channel_binding(tenant_id: str, binding_id: UUID,
                                         body: ChannelBindingUpdateRequest) -> ChannelBinding:
            validate_tenant_id(tenant_id)
            return await channel_binding_repository.update(tenant_id, binding_id, body.expected_version, body.desired)

        @application.get("/admin/v1/tenants/{tenant_id}/channel-bindings/{binding_id}/versions",
                         response_model=ChannelBindingListResponse,
                         dependencies=auth)
        async def list_channel_binding_versions(
            tenant_id: str,
            binding_id: UUID,
            before_version: int | None = Query(default=None, ge=1),
            limit: int = Query(default=50, ge=1, le=100)
        ) -> ChannelBindingListResponse:
            validate_tenant_id(tenant_id)
            return ChannelBindingListResponse(bindings=list(await channel_binding_repository.list_versions(
                tenant_id, binding_id, before_version=before_version, limit=limit)))

        @application.post("/admin/v1/tenants/{tenant_id}/channel-bindings/{binding_id}/rollback",
                          response_model=ChannelBinding,
                          dependencies=auth)
        async def rollback_channel_binding(tenant_id: str, binding_id: UUID,
                                           body: ChannelBindingRollbackRequest) -> ChannelBinding:
            validate_tenant_id(tenant_id)
            return await channel_binding_repository.rollback(tenant_id, binding_id, body.expected_version,
                                                             body.target_version)

    if message_repository is not None:

        @application.get(
            "/admin/v1/tenants/{tenant_id}/message-audit",
            response_model=MessageAuditListResponse,
            dependencies=auth,
        )
        async def list_message_audit(
                tenant_id: str,
                message_id: str = Query(..., min_length=1, max_length=200),
                limit: int = Query(default=50, ge=1, le=100),
        ) -> MessageAuditListResponse:
            validate_tenant_id(tenant_id)
            message_id = message_id.strip()
            if not message_id:
                raise HTTPException(status_code=422, detail=MSG_INVALID_MESSAGE_ID)
            events = await message_repository.list_audit(tenant_id, message_id, limit)
            return MessageAuditListResponse(events=[MessageAuditEventResponse.from_audit_event(e) for e in events])

    if execution_repository is not None:

        @application.get(
            "/admin/v1/tenants/{tenant_id}/receipts/{receipt_id}/execution-audit",
            response_model=ExecutionAuditListResponse,
            dependencies=auth,
        )
        async def list_execution_audit(
                tenant_id: str,
                receipt_id: UUID,
                limit: int = Query(default=50, ge=1, le=100),
        ) -> ExecutionAuditListResponse:
            # Auth ran first (dependency); tenant scope and UUID/limit shape are
            # validated before any repository call, so a wrong tenant or a
            # malformed id can never even reach the data layer.
            validate_tenant_id(tenant_id)
            events = await execution_repository.list_for_receipt(tenant_id, receipt_id, limit)
            return ExecutionAuditListResponse(events=[ExecutionAuditEventResponse.from_event(e) for e in events])

        @application.get(
            "/admin/v1/tenants/{tenant_id}/requests/{request_id}/execution-audit",
            response_model=ExecutionAuditListResponse,
            dependencies=auth,
        )
        async def list_execution_audit_by_request(
                tenant_id: str,
                request_id: UUID,
                limit: int = Query(default=50, ge=1, le=100),
        ) -> ExecutionAuditListResponse:
            # Same boundary as the receipt route: auth first, then tenant scope
            # and UUID/limit shape validation before any repository call.
            # Events without a receipt (channel delivery results) are only
            # enumerable through this route.
            validate_tenant_id(tenant_id)
            events = await execution_repository.list_for_request(tenant_id, request_id, limit)
            return ExecutionAuditListResponse(events=[ExecutionAuditEventResponse.from_event(e) for e in events])

    if usage_repository is not None:

        @application.get(
            "/admin/v1/tenants/{tenant_id}/usage",
            response_model=TenantUsageResponse,
            dependencies=auth,
        )
        async def get_tenant_usage(
                tenant_id: str,
                day: str | None = Query(default=None),
        ) -> TenantUsageResponse:
            from datetime import date as _date, datetime as _dt, timezone as _tz

            # Auth ran first; tenant scope + strict UTC-day shape are
            # validated before any repository call.
            validate_tenant_id(tenant_id)
            if day is None:
                usage_day = _dt.now(_tz.utc).date()
            else:
                try:
                    usage_day = _date.fromisoformat(day)
                except ValueError:
                    raise HTTPException(status_code=422, detail=MSG_INVALID_USAGE_DAY) from None
            usage = await usage_repository.get_daily(tenant_id, usage_day)
            return TenantUsageResponse(
                usage_date=usage.usage_date,
                tenant_id=usage.tenant_id,
                profiles=[
                    UsageProfileResponse(
                        model_profile=p.model_profile,
                        requests=p.requests,
                        input_tokens=p.input_tokens,
                        output_tokens=p.output_tokens,
                        cost_microunits=p.cost_microunits,
                        cost_state=p.cost_state,
                    ) for p in usage.profiles
                ],
            )

    if approval_repository is not None:
        # Stage 6D orphan disposition: an 'executing' approval left behind by
        # a crashed controlled execution can ONLY be queried and terminated
        # to 'failed' here.  Nothing on this surface executes tools; the
        # repository enforces atomicity, the stale threshold and identity.

        @application.get(
            "/admin/v1/tenants/{tenant_id}/approvals/orphaned",
            response_model=OrphanedApprovalListResponse,
            dependencies=auth,
        )
        async def list_orphaned_approvals(
                tenant_id: str,
                stale_seconds: int = Query(default=900, ge=1, le=31_536_000),
                limit: int = Query(default=50, ge=1, le=100),
        ) -> OrphanedApprovalListResponse:
            validate_tenant_id(tenant_id)
            orphans = await approval_repository.list_stale_executing(
                tenant_id=tenant_id,
                stale_seconds=stale_seconds,
                limit=limit,
            )
            return OrphanedApprovalListResponse(approvals=[OrphanedApprovalResponse.from_orphan(o) for o in orphans], )

        @application.post(
            "/admin/v1/tenants/{tenant_id}/approvals/{approval_id}/terminate",
            response_model=ApprovalTerminationResponse,
            dependencies=auth,
        )
        async def terminate_orphaned_approval(
                tenant_id: str,
                approval_id: UUID,
                stale_seconds: int = Query(default=900, ge=1, le=31_536_000),
        ) -> ApprovalTerminationResponse:
            validate_tenant_id(tenant_id)
            result = await approval_repository.terminate_orphan(
                approval_id,
                tenant_id=tenant_id,
                stale_seconds=stale_seconds,
            )
            if result.action in (OrphanTerminationAction.TERMINATED, OrphanTerminationAction.ALREADY_TERMINATED):
                return ApprovalTerminationResponse(
                    approval_id=result.approval_id,
                    disposition=result.action.value,
                    state=result.state or "failed",
                )
            if result.action == OrphanTerminationAction.NOT_AVAILABLE:
                raise HTTPException(status_code=404, detail=MSG_APPROVAL_NOT_FOUND)
            raise HTTPException(status_code=409, detail=MSG_APPROVAL_DISPOSITION_REJECTED)


__all__ = ["register_admin_routes"]
