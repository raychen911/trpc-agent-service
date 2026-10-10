"""Admin API with OIDC RBAC, idempotency, and ETag concurrency."""

import hashlib
import json
import re
from typing import Annotated, Any
from urllib.parse import urlparse
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, model_validator

from trpc_service.config.secrets import SecretRef
from trpc_service.tenant.auth import Authorizer, Principal, Role, require_role
from trpc_service.tenant.control import PostgresControlPlaneRepository
from trpc_service.tenant.models import Channel, ChannelBinding

bearer = HTTPBearer(auto_error=False)


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CreateTenantRequest(RequestModel):
    tenant_id: str = Field(default_factory=lambda: str(uuid4()), min_length=8, max_length=128)
    display_name: str = Field(min_length=1, max_length=256)


class BindingRequest(RequestModel):
    app_id: str = Field(min_length=1, max_length=128)
    channel: Channel
    account_id: str | None = Field(default=None, min_length=1, max_length=256)
    secret_refs: dict[str, SecretRef] = Field(default_factory=dict)
    capabilities: frozenset[str] = frozenset()
    enabled: bool = True

    @model_validator(mode="after")
    def _validate_account_id(self) -> "BindingRequest":
        if not self.account_id:
            raise ValueError("account_id is required")
        if self.channel == Channel.FEISHU and self.enabled:
            required = {"app_secret", "verification_token", "encrypt_key"}
            if missing := sorted(required.difference(self.secret_refs)):
                raise ValueError(f"Feishu secret references are required: {', '.join(missing)}")
        for ref in self.secret_refs.values():
            if ref.uri.startswith("literal://"):
                raise ValueError("tenant secret references must not be literal")
            parsed = urlparse(ref.uri)
            if parsed.scheme == "env":
                name = parsed.netloc or parsed.path.lstrip("/")
                if not _tenant_env_name(name):
                    raise ValueError("tenant environment secret is not registered")
            elif parsed.scheme == "file":
                path = parsed.path.replace("\\", "/")
                if not (path.startswith("/run/secrets/") or path.startswith("/etc/trpc/secrets/")):
                    raise ValueError("tenant file secret is outside the secret root")
            else:
                raise ValueError("tenant secret reference scheme is invalid")
        return self


class ConfigRevisionRequest(RequestModel):
    app_id: str = Field(min_length=1, max_length=128)
    config: dict[str, Any]

    @model_validator(mode="after")
    def _validate_config_shape(self) -> "ConfigRevisionRequest":
        _validate_json_shape(self.config)
        return self


class RolloutRequest(RequestModel):
    app_id: str = Field(min_length=1, max_length=128)
    percentage: float = Field(default=100, ge=0, le=100)


class RollbackRequest(RequestModel):
    app_id: str = Field(min_length=1, max_length=128)


class ReplayRequest(RequestModel):
    confirm_ambiguous: bool = False


class IMAcceptanceEventRequest(RequestModel):
    channel: Channel
    run_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]*$",
    )
    run_nonce: str = Field(min_length=16, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    provider_event_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class IMAcceptanceRunRequest(RequestModel):
    channel: Channel
    run_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]*$",
    )
    run_nonce: str = Field(min_length=16, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    expires_in_seconds: int = Field(default=300, ge=30, le=900)


def create_admin_router(
    repository: PostgresControlPlaneRepository, authorizer: Authorizer
) -> APIRouter:
    router = APIRouter(prefix="/v1", tags=["admin"])

    async def principal(request: Request) -> Principal:
        credentials: HTTPAuthorizationCredentials | None = await bearer(request)
        if credentials is None or credentials.scheme.lower() != "bearer":
            from trpc_service.tenant.auth import AuthenticationError

            raise AuthenticationError("bearer token is required")
        return await authorizer.authenticate(credentials.credentials)

    @router.post("/tenants", status_code=status.HTTP_201_CREATED)
    async def create_tenant(
        body: CreateTenantRequest,
        response: Response,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8)],
        actor: Annotated[Principal, Depends(principal)],
    ) -> dict[str, Any]:
        require_role(actor, Role.PLATFORM_ADMIN)
        result = await repository.create_tenant(
            tenant_id=body.tenant_id,
            display_name=body.display_name,
            actor=actor.subject,
            idempotency_key=idempotency_key,
            request_hash=_request_hash("create_tenant", body),
        )
        response.headers["ETag"] = _etag(int(result["control_version"]))
        return result

    @router.get("/tenants/{tenant_id}")
    async def get_tenant(
        tenant_id: str,
        response: Response,
        actor: Annotated[Principal, Depends(principal)],
    ) -> dict[str, Any]:
        require_role(actor, Role.TENANT_ADMIN, tenant_id=tenant_id)
        result = await repository.get_tenant(tenant_id)
        if result is None:
            raise HTTPException(status_code=404, detail="tenant not found")
        response.headers["ETag"] = _etag(int(result["control_version"]))
        return result

    @router.put("/tenants/{tenant_id}/channel-bindings/{binding_id}")
    async def put_binding(
        tenant_id: str,
        binding_id: str,
        body: BindingRequest,
        response: Response,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8)],
        if_match: Annotated[str, Header(alias="If-Match")],
        actor: Annotated[Principal, Depends(principal)],
    ) -> dict[str, Any]:
        require_role(actor, Role.TENANT_ADMIN, tenant_id=tenant_id)
        expected_version = _parse_etag(if_match)
        if (
            body.channel == Channel.WECOM_AI_BOT
            and body.enabled
            and "bot_secret" not in body.secret_refs
        ):
            raise HTTPException(status_code=422, detail="binding secret references are invalid")
        binding = ChannelBinding(
            binding_id=binding_id,
            tenant_id=tenant_id,
            app_id=body.app_id,
            channel=body.channel,
            account_id=body.account_id or "",
            secret_refs=body.secret_refs,
            capabilities=body.capabilities,
            enabled=body.enabled,
        )
        result = await repository.put_binding(
            tenant_id=tenant_id,
            binding_id=binding_id,
            binding=binding,
            actor=actor.subject,
            expected_version=expected_version,
            idempotency_key=idempotency_key,
            request_hash=_request_hash("put_binding", body),
        )
        response.headers["ETag"] = _etag(int(result["tenant_control_version"]))
        return result

    @router.post("/tenants/{tenant_id}/config-revisions", status_code=201)
    async def create_config(
        tenant_id: str,
        body: ConfigRevisionRequest,
        response: Response,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8)],
        if_match: Annotated[str, Header(alias="If-Match")],
        actor: Annotated[Principal, Depends(principal)],
    ) -> dict[str, Any]:
        require_role(actor, Role.TENANT_ADMIN, tenant_id=tenant_id)
        result = await repository.create_config_revision(
            tenant_id=tenant_id,
            app_id=body.app_id,
            config=body.config,
            actor=actor.subject,
            expected_version=_parse_etag(if_match),
            idempotency_key=idempotency_key,
            request_hash=_request_hash("create_config", body),
        )
        response.headers["ETag"] = _etag(int(result["tenant_control_version"]))
        return result

    @router.post("/tenants/{tenant_id}/config-revisions/{version}:activate")
    async def activate_config(
        tenant_id: str,
        version: int,
        body: RolloutRequest,
        response: Response,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8)],
        if_match: Annotated[str, Header(alias="If-Match")],
        actor: Annotated[Principal, Depends(principal)],
    ) -> dict[str, Any]:
        require_role(actor, Role.TENANT_ADMIN, tenant_id=tenant_id)
        result = await repository.activate_config(
            tenant_id=tenant_id,
            app_id=body.app_id,
            version=version,
            percentage=body.percentage,
            actor=actor.subject,
            expected_version=_parse_etag(if_match),
            idempotency_key=idempotency_key,
            request_hash=_request_hash("activate_config", body),
        )
        response.headers["ETag"] = _etag(int(result["tenant_control_version"]))
        return result

    @router.post("/tenants/{tenant_id}/config-revisions/{version}:rollback")
    async def rollback_config(
        tenant_id: str,
        version: int,
        body: RollbackRequest,
        response: Response,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8)],
        if_match: Annotated[str, Header(alias="If-Match")],
        actor: Annotated[Principal, Depends(principal)],
    ) -> dict[str, Any]:
        require_role(actor, Role.TENANT_ADMIN, tenant_id=tenant_id)
        result = await repository.activate_config(
            tenant_id=tenant_id,
            app_id=body.app_id,
            version=version,
            percentage=100,
            actor=actor.subject,
            expected_version=_parse_etag(if_match),
            idempotency_key=idempotency_key,
            request_hash=_request_hash("rollback_config", body),
            operation="rollback_config",
        )
        response.headers["ETag"] = _etag(int(result["tenant_control_version"]))
        return result

    @router.get("/tenants/{tenant_id}/audit")
    async def audit(
        tenant_id: str,
        actor: Annotated[Principal, Depends(principal)],
        cursor: str | None = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
    ) -> dict[str, Any]:
        require_role(actor, Role.AUDITOR, tenant_id=tenant_id)
        return await repository.audit_page(tenant_id, cursor=cursor, limit=limit)

    @router.get("/tenants/{tenant_id}/dead-letters")
    async def dead_letters(
        tenant_id: str,
        actor: Annotated[Principal, Depends(principal)],
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
    ) -> list[dict[str, Any]]:
        require_role(actor, Role.AUDITOR, tenant_id=tenant_id)
        return await repository.dead_letters(tenant_id, limit=limit)

    @router.get("/tenants/{tenant_id}/bindings/{binding_id}/im-acceptance/wecom")
    async def wecom_acceptance_snapshot(
        tenant_id: str,
        binding_id: str,
        response: Response,
        actor: Annotated[Principal, Depends(principal)],
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
    ) -> dict[str, Any]:
        role = Role.AUDITOR if Role.AUDITOR in actor.roles else Role.TENANT_ADMIN
        require_role(actor, role, tenant_id=tenant_id)
        result = await repository.wecom_acceptance_snapshot(tenant_id, binding_id, limit=limit)
        if result is None:
            raise HTTPException(
                status_code=404,
                detail="resource not found",
                headers={"Cache-Control": "no-store"},
            )
        response.headers["Cache-Control"] = "no-store"
        return result

    @router.get("/tenants/{tenant_id}/bindings/{binding_id}/im-acceptance/evidence")
    async def im_acceptance_evidence(
        tenant_id: str,
        binding_id: str,
        response: Response,
        actor: Annotated[Principal, Depends(principal)],
        run_id: Annotated[
            str,
            Query(
                min_length=1,
                max_length=128,
                pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]*$",
            ),
        ],
        outbound_id: Annotated[UUID, Query()],
    ) -> dict[str, Any]:
        role = Role.AUDITOR if Role.AUDITOR in actor.roles else Role.TENANT_ADMIN
        require_role(actor, role, tenant_id=tenant_id)
        result = await repository.im_acceptance_outbound_evidence(
            tenant_id,
            binding_id,
            run_id=run_id,
            outbound_id=outbound_id,
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail="resource not found",
                headers={"Cache-Control": "no-store"},
            )
        response.headers["Cache-Control"] = "no-store"
        return result

    @router.post(
        "/tenants/{tenant_id}/bindings/{binding_id}/im-acceptance/runs",
        status_code=status.HTTP_201_CREATED,
    )
    async def register_im_acceptance_run(
        tenant_id: str,
        binding_id: str,
        body: IMAcceptanceRunRequest,
        response: Response,
        actor: Annotated[Principal, Depends(principal)],
    ) -> dict[str, Any]:
        require_role(actor, Role.TENANT_ADMIN, tenant_id=tenant_id)
        result = await repository.register_im_acceptance_run(
            tenant_id,
            binding_id,
            channel=body.channel,
            run_id=body.run_id,
            run_nonce=body.run_nonce,
            expires_in_seconds=body.expires_in_seconds,
        )
        if result is None:
            raise HTTPException(
                status_code=409,
                detail="acceptance run is unavailable",
                headers={"Cache-Control": "no-store"},
            )
        response.headers["Cache-Control"] = "no-store"
        return result

    @router.post("/tenants/{tenant_id}/bindings/{binding_id}/im-acceptance/event-evidence")
    async def im_acceptance_event_evidence(
        tenant_id: str,
        binding_id: str,
        body: IMAcceptanceEventRequest,
        response: Response,
        actor: Annotated[Principal, Depends(principal)],
    ) -> dict[str, Any]:
        require_role(actor, Role.TENANT_ADMIN, tenant_id=tenant_id)
        result = await repository.im_acceptance_event_evidence(
            tenant_id,
            binding_id,
            channel=body.channel,
            run_id=body.run_id,
            run_nonce=body.run_nonce,
            provider_event_hash=body.provider_event_hash,
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail="resource not found",
                headers={"Cache-Control": "no-store"},
            )
        response.headers["Cache-Control"] = "no-store"
        return result

    @router.post("/tenants/{tenant_id}/outbound/{outbound_id}:replay", status_code=202)
    async def replay_outbound(
        tenant_id: str,
        outbound_id: str,
        body: ReplayRequest,
        response: Response,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8)],
        if_match: Annotated[str, Header(alias="If-Match")],
        actor: Annotated[Principal, Depends(principal)],
    ) -> dict[str, Any]:
        require_role(actor, Role.TENANT_ADMIN, tenant_id=tenant_id)
        result = await repository.replay_outbound(
            tenant_id=tenant_id,
            outbound_id=outbound_id,
            confirm_ambiguous=body.confirm_ambiguous,
            actor=actor.subject,
            expected_version=_parse_etag(if_match),
            idempotency_key=idempotency_key,
            request_hash=_request_hash("replay_outbound", body),
        )
        response.headers["ETag"] = _etag(int(result["tenant_control_version"]))
        return result

    return router


def _request_hash(operation: str, body: BaseModel) -> str:
    value = json.dumps(
        {"operation": operation, "body": body.model_dump(mode="json")},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(value.encode()).hexdigest()


def _etag(version: int) -> str:
    return f'"{version}"'


def _parse_etag(value: str) -> int:
    normalized = value.removeprefix("W/").strip().strip('"')
    try:
        parsed = int(normalized)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid If-Match header") from exc
    if parsed < 1:
        raise HTTPException(status_code=400, detail="invalid If-Match header")
    return parsed


def _validate_json_shape(value: object, *, depth: int = 0, fields: list[int] | None = None) -> None:
    """Bound admin JSON before it is persisted or fed into Pydantic models."""

    if fields is None:
        fields = [0]
    if depth > 12:
        raise ValueError("configuration nesting is too deep")
    if isinstance(value, dict):
        fields[0] += len(value)
        if fields[0] > 256:
            raise ValueError("configuration has too many fields")
        for key, item in value.items():
            if not isinstance(key, str) or len(key) > 128:
                raise ValueError("configuration field name is invalid")
            _validate_json_shape(item, depth=depth + 1, fields=fields)
    elif isinstance(value, list):
        if len(value) > 256:
            raise ValueError("configuration list is too large")
        for item in value:
            _validate_json_shape(item, depth=depth + 1, fields=fields)
    elif isinstance(value, str) and len(value.encode("utf-8")) > 16 * 1024:
        raise ValueError("configuration string is too large")


def _tenant_env_name(value: str) -> bool:
    return bool(
        re.fullmatch(
            r"(?:TRPC_TENANT_|TRPC_FEISHU_|TRPC_WECOM_|FEISHU_|WECOM_)[A-Z0-9_]{1,120}",
            value,
        )
    )


__all__ = ["create_admin_router"]
