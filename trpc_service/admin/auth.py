"""Bearer/browser authentication and role checks for the management plane."""

import hashlib
import hmac
from base64 import b64decode, b64encode
from binascii import Error as Base64Error
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from typing import Any
from uuid import UUID

from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from cryptography.exceptions import InvalidKey
from cryptography.hazmat.primitives.kdf.argon2 import Argon2id
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.models import (
    ManagementAuditLog,
    ManagementCredential,
    ManagementPrincipal,
    ManagementWebSession,
    RoleAssignment,
)
from trpc_service.admin.schemas import ManagementRole
from trpc_service.storage.database import get_session
from trpc_service.tenant.models import Tenant

_BEARER = HTTPBearer(auto_error=False)
WEB_SESSION_COOKIE = "trpc_management_session"
WEB_CSRF_COOKIE = "trpc_management_csrf"
_PASSWORD_MEMORY_KIB = 64 * 1024
_PASSWORD_ITERATIONS = 3
_PASSWORD_LANES = 4


@dataclass(frozen=True, slots=True)
class ManagementActor:
    """Authenticated subject plus its platform and tenant role assignments."""

    subject: str
    roles: frozenset[str]
    tenant_roles: dict[UUID, frozenset[str]]
    bootstrap: bool = False

    @property
    def is_platform_admin(self) -> bool:
        return ManagementRole.PLATFORM_ADMIN.value in self.roles


def hash_management_token(token: str) -> str:
    """Digest a random bearer token before persistence or lookup."""

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def hash_management_password(password: str) -> str:
    """Hash a management password with an independently salted Argon2id KDF."""

    salt = os.urandom(16)
    digest = Argon2id(
        salt=salt,
        length=32,
        iterations=_PASSWORD_ITERATIONS,
        lanes=_PASSWORD_LANES,
        memory_cost=_PASSWORD_MEMORY_KIB,
    ).derive(password.encode("utf-8"))
    return "$".join((
        "argon2id",
        str(_PASSWORD_MEMORY_KIB),
        str(_PASSWORD_ITERATIONS),
        str(_PASSWORD_LANES),
        b64encode(salt).decode("ascii"),
        b64encode(digest).decode("ascii"),
    ))


def verify_management_password(password: str, encoded: str) -> bool:
    """Verify an encoded password without exposing format errors to the caller."""

    try:
        algorithm, memory, iterations, lanes, salt, digest = encoded.split("$")
        if algorithm != "argon2id":
            return False
        verifier = Argon2id(
            salt=b64decode(salt, validate=True),
            length=32,
            iterations=int(iterations),
            lanes=int(lanes),
            memory_cost=int(memory),
        )
        verifier.verify(password.encode("utf-8"), b64decode(digest, validate=True))
        return True
    except (Base64Error, InvalidKey, ValueError):
        return False


def _auth_error(detail: str = "management authentication required") -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


async def actor_for_principal(
    session: AsyncSession,
    principal: ManagementPrincipal,
) -> ManagementActor:
    """Build the authorization snapshot shared by bearer and browser logins."""

    assignments = (await session.scalars(
        select(RoleAssignment).where(
            RoleAssignment.management_principal_id == principal.management_principal_id, ))).all()
    platform_roles = frozenset(assignment.role for assignment in assignments
                               if assignment.tenant_id is None)
    tenant_roles: dict[UUID, set[str]] = {}
    for assignment in assignments:
        if assignment.tenant_id is not None:
            tenant_roles.setdefault(assignment.tenant_id, set()).add(assignment.role)
    return ManagementActor(
        subject=str(principal.management_principal_id),
        roles=platform_roles,
        tenant_roles={
            key: frozenset(value)
            for key, value in tenant_roles.items()
        },
    )


async def get_management_actor(
        request: Request,
        credentials: HTTPAuthorizationCredentials | None = Depends(_BEARER),
        session: AsyncSession = Depends(get_session),
) -> ManagementActor:
    """Authenticate a bootstrap token, API credential, or browser session."""

    principal_id: UUID | None = None
    if credentials is not None and credentials.scheme.casefold() == "bearer":
        token = credentials.credentials
        bootstrap_token = request.app.state.settings.resolved_admin_bootstrap_token
        if bootstrap_token and hmac.compare_digest(token, bootstrap_token):
            return ManagementActor(
                subject="bootstrap:platform-admin",
                roles=frozenset({ManagementRole.PLATFORM_ADMIN.value}),
                tenant_roles={},
                bootstrap=True,
            )
        credential = await session.scalar(
            select(ManagementCredential).where(
                ManagementCredential.token_hash == hash_management_token(token),
                ManagementCredential.status == "active",
            ))
        if credential is None:
            raise _auth_error("invalid or revoked management credential")
        if credential.expires_at is not None:
            expires_at = credential.expires_at
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at <= datetime.now(timezone.utc):
                raise _auth_error("management credential expired")
        principal_id = credential.management_principal_id
    else:
        session_token = request.cookies.get(WEB_SESSION_COOKIE, "")
        web_session = await session.scalar(
            select(ManagementWebSession).where(
                ManagementWebSession.token_hash == hash_management_token(session_token),
                ManagementWebSession.revoked_at.is_(None),
            )) if session_token else None
        now = datetime.now(timezone.utc)
        if web_session is None:
            raise _auth_error()
        expires_at = web_session.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= now:
            raise _auth_error("management session expired")
        if request.method not in {"GET", "HEAD", "OPTIONS"}:
            csrf_cookie = request.cookies.get(WEB_CSRF_COOKIE, "")
            csrf_header = request.headers.get("X-CSRF-Token", "")
            supplied_hash = hash_management_token(csrf_header)
            if (not csrf_cookie or not csrf_header
                    or not hmac.compare_digest(csrf_cookie, csrf_header)
                    or not hmac.compare_digest(web_session.csrf_hash, supplied_hash)):
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                                    detail="invalid management CSRF token")
        principal_id = web_session.management_principal_id

    principal = await session.get(ManagementPrincipal, principal_id)
    if principal is None or principal.status != "active":
        raise _auth_error("management principal is disabled")
    return await actor_for_principal(session, principal)


async def require_platform_admin(
        request: Request,
        actor: ManagementActor = Depends(get_management_actor),
        session: AsyncSession = Depends(get_session),
) -> ManagementActor:
    """Require the platform-wide administrator role."""

    if not actor.is_platform_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="platform administrator role required")
    if request.method == "GET":
        # Management reads are security-relevant too: record them centrally so
        # new list/query routes cannot accidentally omit the audit obligation.
        session.add(
            ManagementAuditLog(
                actor_subject=actor.subject,
                actor_roles=sorted(actor.roles),
                action="platform_management.read",
                resource_type="management_api",
                resource_id=request.url.path,
                details_redacted={"method": request.method},
            ))
        await session.commit()
    return actor


async def require_platform_tenant_access(
        tenant_id: UUID,
        request: Request,
        actor: ManagementActor = Depends(get_management_actor),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> ManagementActor:
    """Allow only a platform administrator to change tenant-owned runtime policy."""

    if not actor.is_platform_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="platform administrator role required")
    if support_reason is None or len(support_reason.strip()) < 12:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="platform support access requires X-Support-Reason",
        )
    session.add(
        ManagementAuditLog(
            tenant_id=tenant_id,
            actor_subject=actor.subject,
            actor_roles=sorted(actor.roles),
            action="platform_support.access",
            resource_type="tenant_scope",
            resource_id=str(tenant_id),
            reason=support_reason.strip(),
            details_redacted={
                "method": request.method,
                "path": request.url.path
            },
        ))
    await session.commit()
    return actor


def _tenant_role_dependency(
    allowed_roles: frozenset[str], ) -> Callable[..., Coroutine[Any, Any, ManagementActor]]:
    """Build one tenant-scoped FastAPI dependency for the allowed role set."""

    async def require_role(
            tenant_id: UUID,
            request: Request,
            actor: ManagementActor = Depends(get_management_actor),
            support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
            session: AsyncSession = Depends(get_session),
    ) -> ManagementActor:
        if actor.is_platform_admin:
            if support_reason is None or len(support_reason.strip()) < 12:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="platform support access requires X-Support-Reason",
                )
            # Support-mode reads and writes are audited before the route runs, so
            # a later route failure cannot erase evidence of tenant access.
            session.add(
                ManagementAuditLog(
                    tenant_id=tenant_id,
                    actor_subject=actor.subject,
                    actor_roles=sorted(actor.roles),
                    action="platform_support.access",
                    resource_type="tenant_scope",
                    resource_id=str(tenant_id),
                    reason=support_reason.strip(),
                    details_redacted={
                        "method": request.method,
                        "path": request.url.path,
                    },
                ))
            await session.commit()
            return actor
        tenant = await session.get(Tenant, tenant_id)
        if tenant is None or tenant.status != "active":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                                detail="tenant is not active")
        if not (actor.tenant_roles.get(tenant_id, frozenset()) & allowed_roles):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                                detail="tenant role does not permit this operation")
        if request.method == "GET":
            session.add(
                ManagementAuditLog(
                    tenant_id=tenant_id,
                    actor_subject=actor.subject,
                    actor_roles=sorted(actor.tenant_roles.get(tenant_id, frozenset())),
                    action="tenant_management.read",
                    resource_type="tenant_management_api",
                    resource_id=request.url.path,
                    details_redacted={"method": request.method},
                ))
            await session.commit()
        return actor

    return require_role


require_tenant_admin = _tenant_role_dependency(frozenset({ManagementRole.TENANT_ADMIN.value}))
