"""Browser login and logout endpoints for the role-aware management console."""

from datetime import datetime, timedelta, timezone
import secrets

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.login_guard import password_work
from trpc_service.admin.audit import append_management_audit
from trpc_service.admin.auth import (
    ManagementActor,
    WEB_CSRF_COOKIE,
    WEB_SESSION_COOKIE,
    actor_for_principal,
    get_management_actor,
    hash_management_password,
    hash_management_token,
    verify_management_password,
)
from trpc_service.admin.models import (
    ManagementPasswordCredential,
    ManagementPrincipal,
    ManagementWebSession,
)
from trpc_service.admin.schemas import ManagementActorRead, PasswordLogin
from trpc_service.storage.database import get_session
from trpc_service.storage.orm import as_utc

router = APIRouter(prefix="/auth", tags=["management-auth"])

# Unknown users execute the same memory-hard KDF as real users, reducing the
# usefulness of login timing as an account-enumeration side channel.
_DUMMY_PASSWORD_HASH = hash_management_password("not-a-real-management-password")


def _actor_read(actor: ManagementActor) -> ManagementActorRead:
    return ManagementActorRead(
        subject=actor.subject,
        roles=sorted(actor.roles),
        tenant_roles={
            str(key): sorted(value)
            for key, value in actor.tenant_roles.items()
        },
        bootstrap=actor.bootstrap,
    )


@router.post("/login", response_model=ManagementActorRead)
async def login(
        payload: PasswordLogin,
        request: Request,
        response: Response,
        database: AsyncSession = Depends(get_session),
) -> ManagementActorRead:
    """Exchange a username/password for a revocable HttpOnly browser session."""

    query = select(ManagementPasswordCredential).where(
        ManagementPasswordCredential.username == payload.username)
    source = request.client.host if request.client is not None else "unknown"
    async with request.app.state.login_guard.admit(source):
        credential = await database.scalar(query)
        encoded = credential.password_hash if credential is not None else _DUMMY_PASSWORD_HASH
        # Release the connection and avoid holding an account lock during the KDF.
        await database.rollback()
        password_valid = await password_work(verify_management_password,
                                             payload.password.get_secret_value(), encoded)
        credential = await database.scalar(
            query.with_for_update().execution_options(populate_existing=True))
        # A reset concurrent with verification must invalidate this authentication.
        if credential is None or credential.password_hash != encoded:
            password_valid = False
    now = datetime.now(timezone.utc)
    locked = (credential is not None and credential.locked_until is not None
              and as_utc(credential.locked_until) > now)
    principal = (None if credential is None else await database.get(
        ManagementPrincipal, credential.management_principal_id))
    if (not password_valid or locked or principal is None or principal.status != "active"):
        if credential is not None and not locked:
            credential.failed_attempts += 1
            maximum_attempts = request.app.state.settings.management_login_max_attempts
            if credential.failed_attempts >= maximum_attempts:
                credential.locked_until = now + timedelta(
                    seconds=request.app.state.settings.management_login_lock_seconds)
                credential.failed_attempts = 0
            await database.commit()
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="invalid username or password")

    assert credential is not None
    credential.failed_attempts = 0
    credential.locked_until = None
    raw_session = secrets.token_urlsafe(48)
    raw_csrf = secrets.token_urlsafe(32)
    ttl = request.app.state.settings.management_session_ttl_seconds
    web_session = ManagementWebSession(
        management_principal_id=principal.management_principal_id,
        token_hash=hash_management_token(raw_session),
        csrf_hash=hash_management_token(raw_csrf),
        expires_at=now + timedelta(seconds=ttl),
        last_seen_at=now,
    )
    database.add(web_session)
    await database.flush()
    actor = await actor_for_principal(database, principal)
    append_management_audit(
        database,
        actor,
        action="management_session.login",
        resource_type="management_web_session",
        resource_id=str(web_session.web_session_id),
    )
    await database.commit()
    cookie_options = {
        "secure": request.app.state.settings.secure_cookies,
        "samesite": "strict",
        "path": "/",
        "max_age": ttl,
    }
    response.set_cookie(WEB_SESSION_COOKIE, raw_session, httponly=True, **cookie_options)
    response.set_cookie(WEB_CSRF_COOKIE, raw_csrf, httponly=False, **cookie_options)
    response.headers["Cache-Control"] = "no-store"
    return _actor_read(actor)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
        request: Request,
        response: Response,
        actor: ManagementActor = Depends(get_management_actor),
        database: AsyncSession = Depends(get_session),
) -> Response:
    """Revoke the current browser session and remove both browser cookies."""

    raw_session = request.cookies.get(WEB_SESSION_COOKIE, "")
    if raw_session:
        web_session = await database.scalar(
            select(ManagementWebSession).where(
                ManagementWebSession.token_hash == hash_management_token(raw_session),
                ManagementWebSession.revoked_at.is_(None),
            ).with_for_update())
        if web_session is not None:
            web_session.revoked_at = datetime.now(timezone.utc)
            append_management_audit(
                database,
                actor,
                action="management_session.logout",
                resource_type="management_web_session",
                resource_id=str(web_session.web_session_id),
            )
            await database.commit()
    response.delete_cookie(WEB_SESSION_COOKIE, path="/")
    response.delete_cookie(WEB_CSRF_COOKIE, path="/")
    response.status_code = status.HTTP_204_NO_CONTENT
    return response
