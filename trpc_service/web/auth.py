import asyncio
from dataclasses import dataclass
from typing import Annotated, Any

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

bearer_scheme = HTTPBearer(auto_error=False)


@dataclass(frozen=True, slots=True)
class AdminPrincipal:
    subject: str
    roles: frozenset[str]
    tenant_ids: frozenset[str]

    def allows(self, *, write: bool, tenant_id: str | None) -> bool:
        if "platform-admin" in self.roles:
            return True
        if not write and "auditor" in self.roles:
            return tenant_id is None or not self.tenant_ids or tenant_id in self.tenant_ids
        return (
            tenant_id is not None and "tenant-admin" in self.roles and tenant_id in self.tenant_ids
        )


class OidcTokenVerifier:
    def __init__(self, issuer: str, audience: str, jwks_url: str) -> None:
        self._issuer = issuer
        self._audience = audience
        self._jwks = jwt.PyJWKClient(jwks_url, cache_keys=True)

    async def verify(self, token: str) -> dict[str, Any]:
        signing_key = await asyncio.to_thread(self._jwks.get_signing_key_from_jwt, token)
        return jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256", "ES256"],
            audience=self._audience,
            issuer=self._issuer,
            options={"require": ["exp", "iat", "sub"]},
        )


def _values(claim: Any) -> frozenset[str]:
    if isinstance(claim, str):
        return frozenset(item for item in claim.replace(",", " ").split() if item)
    if isinstance(claim, list):
        return frozenset(str(item) for item in claim)
    return frozenset()


async def admin_principal(
    request: Request,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(bearer_scheme)
    ],
) -> AdminPrincipal:
    settings = request.app.state.settings
    if not settings.admin_oidc_enabled:
        return AdminPrincipal("development-admin", frozenset({"platform-admin"}), frozenset())
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Bearer token required")
    try:
        claims = await request.app.state.oidc_verifier.verify(credentials.credentials)
    except Exception as error:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid OIDC token") from error
    principal = AdminPrincipal(
        str(claims["sub"]),
        _values(claims.get(settings.admin_oidc_role_claim)),
        _values(claims.get(settings.admin_oidc_tenant_claim)),
    )
    if not principal.roles:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "no administrative role")
    return principal


async def require_admin_read(
    principal: Annotated[AdminPrincipal, Depends(admin_principal)],
    tenant_id: str | None = None,
) -> AdminPrincipal:
    if not principal.allows(write=False, tenant_id=tenant_id):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "tenant read access denied")
    return principal


async def require_admin_write(
    principal: Annotated[AdminPrincipal, Depends(admin_principal)],
    tenant_id: str | None = None,
) -> AdminPrincipal:
    if not principal.allows(write=True, tenant_id=tenant_id):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "tenant write access denied")
    return principal
