"""OIDC/JWKS authentication and tenant-scoped RBAC."""

from __future__ import annotations

import asyncio
import hmac
from enum import StrEnum
from typing import Protocol
from urllib.parse import urlsplit

import jwt
from jwt import PyJWKClient
from pydantic import BaseModel, ConfigDict

from trpc_service.config.secrets import SecretProvider, SecretRef


class Role(StrEnum):
    PLATFORM_ADMIN = "platform_admin"
    TENANT_ADMIN = "tenant_admin"
    AUDITOR = "auditor"


class Principal(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    subject: str
    roles: frozenset[Role]
    tenant_ids: frozenset[str] = frozenset()


class AuthenticationError(RuntimeError):
    pass


class AuthorizationError(RuntimeError):
    pass


class Authorizer(Protocol):
    async def authenticate(self, token: str) -> Principal: ...


class OidcAuthorizer:
    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        algorithms: tuple[str, ...] = ("RS256",),
        jwks_ttl_seconds: int = 300,
    ) -> None:
        if not algorithms or any(algorithm.lower() == "none" for algorithm in algorithms):
            raise ValueError("an explicit asymmetric OIDC algorithm allow-list is required")
        parsed = urlsplit(issuer)
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("OIDC issuer must use HTTPS")
        if jwks_ttl_seconds < 30:
            raise ValueError("JWKS TTL is too short")
        self._issuer = issuer.rstrip("/")
        self._audience = audience
        self._algorithms = algorithms
        self._jwks = PyJWKClient(
            f"{self._issuer}/.well-known/jwks.json",
            cache_keys=True,
            lifespan=jwks_ttl_seconds,
        )

    async def authenticate(self, token: str) -> Principal:
        try:
            signing_key = await asyncio.to_thread(self._jwks.get_signing_key_from_jwt, token)
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=list(self._algorithms),
                audience=self._audience,
                issuer=self._issuer,
                options={"require": ["exp", "iat", "iss", "sub", "aud"]},
            )
        except jwt.PyJWTError as exc:
            raise AuthenticationError("invalid bearer token") from exc
        raw_roles = claims.get("roles", [])
        if not isinstance(raw_roles, list):
            raise AuthenticationError("invalid roles claim")
        role_values = set(Role)
        if not all(isinstance(value, str) for value in raw_roles):
            raise AuthenticationError("invalid roles claim")
        roles = frozenset(Role(value) for value in raw_roles if value in role_values)
        tenants = claims.get("tenant_ids", [])
        if not isinstance(tenants, list):
            raise AuthenticationError("invalid tenant scope claim")
        if not all(isinstance(value, str) and value for value in tenants):
            raise AuthenticationError("invalid tenant scope claim")
        return Principal(subject=str(claims["sub"]), roles=roles, tenant_ids=frozenset(tenants))


class DevelopmentAuthorizer:
    """Single development token, impossible to construct without explicit opt-in."""

    def __init__(
        self,
        secrets: SecretProvider,
        token_ref: SecretRef,
        *,
        enabled: bool,
    ) -> None:
        if not enabled:
            raise ValueError("development authentication is disabled")
        self._secrets = secrets
        self._token_ref = token_ref

    async def authenticate(self, token: str) -> Principal:
        if not hmac.compare_digest(token, self._secrets.resolve(self._token_ref)):
            raise AuthenticationError("invalid bearer token")
        return Principal(
            subject="development-admin",
            roles=frozenset(Role),
            tenant_ids=frozenset({"*"}),
        )


def require_role(principal: Principal, role: Role, *, tenant_id: str | None = None) -> None:
    if Role.PLATFORM_ADMIN not in principal.roles and role not in principal.roles:
        raise AuthorizationError("required role is missing")
    if (
        tenant_id
        and Role.PLATFORM_ADMIN not in principal.roles
        and tenant_id not in principal.tenant_ids
    ):
        raise AuthorizationError("principal is outside the tenant scope")


__all__ = [
    "AuthenticationError",
    "AuthorizationError",
    "Authorizer",
    "DevelopmentAuthorizer",
    "OidcAuthorizer",
    "Principal",
    "Role",
    "require_role",
]
