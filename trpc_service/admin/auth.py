"""Small API-key RBAC layer for the management API."""

from __future__ import annotations

import base64
import binascii
import hmac
import json
import math
import os
import time
from dataclasses import dataclass
from threading import RLock
from urllib.request import Request, urlopen


class AdminAuthenticationError(PermissionError):
    pass


class OIDCAuthenticationError(AdminAuthenticationError):
    pass


@dataclass(frozen=True, slots=True)
class AdminPrincipal:
    subject: str
    role: str
    tenants: frozenset[str] = frozenset()

    def can_access(self, tenant_id: str) -> bool:
        if self.role in {"superadmin", "platform_admin"}:
            return True
        return bool(self.tenants) and tenant_id in self.tenants


def _configured_keys() -> list[tuple[str, AdminPrincipal]]:
    result: list[tuple[str, AdminPrincipal]] = []
    default_key = os.getenv("ADMIN_API_KEY", "")
    if default_key:
        result.append((default_key, AdminPrincipal("env-admin", os.getenv("ADMIN_ROLE", "superadmin"))))
    # Format: key=role:tenant-a|tenant-b,key2=viewer:tenant-c
    for item in os.getenv("ADMIN_API_KEYS", "").split(","):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        role, _, tenants = value.partition(":")
        result.append(
            (key.strip(), AdminPrincipal("api-key", role.strip(), frozenset(filter(None, tenants.split("|")))))
        )
    return result


_JWKS_CACHE: dict[str, tuple[float, dict]] = {}
_JWKS_LOCK = RLock()


def _oidc_enabled() -> bool:
    return bool(os.getenv("OIDC_JWKS_URL", "").strip() or os.getenv("OIDC_ISSUER", "").strip())


def _b64url_decode(value: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise OIDCAuthenticationError("malformed OIDC token")
    try:
        raw = value.encode("ascii")
        if any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_" for char in value):
            raise ValueError("invalid base64url character")
        return base64.b64decode(raw + b"=" * (-len(raw) % 4), altchars=b"-_", validate=True)
    except (binascii.Error, UnicodeEncodeError, ValueError, TypeError) as exc:
        raise OIDCAuthenticationError("malformed OIDC token") from exc


def _numeric_claim(claims: dict, name: str, required: bool = False) -> float | None:
    value = claims.get(name)
    if value is None:
        if required:
            raise OIDCAuthenticationError(f"OIDC {name} claim is missing")
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise OIDCAuthenticationError(f"OIDC {name} claim is invalid")
    return float(value)


def _load_jwks(url: str) -> dict:
    now = time.time()
    with _JWKS_LOCK:
        cached = _JWKS_CACHE.get(url)
        if cached and cached[0] > now:
            return cached[1]
    try:
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "trpc-agent-service"})
        with urlopen(request, timeout=5) as response:
            body = response.read(1024 * 1024)
        document = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise OIDCAuthenticationError("unable to load OIDC JWKS") from exc
    if not isinstance(document, dict) or not isinstance(document.get("keys"), list):
        raise OIDCAuthenticationError("OIDC JWKS response is invalid")
    try:
        cache_seconds = max(1, int(os.getenv("OIDC_JWKS_CACHE_SECONDS", "300")))
    except (TypeError, ValueError) as exc:
        raise OIDCAuthenticationError("OIDC_JWKS_CACHE_SECONDS must be an integer") from exc
    with _JWKS_LOCK:
        _JWKS_CACHE[url] = (
            now + cache_seconds,
            document,
        )
    return document


def _oidc_principal(token: str) -> AdminPrincipal:
    if not isinstance(token, str) or not token:
        raise OIDCAuthenticationError("OIDC token is required")
    parts = token.split(".")
    if len(parts) != 3:
        raise OIDCAuthenticationError("OIDC token must be a compact JWT")
    try:
        header = json.loads(_b64url_decode(parts[0]))
        claims = json.loads(_b64url_decode(parts[1]))
        signature = _b64url_decode(parts[2])
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OIDCAuthenticationError("OIDC token contains invalid JSON") from exc
    if not isinstance(header, dict) or not isinstance(claims, dict) or not signature:
        raise OIDCAuthenticationError("OIDC token header and claims must be JSON objects")
    if header.get("alg") != "RS256" or not isinstance(header.get("kid"), str) or not header["kid"]:
        raise OIDCAuthenticationError("only RS256 OIDC tokens with kid are supported")

    issuer = os.getenv("OIDC_ISSUER", "").strip()
    jwks_url = os.getenv("OIDC_JWKS_URL", "").strip()
    if not jwks_url:
        if not issuer:
            raise OIDCAuthenticationError("OIDC_ISSUER or OIDC_JWKS_URL is required")
        jwks_url = f"{issuer.rstrip('/')}/.well-known/jwks.json"
    keys = _load_jwks(jwks_url).get("keys", [])
    key_data = next(
        (
            item
            for item in keys
            if isinstance(item, dict)
            and item.get("kid") == header["kid"]
            and item.get("kty") == "RSA"
        ),
        None,
    )
    if key_data is None:
        raise OIDCAuthenticationError("OIDC signing key was not found")
    try:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding, rsa

        public_key = rsa.RSAPublicNumbers(
            int.from_bytes(_b64url_decode(key_data["e"]), "big"),
            int.from_bytes(_b64url_decode(key_data["n"]), "big"),
        ).public_key()
        public_key.verify(
            signature,
            f"{parts[0]}.{parts[1]}".encode("ascii"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
    except OIDCAuthenticationError:
        raise
    except Exception as exc:
        raise OIDCAuthenticationError("OIDC signature verification failed") from exc

    now = int(time.time())
    if issuer and claims.get("iss") != issuer:
        raise OIDCAuthenticationError("OIDC issuer mismatch")
    audience = os.getenv("OIDC_AUDIENCE", "").strip()
    if audience:
        token_audience = claims.get("aud")
        if isinstance(token_audience, list):
            if not all(isinstance(item, str) for item in token_audience):
                raise OIDCAuthenticationError("OIDC audience claim is invalid")
            token_audiences = token_audience
        elif isinstance(token_audience, str):
            token_audiences = [token_audience]
        else:
            raise OIDCAuthenticationError("OIDC audience claim is invalid")
        if audience not in token_audiences:
            raise OIDCAuthenticationError("OIDC audience mismatch")
    exp = _numeric_claim(claims, "exp", required=True)
    if exp is None or exp <= now:
        raise OIDCAuthenticationError("OIDC token is expired")
    nbf = _numeric_claim(claims, "nbf")
    if nbf is not None and nbf > now:
        raise OIDCAuthenticationError("OIDC token is not active yet")
    subject = claims.get("sub")
    if not isinstance(subject, str) or not subject:
        raise OIDCAuthenticationError("OIDC subject is missing")

    role_claim = os.getenv("OIDC_ROLE_CLAIM", "role")
    role_value = claims.get(role_claim)
    if role_value is None:
        role_value = claims.get("roles")
        if role_value is None and claims.get("realm_access") is not None:
            realm_access = claims["realm_access"]
            if not isinstance(realm_access, dict):
                raise OIDCAuthenticationError("OIDC realm_access claim is invalid")
            role_value = realm_access.get("roles")
    if isinstance(role_value, list):
        if not all(isinstance(item, str) for item in role_value):
            raise OIDCAuthenticationError("OIDC role claim is invalid")
        priority = {"superadmin": 0, "platform_admin": 1, "operator": 2, "viewer": 3}
        role = sorted(role_value, key=lambda item: priority.get(item, 99))[0] if role_value else ""
    else:
        if role_value is not None and not isinstance(role_value, str):
            raise OIDCAuthenticationError("OIDC role claim is invalid")
        role = role_value or os.getenv("OIDC_DEFAULT_ROLE", "viewer")
    allowed_roles = {"viewer", "operator", "platform_admin", "superadmin"}
    if role not in allowed_roles:
        raise OIDCAuthenticationError("OIDC role is not allowed")

    tenant_claim = os.getenv("OIDC_TENANTS_CLAIM", "tenants")
    tenant_value = claims.get(tenant_claim, [])
    if isinstance(tenant_value, str):
        tenants = frozenset(filter(None, (item.strip() for item in tenant_value.split(","))))
    elif isinstance(tenant_value, list):
        if not all(isinstance(item, str) for item in tenant_value):
            raise OIDCAuthenticationError("OIDC tenant scope claim is invalid")
        tenants = frozenset(item for item in tenant_value if item)
    elif tenant_value is None:
        tenants = frozenset()
    else:
        raise OIDCAuthenticationError("OIDC tenant scope claim is invalid")
    return AdminPrincipal(subject, role, tenants)


def authenticate(api_key: str | None = None, authorization: str | None = None) -> AdminPrincipal:
    if (
        authorization
        and authorization.lower().startswith("bearer ")
        and _oidc_enabled()
        and not api_key
    ):
        return _oidc_principal(authorization[7:].strip())
    token = api_key or (authorization.removeprefix("Bearer ").strip() if authorization else "")
    if not token:
        raise AdminAuthenticationError("admin credentials are required")
    for expected, principal in _configured_keys():
        if hmac.compare_digest(token, expected):
            return principal
    raise AdminAuthenticationError("invalid admin credentials")


def authorize(principal: AdminPrincipal, tenant_id: str, roles: set[str]) -> None:
    if principal.role not in roles and principal.role not in {"superadmin", "platform_admin"}:
        raise AdminAuthenticationError("insufficient admin role")
    if not principal.can_access(tenant_id):
        raise AdminAuthenticationError("tenant is outside admin scope")
