"""Tenant-scoped encrypted credentials used by dynamically configured channels."""

from base64 import b64decode, b64encode
from collections.abc import Callable, Mapping
import secrets
from uuid import UUID, uuid4

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.admin.models import TenantSecret
from trpc_service.config.secret_scope import (
    validate_tenant_channel_secret_ref,
    validate_tenant_mcp_secret_ref,
)
from trpc_service.config.storage import LocalSecretNotReadyError, resolve_local_secret

_MANAGED_SCHEME = "secret-manager"
_MAX_SECRET_CHARACTERS = 16_384
_SECRET_SCOPE_VALIDATORS = {
    "channels": validate_tenant_channel_secret_ref,
    "mcp": validate_tenant_mcp_secret_ref,
}


def _scope_validator(scope: str) -> Callable[[str, UUID], str]:
    """Reject unknown scopes instead of silently treating them as MCP."""

    try:
        return _SECRET_SCOPE_VALIDATORS[scope]
    except KeyError as error:
        raise ValueError(f"unsupported tenant secret scope: {scope}") from error


class TenantSecretStore:
    """Encrypt tenant secrets at rest and resolve legacy local references safely."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        master_key: bytes | None,
    ) -> None:
        if master_key is not None and len(master_key) != 32:
            raise ValueError("tenant SecretStore requires a 32-byte AES key")
        self._sessions = sessions
        self._cipher = None if master_key is None else AESGCM(master_key)

    @staticmethod
    def _reference(tenant_id: UUID, secret_id: UUID, scope: str = "channels") -> str:
        return f"{_MANAGED_SCHEME}://tenants/{tenant_id}/{scope}/{secret_id}"

    @staticmethod
    def _managed_secret_id(reference: str, tenant_id: UUID, scope: str = "channels") -> UUID:
        validator = _scope_validator(scope)
        validated = validator(reference, tenant_id)
        scheme, _, target = validated.partition("://")
        if scheme != _MANAGED_SCHEME:
            raise ValueError("SecretRef is not managed by the tenant SecretStore")
        prefix = f"tenants/{tenant_id}/{scope}/"
        identifier = target.removeprefix(prefix)
        if target != f"{prefix}{identifier}" or "/" in identifier:
            raise ValueError("invalid tenant SecretStore reference")
        try:
            return UUID(identifier)
        except ValueError as error:
            raise ValueError("invalid tenant SecretStore identifier") from error

    @staticmethod
    def _aad(row: TenantSecret) -> bytes:
        return f"{row.tenant_id}:{row.tenant_secret_id}:{row.name}".encode("utf-8")

    def _require_cipher(self) -> AESGCM:
        if self._cipher is None:
            raise RuntimeError("tenant SecretStore master key is not configured")
        return self._cipher

    async def put(
        self,
        database: AsyncSession,
        tenant_id: UUID,
        name: str,
        value: str,
        *,
        existing_reference: str | None = None,
        scope: str = "channels",
    ) -> str:
        """Create or rotate one credential inside the caller's transaction."""

        normalized_name = name.strip()
        secret_value = value.strip()
        if not normalized_name or len(normalized_name) > 200:
            raise ValueError("tenant secret name must contain 1 to 200 characters")
        if not secret_value or len(secret_value) > _MAX_SECRET_CHARACTERS:
            raise ValueError("tenant secret must contain 1 to 16384 characters")
        row: TenantSecret | None = None
        if existing_reference is not None and existing_reference.startswith(
                f"{_MANAGED_SCHEME}://"):
            secret_id = self._managed_secret_id(existing_reference, tenant_id, scope)
            row = await database.scalar(
                select(TenantSecret).where(
                    TenantSecret.tenant_id == tenant_id,
                    TenantSecret.tenant_secret_id == secret_id,
                ).with_for_update())
        if row is None:
            row = TenantSecret(
                tenant_secret_id=uuid4(),
                tenant_id=tenant_id,
                name=normalized_name,
                ciphertext="pending",
                nonce="pending",
            )
            database.add(row)
        else:
            row.name = normalized_name
            row.status = "active"
        nonce = secrets.token_bytes(12)
        ciphertext = self._require_cipher().encrypt(
            nonce,
            secret_value.encode("utf-8"),
            self._aad(row),
        )
        row.nonce = b64encode(nonce).decode("ascii")
        row.ciphertext = b64encode(ciphertext).decode("ascii")
        await database.flush()
        return self._reference(tenant_id, row.tenant_secret_id, scope)

    async def put_many(
        self,
        database: AsyncSession,
        tenant_id: UUID,
        namespace: str,
        values: Mapping[str, str],
        existing: Mapping[str, str] | None = None,
        scope: str = "channels",
    ) -> dict[str, str]:
        """Store provider fields independently so each value can rotate in place."""

        references: dict[str, str] = {}
        current = existing or {}
        for field, value in values.items():
            references[field] = await self.put(
                database,
                tenant_id,
                f"{namespace}/{field}",
                value,
                existing_reference=current.get(field),
                scope=scope,
            )
        return references

    async def resolve(
        self,
        reference: str,
        tenant_id: UUID,
        *,
        scope: str = "channels",
    ) -> str:
        """Resolve one local or encrypted SecretRef without exposing it to callers."""

        validator = _scope_validator(scope)
        validator(reference, tenant_id)
        if not reference.startswith(f"{_MANAGED_SCHEME}://"):
            return resolve_local_secret(reference)
        secret_id = self._managed_secret_id(reference, tenant_id, scope)
        async with self._sessions() as database:
            row = await database.scalar(
                select(TenantSecret).where(
                    TenantSecret.tenant_id == tenant_id,
                    TenantSecret.tenant_secret_id == secret_id,
                    TenantSecret.status == "active",
                ))
        if row is None:
            raise LocalSecretNotReadyError("tenant secret is unavailable")
        try:
            plaintext = self._require_cipher().decrypt(
                b64decode(row.nonce, validate=True),
                b64decode(row.ciphertext, validate=True),
                self._aad(row),
            )
            return plaintext.decode("utf-8")
        except Exception as error:
            # Ciphertext and provider values must never be included in errors.
            raise LocalSecretNotReadyError("tenant secret cannot be decrypted") from error
