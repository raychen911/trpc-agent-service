"""Tenant-scoped, versioned persistence for authenticated IM accounts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.exc import TimeoutError as SATimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine

from trpc_service.channels.binding import ChannelBinding
from trpc_service.storage.database import (
    DatabaseSettings,
    check_database_readiness,
    create_database_engine,
)
from trpc_service.storage.schema import channel_binding_versions, channel_bindings, tenant_configs


class ChannelBindingRepositoryConfigurationError(ValueError):
    """The binding repository cannot be configured safely."""


class ChannelBindingRepositoryUnavailableError(RuntimeError):
    """The binding store is closed or cannot be reached."""


class ChannelBindingRepositoryDataError(RuntimeError):
    """Binding data violates a durable repository invariant."""


class ChannelBindingAlreadyExistsError(ChannelBindingRepositoryDataError):
    """The authenticated IM account is already owned by a binding."""


class ChannelBindingNotFoundError(ChannelBindingRepositoryDataError):
    """The requested binding is absent or belongs to another tenant."""


class ChannelBindingVersionConflictError(ChannelBindingRepositoryDataError):
    """The supplied optimistic-lock version is stale."""


class ChannelBindingTargetVersionNotFoundError(ChannelBindingRepositoryDataError):
    """The requested immutable binding snapshot does not exist."""


class ChannelBindingRepository(Protocol):

    async def create(self, binding: ChannelBinding) -> ChannelBinding:
        ...

    async def get(self, tenant_id: str, binding_id: UUID) -> ChannelBinding | None:
        ...

    async def list_for_tenant(self, tenant_id: str, *, limit: int = 100) -> tuple[ChannelBinding, ...]:
        ...

    async def resolve_enabled(self, channel: str, external_account_id: str) -> ChannelBinding | None:
        ...

    async def list_enabled(self) -> tuple[ChannelBinding, ...]:
        ...

    async def update(
        self,
        tenant_id: str,
        binding_id: UUID,
        expected_version: int,
        desired: ChannelBinding,
    ) -> ChannelBinding:
        ...

    async def list_versions(
        self,
        tenant_id: str,
        binding_id: UUID,
        *,
        before_version: int | None = None,
        limit: int = 50,
    ) -> tuple[ChannelBinding, ...]:
        ...

    async def rollback(
        self,
        tenant_id: str,
        binding_id: UUID,
        expected_version: int,
        target_version: int,
    ) -> ChannelBinding:
        ...

    async def check_ready(self) -> None:
        ...

    async def close(self) -> None:
        ...


_BINDING_COLUMNS = (
    channel_bindings.c.binding_id,
    channel_bindings.c.tenant_id,
    channel_bindings.c.app_id,
    channel_bindings.c.channel,
    channel_bindings.c.external_account_id,
    channel_bindings.c.secret_ref,
    channel_bindings.c.webhook_token_ref,
    channel_bindings.c.webhook_aes_key_ref,
    channel_bindings.c.enabled,
    channel_bindings.c.version,
)
_VERSION_COLUMNS = (
    channel_binding_versions.c.binding_id,
    channel_binding_versions.c.tenant_id,
    channel_binding_versions.c.app_id,
    channel_binding_versions.c.channel,
    channel_binding_versions.c.external_account_id,
    channel_binding_versions.c.secret_ref,
    channel_binding_versions.c.webhook_token_ref,
    channel_binding_versions.c.webhook_aes_key_ref,
    channel_binding_versions.c.enabled,
    channel_binding_versions.c.version,
)


class SqlChannelBindingRepository:
    """PostgreSQL binding store with immutable snapshots and optimistic CAS."""

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "SqlChannelBindingRepository":
        try:
            settings = DatabaseSettings.from_env(environ)
        except Exception:
            raise ChannelBindingRepositoryConfigurationError(
                "channel binding repository could not be configured") from None
        return cls(create_database_engine(settings))

    def __init__(self, engine: AsyncEngine, *, owns_engine: bool = True) -> None:
        self._engine = engine
        self._owns_engine = owns_engine
        self._closed = False

    async def create(self, binding: ChannelBinding) -> ChannelBinding:
        self._require_open()
        if not isinstance(binding, ChannelBinding):
            raise ChannelBindingRepositoryDataError("invalid channel binding")
        if binding.version != 1:
            raise ChannelBindingRepositoryDataError("new channel binding version must be one")
        normalized = _normalized_binding(binding, version=1)
        try:
            async with self._engine.begin() as conn:
                await self._require_current_app(conn, normalized.tenant_id, normalized.app_id)
                await conn.execute(channel_bindings.insert().values(_binding_values(normalized)))
                await conn.execute(channel_binding_versions.insert().values(_binding_values(normalized)))
        except IntegrityError as exc:
            if _sqlstate(exc) == "23505":
                raise ChannelBindingAlreadyExistsError("channel account is already bound") from None
            raise ChannelBindingRepositoryDataError("channel binding write failed") from None
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ChannelBindingRepositoryUnavailableError("channel binding database is not reachable") from None
        return normalized

    async def get(self, tenant_id: str, binding_id: UUID) -> ChannelBinding | None:
        self._require_open()
        _validate_identity(tenant_id, binding_id)
        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(
                    sa.select(*_BINDING_COLUMNS).where(
                        channel_bindings.c.tenant_id == tenant_id,
                        channel_bindings.c.binding_id == binding_id,
                    ))).first()
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ChannelBindingRepositoryUnavailableError("channel binding database is not reachable") from None
        return None if row is None else _row_to_binding(row)

    async def list_for_tenant(self, tenant_id: str, *, limit: int = 100) -> tuple[ChannelBinding, ...]:
        self._require_open()
        _validate_tenant_limit(tenant_id, limit)
        try:
            async with self._engine.connect() as conn:
                rows = (await conn.execute(
                    sa.select(*_BINDING_COLUMNS).where(channel_bindings.c.tenant_id == tenant_id).order_by(
                        channel_bindings.c.channel.asc(),
                        channel_bindings.c.external_account_id.asc()).limit(limit))).fetchall()
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ChannelBindingRepositoryUnavailableError("channel binding database is not reachable") from None
        return tuple(_row_to_binding(row) for row in rows)

    async def resolve_enabled(self, channel: str, external_account_id: str) -> ChannelBinding | None:
        self._require_open()
        normalized_channel, account = _normalize_lookup(channel, external_account_id)
        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(
                    sa.select(*_BINDING_COLUMNS).where(
                        channel_bindings.c.channel == normalized_channel,
                        channel_bindings.c.external_account_id == account,
                        channel_bindings.c.enabled.is_(True),
                    ))).first()
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ChannelBindingRepositoryUnavailableError("channel binding database is not reachable") from None
        return None if row is None else _row_to_binding(row)

    async def list_enabled(self) -> tuple[ChannelBinding, ...]:
        """Read enabled bindings for the Gateway-owned service manager."""
        self._require_open()
        try:
            async with self._engine.connect() as conn:
                rows = (await conn.execute(
                    sa.select(*_BINDING_COLUMNS).where(channel_bindings.c.enabled.is_(True)).order_by(
                        channel_bindings.c.channel, channel_bindings.c.external_account_id))).fetchall()
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ChannelBindingRepositoryUnavailableError("channel binding database is not reachable") from None
        return tuple(_row_to_binding(row) for row in rows)

    async def update(
        self,
        tenant_id: str,
        binding_id: UUID,
        expected_version: int,
        desired: ChannelBinding,
    ) -> ChannelBinding:
        self._require_open()
        _validate_identity(tenant_id, binding_id)
        _validate_expected_version(expected_version)
        _validate_desired(tenant_id, binding_id, desired)
        try:
            async with self._engine.begin() as conn:
                current = await self._locked_head(conn, tenant_id, binding_id)
                if current is None:
                    raise ChannelBindingNotFoundError("channel binding not found")
                if current.version != expected_version:
                    raise ChannelBindingVersionConflictError("channel binding version conflict")
                next_binding = _normalized_binding(desired, version=expected_version + 1)
                await self._require_current_app(conn, tenant_id, next_binding.app_id)
                await conn.execute(channel_bindings.update().where(
                    channel_bindings.c.binding_id == binding_id).values(**_binding_values(next_binding),
                                                                        updated_at=sa.text("now()")))
                await conn.execute(channel_binding_versions.insert().values(_binding_values(next_binding)))
        except (ChannelBindingNotFoundError, ChannelBindingVersionConflictError, ChannelBindingRepositoryDataError):
            raise
        except IntegrityError as exc:
            if _sqlstate(exc) == "23505":
                raise ChannelBindingAlreadyExistsError("channel account is already bound") from None
            raise ChannelBindingRepositoryDataError("channel binding write failed") from None
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ChannelBindingRepositoryUnavailableError("channel binding database is not reachable") from None
        return next_binding

    async def list_versions(
        self,
        tenant_id: str,
        binding_id: UUID,
        *,
        before_version: int | None = None,
        limit: int = 50,
    ) -> tuple[ChannelBinding, ...]:
        self._require_open()
        _validate_identity(tenant_id, binding_id)
        _validate_tenant_limit(tenant_id, limit)
        if before_version is not None:
            _validate_expected_version(before_version)
        statement = sa.select(*_VERSION_COLUMNS).where(
            channel_binding_versions.c.tenant_id == tenant_id,
            channel_binding_versions.c.binding_id == binding_id,
        )
        if before_version is not None:
            statement = statement.where(channel_binding_versions.c.version < before_version)
        try:
            async with self._engine.connect() as conn:
                rows = (await conn.execute(statement.order_by(channel_binding_versions.c.version.desc()).limit(limit)
                                           )).fetchall()
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ChannelBindingRepositoryUnavailableError("channel binding database is not reachable") from None
        return tuple(_row_to_binding(row) for row in rows)

    async def rollback(
        self,
        tenant_id: str,
        binding_id: UUID,
        expected_version: int,
        target_version: int,
    ) -> ChannelBinding:
        self._require_open()
        _validate_identity(tenant_id, binding_id)
        _validate_expected_version(expected_version)
        _validate_expected_version(target_version)
        try:
            async with self._engine.begin() as conn:
                current = await self._locked_head(conn, tenant_id, binding_id)
                if current is None:
                    raise ChannelBindingNotFoundError("channel binding not found")
                if current.version != expected_version:
                    raise ChannelBindingVersionConflictError("channel binding version conflict")
                target = (await conn.execute(
                    sa.select(*_VERSION_COLUMNS).where(
                        channel_binding_versions.c.tenant_id == tenant_id,
                        channel_binding_versions.c.binding_id == binding_id,
                        channel_binding_versions.c.version == target_version,
                    ))).first()
                if target is None:
                    raise ChannelBindingTargetVersionNotFoundError("channel binding version not found")
                restored = _normalized_binding(_row_to_binding(target), version=expected_version + 1)
                await self._require_current_app(conn, tenant_id, restored.app_id)
                await conn.execute(channel_bindings.update().where(
                    channel_bindings.c.binding_id == binding_id).values(**_binding_values(restored),
                                                                        updated_at=sa.text("now()")))
                await conn.execute(channel_binding_versions.insert().values(_binding_values(restored)))
        except (ChannelBindingNotFoundError, ChannelBindingVersionConflictError,
                ChannelBindingTargetVersionNotFoundError, ChannelBindingRepositoryDataError):
            raise
        except IntegrityError as exc:
            if _sqlstate(exc) == "23505":
                raise ChannelBindingAlreadyExistsError("channel account is already bound") from None
            raise ChannelBindingRepositoryDataError("channel binding write failed") from None
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ChannelBindingRepositoryUnavailableError("channel binding database is not reachable") from None
        return restored

    async def check_ready(self) -> None:
        self._require_open()
        try:
            await check_database_readiness(self._engine)
            async with self._engine.connect() as conn:
                await conn.execute(sa.text("SELECT 1 FROM channel_bindings LIMIT 0"))
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ChannelBindingRepositoryUnavailableError("channel binding database is not reachable") from None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_engine:
            await self._engine.dispose()

    def _require_open(self) -> None:
        if self._closed:
            raise ChannelBindingRepositoryUnavailableError("repository is closed")

    @staticmethod
    async def _require_current_app(conn, tenant_id: str, app_id: str) -> None:
        row = (await conn.execute(
            sa.select(tenant_configs.c.app_id).where(tenant_configs.c.tenant_id == tenant_id).with_for_update()
        )).first()
        if row is None:
            raise ChannelBindingNotFoundError("tenant not found")
        if row._mapping["app_id"] != app_id:
            raise ChannelBindingRepositoryDataError("channel binding app does not match tenant")

    @staticmethod
    async def _locked_head(conn, tenant_id: str, binding_id: UUID) -> ChannelBinding | None:
        row = (await conn.execute(
            sa.select(*_BINDING_COLUMNS).where(
                channel_bindings.c.tenant_id == tenant_id,
                channel_bindings.c.binding_id == binding_id,
            ).with_for_update())).first()
        return None if row is None else _row_to_binding(row)


def _row_to_binding(row) -> ChannelBinding:
    try:
        return ChannelBinding(**dict(row._mapping))
    except Exception:
        raise ChannelBindingRepositoryDataError("channel binding row is corrupt") from None


def _binding_values(binding: ChannelBinding) -> dict:
    return {
        "binding_id": binding.binding_id,
        "tenant_id": binding.tenant_id,
        "app_id": binding.app_id,
        "channel": binding.channel,
        "external_account_id": binding.external_account_id,
        "secret_ref": binding.secret_ref,
        "webhook_token_ref": binding.webhook_token_ref,
        "webhook_aes_key_ref": binding.webhook_aes_key_ref,
        "enabled": binding.enabled,
        "version": binding.version,
    }


def _normalized_binding(binding: ChannelBinding, *, version: int) -> ChannelBinding:
    return binding.model_copy(update={"version": version})


def _validate_identity(tenant_id: str, binding_id: UUID) -> None:
    if not isinstance(tenant_id, str) or not tenant_id:
        raise ChannelBindingRepositoryDataError("invalid tenant ID")
    if not isinstance(binding_id, UUID):
        raise ChannelBindingRepositoryDataError("invalid binding ID")


def _validate_tenant_limit(tenant_id: str, limit: int) -> None:
    if not isinstance(tenant_id, str) or not tenant_id:
        raise ChannelBindingRepositoryDataError("invalid tenant ID")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise ChannelBindingRepositoryDataError("invalid limit")


def _validate_expected_version(version: int) -> None:
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ChannelBindingRepositoryDataError("invalid expected version")


def _validate_desired(tenant_id: str, binding_id: UUID, desired: ChannelBinding) -> None:
    if not isinstance(desired, ChannelBinding):
        raise ChannelBindingRepositoryDataError("invalid channel binding")
    if desired.tenant_id != tenant_id or desired.binding_id != binding_id:
        raise ChannelBindingRepositoryDataError("channel binding identity cannot change")


def _normalize_lookup(channel: str, external_account_id: str) -> tuple[str, str]:
    if channel not in ("wecom", "feishu") or not isinstance(external_account_id, str):
        raise ChannelBindingRepositoryDataError("invalid channel account")
    account = external_account_id.strip()
    if not account:
        raise ChannelBindingRepositoryDataError("invalid channel account")
    return channel, account


def _sqlstate(exc: IntegrityError) -> str | None:
    return getattr(exc, "pgcode", None) or getattr(exc.orig, "sqlstate", None)


__all__ = [
    "ChannelBindingAlreadyExistsError",
    "ChannelBindingNotFoundError",
    "ChannelBindingRepository",
    "ChannelBindingRepositoryConfigurationError",
    "ChannelBindingRepositoryDataError",
    "ChannelBindingRepositoryUnavailableError",
    "ChannelBindingTargetVersionNotFoundError",
    "ChannelBindingVersionConflictError",
    "SqlChannelBindingRepository",
]
