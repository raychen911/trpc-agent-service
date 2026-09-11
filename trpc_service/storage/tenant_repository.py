"""SQL-backed tenant configuration repository."""

from __future__ import annotations

from typing import Mapping

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SATimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine

from trpc_service.config.tenant import (
    AgentAppConfig,
    TenantBackendProfile,
    TenantAuditPolicy,
    TenantConfig,
    TenantConfigDraft,
    TenantGovernanceConfig,
)
from trpc_service.config.tenant_repository import (
    TenantAlreadyExistsError,
    TenantConfigTargetVersionNotFoundError,
    TenantConfigVersionConflictError,
    TenantNotFoundError,
    TenantRepositoryConfigurationError,
    TenantRepositoryDataError,
    TenantRepositoryUnavailableError,
)
from trpc_service.storage.database import (
    DatabaseSettings,
    check_database_readiness,
    create_database_engine,
)
from trpc_service.storage.schema import tenant_config_versions, tenant_configs

_UNIQUE_VIOLATION_SQLSTATE = "23505"


class SqlTenantConfigRepository:
    """Reads tenant configuration from PostgreSQL using SQLAlchemy async."""

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> SqlTenantConfigRepository:
        """Create a repository using ``TRPC_DATABASE_URL`` from the environment."""
        from trpc_service.storage.database import DatabaseConfigurationError

        try:
            settings = DatabaseSettings.from_env(environ)
        except DatabaseConfigurationError as exc:
            raise TenantRepositoryConfigurationError(str(exc)) from exc
        engine = create_database_engine(settings)
        return cls(engine)

    def __init__(self, engine: AsyncEngine, *, owns_engine: bool = True) -> None:
        self._engine = engine
        self._owns_engine = owns_engine
        self._closed = False

    async def get(self, tenant_id: str) -> TenantConfig | None:
        """Look up a single tenant by ID. Returns None if not found."""
        if self._closed:
            raise TenantRepositoryUnavailableError("repository is closed")

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(sa.select(tenant_configs).where(tenant_configs.c.tenant_id == tenant_id))
                row = result.first()
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise TenantRepositoryUnavailableError("database query failed") from exc

        if row is None:
            return None

        return _row_to_tenant_config(row)

    async def get_version(self, tenant_id: str, version: int) -> TenantConfig | None:
        """Read one immutable snapshot, never silently substituting head."""
        if self._closed:
            raise TenantRepositoryUnavailableError("repository is closed")
        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(
                    sa.select(tenant_config_versions).where(tenant_config_versions.c.tenant_id == tenant_id,
                                                            tenant_config_versions.c.version == version))).first()
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise TenantRepositoryUnavailableError("database query failed") from exc
        return _row_to_tenant_config(row) if row is not None else None

    async def check_ready(self) -> None:
        """Verify the database is reachable."""
        if self._closed:
            raise TenantRepositoryUnavailableError("repository is closed")
        try:
            await check_database_readiness(self._engine)
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise TenantRepositoryUnavailableError("database is not reachable") from exc

    async def close(self) -> None:
        """Dispose the engine pool if owned. Idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._owns_engine:
            await self._engine.dispose()

    async def create(self, config: TenantConfig) -> TenantConfig:
        """Insert a new tenant with head and version-1 snapshot in one transaction."""
        self._require_open()
        created = TenantConfig(
            tenant_id=config.tenant_id,
            enabled=config.enabled,
            version=1,
            app=config.app,
            governance=config.governance,
            backend_profile=config.backend_profile,
            audit_policy=config.audit_policy,
        )
        try:
            async with self._engine.begin() as conn:
                await conn.execute(
                    _head_insert(config.tenant_id, created.enabled, created.app, created.governance,
                                 created.backend_profile, created.audit_policy))
                await conn.execute(
                    _snapshot_insert(config.tenant_id, 1, created.enabled, created.app, created.governance,
                                     created.backend_profile, created.audit_policy))
        except IntegrityError as exc:
            if _is_unique_violation(exc):
                raise TenantAlreadyExistsError("tenant already exists") from None
            raise TenantRepositoryDataError("tenant write failed") from None
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise _map_write_failure(exc) from None
        return created

    async def update(
        self,
        tenant_id: str,
        expected_version: int,
        desired: TenantConfigDraft,
    ) -> TenantConfig:
        """Apply desired changes if the head version matches expected_version."""
        return await self._write_new_version(
            tenant_id,
            expected_version,
            desired.enabled,
            desired.app,
            desired.governance,
            desired.backend_profile,
            desired.audit_policy,
        )

    async def rollback(
        self,
        tenant_id: str,
        expected_version: int,
        target_version: int,
    ) -> TenantConfig:
        """Copy a historical snapshot into a new, larger version."""
        self._require_open()
        try:
            async with self._engine.begin() as conn:
                target_row = (await conn.execute(
                    sa.select(
                        tenant_config_versions.c.enabled,
                        tenant_config_versions.c.app_id,
                        tenant_config_versions.c.instruction,
                        tenant_config_versions.c.model_profile,
                        tenant_config_versions.c.allowed_tools,
                        tenant_config_versions.c.governance,
                        tenant_config_versions.c.backend_profile,
                        tenant_config_versions.c.audit_policy,
                    ).where(
                        tenant_config_versions.c.tenant_id == tenant_id,
                        tenant_config_versions.c.version == target_version,
                    ))).first()
                if target_row is None:
                    raise TenantConfigTargetVersionNotFoundError("target version not found")

                target_app = AgentAppConfig(
                    app_id=target_row._mapping["app_id"],
                    instruction=target_row._mapping["instruction"],
                    model_profile=target_row._mapping["model_profile"],
                    allowed_tools=tuple(target_row._mapping["allowed_tools"]),
                )
                try:
                    target_governance = TenantGovernanceConfig(**target_row._mapping["governance"])
                except Exception as exc:
                    raise TenantRepositoryDataError("governance history data corrupt") from exc
                try:
                    target_backend_profile = TenantBackendProfile(**target_row._mapping["backend_profile"])
                except Exception as exc:
                    raise TenantRepositoryDataError("backend_profile history data corrupt") from exc
                try:
                    target_audit_policy = TenantAuditPolicy(**target_row._mapping["audit_policy"])
                except Exception as exc:
                    raise TenantRepositoryDataError("audit_policy history data corrupt") from exc
                new_version = expected_version + 1
                row = await self._conditional_bump(conn, tenant_id, expected_version, new_version,
                                                   target_row._mapping["enabled"], target_app, target_governance,
                                                   target_backend_profile, target_audit_policy)
                if row is None:
                    await self._raise_head_mismatch(conn, tenant_id)
                await conn.execute(
                    _snapshot_insert(tenant_id, new_version, target_row._mapping["enabled"], target_app,
                                     target_governance, target_backend_profile, target_audit_policy))
                return _row_to_tenant_config(row)
        except (TenantConfigTargetVersionNotFoundError, TenantNotFoundError, TenantConfigVersionConflictError):
            raise
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise _map_write_failure(exc) from None

    async def list_versions(
        self,
        tenant_id: str,
        *,
        before_version: int | None = None,
        limit: int = 50,
    ) -> tuple[TenantConfig, ...]:
        """Return history snapshots newest-first with exclusive paging."""
        self._require_open()
        stmt = (sa.select(
            tenant_config_versions.c.tenant_id,
            tenant_config_versions.c.enabled,
            tenant_config_versions.c.version,
            tenant_config_versions.c.app_id,
            tenant_config_versions.c.instruction,
            tenant_config_versions.c.model_profile,
            tenant_config_versions.c.allowed_tools,
            tenant_config_versions.c.governance,
            tenant_config_versions.c.backend_profile,
            tenant_config_versions.c.audit_policy,
        ).where(tenant_config_versions.c.tenant_id == tenant_id).order_by(
            tenant_config_versions.c.version.desc()).limit(limit))
        if before_version is not None:
            stmt = stmt.where(tenant_config_versions.c.version < before_version)
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(stmt)
                rows = result.fetchall()
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise _map_write_failure(exc) from None
        return tuple(_row_to_tenant_config(row) for row in rows)

    def _require_open(self) -> None:
        if self._closed:
            raise TenantRepositoryUnavailableError("repository is closed")

    async def _write_new_version(
        self,
        tenant_id: str,
        expected_version: int,
        enabled: bool,
        app: AgentAppConfig,
        governance: TenantGovernanceConfig,
        backend_profile: TenantBackendProfile,
        audit_policy: TenantAuditPolicy,
    ) -> TenantConfig:
        self._require_open()
        new_version = expected_version + 1
        try:
            async with self._engine.begin() as conn:
                row = await self._conditional_bump(conn, tenant_id, expected_version, new_version, enabled, app,
                                                   governance, backend_profile, audit_policy)
                if row is None:
                    await self._raise_head_mismatch(conn, tenant_id)
                await conn.execute(
                    _snapshot_insert(tenant_id, new_version, enabled, app, governance, backend_profile, audit_policy))
                return _row_to_tenant_config(row)
        except (TenantNotFoundError, TenantConfigVersionConflictError):
            raise
        except (DBAPIError, SATimeoutError, OSError) as exc:
            raise _map_write_failure(exc) from None

    @staticmethod
    async def _conditional_bump(
        conn: sa.ext.asyncio.AsyncConnection,
        tenant_id: str,
        expected_version: int,
        new_version: int,
        enabled: bool,
        app: AgentAppConfig,
        governance: TenantGovernanceConfig,
        backend_profile: TenantBackendProfile,
        audit_policy: TenantAuditPolicy,
    ) -> sa.engine.Row | None:
        stmt = (tenant_configs.update().where(tenant_configs.c.tenant_id == tenant_id).where(
            tenant_configs.c.version == expected_version).values(
                enabled=enabled,
                version=new_version,
                app_id=app.app_id,
                instruction=app.instruction,
                model_profile=app.model_profile,
                allowed_tools=list(app.allowed_tools),
                governance=governance.model_dump(mode="json"),
                backend_profile=backend_profile.model_dump(mode="json"),
                audit_policy=audit_policy.model_dump(mode="json"),
                updated_at=sa.text("now()"),
            ).returning(
                tenant_configs.c.tenant_id,
                tenant_configs.c.enabled,
                tenant_configs.c.version,
                tenant_configs.c.app_id,
                tenant_configs.c.instruction,
                tenant_configs.c.model_profile,
                tenant_configs.c.allowed_tools,
                tenant_configs.c.governance,
                tenant_configs.c.backend_profile,
                tenant_configs.c.audit_policy,
            ))
        result = await conn.execute(stmt)
        return result.first()

    @staticmethod
    async def _raise_head_mismatch(
        conn: sa.ext.asyncio.AsyncConnection,
        tenant_id: str,
    ) -> None:
        """Distinguish a missing tenant from a stale expected_version."""
        exists = await conn.execute(
            sa.select(tenant_configs.c.tenant_id).where(tenant_configs.c.tenant_id == tenant_id))
        if exists.first() is None:
            raise TenantNotFoundError("tenant not found")
        raise TenantConfigVersionConflictError("version conflict")


def _head_insert(
    tenant_id: str,
    enabled: bool,
    app: AgentAppConfig,
    governance: TenantGovernanceConfig,
    backend_profile: TenantBackendProfile,
    audit_policy: TenantAuditPolicy,
) -> sa.Insert:
    return tenant_configs.insert().values(
        tenant_id=tenant_id,
        enabled=enabled,
        version=1,
        app_id=app.app_id,
        instruction=app.instruction,
        model_profile=app.model_profile,
        allowed_tools=list(app.allowed_tools),
        governance=governance.model_dump(mode="json"),
        backend_profile=backend_profile.model_dump(mode="json"),
        audit_policy=audit_policy.model_dump(mode="json"),
    )


def _snapshot_insert(
    tenant_id: str,
    version: int,
    enabled: bool,
    app: AgentAppConfig,
    governance: TenantGovernanceConfig,
    backend_profile: TenantBackendProfile,
    audit_policy: TenantAuditPolicy,
) -> sa.Insert:
    return tenant_config_versions.insert().values(
        tenant_id=tenant_id,
        version=version,
        enabled=enabled,
        app_id=app.app_id,
        instruction=app.instruction,
        model_profile=app.model_profile,
        allowed_tools=list(app.allowed_tools),
        governance=governance.model_dump(mode="json"),
        backend_profile=backend_profile.model_dump(mode="json"),
        audit_policy=audit_policy.model_dump(mode="json"),
    )


def _is_unique_violation(exc: IntegrityError) -> bool:
    sqlstate = getattr(exc, "pgcode", None) or getattr(exc.orig, "sqlstate", None)
    return sqlstate == _UNIQUE_VIOLATION_SQLSTATE


def _map_write_failure(exc: Exception) -> Exception:
    """Map low-level write failures onto repository domain errors."""
    if isinstance(exc, (SATimeoutError, OperationalError, OSError)):
        return TenantRepositoryUnavailableError("database is not reachable")
    return TenantRepositoryDataError("tenant write failed")


def _row_to_tenant_config(row: sa.engine.Row) -> TenantConfig:
    """Map a database row to a strict ``TenantConfig`` domain object.

    asyncpg returns proper Python types (int, list for JSONB), so no
    relaxation via int() or json.loads() is needed. Any mapping failure
    indicates data corruption and raises TenantRepositoryDataError.
    """
    try:
        tools_list = row._mapping["allowed_tools"]
        if not isinstance(tools_list, list):
            raise TenantRepositoryDataError("allowed_tools is not an array")

        governance_raw = row._mapping["governance"]
        if not isinstance(governance_raw, dict):
            raise TenantRepositoryDataError("governance is not an object")

        backend_profile_raw = row._mapping["backend_profile"]
        if not isinstance(backend_profile_raw, dict):
            raise TenantRepositoryDataError("backend_profile is not an object")
        audit_policy_raw = row._mapping["audit_policy"]
        if not isinstance(audit_policy_raw, dict):
            raise TenantRepositoryDataError("audit_policy is not an object")

        return TenantConfig(
            tenant_id=row._mapping["tenant_id"],
            enabled=row._mapping["enabled"],
            version=row._mapping["version"],
            app=AgentAppConfig(
                app_id=row._mapping["app_id"],
                instruction=row._mapping["instruction"],
                model_profile=row._mapping["model_profile"],
                allowed_tools=tuple(tools_list),
            ),
            governance=TenantGovernanceConfig(**governance_raw),
            backend_profile=TenantBackendProfile(**backend_profile_raw),
            audit_policy=TenantAuditPolicy(**audit_policy_raw),
        )
    except TenantRepositoryDataError:
        raise
    except Exception as exc:
        raise TenantRepositoryDataError("database row cannot be mapped") from exc


__all__ = ["SqlTenantConfigRepository"]
