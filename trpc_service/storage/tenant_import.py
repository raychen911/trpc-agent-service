"""One-shot JSON→SQL tenant configuration import."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError

from trpc_service.config.tenant import TenantConfig
from trpc_service.config.tenant_repository import (
    TenantRepositoryConfigurationError,
    TenantRepositoryDataError,
    TenantRepositoryUnavailableError,
    load_tenant_configs,
)
from trpc_service.storage.database import (
    DatabaseSettings,
    create_database_engine,
)
from trpc_service.storage.schema import tenant_config_versions, tenant_configs


async def import_tenant_configs(
    path: Path,
    environ: Mapping[str, str] | None = None,
) -> tuple[int, int]:
    """Import tenant configurations from a JSON file into SQL.

    Returns (inserted, skipped) counts.
    - Missing tenants are inserted.
    - Identical existing tenants are skipped (no-op).
    - Any conflict (different content) causes full transaction rollback.

    Uses INSERT ... ON CONFLICT DO NOTHING for atomic conflict handling,
    then reads back and compares to detect data conflicts.
    """
    from trpc_service.storage.database import DatabaseConfigurationError

    try:
        settings = DatabaseSettings.from_env(environ)
    except DatabaseConfigurationError as exc:
        raise TenantRepositoryConfigurationError(str(exc)) from exc

    configs = load_tenant_configs(path)
    engine = create_database_engine(settings)

    inserted = 0
    skipped = 0

    try:
        async with engine.begin() as conn:
            for config in configs:
                row_count = await _insert_or_skip(conn, config)
                if row_count > 0:
                    inserted += 1
                else:
                    existing = await _fetch_existing(conn, config.tenant_id)
                    if existing is not None and _rows_match(existing, config):
                        skipped += 1
                    else:
                        raise TenantRepositoryDataError("tenant configuration conflict detected")
    except TenantRepositoryDataError:
        raise
    except TenantRepositoryConfigurationError:
        raise
    except IntegrityError as exc:
        raise TenantRepositoryDataError("tenant configuration conflict detected") from exc
    except OperationalError as exc:
        raise TenantRepositoryUnavailableError("database is not available") from exc
    except SQLAlchemyError as exc:
        raise TenantRepositoryUnavailableError("database operation failed") from exc
    finally:
        await engine.dispose()

    return inserted, skipped


async def _insert_or_skip(conn, config: TenantConfig) -> int:
    """Attempt INSERT with ON CONFLICT DO NOTHING. Returns rowcount (1 if inserted, 0 if skipped).

    When a new tenant is inserted, also writes the v1 snapshot to tenant_config_versions
    in the same transaction, ensuring history integrity.
    """
    result = await conn.execute(
        pg_insert(tenant_configs).values(
            tenant_id=config.tenant_id,
            enabled=config.enabled,
            version=config.version,
            app_id=config.app.app_id,
            instruction=config.app.instruction,
            model_profile=config.app.model_profile,
            allowed_tools=list(config.app.allowed_tools),
            governance=config.governance.model_dump(mode="json"),
            backend_profile=config.backend_profile.model_dump(mode="json"),
            audit_policy=config.audit_policy.model_dump(mode="json"),
        ).on_conflict_do_nothing(index_elements=["tenant_id"]))
    if result.rowcount > 0:
        await conn.execute(tenant_config_versions.insert().values(
            tenant_id=config.tenant_id,
            version=config.version,
            enabled=config.enabled,
            app_id=config.app.app_id,
            instruction=config.app.instruction,
            model_profile=config.app.model_profile,
            allowed_tools=list(config.app.allowed_tools),
            governance=config.governance.model_dump(mode="json"),
            backend_profile=config.backend_profile.model_dump(mode="json"),
            audit_policy=config.audit_policy.model_dump(mode="json"),
        ))
    return result.rowcount


async def _fetch_existing(conn, tenant_id: str):
    result = await conn.execute(sa.select(tenant_configs).where(tenant_configs.c.tenant_id == tenant_id))
    return result.first()


def _rows_match(row, config: TenantConfig) -> bool:
    existing_tools = list(row._mapping["allowed_tools"])
    existing_governance = row._mapping["governance"]
    existing_backend_profile = row._mapping["backend_profile"]
    existing_audit_policy = row._mapping["audit_policy"]
    return (row._mapping["enabled"] == config.enabled and row._mapping["version"] == config.version
            and row._mapping["app_id"] == config.app.app_id and row._mapping["instruction"] == config.app.instruction
            and row._mapping["model_profile"] == config.app.model_profile
            and existing_tools == list(config.app.allowed_tools) and isinstance(existing_governance, dict)
            and existing_governance == config.governance.model_dump(mode="json")
            and isinstance(existing_backend_profile, dict)
            and existing_backend_profile == config.backend_profile.model_dump(mode="json")
            and isinstance(existing_audit_policy, dict)
            and existing_audit_policy == config.audit_policy.model_dump(mode="json"))


__all__ = ["import_tenant_configs"]
