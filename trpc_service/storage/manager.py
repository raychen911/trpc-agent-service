"""Per-tenant storage profile resolver."""

from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from threading import RLock

from trpc_service.storage.factory import (
    StorageBundle,
    create_mirrored_storage,
    create_storage,
)
from trpc_service.tenant.models import TenantConfig


class TenantStorageManager:
    def __init__(self, data_dir: str | Path = "data") -> None:
        self.data_dir = Path(data_dir)
        self._bundles: dict[tuple, StorageBundle] = {}
        self._lock = RLock()

    def get(self, config: TenantConfig) -> StorageBundle:
        profile = config.storage_profile
        migration = self._migration_for(config.tenant_id)
        migration_backend = migration.get("target_backend", "") if migration else ""
        migration_phase = migration.get("phase", "") if migration else ""
        if migration_backend and migration_phase in {"cutover", "verify", "cleanup"}:
            profile = deepcopy(profile)
            profile.session_backend = migration_backend
            profile.memory_backend = migration_backend
            profile.summary_backend = migration_backend
            profile.audit_backend = migration_backend
            if migration_backend in {"postgres", "postgresql", "sql", "sqlite", "redis", "memory"}:
                profile.knowledge_backend = migration_backend if profile.knowledge_backend in {
                    "memory", "vector", "local", "redis", "postgres", "postgresql", "sql"
                } else profile.knowledge_backend
        key = (
            config.tenant_id,
            profile.session_backend,
            profile.memory_backend,
            profile.summary_backend,
            profile.knowledge_backend,
            profile.artifact_backend,
            profile.audit_backend,
            profile.redis_url,
            profile.redis_url_ref,
            profile.sql_dsn,
            profile.sql_dsn_ref,
            profile.knowledge_collection,
            profile.external_memory_url,
            profile.external_memory_token_ref,
            profile.vector_url,
            profile.vector_token_ref,
            profile.vector_provider,
            profile.vector_dimension,
            profile.embedding_url,
            profile.embedding_model,
            profile.embedding_token_ref,
            profile.object_endpoint,
            profile.object_bucket,
            profile.object_region,
            profile.object_access_key_ref,
            profile.object_secret_key_ref,
        )
        with self._lock:
            if key not in self._bundles:
                tenant_dir = self.data_dir / "tenants" / config.tenant_id
                migration_backend = (
                    migration_backend
                    if migration_phase == "dual_write"
                    else os.getenv("MIGRATION_DUAL_WRITE_BACKEND", "").strip()
                )
                if migration_backend:
                    target = deepcopy(profile)
                    target.session_backend = migration_backend
                    target.memory_backend = migration_backend
                    target.summary_backend = migration_backend
                    target.audit_backend = migration_backend
                    target.knowledge_backend = os.getenv(
                        "MIGRATION_DUAL_WRITE_KNOWLEDGE_BACKEND",
                        target.knowledge_backend,
                    )
                    self._bundles[key] = create_mirrored_storage(profile, target, tenant_dir)
                else:
                    self._bundles[key] = create_storage(profile, tenant_dir)
            return self._bundles[key]

    @staticmethod
    def _migration_for(tenant_id: str) -> dict | None:
        path = os.getenv("MIGRATION_STATE_FILE", "").strip()
        if not path:
            return None
        try:
            data = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError):
            return None
        if str(data.get("tenant_id", "")) != tenant_id:
            return None
        return data

    def close(self) -> None:
        with self._lock:
            for bundle in self._bundles.values():
                bundle.close()
            self._bundles.clear()
