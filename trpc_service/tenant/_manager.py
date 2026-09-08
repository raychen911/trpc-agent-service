# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant configuration manager with optional MySQL/Redis persistence."""

from __future__ import annotations

import threading
import hashlib
import json
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Callable
from typing import Optional

from pydantic import BaseModel
from pydantic import SecretStr

from ._models import Tenant
from ._models import TenantStatus
from ._persistence import MySqlTenantRepository
from ._redis_cache import ConfigOutboxPublisher
from ._redis_cache import RedisTenantConfigCache

ChangeListener = Callable[[str, Optional[Tenant]], None]
"""Callback signature: ``(tenant_id, new_tenant_or_None)``."""


class ConfigVersion(BaseModel):
    """An immutable snapshot of a tenant configuration at a point in time."""

    model_config = {"extra": "forbid"}

    tenant_id: str
    version: int
    """Monotonically increasing version number per tenant."""
    config_snapshot: dict[str, Any]
    """Full serialized configuration snapshot."""
    created_by: str = "system"
    reason: str = ""
    rolled_back: bool = False
    rolled_back_to: Optional[int] = None


def tenant_config_checksum(tenant: Tenant) -> str:
    """Return a deterministic checksum without exposing secret material."""

    def normalize(value: Any) -> Any:
        if isinstance(value, SecretStr):
            return value.get_secret_value()
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(key): normalize(item) for key, item in sorted(value.items())}
        if isinstance(value, list):
            return [normalize(item) for item in value]
        if hasattr(value, "value"):
            return value.value
        return value

    payload = tenant.model_dump(mode="python")
    payload.pop("created_at", None)
    payload.pop("updated_at", None)
    payload = normalize(payload)
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class ConfigDraft(BaseModel):
    """Mutable proposal that does not affect active request processing."""

    model_config = {"extra": "forbid"}

    tenant_id: str
    based_on_version: int
    checksum: str
    config_snapshot: dict[str, Any]
    created_by: str = "system"
    reason: str = ""


class TenantConfigManager:
    """Tenant registry backed by MySQL, with an optional Redis L2 cache.

    Without a repository this preserves the lightweight in-memory behaviour
    used by SDK examples and unit tests.  When a repository is supplied, MySQL
    is authoritative and the local mapping is only a per-process L1 cache.
    """

    def __init__(
        self,
        repository: Optional[MySqlTenantRepository] = None,
        cache: Optional[RedisTenantConfigCache] = None,
        listen_for_changes: bool = True,
        preflight_checks: Optional[list[Callable[[Tenant], None]]] = None,
    ) -> None:
        self._tenants: dict[str, Tenant] = {}
        self._history: dict[str, list[ConfigVersion]] = {}
        self._versions: dict[str, int] = {}
        self._drafts: dict[str, ConfigDraft] = {}
        self._preflight_checks = list(preflight_checks or [])
        self._listeners: list[ChangeListener] = []
        self._lock = threading.RLock()
        self._repository = repository
        self._cache = cache
        self._publisher = ConfigOutboxPublisher(repository, cache) if repository and cache else None
        if repository is not None:
            self.reload()
        if cache is not None and listen_for_changes:
            cache.start_listener(self._on_remote_change)

    # ------------------------------------------------------------------ CRUD

    def register(self, tenant: Tenant, by: str = "system", reason: str = "register") -> Tenant:
        """Register a new tenant. Raises ``ValueError`` if the id already exists."""
        with self._lock:
            if tenant.tenant_id in self._tenants:
                raise ValueError(f"tenant '{tenant.tenant_id}' already registered")
            self._run_preflight(tenant)
            stored = tenant.model_copy(deep=True)
            version = self._repository.create(stored, by, reason) if self._repository else 1
            self._tenants[tenant.tenant_id] = stored
            self._versions[tenant.tenant_id] = version
            if self._repository:
                self._history[tenant.tenant_id] = self._load_history(tenant.tenant_id)
            else:
                self._record_version(stored, by=by, reason=reason, version=version)
        self._refresh_distributed_state(stored, version, "tenant.created")
        self._notify(stored.tenant_id, stored.model_copy(deep=True))
        return stored.model_copy(deep=True)

    def get(self, tenant_id: str) -> Optional[Tenant]:
        """Return a deep copy of the tenant or ``None``."""
        with self._lock:
            tenant = self._tenants.get(tenant_id)
        if tenant is not None:
            return tenant.model_copy(deep=True)
        loaded = self._load_one(tenant_id)
        return loaded.model_copy(deep=True) if loaded else None

    def list(self, active_only: bool = True) -> list[Tenant]:
        """List tenants, optionally filtering to active ones only."""
        with self._lock:
            tenants = list(self._tenants.values())
            if active_only:
                tenants = [t for t in tenants if t.status == TenantStatus.ACTIVE]
            return [t.model_copy(deep=True) for t in tenants]

    def update(self, tenant: Tenant, by: str = "system", reason: str = "update") -> Tenant:
        """Update an existing tenant and record a new config version."""
        with self._lock:
            if tenant.tenant_id not in self._tenants:
                raise ValueError(f"tenant '{tenant.tenant_id}' not found")
            self._run_preflight(tenant)
            stored = tenant.model_copy(deep=True)
            stored.updated_at = datetime.now(timezone.utc)
            current_version = self._versions.get(tenant.tenant_id, 0)
            version = self._repository.update(stored, current_version, by,
                                              reason) if self._repository else current_version + 1
            self._tenants[tenant.tenant_id] = stored
            self._versions[tenant.tenant_id] = version
            self._record_version(stored, by=by, reason=reason, version=version)
        self._refresh_distributed_state(stored, version, "tenant.updated")
        self._notify(stored.tenant_id, stored.model_copy(deep=True))
        return stored.model_copy(deep=True)

    def delete(self, tenant_id: str, by: str = "system", reason: str = "delete") -> None:
        """Remove a tenant and notify listeners with ``None``."""
        with self._lock:
            if tenant_id not in self._tenants:
                raise ValueError(f"tenant '{tenant_id}' not found")
            version = self._versions.get(tenant_id, 0)
            if self._repository:
                self._repository.delete(tenant_id, version)
            self._tenants.pop(tenant_id)
            self._versions.pop(tenant_id, None)
            self._drafts.pop(tenant_id, None)
        self._refresh_distributed_state(None, version + 1, "tenant.deleted", tenant_id)
        self._notify(tenant_id, None)

    # -------------------------------------------------------------- rollback

    def current_version(self, tenant_id: str) -> int:
        """Return the active immutable revision for one tenant."""
        with self._lock:
            version = self._versions.get(tenant_id)
        if version is None:
            loaded = self._load_one(tenant_id)
            if loaded is None:
                raise ValueError(f"tenant '{tenant_id}' not found")
            with self._lock:
                version = self._versions[tenant_id]
        return version

    def get_version(self, tenant_id: str, version: int) -> Optional[Tenant]:
        """Load the exact configuration snapshot bound to an accepted task."""
        try:
            current = self.current_version(tenant_id)
        except ValueError:
            return None
        if version == current:
            return self.get(tenant_id)
        with self._lock:
            history = list(self._history.get(tenant_id, []))
        target = next((item for item in history if item.version == version), None)
        if target is None and self._repository is not None:
            history = self._load_history(tenant_id)
            with self._lock:
                self._history[tenant_id] = history
            target = next((item for item in history if item.version == version), None)
        return Tenant.model_validate(target.config_snapshot) if target is not None else None

    def stage(
        self,
        tenant: Tenant,
        *,
        expected_version: Optional[int] = None,
        by: str = "system",
        reason: str = "",
    ) -> ConfigDraft:
        """Create or replace a draft without changing the active tenant."""
        current = self.current_version(tenant.tenant_id)
        if expected_version is not None and expected_version != current:
            raise ValueError(f"tenant '{tenant.tenant_id}' version conflict")
        draft = ConfigDraft(
            tenant_id=tenant.tenant_id,
            based_on_version=current,
            checksum=tenant_config_checksum(tenant),
            config_snapshot=tenant.model_dump(mode="python"),
            created_by=by,
            reason=reason,
        )
        if self._repository is not None:
            self._repository.save_draft(
                tenant,
                current,
                draft.checksum,
                by,
                reason,
            )
        with self._lock:
            self._drafts[tenant.tenant_id] = draft
        return draft.model_copy(deep=True)

    def get_draft(self, tenant_id: str) -> Optional[ConfigDraft]:
        """Return the current draft, if present."""
        with self._lock:
            draft = self._drafts.get(tenant_id)
        if draft is None and self._repository is not None:
            stored = self._repository.get_draft(tenant_id)
            if stored is not None:
                draft = ConfigDraft(
                    tenant_id=tenant_id,
                    based_on_version=stored.based_on_version,
                    checksum=stored.checksum,
                    config_snapshot=stored.tenant.model_dump(mode="python"),
                    created_by=stored.created_by,
                    reason=stored.reason,
                )
                with self._lock:
                    self._drafts[tenant_id] = draft
        return draft.model_copy(deep=True) if draft is not None else None

    def discard_draft(self, tenant_id: str) -> None:
        """Discard a proposal without touching active configuration."""
        with self._lock:
            removed = self._drafts.pop(tenant_id, None) is not None
        if self._repository is not None:
            removed = self._repository.delete_draft(tenant_id) or removed
        if not removed:
            raise ValueError(f"no config draft for tenant '{tenant_id}'")

    def publish(self, tenant_id: str, *, expected_version: Optional[int] = None, by: str = "system") -> Tenant:
        """Preflight and atomically activate the tenant's staged draft."""
        draft = self.get_draft(tenant_id)
        if draft is None:
            raise ValueError(f"no config draft for tenant '{tenant_id}'")
        current = self.current_version(tenant_id)
        required = draft.based_on_version if expected_version is None else expected_version
        if current != required or current != draft.based_on_version:
            raise ValueError(f"tenant '{tenant_id}' version conflict")
        tenant = Tenant.model_validate(draft.config_snapshot)
        self._run_preflight(tenant)
        reason = draft.reason or "publish draft"
        with self._lock:
            stored = tenant.model_copy(deep=True)
            stored.updated_at = datetime.now(timezone.utc)
            version = (self._repository.publish_draft(stored, current, by, reason) if self._repository else current + 1)
            self._tenants[tenant_id] = stored
            self._versions[tenant_id] = version
            self._record_version(stored, by=by, reason=reason, version=version)
            self._drafts.pop(tenant_id, None)
        self._refresh_distributed_state(stored, version, "tenant.updated")
        self._notify(tenant_id, stored.model_copy(deep=True))
        return stored.model_copy(deep=True)

    def add_preflight_check(self, check: Callable[[Tenant], None]) -> None:
        """Validate current tenants, then guard every future activation."""
        with self._lock:
            existing = [tenant.model_copy(deep=True) for tenant in self._tenants.values()]
        for tenant in existing:
            check(tenant)
        with self._lock:
            self._preflight_checks.append(check)

    def _run_preflight(self, tenant: Tenant) -> None:
        for check in self._preflight_checks:
            check(tenant.model_copy(deep=True))

    def rollback(self, tenant_id: str, to_version: int, by: str = "system") -> Tenant:
        """Restore a tenant to a previous config version.

        The rollback itself is recorded as a new version so it is auditable and
        can itself be rolled forward/backward.
        """
        with self._lock:
            history = self._history.get(tenant_id)
            if not history:
                raise ValueError(f"no config history for tenant '{tenant_id}'")
            target = next((v for v in history if v.version == to_version), None)
            if target is None:
                raise ValueError(f"version {to_version} not found for tenant '{tenant_id}'")
            restored = Tenant.model_validate(target.config_snapshot)
            self._run_preflight(restored)
            restored.updated_at = restored.updated_at or restored.created_at
            current_version = self._versions.get(tenant_id, 0)
            version = self._repository.update(
                restored,
                current_version,
                by,
                f"rollback to v{to_version}",
                rolled_back=True,
                rolled_back_to=to_version,
            ) if self._repository else current_version + 1
            self._tenants[tenant_id] = restored
            self._versions[tenant_id] = version
            self._record_version(
                restored,
                by=by,
                reason=f"rollback to v{to_version}",
                rolled_back=True,
                rolled_back_to=to_version,
                version=version,
            )
        self._refresh_distributed_state(restored, version, "tenant.rolled_back")
        self._notify(tenant_id, restored.model_copy(deep=True))
        return restored.model_copy(deep=True)

    def history(self, tenant_id: str) -> list[ConfigVersion]:
        """Return the version history for a tenant (oldest first)."""
        with self._lock:
            return [version.model_copy(deep=True) for version in self._history.get(tenant_id, [])]

    # -------------------------------------------------------------- listeners

    def subscribe(self, listener: ChangeListener) -> None:
        """Register a change listener invoked after every mutation."""
        with self._lock:
            self._listeners.append(listener)

    def reload(self) -> None:
        """Reload the L1 registry and complete history from MySQL."""
        if self._repository is None:
            return
        tenants = self._repository.list()
        with self._lock:
            self._tenants = {tenant.tenant_id: tenant for tenant, _ in tenants}
            self._versions = {tenant.tenant_id: version for tenant, version in tenants}
            self._history = {}
            for tenant, _ in tenants:
                self._history[tenant.tenant_id] = self._load_history(tenant.tenant_id)

    def close(self) -> None:
        """Release Redis and SQL connections owned by the manager."""
        if self._cache is not None:
            self._cache.close()
        if self._repository is not None:
            self._repository.close()

    # -------------------------------------------------------------- internal

    def _record_version(
        self,
        tenant: Tenant,
        by: str,
        reason: str,
        rolled_back: bool = False,
        rolled_back_to: Optional[int] = None,
        version: Optional[int] = None,
    ) -> None:
        history = self._history.setdefault(tenant.tenant_id, [])
        version = version if version is not None else len(history) + 1
        history.append(
            ConfigVersion(
                tenant_id=tenant.tenant_id,
                version=version,
                # Python mode retains SecretStr wrappers. API serialization masks
                # them, while an in-process rollback still restores the secret.
                config_snapshot=tenant.model_dump(mode="python"),
                created_by=by,
                reason=reason,
                rolled_back=rolled_back,
                rolled_back_to=rolled_back_to,
            ))

    def _load_history(self, tenant_id: str) -> list[ConfigVersion]:
        if self._repository is None:
            return []
        return [
            ConfigVersion(
                tenant_id=item.tenant_id,
                version=item.version,
                config_snapshot=item.tenant.model_dump(mode="python"),
                created_by=item.created_by,
                reason=item.reason,
                rolled_back=item.rolled_back,
                rolled_back_to=item.rolled_back_to,
            ) for item in self._repository.history(tenant_id)
        ]

    def _load_one(self, tenant_id: str) -> Optional[Tenant]:
        loaded = None
        if self._cache is not None:
            try:
                loaded = self._cache.get(tenant_id)
            except Exception:  # pragma: no cover - corrupt/unavailable cache falls back to MySQL
                self._cache.invalidate(tenant_id)
        if loaded is None and self._repository is not None:
            loaded = self._repository.get(tenant_id)
            if loaded and self._cache is not None:
                try:
                    self._cache.set(*loaded)
                except Exception:  # pragma: no cover - Redis is an optional accelerator
                    pass
        if loaded is None:
            return None
        tenant, version = loaded
        with self._lock:
            self._tenants[tenant_id] = tenant
            self._versions[tenant_id] = version
        return tenant

    def _refresh_distributed_state(
        self,
        tenant: Optional[Tenant],
        version: int,
        event_type: str,
        tenant_id: Optional[str] = None,
    ) -> None:
        if self._cache is None:
            return
        try:
            if tenant is None:
                self._cache.invalidate(tenant_id or "")
            else:
                self._cache.set(tenant, version)
            if self._publisher is not None:
                self._publisher.publish_pending()
        except Exception:  # pragma: no cover - outbox remains pending for later replay
            return

    def _on_remote_change(self, tenant_id: str, version: int, event_type: str) -> None:
        """Refresh a node after another node publishes a committed change."""
        with self._lock:
            if self._versions.get(tenant_id, 0) >= version:
                return
        if event_type == "tenant.deleted":
            with self._lock:
                self._tenants.pop(tenant_id, None)
                self._versions[tenant_id] = version
            if self._cache is not None:
                self._cache.invalidate(tenant_id)
            self._notify(tenant_id, None)
            return
        if self._repository is None:
            return
        loaded = self._repository.get(tenant_id)
        if loaded is None:
            return
        tenant, stored_version = loaded
        with self._lock:
            self._tenants[tenant_id] = tenant
            self._versions[tenant_id] = stored_version
        if self._cache is not None:
            self._cache.set(tenant, stored_version)
        self._notify(tenant_id, tenant.model_copy(deep=True))

    def _notify(self, tenant_id: str, tenant: Optional[Tenant]) -> None:
        with self._lock:
            listeners = list(self._listeners)
        for listener in listeners:
            try:
                listener(tenant_id, tenant)
            except Exception:  # pragma: no cover - listener isolation
                # A failing listener must not break tenant mutation.
                continue
