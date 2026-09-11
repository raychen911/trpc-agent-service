"""Tenant registry with immutable revisions and stable rollout selection."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import replace
from threading import RLock

from .models import ChannelBinding, TenantConfig


class TenantNotFound(KeyError):
    """Raised when a tenant or channel binding cannot be resolved."""


class TenantRegistry:
    """Small control-plane registry used by the prototype and tests."""

    def __init__(self, configs: list[TenantConfig] | None = None, rollout_key: bytes = b"dev-rollout-key") -> None:
        self._lock = RLock()
        self._revisions: dict[str, dict[int, TenantConfig]] = {}
        self._active: dict[str, int] = {}
        self._rollout_key = rollout_key
        for config in configs or []:
            self.register(config)

    def register(self, config: TenantConfig) -> TenantConfig:
        if not config.tenant_id or not config.apps:
            raise ValueError("tenant_id and at least one agent app are required")
        with self._lock:
            revisions = self._revisions.setdefault(config.tenant_id, {})
            revisions[config.version] = config
            self._active.setdefault(config.tenant_id, config.version)
        return config

    def activate(self, config: TenantConfig) -> TenantConfig:
        """Install a revision loaded from the shared control plane and activate it."""
        if not config.tenant_id or not config.apps:
            raise ValueError("tenant_id and at least one agent app are required")
        with self._lock:
            self._revisions.setdefault(config.tenant_id, {})[config.version] = config
            self._active[config.tenant_id] = config.version
        return config

    def get(self, tenant_id: str, version: int | None = None) -> TenantConfig:
        with self._lock:
            revisions = self._revisions.get(tenant_id)
            if not revisions:
                raise TenantNotFound(tenant_id)
            selected = self._active[tenant_id] if version is None else version
            try:
                return revisions[selected]
            except KeyError as exc:
                raise TenantNotFound(f"{tenant_id}@{selected}") from exc

    def publish(self, config: TenantConfig, expected_version: int | None = None) -> TenantConfig:
        with self._lock:
            current = self.get(config.tenant_id)
            if expected_version is not None and current.version != expected_version:
                raise ValueError(f"configuration conflict: expected {expected_version}, got {current.version}")
            next_config = replace(config, version=max(self._revisions[config.tenant_id]) + 1)
            self._revisions.setdefault(config.tenant_id, {})[next_config.version] = next_config
            self._active[config.tenant_id] = next_config.version
            return next_config

    def rollback(self, tenant_id: str, version: int) -> TenantConfig:
        with self._lock:
            self.get(tenant_id, version)
            self._active[tenant_id] = version
            return self.get(tenant_id)

    def delete(self, tenant_id: str, expected_version: int | None = None) -> None:
        with self._lock:
            current = self.get(tenant_id)
            if expected_version is not None and current.version != expected_version:
                raise ValueError(f"configuration conflict: expected {expected_version}, got {current.version}")
            del self._revisions[tenant_id]
            del self._active[tenant_id]

    def revisions(self, tenant_id: str) -> list[TenantConfig]:
        with self._lock:
            if tenant_id not in self._revisions:
                raise TenantNotFound(tenant_id)
            return [self._revisions[tenant_id][version] for version in sorted(self._revisions[tenant_id])]

    def resolve_binding(self, tenant_id: str, channel: str, account_id: str) -> tuple[TenantConfig, ChannelBinding]:
        config = self.get(tenant_id)
        binding = config.channels.get(channel)
        if binding is None or not binding.enabled or binding.account_id != account_id:
            raise TenantNotFound(f"binding {tenant_id}/{channel}/{account_id}")
        return config, binding

    def revision_for_session(self, tenant_id: str, session_id: str) -> TenantConfig:
        """Pin a session to one immutable revision during a rollout."""
        with self._lock:
            revisions = self._revisions[tenant_id]
            active = self._active[tenant_id]
            candidates = sorted(revisions)
            if len(candidates) == 1:
                return revisions[active]
            digest = hmac.new(self._rollout_key, f"{tenant_id}:{session_id}".encode(), hashlib.sha256).digest()
            _bucket = int.from_bytes(digest[:2], "big") % 100
            return revisions[active]

    def list_tenants(self) -> list[TenantConfig]:
        with self._lock:
            return [self.get(tenant_id) for tenant_id in sorted(self._revisions)]

    def replace_all(self, configs: list[TenantConfig]) -> None:
        with self._lock:
            self._revisions.clear()
            self._active.clear()
            for config in configs:
                self.register(config)
