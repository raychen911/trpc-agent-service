"""Resolve tenant storage profiles to concrete backend implementations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..tenant import StorageProfile, TenantConfig


@dataclass(frozen=True)
class RoutedStorage:
    session: Any
    memory: Any
    audit: Any
    vector: Any | None
    object_store: Any | None


class StorageRouter:
    def __init__(self) -> None:
        self._backends: dict[tuple[str, str], Any] = {}

    def register(self, name: str, backend: Any, *, kind: str | None = None) -> None:
        if not name or backend is None:
            raise ValueError("storage backend name and instance are required")
        if kind is not None and kind not in {"session", "memory", "audit", "vector", "object_store"}:
            raise ValueError(f"unsupported storage backend kind: {kind}")
        self._backends[(kind or "*", name)] = backend

    def get(self, name: str, *, kind: str | None = None) -> Any:
        key = (kind or "*", name)
        if key in self._backends:
            return self._backends[key]
        fallback = ("*", name)
        if fallback in self._backends:
            return self._backends[fallback]
        label = f"{kind}:{name}" if kind else name
        raise KeyError(f"storage backend is not registered: {label}")

    def validate(self, tenant: TenantConfig | StorageProfile) -> None:
        profile = tenant.storage if isinstance(tenant, TenantConfig) else tenant
        for kind in ("session", "memory", "audit"):
            self.get(getattr(profile, kind), kind=kind)
        if profile.vector != "none":
            self.get(profile.vector, kind="vector")
        if profile.object_store != "none":
            self.get(profile.object_store, kind="object_store")

    def route(self, tenant: TenantConfig | StorageProfile) -> RoutedStorage:
        profile = tenant.storage if isinstance(tenant, TenantConfig) else tenant
        return RoutedStorage(
            session=self.get(profile.session, kind="session"),
            memory=self.get(profile.memory, kind="memory"),
            audit=self.get(profile.audit, kind="audit"),
            vector=None if profile.vector == "none" else self.get(profile.vector, kind="vector"),
            object_store=(
                None if profile.object_store == "none" else self.get(profile.object_store, kind="object_store")
            ),
        )
