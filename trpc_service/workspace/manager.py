"""Tenant-separated workspace layout for external sandbox runtimes."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from pathlib import Path

from trpc_service.tenant.models import TenantContext


@dataclass(frozen=True, slots=True)
class TenantWorkspace:
    """Opaque, tenant/session-scoped workspace injected into agent/tool code."""

    tenant_id: str
    session_id: str
    path: Path

    @property
    def environment(self) -> dict[str, str]:
        return {"TRPC_WORKSPACE_ROOT": str(self.path)}

    @property
    def metadata(self) -> dict[str, str]:
        # IDs are intentionally not written to the filesystem path or logs;
        # callers use the metadata only for in-process context injection.
        return {"workspace_root": str(self.path)}


class WorkspaceManager:
    def __init__(self, root: Path, *, key: bytes) -> None:
        if len(key) < 32:
            raise ValueError("workspace key must contain at least 32 bytes")
        self._root = root.resolve()
        self._key = key

    def prepare(self, tenant_id: str, session_id: str) -> Path:
        tenant = hmac.new(self._key, tenant_id.encode(), hashlib.sha256).hexdigest()
        session = hmac.new(self._key, session_id.encode(), hashlib.sha256).hexdigest()
        unresolved = self._root / "tenants" / tenant / "sessions" / session / "work"
        current = self._root
        try:
            for component in unresolved.relative_to(self._root).parts:
                current /= component
                if current.is_symlink():
                    raise ValueError("workspace symlinks are disabled")
        except OSError as exc:
            raise ValueError("workspace path cannot be inspected") from exc
        path = unresolved.resolve()
        if self._root not in path.parents:
            raise ValueError("workspace escaped its configured root")
        for directory in (path, path / "inputs", path / "outputs", path / "tmp"):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        return path

    def for_context(self, context: TenantContext) -> TenantWorkspace:
        """Create the isolated workspace associated with one agent turn."""

        path = self.prepare(context.tenant_id, context.session_id)
        return TenantWorkspace(context.tenant_id, context.session_id, path)

    def tool_context(self, context: TenantContext) -> dict[str, str]:
        """Return the explicit environment injection for a governed tool."""

        return self.for_context(context).environment


__all__ = ["TenantWorkspace", "WorkspaceManager"]
