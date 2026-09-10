"""Small tenant-scoped workspace policy contract."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class WorkspacePolicy:
    """Limits passed to a framework sandbox or workspace adapter."""

    root: str = "data/workspaces"
    max_bytes: int = 100 * 1024 * 1024
    allowed_hosts: tuple[str, ...] = field(default_factory=tuple)
    allow_network: bool = False

    def tenant_root(self, tenant_id: str) -> str:
        from trpc_service.security.identifiers import filesystem_component

        return f"{self.root}/{filesystem_component(tenant_id, 'tenant_id')}"
