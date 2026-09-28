"""Provider-neutral contracts for local, container and sandbox workspaces."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from trpc_service.tenant.context import TenantContext


class WorkspaceKind(StrEnum):
    """Execution isolation modes supported by future workspace adapters."""

    LOCAL = "local"
    CONTAINER = "container"
    SANDBOX = "sandbox"


@dataclass(frozen=True, slots=True)
class WorkspaceHandle:
    """Immutable scoped reference returned by a concrete workspace provider."""

    tenant_id: UUID
    agent_app_id: UUID
    workspace_id: str
    kind: WorkspaceKind
    location: str

    def __post_init__(self) -> None:
        if not self.workspace_id.strip():
            raise ValueError("workspace ID cannot be empty")
        if not self.location.strip():
            raise ValueError("workspace location cannot be empty")


class WorkspaceProvider(ABC):
    """Lifecycle and bounded file access for local/container/sandbox adapters."""

    @abstractmethod
    async def acquire(self, context: TenantContext, workspace_id: str) -> WorkspaceHandle:
        """Acquire an isolated workspace inside the mandatory tenant scope."""

    @abstractmethod
    async def release(self, handle: WorkspaceHandle) -> None:
        """Release provider resources without weakening tenant isolation."""

    @abstractmethod
    async def list_files(
        self,
        handle: WorkspaceHandle,
        relative_path: str,
        *,
        max_results: int,
        max_entries: int,
    ) -> tuple[str, ...]:
        """List bounded relative file names without exposing provider paths."""

    @abstractmethod
    async def read_text(
        self,
        handle: WorkspaceHandle,
        relative_path: str,
        *,
        max_bytes: int,
    ) -> str:
        """Read bounded UTF-8 text from the isolated workspace."""
