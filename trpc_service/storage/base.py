"""
Abstract Base Storage Adapter interface.
"""

from abc import ABC, abstractmethod
from typing import List, Optional
from trpc_service.config.models import (
    Session,
    SessionEvent,
    MemoryItem,
    SummaryItem,
    ArtifactItem,
    AuditLogEntry,
)


class BaseStorageAdapter(ABC):
    """
    Unified Data Access Layer interface for Sessions, Events, Memory,
    Summaries, Artifacts, Audit Logs, and Distributed Locks.
    """

    # --- Lock & Idempotency ---
    @abstractmethod
    async def acquire_session_lock(self, session_id: str, ttl_seconds: float = 30.0) -> bool:
        """Acquire distributed exclusive lock on session to prevent concurrent race conditions."""
        pass

    @abstractmethod
    async def release_session_lock(self, session_id: str) -> bool:
        """Release session lock."""
        pass

    @abstractmethod
    async def check_and_set_idempotency(self, idempotency_key: str, ttl_seconds: float = 60.0) -> bool:
        """
        Check if an inbound message has already been processed.
        Returns True if first time (lock acquired), False if duplicate (should be dropped).
        """
        pass

    # --- Session & Event Sourcing ---
    @abstractmethod
    async def get_session(self, tenant_id: str, session_id: str) -> Optional[Session]:
        """Retrieve session by tenant_id and session_id."""
        pass

    @abstractmethod
    async def save_session(self, session: Session) -> Session:
        """Save or update session metadata with optimistic version bump."""
        pass

    @abstractmethod
    async def append_events(
        self,
        tenant_id: str,
        session_id: str,
        events: List[SessionEvent],
        expected_version: int,
    ) -> bool:
        """
        Atomically append new events to the session history.
        Must verify expected_version to guarantee order and consistency.
        """
        pass

    @abstractmethod
    async def get_events(
        self,
        tenant_id: str,
        session_id: str,
        limit: int = 50,
    ) -> List[SessionEvent]:
        """Retrieve chronological event history for a session."""
        pass

    # --- Long-term Memory & Vector ---
    @abstractmethod
    async def add_memory(self, memory: MemoryItem) -> MemoryItem:
        """Persist a semantic memory item."""
        pass

    @abstractmethod
    async def search_memory(
        self,
        tenant_id: str,
        user_id: str,
        query: str,
        top_k: int = 5,
    ) -> List[MemoryItem]:
        """Search relevant long-term memory items for user."""
        pass

    # --- Summary ---
    @abstractmethod
    async def save_summary(self, summary: SummaryItem) -> SummaryItem:
        """Save a new session summary."""
        pass

    @abstractmethod
    async def get_latest_summary(self, tenant_id: str, session_id: str) -> Optional[SummaryItem]:
        """Get the latest condensed summary for a session."""
        pass

    # --- Artifacts ---
    @abstractmethod
    async def save_artifact(self, artifact: ArtifactItem) -> ArtifactItem:
        """Save generated artifact metadata."""
        pass

    # --- Audit Log ---
    @abstractmethod
    async def record_audit_log(self, entry: AuditLogEntry) -> None:
        """Append an audit log record for governance and security."""
        pass

    @abstractmethod
    async def query_audit_logs(
        self,
        tenant_id: str,
        limit: int = 50,
    ) -> List[AuditLogEntry]:
        """Query recent audit logs for a tenant."""
        pass
