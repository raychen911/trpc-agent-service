"""
Data Migration and Dual-Write Synchronization Manager.
Supports smooth migration from Redis to SQL (e.g. active cache to persistent DB)
and from local in-memory vectors to remote vector databases.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
import logging
from trpc_service.storage.base import BaseStorageAdapter
from trpc_service.config.models import Session, SessionEvent, MemoryItem

logger = logging.getLogger(__name__)


@dataclass
class MigrationStats:
    tenant_id: str
    sessions_migrated: int = 0
    events_migrated: int = 0
    memories_migrated: int = 0
    errors: int = 0
    status: str = "completed"


class StorageMigrationManager:
    """
    Orchestrates the 4-phase zero-downtime data migration:
    1. Dual-Write (新旧双写)
    2. Backfill Historical Data (全量历史数据搬迁)
    3. Verification & Read Switch (数据一致性校验与切读)
    4. Decommission Old Backend (安全下线旧后端)
    """

    def __init__(self):
        self._dual_write_enabled: Dict[str, bool] = {}  # tenant_id -> bool

    def enable_dual_write(self, tenant_id: str):
        """Enable dual-write mode where writes hit both source and target."""
        self._dual_write_enabled[tenant_id] = True
        logger.info("[Migration] Enabled dual-write for tenant '%s'", tenant_id)

    def is_dual_write_enabled(self, tenant_id: str) -> bool:
        return self._dual_write_enabled.get(tenant_id, False)

    async def migrate_sessions(
        self,
        tenant_id: str,
        source: BaseStorageAdapter,
        target: BaseStorageAdapter,
        session_ids: List[str],
    ) -> MigrationStats:
        """Migrate sessions and their corresponding event history from source to target."""
        stats = MigrationStats(tenant_id=tenant_id)

        for sid in session_ids:
            try:
                # 1. Fetch Session from source
                s = await source.get_session(tenant_id, sid)
                if not s:
                    continue

                # 2. Save Session to target
                await target.save_session(s)
                stats.sessions_migrated += 1

                # 3. Fetch chronological events
                events = await source.get_events(tenant_id, sid, limit=500)
                if events:
                    # Clean/Prepare events for target insertion
                    target_session = await target.get_session(tenant_id, sid)
                    exp_version = target_session.current_version if target_session else 0
                    success = await target.append_events(tenant_id, sid, events, expected_version=exp_version)
                    if success:
                        stats.events_migrated += len(events)
                    else:
                        stats.errors += 1
            except Exception as e:
                logger.error("[Migration] Failed migrating session %s: %s", sid, e)
                stats.errors += 1

        logger.info("[Migration] Migration completed for tenant %s: %s", tenant_id, stats)
        return stats

    async def migrate_memories(
        self,
        tenant_id: str,
        user_ids: List[str],
        source: BaseStorageAdapter,
        target: BaseStorageAdapter,
    ) -> int:
        """Migrate vector semantic memories from source to target."""
        count = 0
        for uid in user_ids:
            memories = await source.search_memory(tenant_id, uid, query="", top_k=200)
            for mem in memories:
                await target.add_memory(mem)
                count += 1
        return count

    async def verify_integrity(
        self,
        tenant_id: str,
        session_id: str,
        source: BaseStorageAdapter,
        target: BaseStorageAdapter,
    ) -> bool:
        """Verify data parity between source and target before switching traffic."""
        s_src = await source.get_session(tenant_id, session_id)
        s_tgt = await target.get_session(tenant_id, session_id)
        if not s_src or not s_tgt:
            return False

        if s_src.current_version != s_tgt.current_version:
            return False

        evts_src = await source.get_events(tenant_id, session_id)
        evts_tgt = await target.get_events(tenant_id, session_id)
        return len(evts_src) == len(evts_tgt)


migration_manager = StorageMigrationManager()
