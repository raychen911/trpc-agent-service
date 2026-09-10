"""租户 SQL 持久化（TenantStore 的 SQL 实现）。"""
from __future__ import annotations

import asyncio
from typing import List, Optional

from trpc_service.config.tenant_config import TenantConfig
from trpc_service.tenant.models import Tenant
from trpc_service.tenant.store import TenantStore


class SqlTenantStore(TenantStore):
    """基于平台 Database 的租户存储（tenant + agent_app 两表）。"""

    def __init__(self, database) -> None:
        self._db = database

    # ---- TenantStore ABC（异步接口） ----

    async def create(self, tenant: Tenant) -> Tenant:
        await asyncio.to_thread(self._upsert_tenant, tenant)
        return tenant

    async def get(self, tenant_id: str) -> Optional[Tenant]:
        return await asyncio.to_thread(self._get, tenant_id)

    async def list_all(self) -> List[Tenant]:
        return await asyncio.to_thread(self._list)

    async def update(self, tenant_id: str, data: dict) -> Optional[Tenant]:
        return await asyncio.to_thread(self._update, tenant_id, data)

    async def delete(self, tenant_id: str) -> bool:
        return await asyncio.to_thread(self._delete, tenant_id)

    # ---- 配置快照便捷方法（同步，供 web 层直接调用） ----

    def save_config(self, tenant_config: TenantConfig) -> None:
        """保存/更新租户（版本化）：tenant 表存最新快照，历史版本写 tenant_revision。"""
        from trpc_service.tenant.storage.tables import AgentAppRow, TenantRevisionRow, TenantRow

        snapshot = tenant_config.model_dump(mode="json")
        with self._db.session() as session:
            row = session.get(TenantRow, tenant_config.tenant_id)
            if row is None:
                revision = 1
                row = TenantRow(
                    tenant_id=tenant_config.tenant_id,
                    name=tenant_config.name,
                    status=tenant_config.status,
                    config=snapshot,
                    revision=revision,
                )
                session.add(row)
            else:
                revision = row.revision + 1
                row.name = tenant_config.name
                row.status = tenant_config.status
                row.config = snapshot
                row.revision = revision
            session.add(
                TenantRevisionRow(
                    tenant_id=tenant_config.tenant_id, revision=revision, config=snapshot
                )
            )
            # agent_app 投影：先清该租户旧应用再登记当前应用（app_name 全局唯一）
            session.query(AgentAppRow).filter_by(tenant_id=tenant_config.tenant_id).delete()
            session.add(
                AgentAppRow(
                    app_name=tenant_config.app.app_name,
                    tenant_id=tenant_config.tenant_id,
                    instruction=tenant_config.app.instruction,
                    model_name=tenant_config.model.model_name,
                )
            )

    def current_revision(self, tenant_id: str) -> int:
        from trpc_service.tenant.storage.tables import TenantRow

        with self._db.session() as session:
            row = session.get(TenantRow, tenant_id)
            return row.revision if row else 0

    def list_revisions(self, tenant_id: str) -> list[dict]:
        """历史版本列表（新→旧），供回滚前查看。"""
        from trpc_service.tenant.storage.tables import TenantRevisionRow

        with self._db.session() as session:
            rows = (
                session.query(TenantRevisionRow)
                .filter_by(tenant_id=tenant_id)
                .order_by(TenantRevisionRow.revision.desc())
                .all()
            )
            return [
                {"revision": r.revision, "created_at": r.created_at.isoformat()} for r in rows
            ]

    def load_revision(self, tenant_id: str, revision: int) -> Optional[TenantConfig]:
        """读取指定历史版本的租户配置。"""
        from trpc_service.tenant.storage.tables import TenantRevisionRow

        with self._db.session() as session:
            row = (
                session.query(TenantRevisionRow)
                .filter_by(tenant_id=tenant_id, revision=revision)
                .first()
            )
            if row is None:
                return None
            return TenantConfig.model_validate(row.config)

    def rollback_config(self, tenant_id: str, revision: int) -> Optional[TenantConfig]:
        """回滚到指定版本：读历史 → 作为新版本重新保存（回滚本身也可再回滚）。"""
        old = self.load_revision(tenant_id, revision)
        if old is None:
            return None
        self.save_config(old)
        return old

    def load_all_configs(self) -> List[TenantConfig]:
        """恢复全部租户配置（损坏快照跳过并告警，不阻断启动）。"""
        from trpc_service.log import get_logger
        from trpc_service.tenant.storage.tables import TenantRow

        configs: List[TenantConfig] = []
        with self._db.session() as session:
            rows = session.query(TenantRow).all()
            for row in rows:
                try:
                    configs.append(TenantConfig.model_validate(row.config))
                except Exception as exc:
                    get_logger("tenant.store").error(
                        "skip invalid tenant snapshot tenant=%s err=%s", row.tenant_id, exc
                    )
        return configs

    def delete_config(self, tenant_id: str) -> bool:
        from trpc_service.tenant.storage.tables import AgentAppRow, TenantRow

        with self._db.session() as session:
            deleted = (
                session.query(TenantRow)
                .filter_by(tenant_id=tenant_id)
                .delete(synchronize_session=False)
            )
            session.query(AgentAppRow).filter_by(tenant_id=tenant_id).delete(
                synchronize_session=False
            )
        return deleted > 0

    # ---- ABC 内部实现 ----

    def _upsert_tenant(self, tenant: Tenant) -> None:
        from trpc_service.tenant.storage.tables import TenantRow

        with self._db.session() as session:
            row = session.get(TenantRow, tenant.tenant_id)
            if row is None:
                session.add(
                    TenantRow(
                        tenant_id=tenant.tenant_id,
                        name=tenant.name,
                        status=tenant.status,
                        config=dict(tenant.config or {}),
                    )
                )
            else:
                row.name = tenant.name
                row.status = tenant.status
                if tenant.config:
                    row.config = dict(tenant.config)

    def _get(self, tenant_id: str) -> Optional[Tenant]:
        from trpc_service.tenant.storage.tables import TenantRow

        with self._db.session() as session:
            row = session.get(TenantRow, tenant_id)
            return Tenant(
                tenant_id=row.tenant_id, name=row.name, status=row.status, config=row.config
            ) if row else None

    def _list(self) -> List[Tenant]:
        from trpc_service.tenant.storage.tables import TenantRow

        with self._db.session() as session:
            rows = session.query(TenantRow).all()
            return [
                Tenant(tenant_id=r.tenant_id, name=r.name, status=r.status, config=r.config)
                for r in rows
            ]

    def _update(self, tenant_id: str, data: dict) -> Optional[Tenant]:
        from trpc_service.tenant.storage.tables import TenantRow

        with self._db.session() as session:
            row = session.get(TenantRow, tenant_id)
            if row is None:
                return None
            if "name" in data:
                row.name = data["name"]
            if "status" in data:
                row.status = data["status"]
            if "config" in data:
                row.config = dict(data["config"])
            return Tenant(
                tenant_id=row.tenant_id, name=row.name, status=row.status, config=row.config
            )

    def _delete(self, tenant_id: str) -> bool:
        return self.delete_config(tenant_id)
