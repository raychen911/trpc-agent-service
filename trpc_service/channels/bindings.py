import asyncio
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from trpc_service.domain import AppStatus, ChannelType, TenantStatus
from trpc_service.storage.models import AgentApp, ChannelBinding, Tenant


class ChannelBindingNotFoundError(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class ResolvedChannelBinding:
    id: str
    tenant_id: str
    agent_app_id: str
    channel_type: ChannelType
    account_id: str
    webhook_path: str
    token_secret_ref: str | None
    secret_ref: str | None
    options: dict[str, Any]


class ChannelBindingRepository:
    def __init__(self, factory: sessionmaker[Session]) -> None:
        self._factory = factory

    async def resolve(self, channel_type: ChannelType, account_id: str) -> ResolvedChannelBinding:
        return await asyncio.to_thread(self._resolve_sync, channel_type, account_id)

    async def list_active_for_app(
        self, tenant_id: str, agent_app_id: str
    ) -> list[ResolvedChannelBinding]:
        return await asyncio.to_thread(self._list_active_for_app_sync, tenant_id, agent_app_id)

    def _list_active_for_app_sync(
        self, tenant_id: str, agent_app_id: str
    ) -> list[ResolvedChannelBinding]:
        with self._factory() as session:
            rows = list(
                session.scalars(
                    select(ChannelBinding)
                    .join(
                        AgentApp,
                        (AgentApp.id == ChannelBinding.agent_app_id)
                        & (AgentApp.tenant_id == ChannelBinding.tenant_id),
                    )
                    .where(
                        ChannelBinding.tenant_id == tenant_id,
                        ChannelBinding.agent_app_id == agent_app_id,
                        ChannelBinding.enabled.is_(True),
                        ChannelBinding.config_version == AgentApp.active_version,
                    )
                    .order_by(ChannelBinding.channel_type, ChannelBinding.account_id)
                )
            )
        return [
            ResolvedChannelBinding(
                id=row.id,
                tenant_id=row.tenant_id,
                agent_app_id=row.agent_app_id,
                channel_type=row.channel_type,
                account_id=row.account_id,
                webhook_path=row.webhook_path,
                token_secret_ref=row.token_secret_ref,
                secret_ref=row.secret_ref,
                options=dict(row.options),
            )
            for row in rows
        ]

    def _resolve_sync(self, channel_type: ChannelType, account_id: str) -> ResolvedChannelBinding:
        with self._factory() as session:
            row = session.scalar(
                select(ChannelBinding)
                .join(
                    AgentApp,
                    (AgentApp.id == ChannelBinding.agent_app_id)
                    & (AgentApp.tenant_id == ChannelBinding.tenant_id),
                )
                .join(Tenant, Tenant.id == ChannelBinding.tenant_id)
                .where(
                    ChannelBinding.channel_type == channel_type,
                    ChannelBinding.account_id == account_id,
                    ChannelBinding.enabled.is_(True),
                    ChannelBinding.config_version == AgentApp.active_version,
                    AgentApp.status == AppStatus.ACTIVE,
                    Tenant.status == TenantStatus.ACTIVE,
                )
            )
            if row is None:
                raise ChannelBindingNotFoundError(
                    f"no active {channel_type.value} binding for account {account_id}"
                )
            return ResolvedChannelBinding(
                id=row.id,
                tenant_id=row.tenant_id,
                agent_app_id=row.agent_app_id,
                channel_type=row.channel_type,
                account_id=row.account_id,
                webhook_path=row.webhook_path,
                token_secret_ref=row.token_secret_ref,
                secret_ref=row.secret_ref,
                options=dict(row.options),
            )
