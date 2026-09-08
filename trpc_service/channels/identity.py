import asyncio
import uuid
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from trpc_service.channels.bindings import ResolvedChannelBinding
from trpc_service.domain import ChannelType
from trpc_service.storage.models import AgentApp, ChannelBinding, ImUserIdentity

IdentityMode = Literal["passthrough", "strict", "auto"]


class ImIdentityMappingNotFoundError(LookupError):
    pass


class ImAccountNotBoundError(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class ResolvedImIdentity:
    internal_user_id: str
    external_user_id: str
    mapped: bool


class ImIdentityRepository:
    def __init__(self, factory: sessionmaker[Session]) -> None:
        self._factory = factory

    async def resolve(
        self, binding: ResolvedChannelBinding, external_user_id: str
    ) -> ResolvedImIdentity:
        mode = str(binding.options.get("identity_mode", "passthrough"))
        if mode not in {"passthrough", "strict", "auto"}:
            raise ValueError(f"unsupported identity_mode: {mode}")
        return await asyncio.to_thread(self._resolve_sync, binding, external_user_id, mode)

    def _resolve_sync(
        self,
        binding: ResolvedChannelBinding,
        external_user_id: str,
        mode: IdentityMode,
    ) -> ResolvedImIdentity:
        with self._factory() as session:
            row = session.scalar(
                select(ImUserIdentity).where(
                    ImUserIdentity.tenant_id == binding.tenant_id,
                    ImUserIdentity.channel_type == binding.channel_type,
                    ImUserIdentity.account_id == binding.account_id,
                    ImUserIdentity.external_user_id == external_user_id,
                    ImUserIdentity.status == "active",
                )
            )
            if row is not None:
                return ResolvedImIdentity(row.internal_user_id, external_user_id, True)
            if mode == "strict":
                raise ImIdentityMappingNotFoundError(
                    "IM user is not mapped for this tenant account"
                )
            if mode == "passthrough":
                return ResolvedImIdentity(external_user_id, external_user_id, False)
            internal_id = "im-" + str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    ":".join(
                        (
                            binding.tenant_id,
                            binding.channel_type.value,
                            binding.account_id,
                            external_user_id,
                        )
                    ),
                )
            )
            session.add(
                ImUserIdentity(
                    tenant_id=binding.tenant_id,
                    channel_type=binding.channel_type,
                    account_id=binding.account_id,
                    external_user_id=external_user_id,
                    internal_user_id=internal_id,
                )
            )
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                row = session.scalar(
                    select(ImUserIdentity).where(
                        ImUserIdentity.tenant_id == binding.tenant_id,
                        ImUserIdentity.channel_type == binding.channel_type,
                        ImUserIdentity.account_id == binding.account_id,
                        ImUserIdentity.external_user_id == external_user_id,
                    )
                )
                if row is None:
                    raise
                internal_id = row.internal_user_id
            return ResolvedImIdentity(internal_id, external_user_id, True)

    def upsert(
        self,
        tenant_id: str,
        channel_type: ChannelType,
        account_id: str,
        external_user_id: str,
        internal_user_id: str,
        display_name: str | None,
        attributes: dict[str, Any],
    ) -> ImUserIdentity:
        with self._factory() as session:
            self._require_bound_account(session, tenant_id, channel_type, account_id)
            row = session.scalar(
                select(ImUserIdentity).where(
                    ImUserIdentity.tenant_id == tenant_id,
                    ImUserIdentity.channel_type == channel_type,
                    ImUserIdentity.account_id == account_id,
                    ImUserIdentity.external_user_id == external_user_id,
                )
            )
            if row is None:
                row = ImUserIdentity(
                    tenant_id=tenant_id,
                    channel_type=channel_type,
                    account_id=account_id,
                    external_user_id=external_user_id,
                    internal_user_id=internal_user_id,
                )
                session.add(row)
            row.internal_user_id = internal_user_id
            row.display_name = display_name
            row.attributes = attributes
            row.status = "active"
            session.commit()
            session.refresh(row)
            session.expunge(row)
            return row

    def list(
        self, tenant_id: str, channel_type: ChannelType | None, account_id: str | None
    ) -> list[ImUserIdentity]:
        with self._factory() as session:
            statement = select(ImUserIdentity).where(ImUserIdentity.tenant_id == tenant_id)
            if channel_type is not None:
                statement = statement.where(ImUserIdentity.channel_type == channel_type)
            if account_id is not None:
                statement = statement.where(ImUserIdentity.account_id == account_id)
            rows = list(session.scalars(statement.order_by(ImUserIdentity.created_at)))
            for row in rows:
                session.expunge(row)
            return rows

    @staticmethod
    def _require_bound_account(
        session: Session, tenant_id: str, channel_type: ChannelType, account_id: str
    ) -> None:
        row = session.scalar(
            select(ChannelBinding)
            .join(
                AgentApp,
                (AgentApp.id == ChannelBinding.agent_app_id)
                & (AgentApp.tenant_id == ChannelBinding.tenant_id),
            )
            .where(
                ChannelBinding.tenant_id == tenant_id,
                ChannelBinding.channel_type == channel_type,
                ChannelBinding.account_id == account_id,
                ChannelBinding.enabled.is_(True),
                ChannelBinding.config_version == AgentApp.active_version,
            )
        )
        if row is None:
            raise ImAccountNotBoundError("IM account has no active binding for this tenant")
