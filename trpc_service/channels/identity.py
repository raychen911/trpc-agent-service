"""Durable channel identity and conversation resolution."""

from dataclasses import dataclass
from uuid import UUID, NAMESPACE_URL, uuid5

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.channels.contracts import (
    ChannelBindingConfig,
    IncomingMessage,
)
from trpc_service.channels.models import (
    ChannelConversation,
    ChannelIdentity,
    ChannelPrincipal,
    ConversationMember,
)


@dataclass(frozen=True, slots=True)
class ResolvedChannelContext:
    """Internal identities resolved from one provider-specific message."""

    principal_id: UUID
    conversation_id: UUID
    session_id: str


class PostgreSQLChannelIdentityService:
    """Resolve provider IDs into deterministic tenant-scoped identities."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def resolve(
        self,
        binding: ChannelBindingConfig,
        message: IncomingMessage,
    ) -> ResolvedChannelContext:
        """Upsert one sender, provider identity, conversation and membership."""

        scope = f"{binding.tenant_id}:{binding.binding_id}"
        principal_id = uuid5(NAMESPACE_URL, f"trpc:principal:{scope}:{message.principal_id}")
        identity_id = uuid5(NAMESPACE_URL, f"trpc:identity:{scope}:{message.principal_id}")
        conversation_id = uuid5(
            NAMESPACE_URL,
            f"trpc:conversation:{scope}:{message.conversation_id}",
        )
        display_name = message.attributes.get("display_name")
        conversation_kind = self._conversation_kind(message)

        async def upsert(database: AsyncSession) -> None:
            await database.merge(
                ChannelPrincipal(
                    tenant_id=binding.tenant_id,
                    principal_id=principal_id,
                    principal_type="USER",
                    display_name=display_name if isinstance(display_name, str) else None,
                    status="ACTIVE",
                    attributes={},
                ))
            await database.merge(
                ChannelIdentity(
                    tenant_id=binding.tenant_id,
                    identity_id=identity_id,
                    binding_id=binding.binding_id,
                    principal_id=principal_id,
                    provider_principal_id=message.principal_id,
                    status="ACTIVE",
                    last_seen_at=message.occurred_at,
                    attributes={},
                ))
            await database.merge(
                ChannelConversation(
                    tenant_id=binding.tenant_id,
                    conversation_id=conversation_id,
                    binding_id=binding.binding_id,
                    provider_conversation_id=message.conversation_id,
                    kind=conversation_kind,
                    status="ACTIVE",
                    last_message_at=message.occurred_at,
                    attributes={},
                ))
            await database.merge(
                ConversationMember(
                    tenant_id=binding.tenant_id,
                    conversation_id=conversation_id,
                    principal_id=principal_id,
                    role="MEMBER",
                    status="ACTIVE",
                ))

        for attempt in range(2):
            try:
                async with self._sessions.begin() as database:
                    await upsert(database)
                break
            except IntegrityError:
                # Deterministic IDs make concurrent first-message inserts
                # converge. After the winning transaction commits, one retry
                # changes every merge into an update.
                if attempt == 1:
                    raise
        return ResolvedChannelContext(
            principal_id=principal_id,
            conversation_id=conversation_id,
            session_id=f"{binding.binding_id}:{conversation_id}",
        )

    @staticmethod
    def _conversation_kind(message: IncomingMessage) -> str:
        configured = message.attributes.get("conversation_kind")
        if isinstance(configured, str):
            normalized = configured.strip().upper()
            if normalized in {"DIRECT", "GROUP", "THREAD"}:
                return normalized
        return "GROUP" if message.conversation_id.startswith("group:") else "DIRECT"
