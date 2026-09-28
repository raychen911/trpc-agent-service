from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from trpc_service.channels import ChannelBindingConfig, IncomingMessage, MessageKind
from trpc_service.channels.identity import PostgreSQLChannelIdentityService
from trpc_service.channels.models import ChannelConversation, ChannelIdentity, ConversationMember
from trpc_service.storage.orm import Base


def _binding(*, tenant_id=None) -> ChannelBindingConfig:
    return ChannelBindingConfig(
        binding_id=uuid4(),
        tenant_id=tenant_id or uuid4(),
        agent_app_id=uuid4(),
        channel_type="wecom",
    )


def _message(user: str, conversation: str) -> IncomingMessage:
    return IncomingMessage(
        external_message_id=str(uuid4()),
        principal_id=user,
        conversation_id=conversation,
        kind=MessageKind.TEXT,
        occurred_at=datetime.now(timezone.utc),
        text="hello",
        attributes={
            "display_name": f"User {user}",
            "conversation_kind": "group"
        },
    )


@pytest.mark.anyio
async def test_identity_and_conversation_resolution_is_idempotent_and_scoped() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = PostgreSQLChannelIdentityService(sessions)
    binding = _binding()

    first = await service.resolve(binding, _message("external-user-1", "group-1"))
    replay = await service.resolve(binding, _message("external-user-1", "group-1"))

    assert replay.principal_id == first.principal_id
    assert replay.conversation_id == first.conversation_id
    assert replay.session_id == f"{binding.binding_id}:{first.conversation_id}"
    async with sessions() as database:
        assert await database.scalar(select(func.count()).select_from(ChannelIdentity)) == 1
        assert await database.scalar(select(func.count()).select_from(ChannelConversation)) == 1
        assert await database.scalar(select(func.count()).select_from(ConversationMember)) == 1
    await engine.dispose()


@pytest.mark.anyio
async def test_same_provider_ids_never_cross_tenant_or_binding_boundaries() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = PostgreSQLChannelIdentityService(sessions)
    first_binding = _binding()
    second_binding = _binding(tenant_id=uuid4())

    first = await service.resolve(first_binding, _message("user-1", "group-1"))
    second = await service.resolve(second_binding, _message("user-1", "group-1"))

    assert first.principal_id != second.principal_id
    assert first.conversation_id != second.conversation_id
    await engine.dispose()
