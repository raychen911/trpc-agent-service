"""Explicitly rebuild derived memory after session migration, with separate clients."""

from contextlib import AsyncExitStack
import sys

import pytest
from trpc_agent_sdk.memory import SqlMemoryService

from trpc_service.migration import copy_and_verify_trpc_session

from .seed import seed_sessions


@pytest.mark.asyncio
async def test_memory_rebuild_from_migrated_history_is_idempotent_and_tenant_scoped(session_backends):
    source, target = session_backends.source, session_backends.target
    keys = await seed_sessions(source, session_backends.suffix, sessions_per_tenant=1)
    async with AsyncExitStack() as stack:
        asynchronous = sys.platform != "win32" and not session_backends.sql_url.startswith("sqlite:")
        writer = SqlMemoryService(db_url=session_backends.sql_url, enabled=True, is_async=asynchronous)
        stack.push_async_callback(writer.close)
        reader = SqlMemoryService(db_url=session_backends.sql_url, enabled=True, is_async=asynchronous)
        stack.push_async_callback(reader.close)
        for key in keys:
            await copy_and_verify_trpc_session(source, target, target_writes_paused=True, **key.kwargs())
        first = await target.get_session(**keys[0].kwargs())
        other = await target.get_session(**keys[1].kwargs())
        await writer.store_session(first)
        visible = await reader.search_memory(first.save_key, "订单")
        assert visible.memories
        await writer.store_session(first)
        repeated = await reader.search_memory(first.save_key, "订单")
        assert repeated.model_dump(mode="json") == visible.model_dump(mode="json")
        assert not (await reader.search_memory(other.save_key, "订单")).memories
        # A new Session for the same user can address the same memory namespace.
        followup = await target.create_session(app_name=first.app_name, user_id=first.user_id,
                                               session_id=f"followup-{session_backends.suffix}")
        assert (await reader.search_memory(followup.save_key, "订单")).memories
