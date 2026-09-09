# mypy: disable-error-code="import-untyped"
"""Tenant isolation and retrieval tests for projected long-term memory."""

from __future__ import annotations

import pytest
from trpc_agent_sdk.context import new_agent_context

from trpc_service.demo import build_demo_tenant_spec
from trpc_service.memory.projected import (
    MemoryAuthorizationError,
    ProjectedSqlMemoryService,
)
from trpc_service.storage import Database
from trpc_service.storage.models import MemoryRecord, Session
from trpc_service.tenant import TenantConfigService
from trpc_service.tenant.context import ConversationScope, TenantContext
from trpc_service.tool import TENANT_CONTEXT_METADATA_KEY


@pytest.fixture
async def memory_service(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}")
    await database.create_schema()
    await TenantConfigService(database.session_factory).publish(
        build_demo_tenant_spec(),
        actor="test",
    )
    async with database.session_factory() as session, session.begin():
        session.add(
            Session(
                tenant_id="tenant-demo",
                session_id="session-a",
                app_id="assistant",
                app_revision=1,
                binding_id="wecom-demo",
                scope="private",
                principal_id="principal-a",
            )
        )
    async with database.session_factory() as session, session.begin():
        session.add_all(
            [
                MemoryRecord(
                    tenant_id="tenant-demo",
                    principal_id="principal-a",
                    session_id="session-a",
                    source_event_id="event-1",
                    extractor_version="test-v1",
                    record_version=1,
                    content="用户喜欢咖啡和清晨阅读。",
                    metadata_json={},
                ),
                MemoryRecord(
                    tenant_id="tenant-demo",
                    principal_id="principal-a",
                    session_id="session-a",
                    source_event_id="event-2",
                    extractor_version="test-v1",
                    record_version=2,
                    content="用户要求所有日期使用北京时间。",
                    metadata_json={},
                ),
            ]
        )
    service = ProjectedSqlMemoryService(database.session_factory)
    try:
        yield service
    finally:
        await service.close()
        await database.dispose()


def _context(principal_id: str = "principal-a"):
    tenant_context = TenantContext(
        tenant_id="tenant-demo",
        app_id="assistant",
        app_revision=1,
        binding_id="wecom-demo",
        binding_revision=1,
        principal_id=principal_id,
        session_id="session-a",
        scope=ConversationScope.PRIVATE,
        request_id="request-a",
        trace_id="0" * 32,
    )
    return new_agent_context(
        metadata={TENANT_CONTEXT_METADATA_KEY: tenant_context},
    )


async def test_search_returns_only_relevant_principal_memory(memory_service) -> None:
    response = await memory_service.search_memory(
        "tenant-app/principal-a",
        "咖啡",
        agent_context=_context(),
    )

    assert len(response.memories) == 1
    assert response.memories[0].content.parts[0].text == "用户喜欢咖啡和清晨阅读。"

    other = await memory_service.search_memory(
        "tenant-app/principal-b",
        "咖啡",
        agent_context=_context("principal-b"),
    )
    assert other.memories == []


async def test_search_rejects_untrusted_or_cross_principal_key(memory_service) -> None:
    with pytest.raises(MemoryAuthorizationError, match="missing"):
        await memory_service.search_memory("tenant-app/principal-a", "咖啡")
    with pytest.raises(MemoryAuthorizationError, match="does not match"):
        await memory_service.search_memory(
            "tenant-app/principal-b",
            "咖啡",
            agent_context=_context(),
        )
