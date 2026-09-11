from __future__ import annotations

import asyncio

from trpc_service.config import AppSettings
from trpc_service.runtime import TenantContext
from trpc_service.web.app import ServiceContainer
from trpc_service.web.schemas import KnowledgeDocumentCreate, TenantCreate


def test_knowledge_document_is_durable_projected_and_acl_filtered():
    services = ServiceContainer(AppSettings(runtime_backend="memory", admin_api_key="test-key"))
    services.create_tenant(TenantCreate(tenant_id="tenant-a", display_name="Tenant A"))

    saved = services.put_knowledge_document(
        "tenant-a",
        KnowledgeDocumentCreate(
            document_id="tea-faq",
            knowledge_base_id="support",
            content="Tea orders can be changed within thirty minutes.",
            acl={"subject_ids": ["alice"]},
        ),
        "request-knowledge",
        "a" * 32,
    )
    assert saved["version"] == 1
    assert "content" not in saved

    services.dispatch("tenant-a")
    asyncio.run(services.process_published("tenant-a"))
    context = TenantContext("tenant-a", request_id="request-context", trace_id="b" * 32)
    inbox = {"session_id": "session-a", "subject_id": "alice"}
    visible = services._session_context(
        context, inbox, "Can I change my tea order?", {"knowledge_base_ids": ["support"]}
    )
    assert [item["record_id"] for item in visible["knowledge"]] == ["knowledge:tea-faq"]

    denied = services._session_context(
        context,
        {"session_id": "session-a", "subject_id": "mallory"},
        "Can I change my tea order?",
        {"knowledge_base_ids": ["support"]},
    )
    assert denied["knowledge"] == []
