"""Governed natural-language Tools for tenant-owned knowledge operations."""

import json

from trpc_service.agent.contracts import (
    AgentExecutionContext,
    AgentToolCall,
    AgentToolResult,
)
from trpc_service.agent.ports import AgentToolInvoker
from trpc_service.storage.knowledge import TenantKnowledgeService


class KnowledgeToolInvoker(AgentToolInvoker):
    """Execute knowledge operations after the shared governance layer authorizes them."""

    # IM users can retrieve tenant knowledge, while every mutation is performed
    # explicitly by a tenant administrator through the web control plane.
    TOOL_NAMES = frozenset({"knowledge.list", "knowledge.search"})

    def __init__(self, service: TenantKnowledgeService) -> None:
        self._service = service

    @staticmethod
    def _base_name(call: AgentToolCall) -> str:
        name = call.arguments.get("knowledge_base_name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("knowledge_base_name is required")
        if call.resource != name:
            raise PermissionError("knowledge Tool resource does not match its arguments")
        return name

    async def invoke(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AgentToolResult:
        """Route only registered knowledge operations inside the trusted context."""

        base_name = self._base_name(call)
        content: object
        if call.name == "knowledge.list":
            documents = await self._service.list_documents(
                context.request.tenant,
                context.config.knowledge,
                base_name,
                backends=context.config.backends,
            )
            content = [{
                "document_id": str(document.document_id),
                "filename": document.filename,
                "version": document.version,
                "status": document.status,
            } for document in documents]
        elif call.name == "knowledge.search":
            query = call.arguments.get("query")
            if not isinstance(query, str) or not query.strip():
                raise ValueError("knowledge search query is required")
            hits = await self._service.search(
                context.request.tenant,
                {"knowledge_base_names": [base_name]},
                query,
                limit=5,
                backends=context.config.backends,
            )
            content = [{
                "content": hit.document.content,
                "score": hit.score,
                "source": dict(hit.document.attributes),
            } for hit in hits]
        else:
            raise PermissionError(f"tool is not registered: {call.name}")
        return AgentToolResult(
            call_id=call.call_id,
            content=json.dumps(content, ensure_ascii=False, separators=(",", ":")),
        )
