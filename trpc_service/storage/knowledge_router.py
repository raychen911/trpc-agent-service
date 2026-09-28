"""Tenant-admin HTTP boundary for explicit knowledge-base management."""

from collections.abc import AsyncIterator
import logging
from typing import Annotated, NoReturn
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.audit import append_management_audit
from trpc_service.admin.auth import ManagementActor, require_tenant_admin
from trpc_service.agent.models import AgentApp
from trpc_service.storage.database import get_session
from trpc_service.storage.knowledge import KnowledgeArtifact, KnowledgeDocumentState
from trpc_service.tenant.context import TenantContext

router = APIRouter(
    prefix="/tenants/{tenant_id}/knowledge-bases/{knowledge_base_name}/documents",
    tags=["tenant-knowledge"],
)
logger = logging.getLogger(__name__)


class KnowledgeDocumentRead(BaseModel):
    """Safe tenant-visible state of one versioned knowledge document."""

    document_id: UUID
    knowledge_base_id: UUID
    filename: str
    version: int
    status: str
    chunk_count: int


class KnowledgeDocumentList(BaseModel):
    items: list[KnowledgeDocumentRead]
    total: int


def _document_read(document: KnowledgeDocumentState) -> KnowledgeDocumentRead:
    return KnowledgeDocumentRead(
        document_id=document.document_id,
        knowledge_base_id=document.knowledge_base_id,
        filename=document.filename,
        version=document.version,
        status=document.status,
        chunk_count=document.chunk_count,
    )


async def _agent_context(
    database: AsyncSession,
    tenant_id: UUID,
    agent_app_id: UUID,
) -> tuple[AgentApp, TenantContext]:
    """Load one active tenant Agent as the authoritative RAG storage profile."""

    agent = await database.scalar(
        select(AgentApp).where(
            AgentApp.tenant_id == tenant_id,
            AgentApp.agent_app_id == agent_app_id,
            AgentApp.status == "active",
        ))
    if agent is None:
        raise HTTPException(status_code=404, detail="active tenant Agent not found")
    request_id = f"tenant-console-{uuid4().hex}"
    return agent, TenantContext(
        tenant_id=tenant_id,
        agent_app_id=agent_app_id,
        config_version=agent.stable_config_version,
        request_id=request_id,
        trace_id=request_id,
    )


def _raise_knowledge_error(error: Exception) -> NoReturn:
    """Translate expected domain failures without returning internal details."""

    if isinstance(error, PermissionError):
        raise HTTPException(status_code=403, detail=str(error)) from error
    if isinstance(error, LookupError):
        raise HTTPException(status_code=404, detail=str(error)) from error
    if isinstance(error, ValueError):
        raise HTTPException(status_code=422, detail=str(error)) from error
    logger.exception("Tenant knowledge operation failed", exc_info=error)
    raise HTTPException(status_code=503, detail="knowledge storage operation failed") from error


@router.get("", response_model=KnowledgeDocumentList)
async def list_knowledge_documents(
        tenant_id: UUID,
        knowledge_base_name: Annotated[str, Path(min_length=1, max_length=120)],
        request: Request,
        agent_app_id: UUID = Query(),
        _: ManagementActor = Depends(require_tenant_admin),
        database: AsyncSession = Depends(get_session),
) -> KnowledgeDocumentList:
    """List documents in a base granted to the selected tenant Agent."""

    agent, context = await _agent_context(database, tenant_id, agent_app_id)
    try:
        documents = await request.app.state.container.knowledge.list_documents(
            context,
            agent.knowledge_config,
            knowledge_base_name,
            backends=agent.backend_config,
        )
    except Exception as error:
        _raise_knowledge_error(error)
    return KnowledgeDocumentList(
        items=[_document_read(document) for document in documents],
        total=len(documents),
    )


async def _upload_artifact(
    request: Request,
    actor: ManagementActor,
    context: TenantContext,
    agent: AgentApp,
    filename: str,
) -> str:
    """Persist one bounded request body and return its tenant artifact ID."""

    async def blocks() -> AsyncIterator[bytes]:
        async for block in request.stream():
            if block:
                yield block

    artifact: KnowledgeArtifact = await request.app.state.container.knowledge.upload(
        context,
        principal_id=actor.subject,
        filename=filename,
        media_type=request.headers.get("content-type", "application/octet-stream"),
        content=blocks(),
        backends=agent.backend_config,
    )
    return artifact.artifact_id


@router.post("", response_model=KnowledgeDocumentRead, status_code=status.HTTP_201_CREATED)
async def create_knowledge_document(
        tenant_id: UUID,
        knowledge_base_name: Annotated[str, Path(min_length=1, max_length=120)],
        request: Request,
        filename: Annotated[str, Query(min_length=1, max_length=255)],
        agent_app_id: UUID = Query(),
        actor: ManagementActor = Depends(require_tenant_admin),
        database: AsyncSession = Depends(get_session),
) -> KnowledgeDocumentRead:
    """Upload and ingest one explicitly selected tenant document."""

    agent, context = await _agent_context(database, tenant_id, agent_app_id)
    try:
        artifact_id = await _upload_artifact(request, actor, context, agent, filename)
        documents = await request.app.state.container.knowledge.ingest(
            context,
            agent.knowledge_config,
            knowledge_base_name=knowledge_base_name,
            artifact_ids=(artifact_id, ),
            backends=agent.backend_config,
        )
    except Exception as error:
        _raise_knowledge_error(error)
    document = documents[0]
    append_management_audit(
        database,
        actor,
        action="knowledge_document.create",
        resource_type="knowledge_document",
        resource_id=str(document.document_id),
        tenant_id=tenant_id,
        details_redacted={
            "knowledge_base": knowledge_base_name,
            "filename": filename
        },
    )
    await database.commit()
    return _document_read(document)


@router.put("/{document_id}", response_model=KnowledgeDocumentRead)
async def replace_knowledge_document(
        tenant_id: UUID,
        knowledge_base_name: Annotated[str, Path(min_length=1, max_length=120)],
        document_id: UUID,
        request: Request,
        filename: Annotated[str, Query(min_length=1, max_length=255)],
        agent_app_id: UUID = Query(),
        actor: ManagementActor = Depends(require_tenant_admin),
        database: AsyncSession = Depends(get_session),
) -> KnowledgeDocumentRead:
    """Create a new version from a file selected in the tenant console."""

    agent, context = await _agent_context(database, tenant_id, agent_app_id)
    try:
        artifact_id = await _upload_artifact(request, actor, context, agent, filename)
        document = await request.app.state.container.knowledge.update_document(
            context,
            agent.knowledge_config,
            knowledge_base_name,
            document_id,
            artifact_id,
            backends=agent.backend_config,
        )
    except Exception as error:
        _raise_knowledge_error(error)
    append_management_audit(
        database,
        actor,
        action="knowledge_document.update",
        resource_type="knowledge_document",
        resource_id=str(document_id),
        tenant_id=tenant_id,
        details_redacted={
            "knowledge_base": knowledge_base_name,
            "filename": filename
        },
    )
    await database.commit()
    return _document_read(document)


@router.delete("/{document_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_knowledge_document(
        tenant_id: UUID,
        knowledge_base_name: Annotated[str, Path(min_length=1, max_length=120)],
        document_id: UUID,
        request: Request,
        agent_app_id: UUID = Query(),
        actor: ManagementActor = Depends(require_tenant_admin),
        database: AsyncSession = Depends(get_session),
) -> Response:
    """Soft-delete one selected document without any model interpretation."""

    agent, context = await _agent_context(database, tenant_id, agent_app_id)
    try:
        await request.app.state.container.knowledge.delete_document(
            context,
            agent.knowledge_config,
            knowledge_base_name,
            document_id,
            backends=agent.backend_config,
        )
    except Exception as error:
        _raise_knowledge_error(error)
    append_management_audit(
        database,
        actor,
        action="knowledge_document.delete",
        resource_type="knowledge_document",
        resource_id=str(document_id),
        tenant_id=tenant_id,
        details_redacted={"knowledge_base": knowledge_base_name},
    )
    await database.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
