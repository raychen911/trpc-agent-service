"""Authenticated UTF-8 document upload and natural-language knowledge search."""

import asyncio
from dataclasses import asdict

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from .knowledge import QdrantKnowledgeStore
from .knowledge_ingestion import KnowledgeIngestionService, configured_embedder


class KnowledgeQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str = Field(min_length=1, max_length=8000)


def knowledge_router(database):
    from trpc_service.management import ConfigurationStore
    from trpc_service.governance.admin_auth import current_actor
    from .runtime_resources import resolve_env
    from qdrant_client import QdrantClient

    router = APIRouter()
    store = ConfigurationStore(database)

    async def execute(tenant_id, app_id, knowledge_base_id, operation):
        tenant = next((item for item in store.active() if item.tenant_id == tenant_id), None)
        if tenant is None:
            raise HTTPException(404, "active tenant not found")
        app = next((item for item in tenant.applications if item.app_id == app_id and item.enabled), None)
        if app is None or knowledge_base_id not in app.knowledge_base_ids:
            raise HTTPException(403, "knowledge base is outside the application scope")
        if tenant.embedding is None:
            raise HTTPException(409, "tenant embedding is not configured")
        profile = store.resource_profile(tenant_id, tenant.config_version, "knowledge")
        client = None
        try:
            embedder = configured_embedder(tenant)
            client = QdrantClient(url=resolve_env(profile.secret_ref.uri), timeout=15)
            collection = profile.options.get("collection", "trpc_knowledge")
            info = await asyncio.to_thread(client.get_collection, collection)
            if getattr(info.config.params.vectors, "size", None) != tenant.embedding.dimensions:
                raise ValueError("collection and embedding dimensions differ")
            service = KnowledgeIngestionService(QdrantKnowledgeStore(client, collection),
                                                tenant_id=tenant_id, knowledge_base_id=knowledge_base_id,
                                                index_version=profile.options.get("index_version", 1),
                                                embedder=embedder)
            result, decision, metadata = await operation(service)
            with database.sessions.begin() as session:
                store.audit(session, tenant_id, current_actor(), decision, tenant.config_version,
                            metadata={"app_id": app_id, "knowledge_base_id": knowledge_base_id, **metadata})
            return result
        except HTTPException:
            raise
        except (ValueError, UnicodeError):
            raise HTTPException(422, "document or embedding configuration invalid") from None
        except Exception:
            raise HTTPException(502, "knowledge storage or embedding request failed") from None
        finally:
            if client:
                await asyncio.to_thread(client.close)

    prefix = "/tenants/{tenant_id}/apps/{app_id}/knowledge/{knowledge_base_id}"

    @router.put(prefix + "/documents/{document_id}")
    async def upload(tenant_id: str, app_id: str, knowledge_base_id: str, document_id: str, request: Request):
        media = request.headers.get("content-type", "").split(";")[0].strip().lower()
        if media not in {"text/plain", "text/markdown"}:
            raise HTTPException(415, "upload UTF-8 text/plain or text/markdown")
        payload = bytearray()
        async for chunk in request.stream():
            if len(payload) + len(chunk) > 256_000:
                raise HTTPException(413, "document exceeds 256000 bytes")
            payload.extend(chunk)
        try:
            text = payload.decode("utf-8-sig")
        except UnicodeError:
            raise HTTPException(422, "document must be UTF-8") from None

        async def ingest(service):
            result = asdict(await service.ingest_document(document_id, text))
            return result, "knowledge_document_ingested", result

        return await execute(tenant_id, app_id, knowledge_base_id, ingest)

    @router.post(prefix + "/search")
    async def search(tenant_id: str, app_id: str, knowledge_base_id: str, body: KnowledgeQuery):
        async def query(service):
            vector = await service.embedder(body.query)
            matches = await asyncio.to_thread(service.store.search, tenant_id, knowledge_base_id,
                                              service.index_version, vector, 5)
            return {"matches": [asdict(hit) for hit in matches]}, "knowledge_searched", {"count": len(matches)}

        return await execute(tenant_id, app_id, knowledge_base_id, query)

    @router.delete(prefix + "/documents/{document_id}")
    async def delete(tenant_id: str, app_id: str, knowledge_base_id: str, document_id: str):
        async def remove(service):
            await asyncio.to_thread(service.delete_document, document_id)
            return {"status": "deleted"}, "knowledge_document_deleted", {"document_id": document_id}

        return await execute(tenant_id, app_id, knowledge_base_id, remove)

    return router
