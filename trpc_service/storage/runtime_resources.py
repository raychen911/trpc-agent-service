"""Construct the remaining tenant resources from exact operator profile revisions."""

import asyncio
import os
from dataclasses import dataclass

from trpc_agent_sdk.abc import ArtifactId
from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.types import Part

from trpc_service.persistence import Database
from .artifact import S3ArtifactService
from .audit import AuditWriter, AuditRecord
from .knowledge import QdrantKnowledgeStore
from trpc_service.telemetry.runtime import measured


@dataclass
class RuntimeResources:
    artifact: object
    knowledge: object
    audit: object
    index_version: int
    clients: tuple

    async def close(self):
        await self.artifact.close()
        for client in self.clients:
            await asyncio.to_thread(client.close)


def resolve_env(uri):
    if not uri.startswith("env://") or not uri[6:].isidentifier():
        raise ValueError("resource requires an operator env reference")
    return os.environ[uri[6:]]


async def build_artifact_resource(store, tenant, database):
    """Build only the tenant object store needed by the channel process."""
    from minio import Minio

    profile = store.resource_profile(tenant.tenant_id, tenant.config_version, "artifact")
    if profile.kind.value != "object":
        raise ValueError("inbound media requires an object artifact backend")
    client = Minio(resolve_env(profile.secret_ref.uri),
                   access_key=resolve_env(profile.options.get("access_key_ref",
                                                              "env://TRPC_MINIO_ACCESS_KEY")),
                   secret_key=resolve_env(profile.options.get("secret_key_ref",
                                                              "env://TRPC_MINIO_SECRET_KEY")),
                   secure=profile.options.get("secure", True))
    return await asyncio.to_thread(S3ArtifactService,
                                   tenant_id=tenant.tenant_id,
                                   bucket=profile.options.get("bucket", "trpc-artifacts"),
                                   client=client,
                                   database=database)


async def build_resources(store, tenant, database):
    from minio import Minio
    from qdrant_client import QdrantClient
    profiles = {role: store.resource_profile(tenant.tenant_id, tenant.config_version, role)
                for role in ("artifact", "knowledge", "audit")}
    artifact, knowledge, audit = (profiles[role] for role in ("artifact", "knowledge", "audit"))
    if (artifact.kind.value, knowledge.kind.value, audit.kind.value) != ("object", "vector", "sql"):
        raise ValueError("unsupported resource bundle")
    # Dedicated audit database may share the platform URL, but never an SDK native session table set.
    audit_db = Database(resolve_env(audit.secret_ref.uri))
    vector = None
    try:
        await asyncio.to_thread(audit_db.check_schema)
        vector = QdrantClient(url=resolve_env(knowledge.secret_ref.uri), timeout=10)
        collection = knowledge.options.get("collection", "trpc_knowledge")
        await asyncio.to_thread(vector.get_collection, collection)
        client = Minio(resolve_env(artifact.secret_ref.uri),
                       access_key=resolve_env(artifact.options.get("access_key_ref", "env://TRPC_MINIO_ACCESS_KEY")),
                       secret_key=resolve_env(artifact.options.get("secret_key_ref", "env://TRPC_MINIO_SECRET_KEY")),
                       secure=artifact.options.get("secure", True))
        objects = await asyncio.to_thread(S3ArtifactService, tenant_id=tenant.tenant_id,
                                          bucket=artifact.options.get("bucket", "trpc-artifacts"),
                                          client=client, database=database)
        return RuntimeResources(objects, QdrantKnowledgeStore(vector, collection), AuditWriter(audit_db),
                                knowledge.options.get("index_version", 1), (vector, audit_db))
    except BaseException:
        audit_db.close()
        if vector:
            vector.close()
        raise


def resource_tools(resources, tenant, app):
    """The model receives no tenant/user/session parameters to forge."""
    from trpc_service.agent.tool_catalog import extension_tools

    def identity(context, filename):
        return ArtifactId(app_name=context.session.app_name, user_id=context.session.user_id,
                          session_id=context.session.id, filename=filename)

    @measured("object.read")
    async def read_artifact(filename: str, tool_context: InvocationContext):
        """Read a text document from this user's current session."""
        entry = await resources.artifact.load_artifact(artifact_id=identity(tool_context, filename))
        # Keep the model-facing read contract stable; version history remains
        # available through ArtifactService.list_versions for operators.
        return {"found": entry is not None, "text": entry.data.text if entry else None}

    @measured("object.upload")
    async def write_artifact(filename: str, text: str, tool_context: InvocationContext):
        """Save a text document in this user's current session."""
        version = await resources.artifact.save_artifact(artifact_id=identity(tool_context, filename),
                                                         artifact=Part.from_text(text=text))
        return {"filename": filename, "version": version}

    @measured("vector.search")
    async def search_knowledge(knowledge_base_id: str, vector: list[float], tool_context: InvocationContext):
        """Search an allowed legacy knowledge base using its precomputed vector."""
        if knowledge_base_id not in app.knowledge_base_ids or not 1 <= len(vector) <= 65536:
            raise PermissionError("knowledge scope or vector invalid")
        results = await asyncio.to_thread(resources.knowledge.search, tenant.tenant_id, knowledge_base_id,
                                          resources.index_version, vector, 5)
        return {"matches": [{"document_id": hit.document_id, "text": hit.text} for hit in results]}

    selected_search = search_knowledge
    if getattr(tenant, "embedding", None) is not None:
        @measured("vector.search")
        async def search_knowledge_text(knowledge_base_id: str, query: str, tool_context: InvocationContext):
            """Search an allowed knowledge base with the user's natural-language question."""
            if knowledge_base_id not in app.knowledge_base_ids:
                raise PermissionError("knowledge scope invalid")
            if not query.strip() or len(query) > 8000:
                raise ValueError("provide a bounded nonempty query")
            from .knowledge_ingestion import configured_embedder
            vector = await configured_embedder(tenant)(query)
            results = await asyncio.to_thread(resources.knowledge.search, tenant.tenant_id, knowledge_base_id,
                                              resources.index_version, vector, 5)
            return {"matches": [{"document_id": hit.document_id, "text": hit.text} for hit in results]}

        search_knowledge_text.__name__ = "search_knowledge"
        selected_search = search_knowledge_text

    # Built-ins are always available to the trusted assembly path.  Additional
    # tools are explicitly registered by application code and are still
    # filtered by the tenant ToolPolicy below.
    return (read_artifact, write_artifact, selected_search, *extension_tools())


async def audit_resource(resources, tenant_id, execution_id, decision):
    await asyncio.to_thread(resources.audit.write, AuditRecord(
        tenant_id=tenant_id, decision=decision, metadata={"execution_id": execution_id}))
