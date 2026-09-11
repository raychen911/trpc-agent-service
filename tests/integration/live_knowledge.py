"""Real PostgreSQL/HTTP/Qdrant/provider acceptance; no simulated embeddings.

Set TRPC_DATABASE_URL, TRPC_QDRANT_URL and TRPC_MODEL_API_KEY, then run
python -m tests.integration.live_knowledge. Evidence and isolated stores remain.
"""

import asyncio
import json
import os
from pathlib import Path
import secrets
import socket
from types import SimpleNamespace
import uuid

from fastapi import FastAPI
import httpx
from qdrant_client import QdrantClient, models
from sqlalchemy import create_engine, select
from sqlalchemy.schema import CreateSchema
import uvicorn

from trpc_service.management import ConfigurationStore, ConfigDraft, management_router
from trpc_service.persistence import Database
from trpc_service.persistence.models import AuditLogRow
from trpc_service.protected_demo import documents
from trpc_service.storage.knowledge import QdrantKnowledgeStore
from trpc_service.storage.runtime_resources import resource_tools
from trpc_service.tenant import TenantConfig


async def run():
    run_id = uuid.uuid4().hex[:12]
    schema = "knowledge_" + run_id
    url = os.environ["TRPC_DATABASE_URL"]
    engine = create_engine(url, hide_parameters=True)
    if engine.dialect.name != "postgresql":
        raise ValueError("real knowledge acceptance requires PostgreSQL")
    with engine.begin() as connection:
        connection.execute(CreateSchema(schema))
    engine.dispose()
    database = Database(url, connect_args={"options": "-csearch_path=" + schema})
    database.create_schema()
    vector = QdrantClient(url=os.environ["TRPC_QDRANT_URL"], timeout=15)
    collection = "knowledge_acceptance_" + run_id
    await asyncio.to_thread(vector.create_collection, collection,
                            vectors_config=models.VectorParams(size=4096, distance=models.Distance.COSINE))
    report = {"passed": False, "run_id": run_id, "schema": schema, "collection": collection,
              "embedding_model": "qwen/qwen3-embedding-8b", "dimensions": 4096, "checks": []}
    output = Path("reports") / ("knowledge-" + run_id + ".json")
    task = None
    listener = socket.socket()
    try:
        store = ConfigurationStore(database)
        profiles, tenants = documents("knowledge_wecom", "knowledge_feishu")
        for profile in profiles:
            if profile.kind.value == "vector":
                profile = profile.model_copy(update={"options": {"collection": collection, "index_version": 1}})
            store.register_profile(profile, "knowledge_acceptance")
        for index, config in enumerate(tenants):
            data = config.model_dump(mode="json")
            data["channel_bindings"] = []
            data["embedding"] = {
                "model_name": report["embedding_model"], "dimensions": 4096,
                "api_key_ref": {"uri": "env://TRPC_MODEL_API_KEY"}}
            data["applications"][0]["instruction"] = "检索时调用 search_knowledge，传入用户的自然语言 query。"
            data["applications"][0]["tool_policy"] = {"allow": ["search_knowledge"]}
            config = TenantConfig.model_validate(data)
            tenants[index] = config
            store.save_draft(ConfigDraft(config=config, profile_revisions={role: 1 for role in
                                                                           type(config.data_backends).model_fields}),
                             "knowledge_acceptance")
            store.publish(config.tenant_id, 1, 0, "knowledge_acceptance")
        os.environ["TRPC_ADMIN_TOKEN"] = secrets.token_urlsafe(40)
        app = FastAPI()
        app.include_router(management_router(database))
        listener.bind(("127.0.0.1", 0))
        server = uvicorn.Server(uvicorn.Config(app, access_log=False, log_level="critical", lifespan="off"))
        task = asyncio.create_task(server.serve(sockets=[listener]))
        for _ in range(200):
            if server.started:
                break
            if task.done():
                await task
            await asyncio.sleep(0.025)
        assert server.started
        endpoint = f"http://127.0.0.1:{listener.getsockname()[1]}"
        headers = {"Authorization": "Bearer " + os.environ["TRPC_ADMIN_TOKEN"]}
        root = endpoint + "/admin/tenants/demo_wecom/apps/assistant/knowledge/acceptance"
        secret = "青岚-" + run_id
        async with httpx.AsyncClient(timeout=120, trust_env=False, headers=headers) as client:
            response = await client.put(root + "/documents/manual.md", content=f"# 验收手册\n灯塔项目的取件口令是{secret}。".encode(),
                                        headers={"Content-Type": "text/markdown; charset=utf-8"})
            assert response.status_code == 200, f"upload HTTP {response.status_code}"
            report["checks"].append("http_upload_parse_embed_qdrant")
            query = {"query": "灯塔项目的取件口令是什么？"}
            response = await client.post(root + "/search", json=query)
            assert response.status_code == 200 and secret in response.text
            report["checks"].append("natural_language_retrieval")
            other = await client.post(root.replace("demo_wecom", "demo_feishu") + "/search", json=query)
            assert other.status_code == 200 and other.json()["matches"] == []
            denied = await client.post(root.replace("/knowledge/acceptance", "/knowledge/private") + "/search",
                                       json=query)
            assert denied.status_code == 403
            report["checks"].append("tenant_and_knowledge_scope")
            resources = SimpleNamespace(knowledge=QdrantKnowledgeStore(vector, collection), index_version=1)
            search = next(tool for tool in resource_tools(resources, tenants[0], tenants[0].applications[0])
                          if tool.__name__ == "search_knowledge")
            result = await search("acceptance", tool_context=None, query=query["query"])
            assert secret in json.dumps(result, ensure_ascii=False)
            report["checks"].append("runtime_natural_language_tool")
            replacement = await client.put(root + "/documents/manual.md", content="灯塔项目已经停用旧口令。".encode(),
                                           headers={"Content-Type": "text/plain"})
            assert replacement.status_code == 200
            result = await client.post(root + "/search", json=query)
            assert result.status_code == 200 and secret not in result.text
            assert (await client.delete(root + "/documents/manual.md")).status_code == 200
            result = await client.post(root + "/search", json=query)
            assert result.json()["matches"] == []
            report["checks"].append("replacement_and_delete")
        with database.sessions() as session:
            decisions = list(session.scalars(select(AuditLogRow.decision).where(
                AuditLogRow.tenant_id == "demo_wecom")))
            assert decisions.count("knowledge_document_ingested") == 2
            assert "knowledge_document_deleted" in decisions
        report["checks"].append("postgres_audit")
        report["passed"] = True
    except Exception as error:
        report["error_type"] = type(error).__name__
        raise
    finally:
        if task:
            server.should_exit = True
            await asyncio.wait_for(task, 10)
        listener.close()
        vector.close()
        database.close()
        output.parent.mkdir(exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(str(output))
    return report


if __name__ == "__main__":
    from trpc_service.im_setup import load_bundle
    load_bundle(".secrets/im.json")
    print(json.dumps(asyncio.run(run()), ensure_ascii=False))
