"""Durable Qdrant index migration under the same offline maintenance contract."""

import hashlib
import json
from dataclasses import asdict

from trpc_service.management import ConfigConflict
from trpc_service.persistence.models import OfflineMigrationRow, TenantRow, TenantConfigVersionRow, utcnow
from trpc_service.storage.knowledge import QdrantKnowledgeStore
from trpc_service.telemetry.runtime import count
from .vector_migration import VectorMigration


def validate_profiles(store, tenant_id, source_version, target_version):
    source, target = [store.resource_profile(tenant_id, version, "knowledge")
                      for version in (source_version, target_version)]
    embedding = source.options.get("embedding_id")
    if not embedding or target.options.get("embedding_id") != embedding:
        raise ConfigConflict("vector copy requires an explicit matching embedding identity; re-embed separately")
    if target.options.get("index_version", 1) <= source.options.get("index_version", 1):
        raise ConfigConflict("target must use a new higher index version")
    return source, target


def digest(chunks):
    items = []
    for chunk in sorted(chunks, key=lambda chunk: (chunk.document_id, chunk.chunk_id)):
        item = asdict(chunk)
        item.pop("index_version", None)
        item["vector"] = [round(value, 5) for value in item["vector"]]
        items.append(item)
    return hashlib.sha256(json.dumps(items, sort_keys=True).encode()).hexdigest()


def run_vector_job(control, tenant_id, job_id, batch_size, cutover, actor):
    from qdrant_client import QdrantClient, models
    clients = []
    try:
        with control.database.sessions.begin() as session:
            row = session.get(OfflineMigrationRow, job_id, with_for_update=True)
            tenant = session.get(TenantRow, tenant_id, with_for_update=True)
            control._paused(session, tenant_id)
            if row.status not in {"copying", "verified"} or tenant.active_config_version != row.source_version:
                raise ConfigConflict("migration state or active version changed")
            profiles = validate_profiles(control.store, tenant_id, row.source_version, row.target_version)
            stores, schemas = [], []
            for version, profile in zip((row.source_version, row.target_version), profiles):
                client = QdrantClient(url=control.store.connection(tenant_id, version, "knowledge"), timeout=15)
                clients.append(client)
                collection = profile.options.get("collection", "trpc_knowledge")
                vectors = client.get_collection(collection).config.params.vectors
                if not isinstance(vectors, models.VectorParams):
                    raise ConfigConflict("named vectors require a dedicated migration plan")
                schemas.append((profile.options["embedding_id"], vectors.size, vectors.distance))
                stores.append(QdrantKnowledgeStore(client, collection))
            if schemas[0] != schemas[1]:
                raise ConfigConflict("vector dimensions or distance differ; re-embed into a new schema")
            indices = [profile.options.get("index_version", 1) for profile in profiles]
            items = [dict(item) for item in row.items_json]
            stop = min(len(items), row.copied + batch_size)
            for item in items[row.copied:stop]:
                kb = item["knowledge_base_id"]
                if "source_digest" not in item:
                    if stores[1].iter_index(tenant_id, kb, indices[1]):
                        raise ConfigConflict("target index must be empty before preparation")
                    item["source_digest"] = digest(stores[0].iter_index(tenant_id, kb, indices[0]))
            row.items_json = items
        # The prepared manifest survives a write followed by process loss.
        with control.database.sessions.begin() as session:
            row = session.get(OfflineMigrationRow, job_id, with_for_update=True)
            tenant = session.get(TenantRow, tenant_id, with_for_update=True)
            control._paused(session, tenant_id)
            if row.status not in {"copying", "verified"} or tenant.active_config_version != row.source_version:
                raise ConfigConflict("migration state or active version changed")
            items = [dict(item) for item in row.items_json]
            for index in range(row.copied, min(len(items), row.copied + batch_size)):
                item, source, target = items[index], stores[0], stores[1]
                kb = item["knowledge_base_id"]
                if digest(source.iter_index(tenant_id, kb, indices[0])) != item["source_digest"]:
                    raise ConfigConflict("source index changed after preparation")
                copier = VectorMigration(
                    tenant_id=tenant_id, knowledge_base_id=kb,
                    source_index_version=indices[0], target_index_version=indices[1], source=source, target=target,
                    source_embedding=schemas[0][:2], target_embedding=schemas[1][:2])
                report = copier.copy_and_verify()
                item["target_digest"] = digest(target.iter_index(tenant_id, kb, indices[1]))
                row.copied = index + 1
                count("migration.copied", report.copied)
            row.items_json = items
            if row.copied == len(items):
                row.status = "verified"
            if cutover:
                if row.status != "verified":
                    raise ConfigConflict("finish copying before cutover")
                for item in items:
                    for store, index, field in zip(stores, indices, ("source_digest", "target_digest")):
                        if digest(store.iter_index(tenant_id, item["knowledge_base_id"], index)) != item[field]:
                            count("migration.diff")
                            raise ConfigConflict("source or target changed after verification")
                    count("migration.verified")
                target = session.get(TenantConfigVersionRow, (tenant_id, row.target_version))
                target.status, target.published_at = "published", utcnow()
                tenant.active_config_version, row.status = row.target_version, "completed"
                control.store.audit(session, tenant_id, actor, "migration_cutover", row.target_version,
                                    {"migration_id": job_id})
            return {"job_id": job_id, "status": row.status, "copied": row.copied, "total": len(items)}
    finally:
        for client in clients:
            client.close()
