"""Prepare an isolated, published two-tenant deployment using secret references only."""

import json
import os
from pathlib import Path

from trpc_service.agent.openrouter import MODEL, MODEL_ID
from trpc_service.management import ConfigurationStore, ProfileRegistration, ConfigDraft
from trpc_service.tenant import TenantConfig


def documents(wecom_id, feishu_id):
    profiles = json.loads(
        Path(__file__).resolve().parent.parent.joinpath("deploy/management-profiles.json").read_text(encoding="utf-8"))
    profiles[0]["options"] = {"session_format": "protected_v1"}
    for profile in profiles:
        if profile["kind"] == "vector":
            profile["options"]["index_version"] = int(os.environ.get("TRPC_ACCEPTANCE_INDEX_VERSION", "1"))
            if os.environ.get("TRPC_ACCEPTANCE_EMBEDDING_MODEL"):
                profile["options"]["collection"] = os.environ["TRPC_VECTOR_COLLECTION"]
    profiles.append({
        "profile_id": "sql_protected",
        "profile_revision": 1,
        "kind": "sql",
        "provider": "postgresql",
        "endpoint": "postgres:5432/trpc_runtime",
        "secret_ref": {
            "uri": "env://TRPC_SESSION_DATABASE_URL"
        },
        "capabilities": ["session", "summary"],
        "options": {
            "session_format": "protected_v1"
        }
    })
    tenants = []
    for tenant_id, channel, account, kind, profile in (("demo_wecom", "wecom", wecom_id, "sql", "sql_protected"),
                                                       ("demo_feishu", "feishu", feishu_id, "redis", "redis_runtime")):
        references = {
            "session": (kind, profile),
            "summary": (kind, profile),
            "memory": ("sql", "sql_runtime"),
            "knowledge": ("vector", "vector_prod"),
            "artifact": ("object", "object_prod"),
            "audit": ("sql", "sql_audit")
        }
        tenant = TenantConfig.model_validate({
            "tenant_id":
            tenant_id,
            "name":
            "企微租户" if channel == "wecom" else "飞书租户",
            "config_version":
            1,
            "storage_revision":
            1,
            "applications": [{
                "app_id": "assistant",
                "name": "租户助手",
                "agent_name": "tenant_assistant",
                "model_config_id": MODEL_ID,
                "max_tool_iterations": 6,
                "knowledge_base_ids": ["acceptance"],
                "instruction": "简短回答当前用户。用户请求读写文件或检索时必须实际调用对应工具。保存文件需要用户确认；"
                               "工具返回 awaiting_confirmation 后停止，不声称文件已经保存。"
                               "检索只使用用户明确提供的预计算向量，不自行生成向量。",
                "tool_policy": {
                    "allow": ["read_artifact", "write_artifact", "search_knowledge"],
                    "require_confirmation": ["write_artifact"],
                    "max_calls_per_run": 6
                }
            }],
            "models": [{
                "model_config_id": MODEL_ID,
                "provider": "openrouter",
                "model_name": MODEL,
                "api_key_ref": {
                    "uri": "env://TRPC_MODEL_API_KEY"
                },
                "max_output_tokens": 128,
                "max_retries": 0,
                "currency": "USD"
            }],
            "channel_bindings": [{
                "binding_id":
                "primary",
                "tenant_id":
                tenant_id,
                "agent_app_id":
                "assistant",
                "channel":
                channel,
                "external_account_id":
                account,
                "webhook_public_id":
                tenant_id + "_ingress",
                "transport":
                channel + "_ws",
                "reply_mode":
                "async",
                "credential_refs": [{
                    "name": "bot_secret" if channel == "wecom" else "app_secret",
                    "secret_ref": {
                        "uri": "env://TRPC_WECOM_BOT_SECRET" if channel == "wecom" else "env://TRPC_FEISHU_APP_SECRET"
                    }
                }]
            }],
            "data_backends": {
                role: {
                    "kind": backend,
                    "profile_id": name,
                    "namespace": tenant_id
                }
                for role, (backend, name) in references.items()
            }
        })
        if os.environ.get("TRPC_ACCEPTANCE_EMBEDDING_MODEL"):
            data = tenant.model_dump(mode="json")
            data["embedding"] = {
                "model_name": os.environ["TRPC_ACCEPTANCE_EMBEDDING_MODEL"],
                "dimensions": int(os.environ["TRPC_VECTOR_DIMENSIONS"]),
                "api_key_ref": {"uri": "env://TRPC_MODEL_API_KEY"}}
            data["applications"][0]["instruction"] = (
                "用户请求读写文件或检索时必须实际调用工具。保存文件需要用户确认；"
                "awaiting_confirmation 后停止。知识库为 acceptance；检索时使用自然语言 query，"
                "不要生成或传入向量。只回答工具实际返回的内容。")
            tenant = TenantConfig.model_validate(data)
        tenants.append(tenant)
    return [ProfileRegistration.model_validate(value) for value in profiles], tenants


def prepare(database, output="reports/protected-demo-config"):
    if database.engine.dialect.name != "postgresql":
        raise ValueError("demo requires an isolated PostgreSQL platform database")
    database.check_schema()
    store = ConfigurationStore(database)
    from sqlalchemy import select, func
    from trpc_service.persistence.models import TenantRow, BackendProfileRow
    with database.sessions() as session:
        if session.scalar(select(func.count()).select_from(TenantRow)) or session.scalar(
                select(func.count()).select_from(BackendProfileRow)):
            raise ValueError("demo preparation requires an empty isolated database")
    profiles, tenants = documents(os.environ["TRPC_WECOM_BOT_ID"], os.environ["TRPC_FEISHU_APP_ID"])
    os.environ.update(TRPC_RUNTIME_MODE="protected", TRPC_CONFIG_SOURCE="database")
    destination = Path(output)
    destination.mkdir(parents=True, exist_ok=True)
    for profile in profiles:
        store.register_profile(profile, "demo_operator")
    from trpc_service.governance.budget import BudgetLedger
    ledger = BudgetLedger(database)
    for tenant in tenants:
        store.save_draft(
            ConfigDraft(config=tenant, profile_revisions={role: 1
                                                          for role in type(tenant.data_backends).model_fields}),
            "demo_operator")
        ledger.configure(tenant.tenant_id, "USD", "1", "2", "Asia/Shanghai", 0, actor="demo_operator")
        store.publish(tenant.tenant_id, 1, 0, "demo_operator")
    for filename, values in (("profiles.json", profiles), ("tenants.json", tenants)):
        destination.joinpath(filename).write_text(json.dumps([value.model_dump(mode="json") for value in values],
                                                             ensure_ascii=False,
                                                             indent=2),
                                                  encoding="utf-8")
    return str(destination.resolve())
