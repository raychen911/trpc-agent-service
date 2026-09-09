"""Deterministic module demonstrations; default scenarios never call a paid model."""

from __future__ import annotations

import asyncio
import json
import os

from trpc_service.channels import FakeWeComClient
from trpc_service.channels import WeComChannelAdapter
from trpc_service.config import AgentAppConfig
from trpc_service.config import TenantConfig
from trpc_service.gateway import InMemoryAgentTaskQueue
from trpc_service.gateway import InMemoryIdempotencyStore
from trpc_service.gateway import InMemoryOutboxStore
from trpc_service.log import mask_sensitive_text
from trpc_service.metrics import MetricsRegistry
from trpc_service.migration import InMemoryMigrationStore
from trpc_service.migration import MigrationCoordinator
from trpc_service.migration import MigrationPhase
from trpc_service.migration.records import RecordMigrationProvider
from trpc_service.resources import InMemoryArtifactStore
from trpc_service.resources import InMemoryKnowledgeProvider
from trpc_service.resources import KnowledgeDocument
from trpc_service.storage import InMemorySessionExecutionGuard
from trpc_service.tenant import InMemoryApprovalStore
from trpc_service.tenant import InMemoryTenantRegistry

OFFLINE_SCENARIOS = (
    "config",
    "isolation",
    "storage",
    "session",
    "reliability",
    "channels",
    "artifacts",
    "governance",
    "telemetry",
    "migration",
    "e2e",
    "customer-service",
    "summary",
    "group-session",
    "session-lock",
    "idempotency-recovery",
)
LIVE_SCENARIOS = ("redis-live", "postgres-live", "model-live", "wecom-live", "wecom-kf-live", "telegram-live",
                  "migration-live", "migration-reverse-live")


def _tenant(tenant_id: str, version: int = 1) -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant_id,
        version=version,
        apps={"assistant": AgentAppConfig(app_id="assistant", model={"model_name": "fake-model"})},
    )


async def run_demo(name: str) -> dict[str, object]:
    if name in {"customer-service", "summary", "group-session", "session-lock", "idempotency-recovery"}:
        from trpc_service.reliability_demos import run_reliability_demo
        return await run_reliability_demo(name)
    if name == "config":
        registry = InMemoryTenantRegistry([_tenant("demo", 1)])
        await registry.publish(_tenant("demo", 2))
        await registry.rollback("demo", 1)
        return {"active_version": (await registry.get("demo")).version, "immutable_versions": [1, 2]}
    if name == "isolation":
        first, second = _tenant("tenant-a"), _tenant("tenant-b")
        registry = InMemoryTenantRegistry([first, second])
        from trpc_service.gateway.identity import internal_user_id
        ids = [internal_user_id(item.tenant_id, "web", "same-user") for item in await registry.list_active()]
        assert ids[0] != ids[1]
        return {
            "tenant_ids": [item.tenant_id for item in await registry.list_active()],
            "internal_user_ids": ids,
            "same_external_identity_is_namespaced": ids[0] != ids[1]
        }
    if name == "storage":
        from trpc_service.storage import StorageProviderFactory
        from trpc_service.config import StoragePolicy
        bundle = StorageProviderFactory().create(StoragePolicy())
        created = await bundle.session_service.create_session(app_name="tenant:demo:app:assistant",
                                                              user_id="user",
                                                              session_id="session")
        restored = await bundle.session_service.get_session(app_name=created.app_name,
                                                            user_id=created.user_id,
                                                            session_id=created.id)
        return {
            "backend_tested": "memory",
            "session_restored": restored.id == created.id,
            "redis_sql": "require explicit integration tests"
        }
    if name == "session":
        guard = InMemorySessionExecutionGuard()
        active = maximum = 0

        async def enter() -> None:
            nonlocal active, maximum
            async with guard.hold("one-session", wait_timeout=1, lease_seconds=1):
                active += 1
                maximum = max(maximum, active)
                await asyncio.sleep(0)
                active -= 1

        await asyncio.gather(*(enter() for _ in range(20)))
        return {"requests": 20, "max_same_session_concurrency": maximum}
    if name == "reliability":
        store = InMemoryIdempotencyStore()
        first = await store.reserve("message:1", "request-1", ttl_seconds=60, payload_hash="same")
        duplicate = await store.reserve("message:1", "request-2", ttl_seconds=60, payload_hash="same")
        assert first.request_id == duplicate.request_id
        return {
            "first_request": first.request_id,
            "duplicate_request": duplicate.request_id,
            "unique_reservation_owners": len({first.request_id, duplicate.request_id})
        }
    if name == "channels":
        client = FakeWeComClient()
        adapter = WeComChannelAdapter(client.send_message, expected_bot_id="bot")
        normalized = await adapter.normalize(
            "binding", {
                "cmd": "aibot_msg_callback",
                "headers": {
                    "req_id": "request-m1"
                },
                "body": {
                    "msgid": "m1",
                    "aibotid": "bot",
                    "chattype": "single",
                    "from": {
                        "userid": "u1"
                    },
                    "msgtype": "text",
                    "text": {
                        "content": "hello"
                    }
                }
            }, {})
        return {"channel": normalized.channel.value, "contract": type(normalized).__name__}
    if name == "artifacts":
        store = InMemoryArtifactStore()
        metadata = await store.put("tenant-a", "assistant", "../note.txt", "text/plain", b"agent platform")
        knowledge = InMemoryKnowledgeProvider()
        await knowledge.add(
            KnowledgeDocument(tenant_id="tenant-a", app_id="assistant", title="Agent", text="agent platform knowledge"))
        hits = await knowledge.search("tenant-a", "assistant", "agent")
        other_hits = await knowledge.search("tenant-b", "assistant", "agent")
        return {"safe_name": metadata.original_name, "own_hits": len(hits), "cross_tenant_hits": len(other_hits)}
    if name == "governance":
        approvals = InMemoryApprovalStore()
        arguments_hash = approvals.arguments_hash(json.dumps({"path": "demo"}, sort_keys=True))
        record = await approvals.create("tenant-a", "u1", "s1", "write", arguments_hash)
        _, token = await approvals.decide(record.approval_id, approve=True, actor="teacher")
        used = await approvals.consume(record.approval_id,
                                       token,
                                       tenant_id="tenant-a",
                                       user_id="u1",
                                       session_id="s1",
                                       tool_name="write",
                                       arguments_sha256=arguments_hash)
        return {"approval_state": used.state.value, "masked": mask_sensitive_text("Bearer abc token=secret")}
    if name == "telemetry":
        metrics = MetricsRegistry()
        metrics.increment("trpc_service_requests_total", channel="fake", result="ok")
        blocked = False
        try:
            metrics.increment("unsafe", request_id="test")
        except ValueError:
            blocked = True
        assert blocked
        return {"metric": metrics.render().strip(), "high_cardinality_labels_blocked": blocked}
    if name == "migration":
        provider = RecordMigrationProvider({"event-1": {"text": "hello"}}, {})
        coordinator = MigrationCoordinator(InMemoryMigrationStore(), provider.steps)
        job = await coordinator.create("tenant-a", "record", "local-source", "local-target")
        phases = [job.phase.value]
        while job.phase != MigrationPhase.COMPLETED:
            job = await coordinator.advance(job.job_id)
            phases.append(job.phase.value)
        assert provider.source == provider.target
        return {
            "phases": phases,
            "resumable_checkpoint": job.checkpoint,
            "records_verified": job.source_count,
            "real_redis_sql": False
        }
    if name == "e2e":
        from trpc_service.agent import AgentWorker, TenantRuntimeManager
        from trpc_service.gateway.service import GatewayService
        from trpc_service.gateway.dispatcher import AgentTaskProcessor, DeliveryWorker
        from trpc_service.gateway.queue import AgentTaskEnvelope
        from trpc_service.gateway.models import NormalizedInboundMessage
        from trpc_service.config import ChannelType
        from trpc_service.offline import OfflineRuntimeFactory
        tenant = _tenant("demo")
        from trpc_service.config import ChannelBindingConfig
        tenant.channels = [ChannelBindingConfig(binding_id="fake", app_id="assistant", channel="wecom")]
        registry = InMemoryTenantRegistry([tenant])
        factory = OfflineRuntimeFactory()
        runtimes = TenantRuntimeManager(registry, factory)
        worker = AgentWorker(runtimes, InMemorySessionExecutionGuard())
        gateway = GatewayService(registry, worker, InMemoryIdempotencyStore())
        queue = InMemoryAgentTaskQueue()
        outbox = InMemoryOutboxStore()
        client = FakeWeComClient()

        async def sender(conversation, text, reply):
            return await client.send_message(conversation, text)

        adapter = WeComChannelAdapter(sender)
        try:
            request, key = await gateway.inbound_request(
                NormalizedInboundMessage(message_id="1",
                                         binding_id="fake",
                                         channel=ChannelType.WECOM,
                                         external_user_id="u",
                                         external_conversation_id="c",
                                         text="hello"))
            await gateway.prepare(request)
            await queue.enqueue(AgentTaskEnvelope(request=request, idempotency_key=key))
            processor = AgentTaskProcessor(queue, gateway, outbox, consumer="demo")
            assert await processor.process_one(0.1)
            delivered = await DeliveryWorker(outbox, {"fake": adapter}).deliver_due()
            status = await gateway.request_status("demo", request.request_id)
            assert delivered == 1 and "echo:hello" in client.sent[0][1]
            return {
                "state": status.state.value,
                "reply": client.sent[0][1],
                "delivered": delivered,
                "model_calls": sum(m.calls for m in factory.models),
                "real_sdk_runner": True,
                "network_calls": 0
            }
        finally:
            await runtimes.close()
    if name in LIVE_SCENARIOS:
        if name == "redis-live":
            from redis.asyncio import from_url
            url = os.getenv("TRPC_TEST_REDIS_URL") or os.getenv("TRPC_SERVICE_REDIS_URL")
            if not url:
                raise RuntimeError("TRPC_TEST_REDIS_URL or TRPC_SERVICE_REDIS_URL is required")
            client = from_url(url, decode_responses=True)
            try:
                return {"scenario": name, "ping": bool(await client.ping())}
            finally:
                await client.aclose()
        if name == "postgres-live":
            try:
                import asyncpg
            except ImportError as error:
                raise RuntimeError("install the postgres extra") from error
            url = os.getenv("TRPC_TEST_POSTGRES_URL") or os.getenv("TRPC_SERVICE_POSTGRES_URL")
            if not url:
                raise RuntimeError("TRPC_TEST_POSTGRES_URL or TRPC_SERVICE_POSTGRES_URL is required")
            connection = await asyncpg.connect(url)
            try:
                return {"scenario": name, "select_one": await connection.fetchval("SELECT 1")}
            finally:
                await connection.close()
        if name in {"migration-live", "migration-reverse-live"}:
            if os.getenv("TRPC_MIGRATION_CONFIRM") != "YES":
                raise RuntimeError(f"{name} requires explicit --confirm")
            redis_url = os.getenv("TRPC_TEST_REDIS_URL")
            postgres_url = os.getenv("TRPC_TEST_POSTGRES_URL")
            if not redis_url or not postgres_url:
                raise RuntimeError("TRPC_TEST_REDIS_URL and TRPC_TEST_POSTGRES_URL are required")
            import asyncpg
            from trpc_service.config import BackendType, load_tenant_configs
            from trpc_service.migration import PostgresMigrationStore
            from trpc_service.migration.control import PostgresMigrationControlStore
            from trpc_service.migration.provider import RedisPostgresMigrationProvider
            from trpc_service.tenant import PostgresTenantRegistry, TenantNotFoundError
            configs = load_tenant_configs(os.getenv("TRPC_SERVICE_CONFIG", "examples/config/tenants.yaml"))
            if not configs:
                raise RuntimeError("migration-live requires a tenant configuration")
            configured = configs[0]
            reverse = name == "migration-reverse-live"
            source_backend = BackendType.SQL if reverse else BackendType.REDIS
            target_backend = BackendType.REDIS if reverse else BackendType.SQL
            configured = configured.model_copy(
                update={
                    "storage":
                    configured.storage.model_copy(
                        update={
                            "session": source_backend,
                            "memory": source_backend,
                            "redis_url": redis_url,
                            "sql_url": postgres_url,
                        })
                })
            pool = await asyncpg.create_pool(postgres_url, min_size=1, max_size=6)
            try:
                registry = PostgresTenantRegistry(pool)
                try:
                    tenant = await registry.get(configured.tenant_id)
                except TenantNotFoundError:
                    tenant = await registry.publish(configured)
                if tenant.storage.redis_url != redis_url or tenant.storage.sql_url != postgres_url:
                    raise RuntimeError("stored tenant backend URLs do not match the explicit test URLs")
                if tenant.storage.session != source_backend or tenant.storage.memory != source_backend:
                    raise RuntimeError(f"active tenant storage must be {source_backend.value} before running {name}")
                control = PostgresMigrationControlStore(pool)
                provider = RedisPostgresMigrationProvider(registry, pool, control)
                coordinator = MigrationCoordinator(PostgresMigrationStore(pool), provider.steps)
                existing_job_id = await pool.fetchval(
                    "SELECT job_id FROM migration_job WHERE tenant_id=$1 AND resource_type='session_memory' "
                    "AND source_backend->>'type'=$2 AND target_backend->>'type'=$3 "
                    "AND phase NOT IN ('completed','rolled_back') ORDER BY updated_at DESC LIMIT 1", tenant.tenant_id,
                    source_backend.value, target_backend.value)
                if existing_job_id:
                    job = await coordinator.get(existing_job_id)
                else:
                    job = await coordinator.create(tenant.tenant_id,
                                                   "session_memory",
                                                   source_backend.value,
                                                   target_backend.value,
                                                   batch_size=100,
                                                   rollback_window_seconds=0)
                phases = [job.phase.value]
                for _ in range(10000):
                    if job.phase == MigrationPhase.COMPLETED:
                        break
                    job = await coordinator.advance(job.job_id)
                    phases.append(job.phase.value)
                if job.phase != MigrationPhase.COMPLETED:
                    raise RuntimeError("migration-live exceeded the safety batch limit")
                return {
                    "scenario": name,
                    "direction": f"{source_backend.value}_to_{target_backend.value}",
                    "job_id": job.job_id,
                    "phase": job.phase.value,
                    "source_count": job.source_count,
                    "target_count": job.target_count,
                    "mismatch_count": job.mismatch_count,
                    "dirty_count": job.checkpoint.get("dirty_count", 0),
                    "phases": phases
                }
            finally:
                await pool.close()
        if name == "model-live":
            from trpc_service.config import ServiceSettings
            from trpc_service.config import load_environment_file
            from trpc_service.config import load_tenant_configs
            from trpc_service.web import build_container
            load_environment_file(os.getenv("TRPC_SERVICE_ENV_FILE", ".env"))
            path = os.getenv("TRPC_SERVICE_CONFIG", "examples/config/tenants.yaml")
            configs = load_tenant_configs(path)
            if not configs:
                raise RuntimeError("model-live requires at least one tenant config")
            tenant = configs[0]
            app_id = next(iter(tenant.apps))
            container = build_container(ServiceSettings(environment="development"), configs)
            try:
                request, key = await container.gateway.web_request(tenant_id=tenant.tenant_id,
                                                                   app_id=app_id,
                                                                   external_user_id="model-live-user",
                                                                   session_id="model-live-session",
                                                                   text="Reply with exactly: model-live-ok",
                                                                   idempotency_key=f"model-live-{os.getpid()}")
                result = await container.gateway.chat(request, key)
                return {"scenario": name, "text": result.text, "usage": result.usage.model_dump()}
            finally:
                await container.close()
        if name == "wecom-live":
            from trpc_service.channels import AibotWeComClient
            bot_id = os.getenv("WECOM_BOT_ID", "")
            secret = os.getenv("WECOM_BOT_SECRET", "")
            if not bot_id or not secret:
                raise RuntimeError("WECOM_BOT_ID and WECOM_BOT_SECRET are required")
            client = AibotWeComClient(bot_id, secret)
            try:
                await client.connect(lambda frame: asyncio.sleep(0))
                await client.wait_authenticated()
                return {"scenario": name, "authenticated": True}
            finally:
                await client.close()
        if name == "wecom-kf-live":
            import time
            from trpc_service.channels import CustomerServiceAdapter, HttpCustomerServiceClient
            from trpc_service.channels.customer_store import InMemoryCustomerStore
            from trpc_service.config import ChannelBindingConfig, ChannelType
            from trpc_service.gateway import OutboundMessage
            corp_id = os.getenv("WECOM_KF_CORP_ID", "")
            secret = os.getenv("WECOM_KF_SECRET", "")
            open_kfid = os.getenv("WECOM_KF_OPEN_KFID", "")
            user_id = os.getenv("WECOM_KF_TEST_EXTERNAL_USER_ID", "")
            if not all((corp_id, secret, open_kfid, user_id)):
                raise RuntimeError("WECOM_KF_CORP_ID, WECOM_KF_SECRET, WECOM_KF_OPEN_KFID and "
                                   "WECOM_KF_TEST_EXTERNAL_USER_ID are required")
            binding = ChannelBindingConfig(binding_id="wecom-kf-live",
                                           channel=ChannelType.WECOM_KF,
                                           app_id="live",
                                           corp_id=corp_id,
                                           open_kfid=open_kfid)
            store = InMemoryCustomerStore()

            def remember_customer(state):
                state["customers"][user_id] = time.time()

            await store.mutate(binding.binding_id, remember_customer)
            adapter = CustomerServiceAdapter(binding, HttpCustomerServiceClient(corp_id, secret), store)
            try:
                result = await adapter.deliver(
                    OutboundMessage(outbound_id=f"wecom-kf-live-{os.getpid()}",
                                    request_id="wecom-kf-live",
                                    tenant_id="live",
                                    binding_id=binding.binding_id,
                                    channel=ChannelType.WECOM_KF,
                                    external_conversation_id=f"{open_kfid}:{user_id}",
                                    text="trpc-agent-service live test"))
                return {"scenario": name, **result.model_dump()}
            finally:
                await adapter.close()
        if name == "telegram-live":
            from trpc_service.channels import TelegramChannelAdapter
            from trpc_service.config import ChannelType
            from trpc_service.gateway import OutboundMessage
            token = os.getenv("TELEGRAM_BOT_TOKEN", "")
            chat_id = os.getenv("TELEGRAM_TEST_CHAT_ID", "")
            if not token or not chat_id:
                raise RuntimeError("TELEGRAM_BOT_TOKEN and TELEGRAM_TEST_CHAT_ID are required")
            adapter = TelegramChannelAdapter(token, "live-not-used")
            try:
                result = await adapter.deliver(
                    OutboundMessage(outbound_id="telegram-live",
                                    request_id="telegram-live",
                                    tenant_id="live",
                                    binding_id="live",
                                    channel=ChannelType.TELEGRAM,
                                    external_conversation_id=chat_id,
                                    text="trpc-agent-service live test"))
                return {"scenario": name, **result.model_dump()}
            finally:
                await adapter.close()
    raise ValueError(f"unknown demo scenario: {name}")
