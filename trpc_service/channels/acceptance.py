"""Isolated, single-process functional IM acceptance; never a production runtime.

Each run creates a fresh database and accepts exactly one private random challenge.
The normal IM ingress, official Runner, native SQL Session, budget and delivery
ledger are exercised. Multi-worker native fencing remains a separate release gate.
"""

import asyncio
from contextlib import suppress
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import uuid

import httpx
from sqlalchemy import select
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.configs import RunConfig
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import SqlSessionService, SessionServiceConfig

from trpc_service.agent import RunnerRegistry, tenant_app_name
from trpc_service.agent.openrouter import BudgetedOpenRouter, MODEL, MODEL_ID, PRICE_ID, context_window
from trpc_service.governance.budget import BudgetLedger
from trpc_service.governance.filters import TenantBoundaryFilter
from trpc_service.persistence import Database
from trpc_service.persistence.models import (TenantRow, ChannelBindingRow, IMMembershipRow, AuditLogRow,
                                             InboundMessageRow, OutboxMessageRow, ModelAttemptRow)
from trpc_service.reliability import (DurableExecutionPipeline, InboxRepository, OutboxRepository, PostTurnRepository,
                                      SessionCoordinator)
from trpc_service.storage import GuardedSessionService, GuardedMemoryService
from trpc_service.tenant import ChannelBindingRegistry, MessageRouter, SessionIdentityFactory, TenantConfig
from trpc_service.telemetry.runtime import operation
from .delivery import IMDeliveryWorker
from .persistence import IMAccounts, IMIngress, ContextCipher


def test_tenant(channel, account_id):
    tenant_id = "test_" + uuid.uuid4().hex[:16]

    def backend(kind, role):
        return {"kind": kind, "profile_id": "test_" + role, "namespace": tenant_id}

    return TenantConfig.model_validate({
        "tenant_id":
        tenant_id,
        "name":
        "隔离 IM 联调",
        "applications": [{
            "app_id": "acceptance_app",
            "name": "真实链路测试",
            "agent_name": "acceptance_agent",
            "model_config_id": MODEL_ID,
            "instruction": "你正在进行 IM 全链路测试。简短回答用户，只输出中文文本。",
            "max_tool_iterations": 0,
            "tool_policy": {
                "max_calls_per_run": 0
            }
        }],
        "models": [{
            "model_config_id": MODEL_ID,
            "provider": "openrouter",
            "model_name": MODEL,
            "api_key_ref": {
                "uri": "env://TRPC_MODEL_API_KEY"
            },
            "max_retries": 0,
            "max_output_tokens": 128,
            "currency": "USD"
        }],
        "channel_bindings": [{
            "binding_id": "binding_test",
            "tenant_id": tenant_id,
            "agent_app_id": "acceptance_app",
            "channel": channel,
            "external_account_id": account_id,
            "webhook_public_id": "acceptance_" + uuid.uuid4().hex,
            "transport": {
                "wecom": "wecom_ws",
                "telegram": "telegram_polling",
                "feishu": "feishu_ws"
            }[channel]
        }],
        "data_backends": {
            "session": backend("sql", "session"),
            "summary": backend("sql", "session"),
            "memory": backend("sql", "memory"),
            "knowledge": backend("vector", "knowledge"),
            "artifact": backend("object", "artifact"),
            "audit": backend("sql", "audit")
        }
    })


class AcceptanceRuntime:

    def __init__(self, directory, tenant, identity_key, encryption_keys):
        self.directory, self.tenant = Path(directory), tenant
        # Refuse to reuse state: this entrypoint cannot restart production work.
        self.directory.mkdir(parents=True, exist_ok=False)
        self.binding = tenant.channel_bindings[0]
        self.database = Database("sqlite+pysqlite:///" + (self.directory / "platform.db").as_posix())
        self.database.create_schema()
        with self.database.sessions.begin() as session:
            session.add(
                TenantRow(tenant_id=tenant.tenant_id, name=tenant.name, status="active", active_config_version=1))
            session.add(
                ChannelBindingRow(tenant_id=tenant.tenant_id,
                                  binding_id=self.binding.binding_id,
                                  channel=self.binding.channel.value,
                                  external_account_id=self.binding.external_account_id,
                                  webhook_public_id=self.binding.webhook_public_id,
                                  agent_app_id=self.binding.agent_app_id,
                                  credential_ref="[]",
                                  enabled=True))
        identities = SessionIdentityFactory(lambda _: identity_key)
        self.router = MessageRouter(ChannelBindingRegistry([tenant]), identities)
        self.cipher = ContextCipher(encryption_keys)
        self.accounts = IMAccounts(self.database)
        self.lease = self.accounts.acquire(self.binding, "acceptance")
        self.ingress = IMIngress(self.database, self.cipher, identities)
        self.inbox, self.outbox = InboxRepository(self.database), OutboxRepository(self.database)
        self.pipeline = DurableExecutionPipeline(self.inbox, self.outbox, PostTurnRepository(self.database),
                                                 SessionCoordinator(self.database))
        self.ledger = BudgetLedger(self.database)
        self.ledger.register_price(tenant.tenant_id, PRICE_ID, MODEL_ID, "USD", "1", "2", actor="acceptance")
        self.ledger.configure(tenant.tenant_id, "USD", "0.5", "0.5", "UTC", 0, actor="acceptance")
        self.runner = self.session = self.model = self.memory = None
        self.post_turn_enabled = False
        self.accepted = False
        self.accepted_event_id = None
        self.executed = False

    async def start(self, api_key, context_length, *, transport=None, resources=None):
        app = self.tenant.applications[0]
        # Optional trusted resources extend this isolated harness, never the
        # production worker. The existing CLI text challenge remains unchanged.
        self.post_turn_enabled = resources is not None
        allowed = (app.tool_policy.allow - app.tool_policy.deny) if resources is not None else ()
        self.model = BudgetedOpenRouter(api_key,
                                        self.ledger,
                                        self.tenant.tenant_id,
                                        app.app_id,
                                        context_length,
                                        transport=transport,
                                        allowed_tools=allowed,
                                        max_calls=app.max_tool_iterations + 1 + int(self.post_turn_enabled))
        from trpc_service.governance.privacy import PrivacyFilter
        self.model.model.add_one_filter(
            PrivacyFilter(self.tenant.tenant_id, app.app_id, self.tenant.audit_policy, secrets=[api_key]))
        raw = SqlSessionService(db_url="sqlite+pysqlite:///" + (self.directory / "native.db").as_posix(),
                                is_async=False,
                                expire_on_commit=False,
                                session_config=SessionServiceConfig(store_historical_events=True))
        self.session = GuardedSessionService(raw, self.tenant.tenant_id, app.app_id, threaded=True)
        tools = ()
        if resources is not None:
            from trpc_agent_sdk.memory import SqlMemoryService
            from trpc_agent_sdk.tools import PreloadMemoryTool
            from trpc_service.storage.runtime_resources import resource_tools
            from trpc_service.agent.tool_assembly import assemble_tools
            self.memory = SqlMemoryService(db_url="sqlite+pysqlite:///" + (self.directory / "memory.db").as_posix(),
                                           enabled=True,
                                           is_async=False,
                                           expire_on_commit=False)
            tools = assemble_tools(self.database,
                                   self.tenant,
                                   app,
                                   resource_tools(resources, self.tenant, app),
                                   model=self.model.model)
            # The public SDK preloader has no model-callable declaration. Memory
            # lookup is bound to InvocationContext and guarded tenant/user scope.
            tools = (*tools, PreloadMemoryTool())
        agent = LlmAgent(name=app.agent_name,
                         model=self.model.model,
                         instruction=app.instruction,
                         generate_content_config=self.model.accounting.generation_config(),
                         tools=list(tools),
                         filters=[TenantBoundaryFilter(self.tenant.tenant_id, app.app_id)])
        self.runner = Runner(app_name=tenant_app_name(self.tenant.tenant_id, app.app_id),
                             agent=agent,
                             session_service=self.session,
                             artifact_service=resources.artifact if resources is not None else None,
                             memory_service=GuardedMemoryService(self.memory, self.tenant.tenant_id, app.app_id)
                             if self.memory is not None else None,
                             enable_post_turn_processing=False,
                             close_memory_service_on_close=False,
                             close_session_service_on_close=False)
        self.registry = RunnerRegistry()
        key = self.tenant.tenant_id, app.app_id, 1
        self.registry.register(*key, self.runner)
        self.registry.run_configs[key] = RunConfig(max_llm_calls=app.max_tool_iterations + 1,
                                                   max_tool_calls=app.tool_policy.max_calls_per_run,
                                                   streaming=False)

    def accept_challenge(self, event, challenge):
        if (event.kind != "chat" or event.message.conversation_type.value != "direct"
                or event.message.text != challenge):
            raise PermissionError("only one exact private test challenge is permitted")
        if self.accepted and event.event_id != self.accepted_event_id:
            raise PermissionError("test challenge already accepted")
        route = self.router.route_message(event.message)
        if not self.accepted:
            with self.database.sessions.begin() as session:
                session.merge(
                    IMMembershipRow(tenant_id=route.tenant_id,
                                    binding_id=route.channel_binding_id,
                                    actor_id=route.actor_id,
                                    active=True,
                                    grants_json=["chat"]))
                session.add(
                    AuditLogRow(audit_id=uuid.uuid4().hex,
                                tenant_id=route.tenant_id,
                                user_id=route.actor_id,
                                decision="acceptance_challenge_enrolled",
                                redacted=True,
                                metadata_json={}))
        with operation("im.callback", attributes={"tenant.id": route.tenant_id, "channel": self.binding.channel.value}):
            receipt = self.ingress.record(event, self.binding, self.lease, route=route)
            duplicate = self.ingress.record(event, self.binding, self.lease, route=route)
        if duplicate.inbound_id != receipt.inbound_id or not duplicate.duplicate:
            raise RuntimeError("duplicate callback check failed")
        self.accepted, self.accepted_event_id = True, event.event_id
        return route

    async def execute(self, event, adapter, challenge):
        if self.executed:
            raise PermissionError("test execution already started")
        route = self.accept_challenge(event, challenge)
        self.executed = True
        work = self.inbox.claim(worker_id="acceptance-worker", lease_seconds=120, include_im=True)
        await asyncio.wait_for(
            self.pipeline.execute(work, self.registry, worker_id="acceptance-worker", lease_seconds=120),
            max(70, 45 * (self.tenant.applications[0].max_tool_iterations + 1)))
        native = await self.session.get_session(app_name=self.session.app_name,
                                                user_id=route.internal_user_id,
                                                session_id=route.session_id)
        delivery = IMDeliveryWorker(self.database, self.lease, adapter, self.cipher)
        # Bound the drain; explicit rejection stays in its ledger for inspection.
        for _ in range(16):
            if not await delivery.run_once():
                break
        if self.post_turn_enabled:
            await self.process_post_turn(route, event.message, work.execution_id)
        report = self.report()
        report.update(native_session_events=len(native.events) if native else 0, duplicate_suppressed=True)
        return report

    async def process_post_turn(self, route, message, execution_id):
        from trpc_agent_sdk.context import InvocationContext
        from trpc_agent_sdk.sessions import SummarizerSessionManager
        from trpc_service.agent.bridge import build_agent_context
        from trpc_service.agent.summary_model import ConfiguredSummaryModel
        from trpc_service.persistence.models import SessionEventRow
        from trpc_service.reliability import SummaryRepository
        from trpc_service.service_runtime import BoundarySummarizer
        from trpc_service.storage import GuardedMemoryService
        tasks = PostTurnRepository(self.database)
        with self.database.sessions() as session:
            inbound = session.scalar(select(InboundMessageRow).where(InboundMessageRow.execution_id == execution_id))
            parent = inbound.payload_json.get("trace_context")
            boundary = session.scalar(
                select(SessionEventRow.seq_no).where(SessionEventRow.execution_id == execution_id).order_by(
                    SessionEventRow.seq_no.desc()))
        context = build_agent_context(route, message)
        context.with_metadata("execution_id", execution_id)
        for task in sorted(tasks.claim(limit=16, worker_id="acceptance-post", lease_seconds=120),
                           key=lambda item: item.task_type):
            try:
                with operation("post_turn.execute", parent=parent, attributes={"tenant.id": route.tenant_id}):
                    native = await self.session.get_session(app_name=self.session.app_name,
                                                            user_id=route.internal_user_id,
                                                            session_id=route.session_id)
                    if task.task_type == "memory":
                        memory = GuardedMemoryService(self.memory, route.tenant_id, route.agent_app_id)
                        await memory.store_session(native, agent_context=context)
                    else:
                        summary_model = ConfiguredSummaryModel(self.model)
                        summarizer = BoundarySummarizer(model=summary_model,
                                                        keep_recent_count=1,
                                                        start_by_user_turn=False,
                                                        boundary=boundary,
                                                        native_ids=[item.id for item in native.events])
                        manager = SummarizerSessionManager(model=summary_model,
                                                           summarizer=summarizer,
                                                           auto_summarize=False)
                        manager.set_session_service(self.session)
                        ctx = InvocationContext(session_service=self.session,
                                                session=native,
                                                invocation_id="summary_" + task.task_id,
                                                agent=self.registry.resolve(route.tenant_id, route.agent_app_id,
                                                                            route.config_version).agent,
                                                agent_context=context)
                        with operation("summary.generate"):
                            await manager.create_session_summary(native, force=True, ctx=ctx)
                        anchor = next((item for item in native.events if item.is_summary_event()), None)
                        if anchor is None:
                            raise RuntimeError("native summary was not produced")
                        SummaryRepository(self.database).commit(route.tenant_id, route.session_id, boundary,
                                                                anchor.get_text(), summary_model.name)
                tasks.complete(task.task_id, "acceptance-post")
            except Exception as error:
                tasks.fail(task.task_id, "acceptance-post", type(error).__name__, retry=False, delay_seconds=0)
                raise

    def report(self):
        from trpc_service.persistence.models import PostTurnTaskRow
        with self.database.sessions() as session:
            inputs = list(session.scalars(select(InboundMessageRow)))
            outputs = list(session.scalars(select(OutboxMessageRow)))
            attempts = list(session.scalars(select(ModelAttemptRow)))
            return {
                "scope":
                "isolated_single_process_tool_acceptance"
                if self.post_turn_enabled else "isolated_single_process_text_acceptance",
                "production_ready":
                False,
                "channel":
                self.binding.channel.value,
                "timestamp":
                datetime.now(timezone.utc).isoformat(),
                "input_statuses": [row.status for row in inputs],
                "reply_statuses": [row.status for row in outputs],
                "model_attempts": [{
                    "status": row.status,
                    "input_tokens": row.input_tokens,
                    "output_tokens": row.output_tokens,
                    "actual_amount": str(row.actual_amount) if row.actual_amount is not None else None,
                    "currency": row.currency
                } for row in attempts],
                "post_turn":
                "executed" if self.post_turn_enabled and self._post_turn_complete(session) else "not_executed",
                "post_turn_statuses": {
                    row.task_type: row.status
                    for row in session.scalars(select(PostTurnTaskRow))
                },
                "human_read_confirmation":
                False
            }

    def _post_turn_complete(self, session):
        from trpc_service.persistence.models import PostTurnTaskRow
        states = list(session.scalars(select(PostTurnTaskRow.status)))
        return len(states) >= 2 and all(state == "succeeded" for state in states)

    async def close(self):
        errors = []
        for component in (self.runner, self.session.raw_service if self.session else None, self.memory, self.model):
            if component:
                try:
                    await component.close()
                except Exception:
                    errors.append("resource_close_failed")
        try:
            self.accounts.release(self.lease)
        except Exception:
            errors.append("account_release_failed")
        finally:
            self.database.close()
        return errors


async def await_wecom(adapter, binding, challenge, timeout, emit, runtime=None):
    ready, lost = asyncio.Event(), asyncio.Event()
    queue = asyncio.Queue(maxsize=1)

    async def authenticated(_):
        ready.set()

    async def disconnected(_):
        lost.set()

    async def receive(frame):
        with suppress(Exception):
            event = adapter.normalize(frame, binding)
            if (event.kind == "chat" and event.message.conversation_type.value == "direct"
                    and event.message.text == challenge and queue.empty()):
                if runtime is not None:
                    if runtime.accepted and runtime.accepted_event_id != event.event_id:
                        return
                    await asyncio.to_thread(runtime.accept_challenge, event, challenge)
                queue.put_nowait(event)

    adapter.client.on("authenticated", authenticated)
    adapter.client.on("disconnected", disconnected)
    adapter.client.on("error", disconnected)
    adapter.client.on("message", receive)
    await asyncio.wait_for(adapter.client.connect_async(), 20)
    await asyncio.wait_for(ready.wait(), 20)
    emit("企微认证成功。请在机器人单聊中发送完整测试口令：")
    emit(challenge)
    message, broken = asyncio.create_task(queue.get()), asyncio.create_task(lost.wait())
    try:
        done, _ = await asyncio.wait({message, broken}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        if broken in done or message not in done:
            raise TimeoutError("test message not received")
        return message.result()
    finally:
        message.cancel()
        broken.cancel()
        await asyncio.gather(message, broken, return_exceptions=True)


async def run_from_environment(channel, timeout=180):
    from trpc_service.sdk_provenance import verify_official_sdk
    from .runtime import quiet_transport_logging
    verify_official_sdk()
    quiet_transport_logging()
    required = ["TRPC_MODEL_API_KEY", "TRPC_IDENTITY_KEY", "TRPC_IM_CONTEXT_KEYS"]
    required += {
        "wecom": ["TRPC_WECOM_BOT_ID", "TRPC_WECOM_BOT_SECRET"],
        "telegram": ["TRPC_TELEGRAM_BOT_TOKEN"],
        "feishu": ["TRPC_FEISHU_APP_ID", "TRPC_FEISHU_APP_SECRET"]
    }[channel]
    if any(not os.getenv(name) for name in required):
        print("缺少联调凭据，请先用 configure-im 配置所选通道与模型。")
        return False
    runtime = adapter = renewal = receiver = None
    report = {"channel": channel, "status": "not_started", "production_ready": False}
    stage = "preparing"
    run_id = uuid.uuid4().hex
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=False) as client:
            window = await context_window(client)
        if channel == "wecom":
            from .wecom import WecomAdapter
            account = os.environ["TRPC_WECOM_BOT_ID"]
            adapter = WecomAdapter.create(account, os.environ["TRPC_WECOM_BOT_SECRET"])
        elif channel == "feishu":
            from .feishu import FeishuAdapter
            account = os.environ["TRPC_FEISHU_APP_ID"]
            adapter = FeishuAdapter(account, os.environ["TRPC_FEISHU_APP_SECRET"])
            await adapter.initialize()
        else:
            from telegram import Bot
            from .telegram import TelegramAdapter
            adapter = TelegramAdapter(Bot(os.environ["TRPC_TELEGRAM_BOT_TOKEN"]))
            await adapter.bot.initialize()
            account = str(adapter.bot.id)
            if (await adapter.bot.get_webhook_info()).url:
                raise ValueError("polling test requires webhook to be explicitly removed")
        runtime = AcceptanceRuntime(
            Path(".secrets/acceptance") / run_id, test_tenant(channel,
                                                              account), os.environ["TRPC_IDENTITY_KEY"].encode(),
            [key.encode() for key in os.environ["TRPC_IM_CONTEXT_KEYS"].split(",")])
        await runtime.start(os.environ["TRPC_MODEL_API_KEY"], window)

        async def renew():
            while True:
                await asyncio.sleep(15)
                await asyncio.to_thread(runtime.accounts.renew, runtime.lease)

        renewal = asyncio.create_task(renew())
        phrase = "全链路测试 " + secrets.token_hex(6) + " 请只回复：真实 Agent 链路成功"

        def emit(value):
            print("[" + channel + "] " + value, flush=True)

        emit("本次只接收一条测试口令，最多一次模型调用、128 输出 tokens；预算上限 0.50 USD。")
        stage = "waiting_message"
        if channel == "wecom":
            event = await await_wecom(adapter, runtime.binding, phrase, timeout, emit, runtime)
        elif channel == "feishu":
            ready, queue = asyncio.Event(), asyncio.Queue(maxsize=1)
            diagnostic_messages = set()

            async def receive(candidate):
                reason = ("matched" if candidate.kind == "chat" and candidate.message.text == phrase else
                          "text_mismatch" if candidate.kind == "chat" else candidate.kind)
                if reason not in diagnostic_messages:
                    diagnostic_messages.add(reason)
                    emit("收到飞书事件：" + reason + "（不记录消息正文）。")
                if (candidate.kind == "chat" and candidate.message.conversation_type.value == "direct"
                        and candidate.message.text == phrase):
                    if runtime.accepted and runtime.accepted_event_id != candidate.event_id:
                        return
                    await asyncio.to_thread(runtime.accept_challenge, candidate, phrase)
                    if queue.empty():
                        queue.put_nowait(candidate)

            receiver = asyncio.create_task(adapter.receive(runtime.binding, receive, ready))
            ready_wait = asyncio.create_task(ready.wait())
            try:
                done, _ = await asyncio.wait({ready_wait, receiver}, timeout=35, return_when=asyncio.FIRST_COMPLETED)
                if receiver in done:
                    receiver.result()
                if not ready.is_set():
                    raise TimeoutError("Feishu did not connect")
            finally:
                ready_wait.cancel()
                await asyncio.gather(ready_wait, return_exceptions=True)
            emit("飞书长连接已连接；可保存接收消息事件并发布。请在机器人单聊中发送完整测试口令：")
            emit(phrase)
            message_wait = asyncio.create_task(queue.get())
            try:
                done, _ = await asyncio.wait({message_wait, receiver},
                                             timeout=timeout,
                                             return_when=asyncio.FIRST_COMPLETED)
                if receiver in done:
                    receiver.result()
                if message_wait not in done:
                    raise TimeoutError("test message not received")
                event = message_wait.result()
            finally:
                message_wait.cancel()
                await asyncio.gather(message_wait, return_exceptions=True)
        else:
            emit("Telegram 认证成功。请在机器人单聊中发送完整测试口令：")
            emit(phrase)

            async def wait_telegram():
                offset = None
                while True:
                    updates = await adapter.poll(offset)
                    for update in sorted(updates, key=lambda item: item.update_id):
                        offset = update.update_id + 1
                        candidate = adapter.normalize(update.to_dict(), runtime.binding)
                        if (candidate.kind == "chat" and candidate.message.conversation_type.value == "direct"
                                and candidate.message.text == phrase):
                            return candidate

            event = await asyncio.wait_for(wait_telegram(), timeout)
        emit("已收到测试消息，正在执行权限、预算、Runner、原生 Session 和投递链路。")
        stage = "executing"
        report = await runtime.execute(event, adapter, phrase)
        report["status"] = ("passed" if report["input_statuses"] == ["succeeded"] and report["reply_statuses"] and all(
            value == "delivered"
            for value in report["reply_statuses"]) and all(value["status"] == "settled"
                                                           for value in report["model_attempts"]) else "incomplete")
        emit("联调结果：" + report["status"] + "；详情已写入脱敏报告。")
    except Exception as error:
        report = runtime.report() if runtime else report
        report.update(status="incomplete", error_type=type(error).__name__, stage=stage)
        print("联调未完成；不自动重试模型或未知投递，脱敏报告保留了当前状态。", flush=True)
    finally:
        if channel == "feishu" and adapter:
            report["transport_diagnostics"] = dict(adapter.diagnostics)
        if receiver:
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)
        if renewal:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
        if adapter:
            with suppress(Exception):
                if channel in {"wecom", "feishu"}:
                    await adapter.close()
                else:
                    await adapter.bot.shutdown()
        if runtime:
            cleanup_errors = await runtime.close()
            if cleanup_errors:
                report.update(cleanup_errors=cleanup_errors, status="incomplete")
        Path("reports").mkdir(exist_ok=True)
        Path("reports", f"live-{channel}-acceptance-{run_id}.json").write_text(json.dumps(report,
                                                                                          ensure_ascii=False,
                                                                                          indent=2),
                                                                               encoding="utf-8")
    return report.get("status") == "passed"
