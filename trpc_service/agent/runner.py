"""Stateless worker coordinator around tRPC-Agent-Python or a local fallback."""

from __future__ import annotations

import asyncio
import hashlib
import re
import time
from collections.abc import Callable
from typing import Any, Protocol
from uuid import uuid4

from ..channels import ChannelDispatcher
from ..log import redact, redact_text
from ..metrics import MetricsRegistry, TraceContext
from ..metrics.prometheus import (
    observe_operation,
    record_fencing_conflict,
    record_inbound,
    record_model_usage,
    record_queue_depth,
)
from ..metrics.telemetry import span
from ..queue import InMemoryMessageQueue, QueueDelivery
from ..storage.artifacts import Artifact
from ..storage.models import FencingConflict, LeaseBusy
from ..tenant import (
    AuditStore,
    IdempotencyStore,
    InboundMessage,
    MemoryStore,
    SessionStore,
    TenantRegistry,
)
from ..tool import BudgetExceeded, InMemoryBudgetLedger, TenantPolicyFilter, active_tool_turn

_HIDDEN_REASONING_BLOCK = re.compile(
    r"<(?P<tag>think|thinking|reasoning)\b[^>]*>.*?</(?P=tag)\s*>",
    re.IGNORECASE | re.DOTALL,
)
_UNCLOSED_REASONING_BLOCK = re.compile(
    r"<(?:think|thinking|reasoning)\b[^>]*>.*\Z",
    re.IGNORECASE | re.DOTALL,
)


def _strip_hidden_reasoning(text: str) -> str:
    """Remove provider-specific reasoning markup from user-visible output."""
    cleaned = _HIDDEN_REASONING_BLOCK.sub("", text)
    cleaned = _UNCLOSED_REASONING_BLOCK.sub("", cleaned)
    return cleaned.strip()


def _final_answer_text(event: object) -> str:
    """Extract only user-visible answer text from a final tRPC event."""
    is_final = getattr(event, "is_final_response", None)
    if not callable(is_final) or not is_final():
        return ""
    if getattr(event, "partial", False) or not getattr(event, "visible", True):
        return ""

    content = getattr(event, "content", None)
    parts = getattr(content, "parts", []) if content is not None else []
    if any(
        getattr(part, field, None) is not None
        for part in parts or []
        for field in ("function_call", "function_response", "executable_code", "code_execution_result")
    ):
        return ""

    chunks = [
        text
        for part in parts or []
        if not getattr(part, "thought", False) and isinstance((text := getattr(part, "text", None)), str) and text
    ]
    return _strip_hidden_reasoning("".join(chunks))


class AgentExecutor(Protocol):
    async def reply(self, message: InboundMessage, memory: list[str], instruction: str) -> str: ...


class EchoExecutor:
    """Deterministic executor for local development and acceptance tests."""

    async def reply(self, message: InboundMessage, memory: list[str], instruction: str) -> str:
        if not message.text.strip():
            return "请发送文字消息，我会继续协助。"
        suffix = f"（已关联 {len(memory)} 条长期记忆）" if memory else ""
        return f"已收到：{message.text.strip()}{suffix}"


class TRPCAgentExecutor:
    """Adapter for a pre-built tRPC-Agent ``Runner``.

    The platform owns routing and persistence; the framework still owns agent
    orchestration, model calls, Tool/MCP and event generation.
    """

    def __init__(
        self,
        runner: object,
        user_id: str = "",
        artifact_store: object | None = None,
        timeout_seconds: float = 120.0,
    ) -> None:
        self.runner = runner
        self.user_id = user_id
        self.artifact_store = artifact_store
        self.timeout_seconds = timeout_seconds

    async def reply(self, message: InboundMessage, memory: list[str], instruction: str) -> str:
        try:
            from trpc_agent_sdk.types import Content, Part
        except ImportError as exc:
            raise RuntimeError("trpc-agent-py is required for TRPCAgentExecutor") from exc
        prompt = message.text
        if memory:
            recalled = "\n".join(f"- {item}" for item in memory[-20:])[:8000]
            prompt = f"Relevant tenant-scoped memory:\n{recalled}\n\nCurrent user message:\n{message.text}"
        parts = [Part.from_text(text=prompt)]
        media = message.raw.get("normalized_media")
        artifact_data = media.get("artifact") if isinstance(media, dict) else None
        if (
            message.raw.get("_artifact_materialized") is True
            and isinstance(artifact_data, dict)
            and self.artifact_store is not None
        ):
            artifact_fields = {
                key: artifact_data[key]
                for key in ("tenant_id", "artifact_id", "key", "checksum", "size", "content_type")
            }
            artifact = Artifact(**artifact_fields)
            if artifact.tenant_id != message.tenant_id:
                raise ValueError("artifact tenant does not match inbound message")
            data = await self.artifact_store.get(artifact)
            parts.append(Part.from_bytes(data=data, mime_type=artifact.content_type))
        content = Content(parts=parts)
        final = ""
        iterator = self.runner.run_async(
            user_id=self.user_id or message.external_user_id,
            session_id=message.session_id,
            new_message=content,
        ).__aiter__()
        deadline = time.monotonic() + self.timeout_seconds
        with active_tool_turn(message):
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("tRPC-Agent turn exceeded its timeout")
                try:
                    event = await asyncio.wait_for(iterator.__anext__(), timeout=remaining)
                except StopAsyncIteration:
                    break
                answer = _final_answer_text(event)
                if answer:
                    final = answer
        return final or "已完成处理，但没有可展示的最终回复。"


class TenantExecutorRouter:
    """Route each tenant/app pair to its isolated Agent executor."""

    def __init__(
        self,
        executors: dict[tuple[str, ...], AgentExecutor],
        fallback: AgentExecutor | None = None,
        factory: Callable[[object, object], AgentExecutor] | None = None,
    ) -> None:
        self.executors = executors
        self.fallback = fallback or EchoExecutor()
        self.factory = factory

    def configure(self, config: object) -> None:
        if self.factory is None:
            return
        tenant_id = str(config.tenant_id)
        version = int(config.version)
        for app in config.apps.values():
            key = (tenant_id, str(app.app_id), version)
            if key not in self.executors:
                self.executors[key] = self.factory(config, app)

    async def reply(self, message: InboundMessage, memory: list[str], instruction: str) -> str:
        executor = self.executors.get(
            (message.tenant_id, message.app_id, message.config_version),
            self.executors.get((message.tenant_id, message.app_id), self.fallback),
        )
        return await executor.reply(message, memory, instruction)


class AgentService:
    """Gateway-to-worker runtime with at-least-once queue semantics."""

    def __init__(
        self,
        registry: TenantRegistry,
        dispatcher: ChannelDispatcher,
        executor: AgentExecutor | None = None,
        queue_size: int = 1000,
        sessions: SessionStore | None = None,
        memories: MemoryStore | None = None,
        audits: AuditStore | None = None,
        queue: object | None = None,
        idempotency: object | None = None,
        consumer_id: str | None = None,
        max_delivery_attempts: int = 3,
        budgets: object | None = None,
        config_loader: object | None = None,
        storage_router: object | None = None,
        media_materializer: object | None = None,
        projector: object | None = None,
        knowledge_retriever: object | None = None,
    ) -> None:
        self.registry = registry
        self.dispatcher = dispatcher
        self.executor = executor or EchoExecutor()
        self.queue = queue or InMemoryMessageQueue(queue_size)
        self.idempotency = idempotency or IdempotencyStore()
        self.sessions = sessions or SessionStore()
        self.memories = memories or MemoryStore()
        self.audits = audits or AuditStore()
        self.metrics = MetricsRegistry()
        self.consumer_id = consumer_id or f"worker-{uuid4().hex[:12]}"
        self.max_delivery_attempts = max_delivery_attempts
        self.budgets = budgets or InMemoryBudgetLedger()
        self.config_loader = config_loader
        self.storage_router = storage_router
        self.media_materializer = media_materializer
        self.projector = projector
        self.knowledge_retriever = knowledge_retriever
        self._projection_tasks: set[asyncio.Task[Any]] = set()

    @staticmethod
    def idempotency_key(message: InboundMessage) -> str:
        return f"{message.tenant_id}:{message.channel}:{message.account_id}:{message.external_message_id}"

    async def enqueue(self, message: InboundMessage) -> bool:
        """Claim the external message and queue it; duplicate delivery is a no-op."""
        key = self.idempotency_key(message)
        accept = getattr(self.idempotency, "accept", None)
        if accept is not None:
            with span(
                "gateway.inbox",
                tenant_id=message.tenant_id,
                channel=message.channel,
                trace_id=message.trace_id,
            ):
                accepted = await accept(message)
            if not accepted:
                self.metrics.inc("inbound_duplicate", tenant=message.tenant_id, channel=message.channel)
                record_inbound(message.tenant_id, message.channel, "duplicate")
                return False
            self.metrics.inc("inbound_accepted", tenant=message.tenant_id, channel=message.channel)
            record_inbound(message.tenant_id, message.channel, "accepted")
            return True
        if not await self.idempotency.claim(key):
            self.metrics.inc("inbound_duplicate", tenant=message.tenant_id, channel=message.channel)
            record_inbound(message.tenant_id, message.channel, "duplicate")
            return False
        try:
            with span("queue.publish", tenant_id=message.tenant_id, channel=message.channel, trace_id=message.trace_id):
                await self.queue.put(message)
        except asyncio.QueueFull:
            await self.idempotency.release(key)
            self.metrics.inc("inbound_rejected", tenant=message.tenant_id, channel=message.channel)
            record_inbound(message.tenant_id, message.channel, "rejected")
            raise RuntimeError("agent queue is full")
        self.metrics.inc("inbound_accepted", tenant=message.tenant_id, channel=message.channel)
        record_inbound(message.tenant_id, message.channel, "accepted")
        record_queue_depth(type(self.queue).__name__, self.queue.qsize())
        return True

    async def already_received(self, message: InboundMessage) -> bool:
        """Check a durable Inbox before admission controls consume quota."""
        key = self.idempotency_key(message)
        contains = getattr(self.idempotency, "contains", None)
        if contains is not None:
            return bool(await contains(key))
        return await self.idempotency.result(key) is not None

    async def process_one(self) -> list[object]:
        delivery = await self.queue.get(self.consumer_id)
        if delivery is None:
            return []
        try:
            with span(
                "queue.consume", tenant_id=delivery.message.tenant_id,
                attempts=delivery.attempts, trace_id=delivery.message.trace_id,
            ):
                result = await self.process(delivery.message)
        except Exception as exc:
            await self.queue.fail(delivery, type(exc).__name__, self.max_delivery_attempts)
            if delivery.attempts + 1 >= self.max_delivery_attempts:
                fail = getattr(self.idempotency, "fail", None)
                if fail is not None:
                    await fail(self.idempotency_key(delivery.message), type(exc).__name__)
            raise
        await self.queue.ack(delivery)
        record_queue_depth(type(self.queue).__name__, self.queue.qsize())
        return result

    async def process(self, message: InboundMessage) -> list[object]:
        start = time.perf_counter()
        with TraceContext(message.trace_id), span("agent.turn", tenant_id=message.tenant_id, trace_id=message.trace_id):
            key = self.idempotency_key(message)
            previous = await self.idempotency.result(key)
            if isinstance(previous, dict) and previous.get("status") == "completed":
                return []
            if isinstance(previous, dict) and previous.get("status") == "failed":
                return []
            if self.config_loader is None:
                config = self.registry.get(message.tenant_id, message.config_version)
            else:
                config = await self.config_loader(message.tenant_id, message.config_version)
            if self.storage_router is not None:
                self.storage_router.validate(config)
            binding = config.channels.get(message.channel)
            if binding is None or not binding.enabled or binding.account_id != message.account_id:
                raise ValueError("pinned channel binding is unavailable")
            if self.storage_router is None:
                sessions, memories, audits = self.sessions, self.memories, self.audits
            else:
                routed = self.storage_router.route(config)
                sessions, memories, audits = routed.session, routed.memory, routed.audit
            if isinstance(previous, dict) and previous.get("status") in {"prepared", "delivery_failed"}:
                await self._remember(memories, message, key)
                deliveries = await self.dispatcher.reply(message, str(previous.get("text", "")), binding)
                await audits.write(self._audit(message, "allow", "ok", start, 2))
                await self.idempotency.complete(key, {"status": "completed", "text": previous.get("text", "")})
                return deliveries
            configure = getattr(self.executor, "configure", None)
            if configure is not None:
                configure(config)
            policy = TenantPolicyFilter(config)
            user_decision = policy.check_user(message.external_user_id, message.channel, message.account_id)
            input_decision = policy.check_input(message.text)
            if not user_decision.allowed or not input_decision.allowed:
                text = "当前消息未通过租户安全策略。"
                await audits.write(
                    self._audit(message, "deny", input_decision.reason or user_decision.reason, start)
                )
                deliveries = await self.dispatcher.reply(message, text, binding)
                await self.idempotency.complete(key, {"status": "completed", "text": text})
                return deliveries

            if self.media_materializer is not None:
                adapter = self.dispatcher.adapters.get(message.channel)
                if adapter is None:
                    raise ValueError("pinned channel binding is unavailable")
                message = await self.media_materializer.materialize(config, message, adapter, binding)

            await sessions.get_or_create(
                message.tenant_id, message.session_id, message.app_id, message.external_user_id
            )
            async with sessions.lock(message.tenant_id, message.session_id) as lease:
                # A compensating Outbox publish can create another notification
                # while the original Worker is still running. Recheck after the
                # session lease serializes those deliveries.
                latest = await self.idempotency.result(key)
                if previous is None and isinstance(latest, dict) and latest.get("status") in {
                    "completed",
                    "failed",
                    "prepared",
                    "delivery_failed",
                }:
                    return []
                recover = getattr(sessions, "prepared_result", None)
                if previous is None and latest is None and recover is not None:
                    recovered = await recover(message.tenant_id, key)
                    if isinstance(recovered, dict) and recovered.get("status") == "prepared":
                        reply = str(recovered.get("text", ""))
                        await self.idempotency.complete(key, recovered)
                        await self._remember(memories, message, key)
                        deliveries = await self.dispatcher.reply(message, reply, binding)
                        await audits.write(self._audit(message, "allow", "recovered", start, 2))
                        await self.idempotency.complete(key, {"status": "completed", "text": reply})
                        return deliveries
                with span("session.read", tenant_id=message.tenant_id, trace_id=message.trace_id):
                    snapshot = await sessions.get_or_create(
                        message.tenant_id, message.session_id, message.app_id, message.external_user_id
                    )
                with span("memory.read", tenant_id=message.tenant_id, trace_id=message.trace_id):
                    memory = await memories.list(message.tenant_id, message.external_user_id)
                app = config.apps.get(message.app_id) or next(iter(config.apps.values()))
                if self.knowledge_retriever is not None and app.knowledge_collections:
                    knowledge = await self.knowledge_retriever.recall(
                        config,
                        message.tenant_id,
                        message.text,
                        app.knowledge_collections,
                    )
                    memory.extend(f"[knowledge] {value}" for value in knowledge)
                reservation_tokens = min(config.policy.daily_token_budget, max(512, len(message.text) // 2 + 2048))
                try:
                    budget_lease = await self.budgets.reserve(
                        message.tenant_id, config.policy.daily_token_budget, reservation_tokens
                    )
                except BudgetExceeded:
                    text = "当前租户今日调用预算已用完。"
                    await audits.write(self._audit(message, "deny", "daily_token_budget_exceeded", start))
                    deliveries = await self.dispatcher.reply(message, text, binding)
                    await self.idempotency.complete(key, {"status": "completed", "text": text})
                    return deliveries
                try:
                    with (
                        span(
                            "runner",
                            tenant_id=message.tenant_id,
                            channel=message.channel,
                            trace_id=message.trace_id,
                        ),
                        observe_operation("model", message.tenant_id, "reply"),
                    ):
                        reply = await self.executor.reply(message, memory, app.instruction)
                    if config.policy.redact_output:
                        reply = redact_text(reply)
                except BaseException:
                    await self.budgets.settle(budget_lease, reservation_tokens)
                    raise
                input_tokens = max(1, len(message.text) // 4)
                output_tokens = max(1, len(reply) // 4)
                actual_tokens = input_tokens + output_tokens
                cost = (
                    input_tokens * app.input_cost_per_million
                    + output_tokens * app.output_cost_per_million
                ) / 1_000_000
                record_model_usage(message.tenant_id, input_tokens, output_tokens, cost)
                await self.budgets.settle(budget_lease, actual_tokens)
                with (
                    span("session.write", tenant_id=message.tenant_id, trace_id=message.trace_id),
                    observe_operation("storage", message.tenant_id, "session.write"),
                ):
                    events = await sessions.append_turn(
                        snapshot,
                        message.text,
                        reply,
                        message.trace_id,
                        lease,
                        inbox_key=key,
                        config_version=message.config_version,
                    )
                await self.idempotency.complete(key, {"status": "prepared", "text": reply})
            with (
                span("memory.write", tenant_id=message.tenant_id, trace_id=message.trace_id),
                observe_operation("storage", message.tenant_id, "memory.write"),
            ):
                await self._remember(memories, message, key)
            if self.projector is not None:
                self._schedule_projection(config, message, snapshot.version)
            with (
                span(
                    "im.reply",
                    tenant_id=message.tenant_id,
                    channel=message.channel,
                    trace_id=message.trace_id,
                ),
                observe_operation("im", message.tenant_id, "reply"),
            ):
                deliveries = await self.dispatcher.reply(message, reply, binding)
            await audits.write(self._audit(message, "allow", "ok", start, len(events), cost))
            await self.idempotency.complete(key, {"status": "completed", "text": reply})
            self.metrics.inc("agent_turn_completed", tenant=message.tenant_id, channel=message.channel)
            return deliveries

    def _schedule_projection(self, config: object, message: InboundMessage, source_version: int) -> None:
        task = asyncio.create_task(
            self.projector.project(config, message.tenant_id, message.session_id, source_version),
            name=f"session-projection:{message.tenant_id}:{message.session_id}:{source_version}",
        )
        self._projection_tasks.add(task)
        task.add_done_callback(self._projection_done)

    def _projection_done(self, task: asyncio.Task[Any]) -> None:
        self._projection_tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self.metrics.inc("projection_failed", error_type=type(task.exception()).__name__)

    async def drain_projections(self) -> None:
        if self._projection_tasks:
            await asyncio.gather(*tuple(self._projection_tasks), return_exceptions=True)

    @staticmethod
    def _audit(
        message: InboundMessage,
        decision: str,
        reason: str,
        start: float,
        event_count: int = 0,
        cost: float = 0.0,
    ) -> dict[str, object]:
        return redact(
            {
                "audit_id": hashlib.sha256(
                    f"{message.tenant_id}:{message.channel}:{message.account_id}:"
                    f"{message.external_message_id}:turn:{decision}".encode()
                ).hexdigest(),
                "tenant_id": message.tenant_id,
                "channel": message.channel,
                "user_id": message.external_user_id,
                "session_id": message.session_id,
                "agent_name": message.app_id,
                "tool_name": None,
                "decision": decision,
                "reason": reason,
                "event_count": event_count,
                "latency_ms": round((time.perf_counter() - start) * 1000, 2),
                "error_type": None,
                "cost": cost,
                "trace_id": message.trace_id,
            }
        )

    @staticmethod
    async def _remember(memories: object, message: InboundMessage, source_id: str) -> None:
        try:
            await memories.add(message.tenant_id, message.external_user_id, message.text, source_id=source_id)
        except TypeError:
            # Compatibility for a user-supplied MemoryStore implementing the original three-argument contract.
            await memories.add(message.tenant_id, message.external_user_id, message.text)

    async def worker_loop(self, stop: asyncio.Event) -> None:
        await self.queue.ensure()
        while not stop.is_set():
            delivery: QueueDelivery | None = await self.queue.get(self.consumer_id, 250)
            if delivery is None:
                continue
            heartbeat_stop = asyncio.Event()
            heartbeat = asyncio.create_task(
                self.queue.heartbeat(delivery, self.consumer_id, heartbeat_stop),
                name=f"queue-heartbeat:{delivery.delivery_id}",
            )
            try:
                with span(
                    "queue.consume", tenant_id=delivery.message.tenant_id,
                    attempts=delivery.attempts, trace_id=delivery.message.trace_id,
                ):
                    await self.process(delivery.message)
            except asyncio.CancelledError:
                raise
            except LeaseBusy:
                self.metrics.inc("session_busy", tenant=delivery.message.tenant_id)
                await self.queue.defer(delivery)
            except FencingConflict:
                self.metrics.inc("session_fencing_conflict", tenant=delivery.message.tenant_id)
                record_fencing_conflict(delivery.message.tenant_id)
                await self.queue.defer(delivery)
            except Exception as exc:  # noqa: BLE001 - isolate a failed provider turn from the worker loop
                # One provider failure must not terminate the whole worker.
                self.metrics.inc("agent_turn_failed", error_type=type(exc).__name__)
                await self.queue.fail(delivery, type(exc).__name__, self.max_delivery_attempts)
                if delivery.attempts + 1 >= self.max_delivery_attempts:
                    fail = getattr(self.idempotency, "fail", None)
                    if fail is not None:
                        await fail(self.idempotency_key(delivery.message), type(exc).__name__)
            else:
                await self.queue.ack(delivery)
            finally:
                heartbeat_stop.set()
                await heartbeat
                record_queue_depth(type(self.queue).__name__, self.queue.qsize())
