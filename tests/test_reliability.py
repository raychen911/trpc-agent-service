from __future__ import annotations

import asyncio
import hashlib
import unittest
from dataclasses import replace

from trpc_service.agent import AgentService, FailoverModel, TRPCAgentExecutor
from trpc_service.channels import ChannelDispatcher, TelegramAdapter, make_session_id
from trpc_service.queue import InMemoryMessageQueue
from trpc_service.storage import Artifact, StorageRouter
from trpc_service.tenant import (
    AgentApp,
    AuditStore,
    ChannelBinding,
    InboundMessage,
    MemoryStore,
    SessionStore,
    StorageProfile,
    TenantConfig,
    TenantRegistry,
)
from trpc_service.tool import HumanReviewRequired, ToolExecutor
from trpc_service.tool.policy import TenantPolicyFilter


def message(identifier: str = "m1", tenant_id: str = "acme") -> InboundMessage:
    return InboundMessage(
        tenant_id,
        "telegram",
        f"{tenant_id}-bot",
        identifier,
        "user",
        "chat",
        "direct",
        "hello",
        trace_id="trace",
        session_id=make_session_id(tenant_id, "telegram", "chat", "direct", "key"),
    )


class FlakyAdapter(TelegramAdapter):
    def __init__(self) -> None:
        super().__init__(dry_run=True)
        self.calls = 0

    async def send(self, outbound, binding=None):  # type: ignore[no-untyped-def]
        self.calls += 1
        if self.calls == 1:
            return {"ok": False, "code": "temporary", "retryable": True}
        return {"ok": True, "provider_message_id": "sent"}


class AlwaysFailAdapter(TelegramAdapter):
    def __init__(self) -> None:
        super().__init__(dry_run=True)

    async def send(self, outbound, binding=None):  # type: ignore[no-untyped-def]
        return {"ok": False, "code": "temporary", "retryable": True}


class CountingExecutor:
    def __init__(self) -> None:
        self.calls = 0

    async def reply(self, inbound, memory, instruction):  # type: ignore[no-untyped-def]
        self.calls += 1
        return "answer"


def service(adapter: TelegramAdapter | None = None) -> tuple[AgentService, CountingExecutor]:
    binding = ChannelBinding("telegram", "acme-bot", "verify")
    tenant = TenantConfig("acme", "Acme", {"default": AgentApp("default", "agent")}, {"telegram": binding})
    registry = TenantRegistry([tenant])
    dispatcher = ChannelDispatcher(
        {"telegram": adapter or TelegramAdapter(dry_run=True)},
        binding_resolver=lambda tenant_id, channel, account: registry.resolve_binding(tenant_id, channel, account)[1],
    )
    executor = CountingExecutor()
    return AgentService(registry, dispatcher, executor=executor), executor


class ReliabilityTest(unittest.IsolatedAsyncioTestCase):
    async def test_trpc_executor_uses_only_final_response(self) -> None:
        class Part:
            def __init__(self, text: str, *, thought: bool = False) -> None:
                self.text = text
                self.thought = thought

        class Content:
            def __init__(self, *parts: Part) -> None:
                self.parts = list(parts)

        class Event:
            def __init__(self, content: Content, final: bool) -> None:
                self.content = content
                self.final = final
                self.partial = False
                self.visible = True

            def is_final_response(self) -> bool:
                return self.final

        class Runner:
            async def run_async(self, **kwargs):  # type: ignore[no-untyped-def]
                yield Event(Content(Part("intermediate reasoning", thought=True)), False)
                yield Event(Content(Part("private reasoning", thought=True), Part("answer")), True)

        executor = TRPCAgentExecutor(Runner())
        self.assertEqual("answer", await executor.reply(message(), [], "instruction"))

    async def test_trpc_executor_ignores_tool_events_and_requires_final_response(self) -> None:
        class Part:
            thought = False
            function_response = None
            executable_code = None
            code_execution_result = None

            def __init__(self, text: str, *, function_call=None) -> None:  # type: ignore[no-untyped-def]
                self.text = text
                self.function_call = function_call

        class Event:
            partial = False
            visible = True

            def __init__(self, parts, final: bool) -> None:  # type: ignore[no-untyped-def]
                self.content = type("Content", (), {"parts": parts})()
                self.final = final

            def is_final_response(self) -> bool:
                return self.final

        class Runner:
            async def run_async(self, **kwargs):  # type: ignore[no-untyped-def]
                yield Event([Part("calling internal tool", function_call=object())], False)
                yield Event([Part("draft answer")], False)

        executor = TRPCAgentExecutor(Runner())
        self.assertEqual(
            "已完成处理，但没有可展示的最终回复。",
            await executor.reply(message(), [], "instruction"),
        )

    async def test_trpc_executor_removes_inline_reasoning_markup(self) -> None:
        class Part:
            thought = False

            def __init__(self, text: str) -> None:
                self.text = text

        class Event:
            partial = False
            visible = True

            def __init__(self) -> None:
                self.content = type(
                    "Content",
                    (),
                    {"parts": [Part("<think>private chain of thought</think>\nFinal answer")]},
                )()

            def is_final_response(self) -> bool:
                return True

        class Runner:
            async def run_async(self, **kwargs):  # type: ignore[no-untyped-def]
                yield Event()

        executor = TRPCAgentExecutor(Runner())
        self.assertEqual("Final answer", await executor.reply(message(), [], "instruction"))

    async def test_delivery_retry_does_not_repeat_model_or_session_commit(self) -> None:
        runtime, executor = service(FlakyAdapter())
        self.assertTrue(await runtime.enqueue(message()))

        with self.assertRaises(RuntimeError):
            await runtime.process_one()
        await runtime.process_one()

        self.assertEqual(1, executor.calls)
        self.assertEqual(2, len(await runtime.sessions.events("acme", message().session_id)))
        self.assertEqual("completed", (await runtime.idempotency.result(runtime.idempotency_key(message())))["status"])

    async def test_memory_retry_after_session_commit_is_idempotent(self) -> None:
        class FlakyMemory(MemoryStore):
            def __init__(self) -> None:
                super().__init__()
                self.calls = 0

            async def add(self, tenant_id, user_id, value, source_id=None):  # type: ignore[no-untyped-def]
                self.calls += 1
                if self.calls == 1:
                    raise ConnectionError("memory unavailable")
                await super().add(tenant_id, user_id, value, source_id=source_id)

        runtime, executor = service()
        runtime.memories = FlakyMemory()
        self.assertTrue(await runtime.enqueue(message()))

        with self.assertRaises(ConnectionError):
            await runtime.process_one()
        await runtime.process_one()

        self.assertEqual(1, executor.calls)
        self.assertEqual(["hello"], await runtime.memories.list("acme", "user"))
        self.assertEqual(2, len(await runtime.sessions.events("acme", message().session_id)))

    async def test_delivery_dlq_preserves_prepared_reply_for_manual_replay(self) -> None:
        runtime, executor = service(AlwaysFailAdapter())
        runtime.max_delivery_attempts = 3
        inbound = message()
        self.assertTrue(await runtime.enqueue(inbound))

        for _ in range(3):
            with self.assertRaises(RuntimeError):
                await runtime.process_one()

        key = runtime.idempotency_key(inbound)
        failed = await runtime.idempotency.result(key)
        self.assertEqual("delivery_failed", failed["status"])
        self.assertEqual("answer", failed["text"])
        self.assertEqual(1, executor.calls)
        self.assertEqual(1, len(runtime.queue.dead_letters))

        runtime.dispatcher.adapters["telegram"] = TelegramAdapter(dry_run=True)
        await runtime.idempotency.complete(key, {**failed, "status": "prepared"})
        await runtime.queue.put(inbound)
        await runtime.process_one()

        self.assertEqual(1, executor.calls)
        self.assertEqual("completed", (await runtime.idempotency.result(key))["status"])

    async def test_queue_moves_poison_message_to_dlq(self) -> None:
        queue = InMemoryMessageQueue()
        await queue.put(message())
        for _ in range(3):
            delivery = await queue.get()
            self.assertIsNotNone(delivery)
            await queue.fail(delivery, "failure", max_attempts=3)  # type: ignore[arg-type]

        self.assertEqual(1, len(queue.dead_letters))
        self.assertEqual(3, queue.dead_letters[0].attempts)

    async def test_concurrent_turns_keep_contiguous_sequences(self) -> None:
        runtime, _ = service()
        first = message("m1")
        second = message("m2")

        await asyncio.gather(runtime.process(first), runtime.process(second))

        events = await runtime.sessions.events("acme", first.session_id)
        self.assertEqual([1, 2, 3, 4], [event.sequence for event in events])

    async def test_concurrent_duplicate_is_rechecked_after_session_lock(self) -> None:
        runtime, executor = service()
        inbound = message("same")

        await asyncio.gather(runtime.process(inbound), runtime.process(inbound))

        events = await runtime.sessions.events("acme", inbound.session_id)
        self.assertEqual(1, executor.calls)
        self.assertEqual([1, 2], [event.sequence for event in events])

    async def test_inmemory_processing_uses_pinned_revision(self) -> None:
        runtime, _ = service()
        original = runtime.registry.get("acme")
        disabled = replace(original.channels["telegram"], enabled=False)
        runtime.registry.publish(
            replace(original, name="new", channels={"telegram": disabled}),
            expected_version=1,
        )
        inbound = replace(message(), config_version=1)

        await runtime.process(inbound)

        self.assertEqual("completed", (await runtime.idempotency.result(runtime.idempotency_key(inbound)))["status"])

    async def test_tenant_keys_and_storage_routes_are_isolated(self) -> None:
        self.assertNotEqual(message(tenant_id="acme").session_id, message(tenant_id="globex").session_id)
        router = StorageRouter()
        acme_store, globex_store = object(), object()
        router.register("acme-sql", acme_store)
        router.register("globex-sql", globex_store)
        acme = TenantConfig(
            "acme",
            "Acme",
            {"default": AgentApp("default", "a")},
            {},
            storage=StorageProfile(session="acme-sql", memory="acme-sql", audit="acme-sql"),
        )
        self.assertIs(acme_store, router.route(acme).session)

    async def test_revision_numbers_remain_monotonic_after_rollback(self) -> None:
        original = TenantConfig("acme", "A", {"default": AgentApp("default", "a")}, {})
        registry = TenantRegistry([original])
        second = registry.publish(replace(original, name="B"), expected_version=1)
        self.assertEqual(2, second.version)
        registry.rollback("acme", 1)
        third = registry.publish(replace(original, name="C"), expected_version=1)
        self.assertEqual(3, third.version)
        self.assertEqual("B", registry.get("acme", 2).name)

    async def test_user_allowlist_is_scoped_to_current_channel(self) -> None:
        config = TenantConfig(
            "acme",
            "A",
            {"default": AgentApp("default", "a")},
            {
                "telegram": ChannelBinding("telegram", "tg", allowed_users=frozenset({"telegram-user"})),
                "wecom": ChannelBinding("wecom", "wx", allowed_users=frozenset({"shared-user"})),
            },
        )
        policy = TenantPolicyFilter(config)
        self.assertFalse(policy.check_user("shared-user", "telegram", "tg").allowed)
        self.assertTrue(policy.check_user("telegram-user", "telegram", "tg").allowed)

    async def test_non_idempotent_unknown_tool_requires_review(self) -> None:
        executor = ToolExecutor(b"x" * 32)

        async def fail() -> None:
            raise TimeoutError

        with self.assertRaises(HumanReviewRequired):
            await executor.execute("acme", "session", "turn", "charge", {"amount": 1}, fail, idempotent=False)
        with self.assertRaises(HumanReviewRequired):
            await executor.execute("acme", "session", "turn", "charge", {"amount": 1}, fail, idempotent=False)

    async def test_agent_service_uses_tenant_storage_profile(self) -> None:
        runtime, executor = service()
        selected_session = SessionStore()
        selected_memory = MemoryStore()
        selected_audit = AuditStore()
        router = StorageRouter()
        router.register("selected", selected_session, kind="session")
        router.register("selected", selected_memory, kind="memory")
        router.register("selected", selected_audit, kind="audit")
        original = runtime.registry.get("acme")
        runtime.registry = TenantRegistry(
            [replace(original, storage=StorageProfile(session="selected", memory="selected", audit="selected"))]
        )
        runtime.storage_router = router
        await runtime.process(message())
        self.assertEqual(2, len(await selected_session.events("acme", message().session_id)))
        self.assertEqual([], await runtime.sessions.events("acme", message().session_id))
        self.assertEqual(1, executor.calls)

    async def test_agent_service_injects_only_configured_knowledge(self) -> None:
        class CaptureExecutor:
            async def reply(self, inbound, memory, instruction):  # type: ignore[no-untyped-def]
                self.memory = memory
                return "answer"

        class Knowledge:
            async def recall(self, config, tenant_id, query, collections):  # type: ignore[no-untyped-def]
                self.scope = (config.tenant_id, tenant_id, query, collections)
                return ["faq answer"]

        runtime, _ = service()
        original = runtime.registry.get("acme")
        app = replace(original.apps["default"], knowledge_collections=frozenset({"faq"}))
        runtime.registry = TenantRegistry([replace(original, apps={"default": app})])
        executor = CaptureExecutor()
        knowledge = Knowledge()
        runtime.executor = executor
        runtime.knowledge_retriever = knowledge

        await runtime.process(message())

        self.assertIn("[knowledge] faq answer", executor.memory)
        self.assertEqual(("acme", "acme", "hello", frozenset({"faq"})), knowledge.scope)

    async def test_delivery_uses_binding_from_pinned_revision(self) -> None:
        class BindingAdapter(TelegramAdapter):
            def __init__(self) -> None:
                super().__init__(dry_run=True)
                self.secret = None

            async def send(self, outbound, binding=None):  # type: ignore[no-untyped-def]
                self.secret = binding.secret_ref
                return {"ok": True}

        adapter = BindingAdapter()
        binding_v1 = ChannelBinding("telegram", "acme-bot", "verify", secret_ref="literal://old")
        config_v1 = TenantConfig("acme", "Acme", {"default": AgentApp("default", "agent")}, {"telegram": binding_v1})
        binding_v2 = replace(binding_v1, secret_ref="literal://new")
        registry = TenantRegistry([config_v1])
        registry.publish(replace(config_v1, channels={"telegram": binding_v2}), expected_version=1)
        dispatcher = ChannelDispatcher({"telegram": adapter})

        async def load(tenant_id, version):
            return registry.get(tenant_id, version)

        runtime = AgentService(registry, dispatcher, executor=CountingExecutor(), config_loader=load)
        await runtime.process(replace(message(), config_version=1))
        self.assertEqual("literal://old", adapter.secret)

    async def test_trpc_executor_enforces_turn_timeout(self) -> None:
        class Runner:
            async def run_async(self, **kwargs):  # type: ignore[no-untyped-def]
                await asyncio.sleep(0.05)
                if False:
                    yield None

        executor = TRPCAgentExecutor(Runner(), timeout_seconds=0.01)
        with self.assertRaises(TimeoutError):
            await executor.reply(message(), [], "instruction")

    async def test_trpc_executor_adds_verified_artifact_part(self) -> None:
        data = b"image"
        artifact = Artifact(
            "acme", "a1", "tenants/acme/artifacts/a1", hashlib.sha256(data).hexdigest(), len(data), "image/png"
        )

        class Artifacts:
            async def get(self, selected):
                self.selected = selected
                return data

        class Runner:
            async def run_async(self, **kwargs):  # type: ignore[no-untyped-def]
                self.content = kwargs["new_message"]
                if False:
                    yield None

        runner = Runner()
        executor = TRPCAgentExecutor(runner, artifact_store=Artifacts())
        enriched = replace(
            message(),
            raw={
                "_artifact_materialized": True,
                "normalized_media": {
                    "type": "image",
                    "artifact": {
                        "tenant_id": artifact.tenant_id,
                        "artifact_id": artifact.artifact_id,
                        "key": artifact.key,
                        "checksum": artifact.checksum,
                        "size": artifact.size,
                        "content_type": artifact.content_type,
                    },
                },
            },
        )
        await executor.reply(enriched, ["remembered"], "instruction")
        self.assertEqual(2, len(runner.content.parts))
        self.assertEqual(data, runner.content.parts[1].inline_data.data)

    async def test_model_fallback_only_runs_before_visible_content(self) -> None:
        class Response:
            def __init__(self, value, *, content=False, error=False):
                self.value = value
                self.error_code = "error" if error else None
                self._content = content

            def has_content(self):
                return self._content

        class Model:
            def __init__(self, name, responses):
                self.name = name
                self.responses = responses
                self.calls = 0

            async def generate_async(self, request, stream=False, ctx=None):
                self.calls += 1
                for response in self.responses:
                    yield response

        primary = Model("primary", [Response("failed", error=True)])
        fallback = Model("fallback", [Response("answer", content=True)])
        model = FailoverModel(primary, fallback)
        values = [value.value async for value in model._generate_async_impl(object())]
        self.assertEqual(["answer"], values)
        self.assertEqual(1, fallback.calls)

        primary = Model("primary", [Response("partial", content=True), Response("failed", error=True)])
        fallback = Model("fallback", [Response("duplicate", content=True)])
        model = FailoverModel(primary, fallback)
        values = [value.value async for value in model._generate_async_impl(object(), stream=True)]
        self.assertEqual(["partial", "failed"], values)
        self.assertEqual(0, fallback.calls)

    async def test_model_fallback_handles_transport_exception_before_content(self) -> None:
        class Response:
            error_code = None
            value = "fallback"

            def has_content(self):
                return True

        class Primary:
            name = "primary"

            async def generate_async(self, request, stream=False, ctx=None):
                _ = request, stream, ctx
                raise ConnectionError("provider unavailable")
                yield  # pragma: no cover

        class Fallback:
            name = "fallback"

            async def generate_async(self, request, stream=False, ctx=None):
                _ = request, stream, ctx
                yield Response()

        values = [value.value async for value in FailoverModel(Primary(), Fallback())._generate_async_impl(object())]
        self.assertEqual(["fallback"], values)


if __name__ == "__main__":
    unittest.main()
