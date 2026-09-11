from __future__ import annotations

import unittest
from collections import defaultdict

from trpc_service.agent import AgentService
from trpc_service.channels import ChannelDispatcher, TelegramAdapter, make_session_id
from trpc_service.storage import FencingConflict, LeaseBusy, RedisStateStore
from trpc_service.storage.projection import Summary
from trpc_service.tenant import (
    AgentApp,
    AuditStore,
    ChannelBinding,
    IdempotencyStore,
    InboundMessage,
    TenantConfig,
    TenantRegistry,
)


class FakeRedisState:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = defaultdict(dict)
        self.lists: dict[str, list[str]] = defaultdict(list)
        self.sets: dict[str, set[str]] = defaultdict(set)
        self.strings: dict[str, str] = {}
        self.now_ms = 1_000_000

    async def eval(self, script, numkeys, *values):  # type: ignore[no-untyped-def]
        keys = values[:numkeys]
        args = tuple(str(value) for value in values[numkeys:])
        if "HSETNX" in script:
            value = self.hashes[keys[0]]
            for field, item in zip(
                ("app_id", "user_id", "updated_at"),
                args,
                strict=True,
            ):
                value.setdefault(field, item)
            value.setdefault("state", "{}")
            value.setdefault("version", "0")
            value.setdefault("fencing_epoch", "0")
            return 1
        if "HINCRBY" in script:
            value = self.hashes[keys[0]]
            if value.get("lease_owner") and int(value.get("lease_expires_ms", "0")) > self.now_ms:
                return []
            epoch = int(value.get("fencing_epoch", "0")) + 1
            expiry = self.now_ms + int(args[1])
            value.update(lease_owner=args[0], lease_expires_ms=str(expiry), fencing_epoch=str(epoch))
            return [int(value.get("version", "0")), epoch, expiry]
        if "RPUSH', KEYS[2]" in script:
            value = self.hashes[keys[0]]
            if (
                value.get("lease_owner") != args[1]
                or int(value.get("fencing_epoch", "-1")) != int(args[2])
                or int(value.get("version", "-1")) != int(args[0])
                or int(value.get("lease_expires_ms", "0")) <= self.now_ms
            ):
                return 0
            self.lists[keys[1]].extend((args[3], args[4]))
            value.update(state=args[5], version=args[6], updated_at=args[7])
            self.strings[keys[2]] = args[8]
            value.pop("lease_owner", None)
            value.pop("lease_expires_ms", None)
            return 1
        if "SADD" in script:
            source = args[0]
            if source and source in self.sets[keys[1]]:
                return 0
            if source:
                self.sets[keys[1]].add(source)
            self.lists[keys[0]].append(args[1])
            self.lists[keys[0]] = self.lists[keys[0]][-int(args[2]) :]
            return 1
        if "source_version" in script:
            value = self.hashes[keys[0]]
            if int(value.get("source_version", "-1")) > int(args[0]):
                return 0
            value.update(source_version=args[0], content=args[1])
            return 1
        value = self.hashes[keys[0]]
        if "lease_expires_ms', now_ms" in script:
            if value.get("lease_owner") != args[0] or value.get("fencing_epoch") != args[1]:
                return 0
            value["lease_expires_ms"] = str(self.now_ms + int(args[2]))
            return 1
        if value.get("lease_owner") != args[0] or value.get("fencing_epoch") != args[1]:
            return 0
        value.pop("lease_owner", None)
        value.pop("lease_expires_ms", None)
        return 1

    async def hgetall(self, key):  # type: ignore[no-untyped-def]
        return dict(self.hashes[key])

    async def get(self, key):  # type: ignore[no-untyped-def]
        return self.strings.get(key)

    async def lrange(self, key, start, end):  # type: ignore[no-untyped-def]
        values = self.lists[key]
        if start < 0:
            start = max(0, len(values) + start)
        if end == -1:
            return values[start:]
        return values[start : end + 1]


class RedisStateStoreTest(unittest.IsolatedAsyncioTestCase):
    async def test_session_events_memory_and_summary_are_tenant_scoped(self) -> None:
        client = FakeRedisState()
        store = RedisStateStore("redis://unused", client=client)
        snapshot = await store.get_or_create("acme", "session", "app", "user")

        async with store.lock("acme", "session") as lease:
            events = await store.append_turn(snapshot, "hello", "answer", "trace", lease)

        self.assertEqual([1, 2], [event.sequence for event in events])
        self.assertEqual([1, 2], [event.sequence for event in await store.events("acme", "session")])
        self.assertEqual([], await store.events("globex", "session"))

        await store.add("acme", "user", "memory", source_id="message-1")
        await store.add("acme", "user", "memory", source_id="message-1")
        self.assertEqual(["memory"], await store.list("acme", "user"))
        self.assertEqual([], await store.list("globex", "user"))

        self.assertTrue(await store.put_summary(Summary("acme", "session", 2, "new")))
        self.assertFalse(await store.put_summary(Summary("acme", "session", 1, "old")))
        self.assertEqual("new", (await store.get_summary("acme", "session")).content)  # type: ignore[union-attr]

    async def test_lease_is_exclusive_and_stale_commit_is_rejected(self) -> None:
        client = FakeRedisState()
        store = RedisStateStore("redis://unused", client=client)
        snapshot = await store.get_or_create("acme", "session", "app", "user")

        async with store.lock("acme", "session") as lease:
            with self.assertRaises(LeaseBusy):
                async with store.lock("acme", "session"):
                    pass
        with self.assertRaises(FencingConflict):
            await store.append_turn(snapshot, "late", "late", "trace", lease)

    async def test_prepared_reply_recovers_and_projection_is_idempotently_requested(self) -> None:
        class ProjectionSink:
            def __init__(self) -> None:
                self.calls = []

            async def enqueue_session_projection(self, *args):  # type: ignore[no-untyped-def]
                self.calls.append(args)

        client = FakeRedisState()
        sink = ProjectionSink()
        store = RedisStateStore("redis://unused", client=client, projection_sink=sink)
        snapshot = await store.get_or_create("acme", "session", "app", "user")
        async with store.lock("acme", "session") as lease:
            await store.append_turn(
                snapshot,
                "hello",
                "answer",
                "trace",
                lease,
                inbox_key="inbox",
                config_version=3,
            )

        recovered = await store.prepared_result("acme", "inbox")
        self.assertEqual("answer", recovered["text"])  # type: ignore[index]
        self.assertEqual(
            ("acme", "session", 2, 3, "trace"),
            sink.calls[-1],
        )

    async def test_agent_recovers_redis_commit_without_calling_model_again(self) -> None:
        class NeverExecutor:
            def __init__(self) -> None:
                self.calls = 0

            async def reply(self, *args):  # type: ignore[no-untyped-def]
                self.calls += 1
                raise AssertionError("model must not run for a recovered reply")

        binding = ChannelBinding("telegram", "bot", "verify")
        config = TenantConfig("acme", "Acme", {"default": AgentApp("default", "agent")}, {"telegram": binding})
        registry = TenantRegistry([config])
        dispatcher = ChannelDispatcher({"telegram": TelegramAdapter(dry_run=True)})
        client = FakeRedisState()
        store = RedisStateStore("redis://unused", client=client)
        idempotency = IdempotencyStore()
        executor = NeverExecutor()
        runtime = AgentService(
            registry,
            dispatcher,
            executor=executor,
            sessions=store,
            memories=store,
            audits=AuditStore(),
            idempotency=idempotency,
        )
        inbound = InboundMessage(
            "acme",
            "telegram",
            "bot",
            "message",
            "user",
            "chat",
            "direct",
            "hello",
            trace_id="trace",
            session_id=make_session_id("acme", "telegram", "chat", "direct", "key"),
        )
        key = runtime.idempotency_key(inbound)
        await idempotency.claim(key)
        snapshot = await store.get_or_create("acme", inbound.session_id, "default", "user")
        async with store.lock("acme", inbound.session_id) as lease:
            await store.append_turn(snapshot, "hello", "answer", "trace", lease, inbox_key=key, config_version=1)

        deliveries = await runtime.process(inbound)

        self.assertEqual(0, executor.calls)
        self.assertEqual("answer", deliveries[0].text)
        self.assertEqual("completed", (await idempotency.result(key))["status"])
