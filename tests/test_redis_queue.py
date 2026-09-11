from __future__ import annotations

import asyncio
import unittest

from trpc_service.queue import RedisStreamQueue
from trpc_service.tenant import InboundMessage, inbound_to_dict


def inbound(identifier: str = "m1") -> InboundMessage:
    return InboundMessage("acme", "telegram", "bot", identifier, "user", "chat", "direct", "hello")


class FakeRedis:
    def __init__(self) -> None:
        self.group_calls = []
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.new_entries: list[tuple[str, dict[str, str]]] = []
        self.reclaimed: list[tuple[str, dict[str, str]]] = []
        self.acked: list[str] = []
        self.claimed: list[str] = []
        self.deleted: list[str] = []
        self.xadd_options: list[tuple[str, int | None, bool | None]] = []
        self.closed = False
        self.fail_delete = False
        self._next = 1

    async def xgroup_create(self, stream, group, id, mkstream):
        self.group_calls.append((stream, group, id, mkstream))

    async def xadd(self, stream, fields, maxlen=None, approximate=None):
        self.xadd_options.append((stream, maxlen, approximate))
        identifier = f"{self._next}-0"
        self._next += 1
        entry = (identifier, dict(fields))
        self.streams.setdefault(stream, []).append(entry)
        if stream == "messages":
            self.new_entries.append(entry)
        return identifier

    async def xautoclaim(self, stream, group, consumer, min_idle_time, start_id, count):
        values = self.reclaimed[:count]
        self.reclaimed = self.reclaimed[count:]
        return ("0-0", values, [])

    async def xreadgroup(self, group, consumer, streams, count, block):
        if not self.new_entries:
            return []
        return [("messages", [self.new_entries.pop(0)])]

    async def xack(self, stream, group, identifier):
        self.acked.append(identifier)
        return 1

    async def xdel(self, stream, identifier):
        if self.fail_delete:
            raise ConnectionError("cleanup unavailable")
        self.deleted.append(identifier)
        self.streams[stream] = [entry for entry in self.streams.get(stream, []) if entry[0] != identifier]
        return 1

    async def xclaim(self, stream, group, consumer, min_idle_time, message_ids, justid):
        self.claimed.extend(message_ids)
        return message_ids

    async def xpending(self, stream, group):
        return {"pending": 0}

    async def aclose(self):
        self.closed = True


class RedisQueueProtocolTest(unittest.IsolatedAsyncioTestCase):
    async def test_group_delivery_heartbeat_and_dlq(self) -> None:
        client = FakeRedis()
        queue = RedisStreamQueue(
            "redis://unused",
            stream="messages",
            group="workers",
            dlq_stream="dead",
            reclaim_after_ms=1,
            client=client,
        )
        await queue.put(inbound())
        delivery = await queue.get("worker-1")
        self.assertIsNotNone(delivery)
        self.assertEqual([("messages", "workers", "0-0", True)], client.group_calls)

        stop = asyncio.Event()
        heartbeat = asyncio.create_task(queue.heartbeat(delivery, "worker-1", stop))
        await asyncio.sleep(0.12)
        stop.set()
        await heartbeat
        self.assertEqual([delivery.delivery_id], client.claimed)

        await queue.fail(delivery, "Poison", max_attempts=1)
        self.assertEqual(delivery.delivery_id, client.acked[-1])
        self.assertEqual(delivery.delivery_id, client.deleted[-1])
        self.assertEqual("1", client.streams["dead"][0][1]["attempts"])
        self.assertEqual(("dead", 10_000, True), client.xadd_options[-1])
        self.assertEqual(0, queue.qsize())

    async def test_ack_succeeds_when_best_effort_delete_fails(self) -> None:
        client = FakeRedis()
        queue = RedisStreamQueue("redis://unused", stream="messages", group="workers", client=client)
        await queue.put(inbound())
        delivery = await queue.get("worker-1")
        self.assertIsNotNone(delivery)
        client.fail_delete = True

        await queue.ack(delivery)

        self.assertEqual([delivery.delivery_id], client.acked)
        self.assertEqual(0, queue.qsize())
        await queue.close()
        self.assertTrue(client.closed)

    async def test_xautoclaim_decodes_pending_message(self) -> None:
        import json

        client = FakeRedis()
        client.reclaimed.append(
            ("9-0", {"payload": json.dumps(inbound_to_dict(inbound("old"))), "attempts": "2"})
        )
        queue = RedisStreamQueue("redis://unused", stream="messages", group="workers", client=client)
        values = await queue.reclaim("worker-2")
        self.assertEqual("9-0", values[0].delivery_id)
        self.assertEqual("old", values[0].message.external_message_id)
        self.assertEqual(2, values[0].attempts)

    async def test_ack_deletes_entry_and_stream_writes_are_bounded(self) -> None:
        client = FakeRedis()
        queue = RedisStreamQueue(
            "redis://unused",
            stream="messages",
            group="workers",
            stream_maxlen=50,
            dlq_maxlen=5,
            client=client,
        )
        await queue.put(inbound())
        delivery = await queue.get("worker-1")
        self.assertIsNotNone(delivery)
        self.assertEqual(("messages", 50, True), client.xadd_options[0])

        await queue.ack(delivery)
        self.assertEqual([delivery.delivery_id], client.deleted)
        self.assertEqual([], client.streams["messages"])
        self.assertEqual(0, queue.qsize())


if __name__ == "__main__":
    unittest.main()
