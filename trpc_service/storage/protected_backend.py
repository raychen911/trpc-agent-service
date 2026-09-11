"""Atomic storage primitives for the service-owned Session format (version 1)."""

import asyncio
import json
from datetime import timezone

from sqlalchemy import MetaData, Table, Column, String, Text, select, func, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert


class WriteRejected(RuntimeError):
    """Expired owner, conflicting snapshot or conflicting idempotency key."""


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def timestamp(value):
    return (value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)).timestamp()


metadata = MetaData()
records = Table("trpc_protected_session_v1", metadata, Column("key", String(255), primary_key=True),
                Column("payload", Text, nullable=False))


class SqlAtomicBackend:

    def __init__(self, url):
        self.engine = create_engine(url, pool_pre_ping=True, hide_parameters=True)
        if self.engine.dialect.name not in {"postgresql", "sqlite"}:
            raise ValueError("protected SQL Session supports PostgreSQL and development SQLite")

    async def initialize(self):

        def create():
            with self.engine.begin() as connection:
                if self.engine.dialect.name == "postgresql":
                    connection.execute(text("SELECT pg_advisory_xact_lock(8473229185401)"))
                metadata.create_all(connection)

        await asyncio.to_thread(create)

    def _transact(self, keys, transform):
        insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
        with self.engine.begin() as connection:
            # One short application lock serializes shared app/user state as well
            # as session leases. No model/tool/network call runs inside this lock.
            connection.execute(insert(records).values(key=keys[0], payload="{}").on_conflict_do_nothing())
            connection.execute(select(records.c.key).where(records.c.key == keys[0]).with_for_update()).scalar_one()
            values = dict(connection.execute(select(records).where(records.c.key.in_(keys))).all())
            docs = [json.loads(values.get(key, "{}")) for key in keys]
            clock = func.clock_timestamp() if self.engine.dialect.name == "postgresql" else func.current_timestamp()
            now = timestamp(connection.scalar(select(clock)))
            result, deadline = transform(docs, now)
            if deadline and timestamp(connection.scalar(select(clock))) >= deadline:
                raise WriteRejected("native lease expired before commit")
            for key, doc in zip(keys, docs):
                statement = insert(records).values(key=key, payload=encode(doc))
                connection.execute(
                    statement.on_conflict_do_update(index_elements=[records.c.key],
                                                    set_={"payload": statement.excluded.payload}))
            return result

    async def transact(self, keys, transform):
        # Cancellation cannot release the enclosing execution before a database
        # transaction has completed or rolled back.
        task = asyncio.create_task(asyncio.to_thread(self._transact, keys, transform))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def list_records(self, prefix):

        def read():
            with self.engine.connect() as connection:
                return [
                    json.loads(value)
                    for value in connection.scalars(select(records.c.payload).where(records.c.key.startswith(prefix)))
                ]

        return await asyncio.to_thread(read)

    async def close(self):
        await asyncio.to_thread(self.engine.dispose)


COMMIT = """
for i=1,3 do
  if (redis.call('GET', KEYS[i]) or '') ~= ARGV[i] then return 0 end
end
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local deadline = tonumber(ARGV[7])
if deadline > 0 and now >= deadline then return -1 end
for i=1,3 do redis.call('SET', KEYS[i], ARGV[i+3]) end
return 1
"""


class RedisAtomicBackend:

    def __init__(self, url):
        from redis.asyncio import Redis
        self.client = Redis.from_url(url, decode_responses=True, socket_timeout=10, socket_connect_timeout=10)

    async def initialize(self):
        await self.client.ping()

    async def transact(self, keys, transform):
        # All keys share an application hash tag. Compare all three snapshots
        # and check server-time expiry inside the same Lua commit.
        for _ in range(12):
            old = await self.client.mget(keys)
            seconds, micros = await self.client.time()
            docs = [json.loads(value) if value else {} for value in old]
            result, deadline = transform(docs, seconds + micros / 1_000_000)
            committed = await self.client.eval(COMMIT, 3, *keys, *(value or "" for value in old),
                                               *(encode(doc) for doc in docs), str(deadline or 0))
            if committed == 1:
                return result
            if committed == -1:
                raise WriteRejected("native lease expired before commit")
        raise WriteRejected("native state changed repeatedly")

    async def list_records(self, prefix):
        result = []
        async for key in self.client.scan_iter(match=prefix + "*"):
            value = await self.client.get(key)
            if value:
                result.append(json.loads(value))
        return result

    async def close(self):
        await self.client.aclose()
