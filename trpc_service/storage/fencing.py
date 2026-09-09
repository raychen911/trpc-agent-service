"""Storage-native fences. Redis commands and SQL commits reject stale owners.

Adapters retain SDK data formats. Redis coordination lives in the *data* Redis,
not necessarily the queue Redis. SQL lease rows live in the data database.
"""
import asyncio
import contextlib
from contextvars import ContextVar
import hashlib
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .guard import SessionLease, SessionLockLostError, SessionLockTimeoutError, current_lease

_leases = ContextVar("storage_fences", default={})
_control_lease = ContextVar("control_fence", default=None)

LEASE_DDL = """CREATE TABLE IF NOT EXISTS platform_execution_lease (
    lease_key TEXT PRIMARY KEY, token TEXT NOT NULL, epoch BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL)"""


def database_identity(url):
    # SQLAlchemy driver names must not split leases for the same PostgreSQL DB.
    canonical = url.replace("postgresql+psycopg2://", "postgresql://").replace("postgresql+asyncpg://", "postgresql://")
    return hashlib.sha256(canonical.encode()).hexdigest()


@contextlib.contextmanager
def storage_lease_scope(identity, lease):
    token = _leases.set({**_leases.get(), identity: lease})
    try:
        yield
    finally:
        _leases.reset(token)


def require_lease(identity):
    lease = _leases.get().get(identity)
    if lease is None:
        raise SessionLockLostError("storage write requires a native execution lease")
    lease.assert_owned()
    return lease


class PostgresExecutionGuard:

    def __init__(self, url="", pool=None, control=False):
        self.url = url
        self.pool = pool
        self.control = control
        self._owns_pool = pool is None
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def initialize(self):
        async with self._init_lock:
            if self._initialized:
                return
            if self.pool is None:
                import asyncpg
                url = self.url.replace("postgresql+psycopg2://",
                                       "postgresql://").replace("postgresql+asyncpg://", "postgresql://")
                self.pool = await asyncpg.create_pool(url, min_size=1, max_size=4, command_timeout=5)
            await self.pool.execute(LEASE_DDL)
            self._initialized = True

    @contextlib.asynccontextmanager
    async def hold(self, key, *, wait_timeout=10, lease_seconds=30):
        await self.initialize()
        key = ("control:" if self.control else "data:") + key
        token = uuid.uuid4().hex
        deadline = asyncio.get_running_loop().time() + wait_timeout
        while True:
            row = await self.pool.fetchrow(
                "INSERT INTO platform_execution_lease VALUES ($1,$2,1,clock_timestamp()+$3*interval '1 second') "
                "ON CONFLICT (lease_key) DO UPDATE SET token=$2,epoch=platform_execution_lease.epoch+1,"
                "expires_at=clock_timestamp()+$3*interval '1 second' "
                "WHERE platform_execution_lease.expires_at <= clock_timestamp() RETURNING epoch", key, token,
                float(lease_seconds))
            if row:
                break
            if asyncio.get_running_loop().time() >= deadline:
                raise SessionLockTimeoutError("postgres execution lease wait timed out")
            await asyncio.sleep(.05)

        async def verify():
            return bool(await self.pool.fetchval(
                "SELECT 1 FROM platform_execution_lease WHERE lease_key=$1 AND token=$2 AND epoch=$3 "
                "AND expires_at>clock_timestamp()", key, token, row["epoch"]))

        lease = SessionLease(token, asyncio.Event(), verify, row["epoch"], key)

        async def renew():
            while True:
                await asyncio.sleep(max(.05, lease_seconds / 3))
                try:
                    status = await asyncio.wait_for(self.pool.execute(
                        "UPDATE platform_execution_lease SET expires_at=clock_timestamp()+$4*interval '1 second' "
                        "WHERE lease_key=$1 AND token=$2 AND epoch=$3 AND expires_at>clock_timestamp()", key, token,
                        lease.epoch, float(lease_seconds)),
                                                    timeout=2)
                    if status != "UPDATE 1":
                        lease.lost.set()
                        return
                except Exception:
                    lease.lost.set()
                    return

        renewal = asyncio.create_task(renew())
        marker = _control_lease.set(lease) if self.control else None
        try:
            yield lease
            lease.assert_owned()
        finally:
            if marker is not None:
                _control_lease.reset(marker)
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
            with contextlib.suppress(Exception):
                await self.pool.execute(
                    "UPDATE platform_execution_lease SET expires_at=clock_timestamp() "
                    "WHERE lease_key=$1 AND token=$2 AND epoch=$3", key, token, lease.epoch)

    async def close(self):
        if self._owns_pool and self.pool:
            await self.pool.close()

    async def ping(self):
        await self.initialize()
        return bool(await self.pool.fetchval("SELECT 1"))


async def verify_postgres_commit(conn):
    lease = _control_lease.get()
    if lease is None:
        raise SessionLockLostError("control-plane success requires an execution lease")
    lease.assert_owned()
    row = await conn.fetchrow(
        "SELECT token,epoch,expires_at>clock_timestamp() AS valid FROM platform_execution_lease "
        "WHERE lease_key=$1 FOR UPDATE", lease.key)
    if not row or row["token"] != lease.token or row["epoch"] != lease.epoch or not row["valid"]:
        lease.lost.set()
        raise SessionLockLostError("control-plane execution fence rejected commit")


_REDIS_WRITE = """
if redis.call('get', KEYS[1]) ~= ARGV[1] or
   redis.call('get', KEYS[2]) ~= ARGV[2] then return redis.error_reply('STALE_FENCE') end
local result = redis.call(ARGV[3], unpack(ARGV, 5))
if tonumber(ARGV[4]) > 0 then redis.call('expire', KEYS[3], ARGV[4]) end
return result
"""
_REDIS_REPLACE = """
if redis.call('get', KEYS[1]) ~= ARGV[1] or
   redis.call('get', KEYS[2]) ~= ARGV[2] then return redis.error_reply('STALE_FENCE') end
redis.call('del', KEYS[3])
if #ARGV > 3 then redis.call('rpush', KEYS[3], unpack(ARGV, 4)) end
if tonumber(ARGV[3]) > 0 then redis.call('expire', KEYS[3], ARGV[3]) end
return 1
"""


class FencedRedisStorage:

    def __init__(self, delegate, identity):
        self.delegate, self.identity = delegate, identity

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    async def _write(self, conn, command, args, ttl=0):
        lease = require_lease(self.identity)
        try:
            return await conn.eval(_REDIS_WRITE, 3, lease.key, f"{lease.key}:epoch", args[0], lease.token,
                                   str(lease.epoch), command, ttl, *args)
        except Exception as error:
            if "STALE_FENCE" in str(error):
                lease.lost.set()
                raise SessionLockLostError("redis storage fence rejected write") from None
            raise

    async def execute_command(self, conn, command):
        method = command.method.lower()
        if method in {"set", "hset", "rpush", "lpush", "del", "sadd", "zadd"}:
            if command.kwargs:
                raise ValueError("unsupported fenced Redis command options")
            ttl = command.expire.ttl
            seconds = int(ttl.ttl_seconds) if ttl.need_ttl_expire() else 0
            return await self._write(conn, method, command.args, seconds)
        if method in {"get", "hget", "hgetall", "mget", "keys", "scan", "lrange", "exists", "ttl", "pttl"}:
            return await self.delegate.execute_command(conn, command)
        raise ValueError("unsupported Redis operation must declare its fencing behavior")

    async def delete(self, conn, key, conditions=None):
        return await self._write(conn, "del", (key, ))

    async def expire(self, conn, command):
        # Read-side TTL refresh is not allowed to make an expired lease a writer.
        if self.identity not in _leases.get():
            return
        if command.ttl.need_ttl_expire():
            await self._write(conn, "expire", (command.key, int(command.ttl.ttl_seconds)))

    async def replace_memory(self, conn, key, events, ttl):
        lease = require_lease(self.identity)
        try:
            await conn.eval(_REDIS_REPLACE, 3, lease.key, f"{lease.key}:epoch", key, lease.token, str(lease.epoch), ttl,
                            *events)
        except Exception as error:
            if "STALE_FENCE" in str(error):
                lease.lost.set()
                raise SessionLockLostError("redis memory fence rejected replacement") from None
            raise


class FencedSqlStorage:

    def __init__(self, delegate, identity):
        self.delegate, self.identity = delegate, identity

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    async def commit(self, conn):
        lease = require_lease(self.identity)
        statement = text("SELECT token,epoch,expires_at>clock_timestamp() AS valid "
                         "FROM platform_execution_lease WHERE lease_key=:key FOR UPDATE")
        # Avoid ORM autoflush before the fence; the lock remains held through
        # the SDK commit, serializing this write against lease takeover.
        with conn.no_autoflush:
            result = (await conn.execute(statement, {"key": lease.key})
                      if isinstance(conn, AsyncSession) else conn.execute(statement, {"key": lease.key}))
        row = result.mappings().first()
        if not row or row["token"] != lease.token or row["epoch"] != lease.epoch or not row["valid"]:
            if isinstance(conn, AsyncSession):
                await conn.rollback()
            else:
                conn.rollback()
            lease.lost.set()
            raise SessionLockLostError("sql storage fence rejected commit")
        await self.delegate.commit(conn)


@contextlib.asynccontextmanager
async def hold_storage_guards(guards, key):
    monitors = []
    root = current_lease()
    async with contextlib.AsyncExitStack() as stack:
        try:
            for identity, guard in sorted(guards.items()):
                lease = await stack.enter_async_context(guard.hold(key, wait_timeout=10, lease_seconds=30))
                stack.enter_context(storage_lease_scope(identity, lease))
                if root:

                    async def forward(child=lease):
                        await child.lost.wait()
                        root.lost.set()

                    monitors.append(asyncio.create_task(forward()))
            yield
        finally:
            for task in monitors:
                task.cancel()
            await asyncio.gather(*monitors, return_exceptions=True)
