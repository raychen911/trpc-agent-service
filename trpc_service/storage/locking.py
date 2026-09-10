"""Session-scoped coordination primitives for multi-worker execution."""

from __future__ import annotations

import hashlib
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Event, RLock, Thread, current_thread
from typing import Any, Iterator, Self, cast
from uuid import uuid4


class SessionLockTimeout(TimeoutError):
    """Raised when a session cannot be acquired before the deadline."""


class SessionLeaseLost(RuntimeError):
    """Raised when a worker tries to use an expired or superseded lease."""


@dataclass(frozen=True, slots=True)
class SessionLease:
    tenant_id: str
    session_id: str
    owner: str
    fencing_token: int
    expires_at: datetime


_LOCAL_LOCKS: dict[str, RLock] = {}
_LOCAL_LOCKS_GUARD = RLock()
_LOCAL_FENCING_TOKENS: dict[str, int] = {}


def _local_lock(key: str) -> RLock:
    with _LOCAL_LOCKS_GUARD:
        return _LOCAL_LOCKS.setdefault(key, RLock())


def _lock_key(tenant_id: str, session_id: str) -> str:
    return f"{tenant_id}:{session_id}"


def _new_local_lease(tenant_id: str, session_id: str, owner: str, ttl: float) -> SessionLease:
    key = _lock_key(tenant_id, session_id)
    with _LOCAL_LOCKS_GUARD:
        token = _LOCAL_FENCING_TOKENS.get(key, 0) + 1
        _LOCAL_FENCING_TOKENS[key] = token
    return SessionLease(
        tenant_id=tenant_id,
        session_id=session_id,
        owner=owner,
        fencing_token=token,
        expires_at=datetime.now(UTC) + timedelta(seconds=max(1.0, ttl)),
    )


@contextmanager
def session_lease(
    store: object,
    tenant_id: str,
    session_id: str,
    timeout: float | None = None,
) -> Iterator[SessionLease | str | None]:
    """Acquire a fencing-token lease, falling back to the legacy lock API."""

    timeout = float(os.getenv("SESSION_LOCK_TIMEOUT_SECONDS", "30") if timeout is None else timeout)
    acquire = getattr(store, "acquire_session_lease", None)
    release = getattr(store, "release_session_lease", None)
    if acquire and release:
        lease = acquire(tenant_id, session_id, timeout)
        try:
            yield lease
        finally:
            release(tenant_id, session_id, lease)
        return

    acquire_lock = getattr(store, "acquire_session_lock", None)
    release_lock = getattr(store, "release_session_lock", None)
    if acquire_lock and release_lock:
        token = acquire_lock(tenant_id, session_id, timeout)
        try:
            yield token
        finally:
            release_lock(tenant_id, session_id, token)
        return

    key = _lock_key(tenant_id, session_id)
    lock = _local_lock(key)
    if not lock.acquire(timeout=max(0.0, timeout)):
        raise SessionLockTimeout(f"session lock timeout: {key}")
    owner = str(uuid4())
    lease = _new_local_lease(
        tenant_id,
        session_id,
        owner,
        float(os.getenv("SESSION_LOCK_TTL_SECONDS", "120")),
    )
    try:
        yield lease
    finally:
        lock.release()


def validate_session_lease(store: object, lease: SessionLease | None) -> None:
    if not isinstance(lease, SessionLease) or lease.fencing_token == 0:
        return
    validate = getattr(store, "validate_session_lease", None)
    if validate:
        validate(lease)


def renew_session_lease(store: object, lease: SessionLease | None) -> SessionLease | None:
    if not isinstance(lease, SessionLease):
        return lease
    renew = getattr(store, "renew_session_lease", None)
    if renew:
        renewed = cast(SessionLease, renew(lease))
        validate_session_lease(store, renewed)
        return renewed
    return lease


class SessionLeaseHeartbeat:
    """Refresh a session lease while synchronous model/tool work is running.

    The worker path is synchronous, so a daemon thread is used to keep the
    backend lease alive during an otherwise uninterruptible provider call.
    Errors are retained and surfaced by ``raise_if_failed`` before commit.
    """

    def __init__(self, store: object, lease: SessionLease, interval: float | None = None) -> None:
        self.store = store
        self.lease = lease
        ttl = float(os.getenv("SESSION_LEASE_TTL_SECONDS", "120"))
        self.interval = max(0.1, float(interval if interval is not None else ttl / 3.0))
        self._stop = Event()
        self._done = Event()
        self._error: BaseException | None = None
        self._thread: Thread | None = None

    def start(self) -> Self:
        if self._thread is not None:
            return self

        def beat() -> None:
            try:
                while not self._stop.wait(self.interval):
                    self.lease = renew_session_lease(self.store, self.lease) or self.lease
            except BaseException as exc:  # retained for the owning worker thread
                self._error = exc
            finally:
                self._done.set()

        self._thread = Thread(target=beat, name="session-lease-heartbeat", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not current_thread():
            thread.join(timeout=max(1.0, self.interval))

    def raise_if_failed(self) -> None:
        if self._error is not None:
            raise self._error

    def __enter__(self) -> Self:
        return self.start()

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: Any) -> None:
        self.stop()
        if exc_type is None:
            self.raise_if_failed()


@contextmanager
def session_lock(
    store: object,
    tenant_id: str,
    session_id: str,
    timeout: float | None = None,
) -> Iterator[SessionLease | str | None]:
    """Acquire the strongest session lock supported by the backend.

    The helper keeps the storage protocol backwards compatible: custom stores
    without a lock method still work, while Redis/PostgreSQL stores coordinate
    across processes and local stores coordinate threads.
    """

    timeout = float(os.getenv("SESSION_LOCK_TIMEOUT_SECONDS", "30") if timeout is None else timeout)
    acquire = getattr(store, "acquire_session_lease", None)
    release = getattr(store, "release_session_lease", None)
    if acquire and release:
        lease = acquire(tenant_id, session_id, timeout)
        try:
            yield lease
        finally:
            release(tenant_id, session_id, lease)
        return
    acquire = getattr(store, "acquire_session_lock", None)
    release = getattr(store, "release_session_lock", None)
    if acquire and release:
        token = acquire(tenant_id, session_id, timeout)
        try:
            yield token
        finally:
            release(tenant_id, session_id, token)
        return

    key = _lock_key(tenant_id, session_id)
    lock = _local_lock(key)
    if not lock.acquire(timeout=max(0.0, timeout)):
        raise SessionLockTimeout(f"session lock timeout: {key}")
    try:
        yield None
    finally:
        lock.release()


class RedisSessionLockMixin:
    """Mixin implementing a crash-safe Redis SET NX EX lock."""

    client: Any

    def _key(self, kind: str, tenant_id: str, suffix: str = "") -> str:
        """Return a namespaced backend key supplied by the concrete store."""
        raise NotImplementedError

    def acquire_session_lock(self, tenant_id: str, session_id: str, timeout: float) -> str:
        key = self._key("session-lock", tenant_id, session_id)
        token = str(uuid4())
        deadline = time.monotonic() + max(0.0, timeout)
        ttl = max(1, int(os.getenv("SESSION_LOCK_TTL_SECONDS", "120")))
        while True:
            if self.client.set(key, token, nx=True, ex=ttl):
                return token
            if time.monotonic() >= deadline:
                raise SessionLockTimeout(f"session lock timeout: {tenant_id}/{session_id}")
            time.sleep(0.05)

    def release_session_lock(self, tenant_id: str, session_id: str, token: str) -> None:
        key = self._key("session-lock", tenant_id, session_id)
        script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
          return redis.call('DEL', KEYS[1])
        end
        return 0
        """
        self.client.eval(script, 1, key, token)

    def acquire_session_lease(self, tenant_id: str, session_id: str, timeout: float) -> SessionLease:
        key = self._key("session-lease", tenant_id, session_id)
        counter_key = self._key("session-fence", tenant_id, session_id)
        owner = str(uuid4())
        deadline = time.monotonic() + max(0.0, timeout)
        ttl = max(1, int(os.getenv("SESSION_LEASE_TTL_SECONDS", "120")))
        script = """
        if redis.call('EXISTS', KEYS[1]) == 1 then
          return 0
        end
        local fence = redis.call('INCR', KEYS[2])
        redis.call('SET', KEYS[1], cjson.encode({owner=ARGV[1], fencing_token=fence}), 'EX', ARGV[2])
        return fence
        """
        while True:
            fence = int(self.client.eval(script, 2, key, counter_key, owner, ttl))
            if fence:
                return SessionLease(
                    tenant_id,
                    session_id,
                    owner,
                    fence,
                    datetime.now(UTC) + timedelta(seconds=ttl),
                )
            if time.monotonic() >= deadline:
                raise SessionLockTimeout(f"session lease timeout: {tenant_id}/{session_id}")
            time.sleep(0.05)

    def release_session_lease(self, tenant_id: str, session_id: str, lease: SessionLease) -> None:
        key = self._key("session-lease", tenant_id, session_id)
        script = """
        local raw = redis.call('GET', KEYS[1])
        if not raw then return 0 end
        local item = cjson.decode(raw)
        if item['owner'] == ARGV[1] and tonumber(item['fencing_token']) == tonumber(ARGV[2]) then
          return redis.call('DEL', KEYS[1])
        end
        return 0
        """
        self.client.eval(script, 1, key, lease.owner, lease.fencing_token)

    def renew_session_lease(self, lease: SessionLease) -> SessionLease:
        key = self._key("session-lease", lease.tenant_id, lease.session_id)
        ttl = max(1, int(os.getenv("SESSION_LEASE_TTL_SECONDS", "120")))
        script = """
        local raw = redis.call('GET', KEYS[1])
        if not raw then return 0 end
        local item = cjson.decode(raw)
        if item['owner'] ~= ARGV[1] or tonumber(item['fencing_token']) ~= tonumber(ARGV[2]) then
          return 0
        end
        redis.call('EXPIRE', KEYS[1], ARGV[3])
        return 1
        """
        renewed = self.client.eval(script, 1, key, lease.owner, lease.fencing_token, ttl)
        if int(renewed) != 1:
            raise SessionLeaseLost(f"session lease lost: {lease.tenant_id}/{lease.session_id}")
        return SessionLease(
            lease.tenant_id,
            lease.session_id,
            lease.owner,
            lease.fencing_token,
            datetime.now(UTC) + timedelta(seconds=ttl),
        )

    def validate_session_lease(self, lease: SessionLease) -> None:
        raw = self.client.get(self._key("session-lease", lease.tenant_id, lease.session_id))
        if not raw:
            raise SessionLeaseLost(f"session lease expired: {lease.tenant_id}/{lease.session_id}")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        import json

        current = json.loads(raw)
        if current.get("owner") != lease.owner or int(current.get("fencing_token", 0)) != lease.fencing_token:
            raise SessionLeaseLost(f"session fencing token rejected: {lease.tenant_id}/{lease.session_id}")


class PostgresSessionLockMixin:
    """Mixin using a PostgreSQL advisory lock scoped to the connection."""

    _conn: Any
    _lock: RLock

    @staticmethod
    def _lock_name(tenant_id: str, session_id: str) -> str:
        digest = hashlib.sha256(f"{tenant_id}:{session_id}".encode()).hexdigest()
        return f"trpc-agent-session:{digest}"

    def acquire_session_lock(self, tenant_id: str, session_id: str, timeout: float) -> str:
        lock_name = self._lock_name(tenant_id, session_id)
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_try_advisory_lock(hashtext(%s))",
                    (lock_name,),
                )
                acquired = bool(cur.fetchone()[0])
            if acquired:
                return lock_name
            if time.monotonic() >= deadline:
                raise SessionLockTimeout(f"session lock timeout: {tenant_id}/{session_id}")
            time.sleep(0.05)

    def release_session_lock(self, tenant_id: str, session_id: str, token: str) -> None:
        del tenant_id, session_id
        with self._conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (token,))

    def acquire_session_lease(self, tenant_id: str, session_id: str, timeout: float) -> SessionLease:
        owner = str(uuid4())
        deadline = time.monotonic() + max(0.0, timeout)
        ttl = max(1, int(os.getenv("SESSION_LEASE_TTL_SECONDS", "120")))
        while True:
            now = datetime.now(UTC)
            expires_at = now + timedelta(seconds=ttl)
            with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
                cur.execute(
                    (
                        "INSERT INTO session_fence (tenant_id, session_id, fencing_token) "
                        "VALUES (%s,%s,0) ON CONFLICT DO NOTHING"
                    ),
                    (tenant_id, session_id),
                )
                cur.execute(
                    (
                        "SELECT owner, fencing_token, expires_at FROM session_lease "
                        "WHERE tenant_id=%s AND session_id=%s FOR UPDATE"
                    ),
                    (tenant_id, session_id),
                )
                current = cur.fetchone()
                if not (current and current[2] and current[2] > now):
                    cur.execute(
                        (
                            "UPDATE session_fence SET fencing_token=fencing_token+1 "
                            "WHERE tenant_id=%s AND session_id=%s RETURNING fencing_token"
                        ),
                        (tenant_id, session_id),
                    )
                    fence = int(cur.fetchone()[0])
                    cur.execute(
                        """
                        INSERT INTO session_lease (tenant_id, session_id, owner, fencing_token, expires_at)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (tenant_id, session_id) DO UPDATE SET
                          owner=EXCLUDED.owner, fencing_token=EXCLUDED.fencing_token,
                          expires_at=EXCLUDED.expires_at
                        """,
                        (tenant_id, session_id, owner, fence, expires_at),
                    )
                    return SessionLease(tenant_id, session_id, owner, fence, expires_at)
            if time.monotonic() >= deadline:
                raise SessionLockTimeout(f"session lease timeout: {tenant_id}/{session_id}")
            time.sleep(0.05)

    def release_session_lease(self, tenant_id: str, session_id: str, lease: SessionLease) -> None:
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM session_lease WHERE tenant_id=%s AND session_id=%s AND owner=%s AND fencing_token=%s",
                (tenant_id, session_id, lease.owner, lease.fencing_token),
            )

    def renew_session_lease(self, lease: SessionLease) -> SessionLease:
        ttl = max(1, int(os.getenv("SESSION_LEASE_TTL_SECONDS", "120")))
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl)
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE session_lease SET expires_at=%s
                WHERE tenant_id=%s AND session_id=%s AND owner=%s AND fencing_token=%s
                """,
                (expires_at, lease.tenant_id, lease.session_id, lease.owner, lease.fencing_token),
            )
            if cur.rowcount != 1:
                raise SessionLeaseLost(f"session lease lost: {lease.tenant_id}/{lease.session_id}")
        return SessionLease(lease.tenant_id, lease.session_id, lease.owner, lease.fencing_token, expires_at)

    def validate_session_lease(self, lease: SessionLease) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM session_lease
                WHERE tenant_id=%s AND session_id=%s AND owner=%s
                  AND fencing_token=%s AND expires_at > CURRENT_TIMESTAMP
                """,
                (lease.tenant_id, lease.session_id, lease.owner, lease.fencing_token),
            )
            if cur.fetchone() is None:
                raise SessionLeaseLost(f"session fencing token rejected: {lease.tenant_id}/{lease.session_id}")


@contextmanager
def postgres_advisory_lock(connection: Any, name: str, timeout: float = 60.0) -> Iterator[None]:
    """Serialize process-wide database operations such as schema migrations."""

    deadline = time.monotonic() + max(0.0, timeout)
    acquired = False
    while not acquired:
        with connection.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (name,))
            acquired = bool(cur.fetchone()[0])
        if acquired:
            break
        if time.monotonic() >= deadline:
            raise SessionLockTimeout(f"postgres advisory lock timeout: {name}")
        time.sleep(0.05)
    try:
        yield
    finally:
        with connection.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (name,))


@contextmanager
def local_session_lock(tenant_id: str, session_id: str) -> Iterator[None]:
    """Explicit local lock context for stores that do not need a backend lock."""

    with session_lock(object(), tenant_id, session_id, timeout=30):
        yield
