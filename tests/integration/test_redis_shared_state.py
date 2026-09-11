"""Redis integration tests for shared state backend.

These tests automatically start a temporary Redis container if TRPC_REDIS_URL
is not set. The container is uniquely named and cleaned up after tests complete.

Tests are skipped when Docker is not available.
"""

import asyncio
import os
import socket
import subprocess
import time
import uuid

import pytest

from google.genai import types as genai_types

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.memory import MemoryServiceConfig, RedisMemoryService
from trpc_agent_sdk.sessions import RedisSessionService, SessionServiceConfig
from trpc_agent_sdk.types import Ttl

from trpc_service.storage.state_backend import RedisStateBackend, StateBackendConfigurationError


def _docker_is_available() -> bool:
    """Check if Docker is available."""
    try:
        result = subprocess.run(
            ["docker", "ps"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _free_port() -> int:
    """Find a free port on localhost."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_redis(redis_url: str, timeout: float = 10.0) -> bool:
    """Wait for Redis to respond to PING."""
    import redis
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            r = redis.from_url(redis_url, socket_timeout=1.0, socket_connect_timeout=1.0)
            r.ping()
            r.close()
            return True
        except Exception:
            time.sleep(0.2)
    return False


requires_docker = pytest.mark.skipif(
    not _docker_is_available(),
    reason="Docker not available",
)


@pytest.fixture(scope="module")
def redis_url():
    """Start a temporary Redis container and return its URL.

    If TRPC_REDIS_URL is set, use it directly (external Redis).
    Otherwise, start a uniquely-named temporary container and clean it up after tests.
    """
    external_url = os.environ.get("TRPC_REDIS_URL")
    if external_url:
        yield external_url
        return

    if not _docker_is_available():
        pytest.skip("Docker not available")

    container_name = f"trpc-test-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    redis_url = f"redis://127.0.0.1:{port}"

    subprocess.run(
        ["docker", "run", "-d", "--name", container_name, "-p", f"{port}:6379", "redis:7"],
        capture_output=True,
        check=True,
        timeout=30,
    )

    if not _wait_for_redis(redis_url):
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, timeout=10)
        pytest.fail(f"Redis container {container_name} did not become ready")

    try:
        yield redis_url
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, timeout=10)


def _unique_ns() -> str:
    """Generate a unique namespace for test isolation."""
    return f"test:{uuid.uuid4().hex[:8]}:v1"


def _make_session_service(redis_url: str, ttl_seconds: int = 3600) -> RedisSessionService:
    return RedisSessionService(
        db_url=redis_url,
        session_config=SessionServiceConfig(ttl=Ttl(enable=True, ttl_seconds=ttl_seconds)),
    )


def _make_memory_service(redis_url: str, ttl_seconds: int = 3600) -> RedisMemoryService:
    return RedisMemoryService(
        db_url=redis_url,
        enabled=True,
        memory_service_config=MemoryServiceConfig(
            enabled=True,
            ttl=Ttl(enable=True, ttl_seconds=ttl_seconds),
        ),
    )


@requires_docker
class TestRedisStateBackendIntegration:
    """Integration tests for RedisStateBackend with a real Redis instance."""

    def test_backend_connects_to_redis(self, redis_url):
        """Backend should successfully connect to Redis."""
        env = {"TRPC_REDIS_URL": redis_url}
        backend = RedisStateBackend.from_env(env)
        try:
            backend.check_ready()
        finally:
            asyncio.run(backend.close())

    def test_backend_validates_redis_url_scheme(self, redis_url):
        """Backend should reject invalid Redis URL schemes."""
        env = {"TRPC_REDIS_URL": "http://localhost:6379"}
        with pytest.raises(StateBackendConfigurationError):
            RedisStateBackend.from_env(env)

    def test_backend_uses_configured_ttls(self, redis_url):
        """Backend should use configured TTL values."""
        env = {
            "TRPC_REDIS_URL": redis_url,
            "TRPC_REDIS_SESSION_TTL_SECONDS": "3600",
            "TRPC_REDIS_MEMORY_TTL_SECONDS": "7200",
        }
        backend = RedisStateBackend.from_env(env)
        try:
            assert backend.session_ttl == 3600
            assert backend.memory_ttl == 7200
        finally:
            asyncio.run(backend.close())

    def test_backend_default_ttls(self, redis_url):
        """Backend should use default TTLs when not configured."""
        env = {"TRPC_REDIS_URL": redis_url}
        backend = RedisStateBackend.from_env(env)
        try:
            assert backend.session_ttl == 604800
            assert backend.memory_ttl == 2592000
        finally:
            asyncio.run(backend.close())


@requires_docker
class TestSessionRecoveryAfterRestart:
    """Verify sessions persist in Redis and survive backend restart."""

    def test_session_survives_backend_restart(self, redis_url):
        """A session created by one backend instance should be retrievable by a new one."""
        ns = _unique_ns()
        user_id = "user-restart"

        async def _run():
            ss1 = _make_session_service(redis_url)
            session = await ss1.create_session(app_name=ns, user_id=user_id)
            original_id = session.id

            content = genai_types.Content(
                role="model",
                parts=[genai_types.Part.from_text(text="persistent message")],
            )
            await ss1.append_event(session, Event(author="agent", content=content))
            await ss1.close()

            ss2 = _make_session_service(redis_url)
            try:
                recovered = await ss2.get_session(app_name=ns, user_id=user_id, session_id=original_id)
                assert recovered is not None
                assert recovered.id == original_id
                assert len(recovered.events) >= 1
            finally:
                await ss2.close()

        asyncio.run(_run())

    def test_multiple_sessions_persist(self, redis_url):
        """Multiple sessions for the same user should all persist."""
        ns = _unique_ns()
        user_id = "user-multi"

        async def _run():
            ss1 = _make_session_service(redis_url)
            s1 = await ss1.create_session(app_name=ns, user_id=user_id)
            s2 = await ss1.create_session(app_name=ns, user_id=user_id)
            await ss1.close()

            ss2 = _make_session_service(redis_url)
            try:
                r1 = await ss2.get_session(app_name=ns, user_id=user_id, session_id=s1.id)
                r2 = await ss2.get_session(app_name=ns, user_id=user_id, session_id=s2.id)
                assert r1 is not None
                assert r2 is not None
                assert r1.id != r2.id
            finally:
                await ss2.close()

        asyncio.run(_run())


@requires_docker
class TestCrossSessionMemory:
    """Verify memory is stored in Redis and searchable across sessions."""

    def test_memory_stored_and_searchable(self, redis_url):
        """Events from a session should be stored in memory and searchable."""
        ns = _unique_ns()
        user_id = "user-memory"

        async def _run():
            ss = _make_session_service(redis_url)
            ms = _make_memory_service(redis_url)

            session = await ss.create_session(app_name=ns, user_id=user_id)
            content = genai_types.Content(
                role="model",
                parts=[genai_types.Part.from_text(text="unique_memory_keyword_alpha")],
            )
            await ss.append_event(session, Event(author="agent", content=content))
            await ms.store_session(session)

            result = await ms.search_memory(key=f"{ns}/{user_id}", query="unique_memory_keyword_alpha")
            assert len(result.memories) >= 1

            await ss.close()
            await ms.close()

        asyncio.run(_run())

    def test_memory_persists_across_sessions(self, redis_url):
        """Memory from session A should be searchable when session B is created."""
        ns = _unique_ns()
        user_id = "user-cross-session"

        async def _run():
            ss = _make_session_service(redis_url)
            ms = _make_memory_service(redis_url)

            session_a = await ss.create_session(app_name=ns, user_id=user_id)
            content_a = genai_types.Content(
                role="model",
                parts=[genai_types.Part.from_text(text="cross_session_keyword_beta")],
            )
            await ss.append_event(session_a, Event(author="agent", content=content_a))
            await ms.store_session(session_a)

            session_b = await ss.create_session(app_name=ns, user_id=user_id)
            content_b = genai_types.Content(
                role="model",
                parts=[genai_types.Part.from_text(text="second session content")],
            )
            await ss.append_event(session_b, Event(author="agent", content=content_b))

            result = await ms.search_memory(key=f"{ns}/{user_id}", query="cross_session_keyword_beta")
            assert len(result.memories) >= 1

            await ss.close()
            await ms.close()

        asyncio.run(_run())


@requires_docker
class TestTenantIsolation:
    """Verify namespace isolation between tenants."""

    def test_different_namespaces_are_isolated(self, redis_url):
        """Sessions in different namespaces should not be visible to each other."""
        ns_a = _unique_ns()
        ns_b = _unique_ns()
        user_id = "user-shared"

        async def _run():
            ss = _make_session_service(redis_url)
            try:
                session_a = await ss.create_session(app_name=ns_a, user_id=user_id)
                content = genai_types.Content(
                    role="model",
                    parts=[genai_types.Part.from_text(text="tenant A secret")],
                )
                await ss.append_event(session_a, Event(author="agent", content=content))

                got_a = await ss.get_session(app_name=ns_a, user_id=user_id, session_id=session_a.id)
                assert got_a is not None

                got_b = await ss.get_session(app_name=ns_b, user_id=user_id, session_id=session_a.id)
                assert got_b is None
            finally:
                await ss.close()

        asyncio.run(_run())

    def test_memory_isolated_by_namespace(self, redis_url):
        """Memory stored under one namespace should not be searchable under another."""
        ns_a = _unique_ns()
        ns_b = _unique_ns()
        user_id = "user-iso"

        async def _run():
            ss = _make_session_service(redis_url)
            ms = _make_memory_service(redis_url)

            session = await ss.create_session(app_name=ns_a, user_id=user_id)
            content = genai_types.Content(
                role="model",
                parts=[genai_types.Part.from_text(text="isolated_keyword_gamma")],
            )
            await ss.append_event(session, Event(author="agent", content=content))
            await ms.store_session(session)

            result_a = await ms.search_memory(key=f"{ns_a}/{user_id}", query="isolated_keyword_gamma")
            assert len(result_a.memories) >= 1

            result_b = await ms.search_memory(key=f"{ns_b}/{user_id}", query="isolated_keyword_gamma")
            assert len(result_b.memories) == 0

            await ss.close()
            await ms.close()

        asyncio.run(_run())


@requires_docker
class TestTTLBehavior:
    """Verify TTL is actually applied to Redis keys."""

    def test_session_key_has_ttl(self, redis_url):
        """Session keys in Redis should have a TTL set."""
        ns = _unique_ns()
        user_id = "user-ttl"

        async def _run():
            ss = _make_session_service(redis_url, ttl_seconds=120)
            try:
                session = await ss.create_session(app_name=ns, user_id=user_id)
                import redis
                r = redis.from_url(redis_url, socket_timeout=5.0)
                try:
                    redis_key = f"session:{ns}:{user_id}:{session.id}"
                    ttl = r.ttl(redis_key)
                    assert ttl > 0, f"Expected positive TTL, got {ttl}"
                    assert ttl <= 120, f"Expected TTL <= 120, got {ttl}"
                finally:
                    r.close()
            finally:
                await ss.close()

        asyncio.run(_run())

    def test_memory_key_has_ttl(self, redis_url):
        """Memory keys in Redis should have a TTL set."""
        ns = _unique_ns()
        user_id = "user-ttl-mem"

        async def _run():
            ss = _make_session_service(redis_url, ttl_seconds=120)
            ms = _make_memory_service(redis_url, ttl_seconds=120)

            session = await ss.create_session(app_name=ns, user_id=user_id)
            content = genai_types.Content(
                role="model",
                parts=[genai_types.Part.from_text(text="ttl test content")],
            )
            await ss.append_event(session, Event(author="agent", content=content))
            await ms.store_session(session)

            import redis
            r = redis.from_url(redis_url, socket_timeout=5.0)
            try:
                memory_key_pattern = f"memory:{ns}/{user_id}:*"
                keys = r.keys(memory_key_pattern)
                assert len(keys) >= 1, "Expected at least one memory key"
                ttl = r.ttl(keys[0])
                assert ttl > 0, f"Expected positive TTL, got {ttl}"
                assert ttl <= 120, f"Expected TTL <= 120, got {ttl}"
            finally:
                r.close()

            await ss.close()
            await ms.close()

        asyncio.run(_run())


@requires_docker
class TestRedisOutageDetection:
    """Verify check_ready detects Redis outage with timeout."""

    def test_check_ready_fails_on_unreachable_redis(self, redis_url):
        """check_ready should raise when Redis is unreachable, with timeout."""
        env = {"TRPC_REDIS_URL": "redis://192.0.2.1:6379"}
        backend = RedisStateBackend.from_env(env)
        try:
            start = time.monotonic()
            with pytest.raises(StateBackendConfigurationError):
                backend.check_ready()
            elapsed = time.monotonic() - start
            assert elapsed < 15.0, f"check_ready took {elapsed:.1f}s, expected < 15s (timeout should kick in)"
        finally:
            asyncio.run(backend.close())


__all__ = [
    "TestRedisStateBackendIntegration",
    "TestSessionRecoveryAfterRestart",
    "TestCrossSessionMemory",
    "TestTenantIsolation",
    "TestTTLBehavior",
    "TestRedisOutageDetection",
]
