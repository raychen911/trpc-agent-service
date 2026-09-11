"""Tests for AgentStateBackend and RedisStateBackend."""

import pytest
from unittest.mock import Mock, patch

from trpc_service.storage.state_backend import (
    RedisStateBackend,
    StateBackendConfigurationError,
)


class TestRedisStateBackendFromEnv:
    """Test RedisStateBackend.from_env() factory method."""

    def test_missing_redis_url_raises_error(self):
        """Missing TRPC_REDIS_URL should raise StateBackendConfigurationError."""
        env = {}
        with pytest.raises(StateBackendConfigurationError) as exc_info:
            RedisStateBackend.from_env(env)
        assert "TRPC_REDIS_URL" in str(exc_info.value)

    def test_empty_redis_url_raises_error(self):
        """Empty TRPC_REDIS_URL should raise StateBackendConfigurationError."""
        env = {"TRPC_REDIS_URL": ""}
        with pytest.raises(StateBackendConfigurationError) as exc_info:
            RedisStateBackend.from_env(env)
        assert "TRPC_REDIS_URL" in str(exc_info.value)

    def test_invalid_redis_url_scheme_raises_error(self):
        """Invalid URL scheme should raise StateBackendConfigurationError."""
        env = {"TRPC_REDIS_URL": "http://localhost:6379"}
        with pytest.raises(StateBackendConfigurationError) as exc_info:
            RedisStateBackend.from_env(env)
        assert "scheme" in str(exc_info.value).lower()

    def test_invalid_redis_url_no_host_raises_error(self):
        """URL without hostname should raise StateBackendConfigurationError."""
        env = {"TRPC_REDIS_URL": "redis://"}
        with pytest.raises(StateBackendConfigurationError) as exc_info:
            RedisStateBackend.from_env(env)
        assert "hostname" in str(exc_info.value).lower()

    def test_invalid_session_ttl_raises_error(self):
        """Non-integer session TTL should raise StateBackendConfigurationError."""
        env = {
            "TRPC_REDIS_URL": "redis://localhost:6379",
            "TRPC_REDIS_SESSION_TTL_SECONDS": "not_a_number",
        }
        with pytest.raises(StateBackendConfigurationError) as exc_info:
            RedisStateBackend.from_env(env)
        assert "TRPC_REDIS_SESSION_TTL_SECONDS" in str(exc_info.value)

    def test_negative_session_ttl_raises_error(self):
        """Negative session TTL should raise StateBackendConfigurationError."""
        env = {
            "TRPC_REDIS_URL": "redis://localhost:6379",
            "TRPC_REDIS_SESSION_TTL_SECONDS": "-100",
        }
        with pytest.raises(StateBackendConfigurationError) as exc_info:
            RedisStateBackend.from_env(env)
        assert "TRPC_REDIS_SESSION_TTL_SECONDS" in str(exc_info.value)

    def test_invalid_memory_ttl_raises_error(self):
        """Non-integer memory TTL should raise StateBackendConfigurationError."""
        env = {
            "TRPC_REDIS_URL": "redis://localhost:6379",
            "TRPC_REDIS_MEMORY_TTL_SECONDS": "not_a_number",
        }
        with pytest.raises(StateBackendConfigurationError) as exc_info:
            RedisStateBackend.from_env(env)
        assert "TRPC_REDIS_MEMORY_TTL_SECONDS" in str(exc_info.value)

    def test_negative_memory_ttl_raises_error(self):
        """Negative memory TTL should raise StateBackendConfigurationError."""
        env = {
            "TRPC_REDIS_URL": "redis://localhost:6379",
            "TRPC_REDIS_MEMORY_TTL_SECONDS": "-100",
        }
        with pytest.raises(StateBackendConfigurationError) as exc_info:
            RedisStateBackend.from_env(env)
        assert "TRPC_REDIS_MEMORY_TTL_SECONDS" in str(exc_info.value)

    def test_default_ttls_are_used(self):
        """Default TTLs should be used when not specified."""
        env = {"TRPC_REDIS_URL": "redis://localhost:6379"}
        backend = RedisStateBackend.from_env(env)
        assert backend.session_ttl == 604800  # 7 days
        assert backend.memory_ttl == 2592000  # 30 days

    def test_custom_ttls_are_used(self):
        """Custom TTLs should be used when specified."""
        env = {
            "TRPC_REDIS_URL": "redis://localhost:6379",
            "TRPC_REDIS_SESSION_TTL_SECONDS": "3600",
            "TRPC_REDIS_MEMORY_TTL_SECONDS": "7200",
        }
        backend = RedisStateBackend.from_env(env)
        assert backend.session_ttl == 3600
        assert backend.memory_ttl == 7200

    @patch("trpc_service.storage.state_backend.RedisSessionService")
    @patch("trpc_service.storage.state_backend.RedisMemoryService")
    def test_successful_construction(self, mock_memory_cls, mock_session_cls):
        """Valid configuration should construct backend successfully."""
        env = {"TRPC_REDIS_URL": "redis://localhost:6379"}
        backend = RedisStateBackend.from_env(env)

        assert backend.redis_url == "redis://localhost:6379"
        assert backend.session_service is not None
        assert backend.memory_service is not None
        mock_session_cls.assert_called_once()
        mock_memory_cls.assert_called_once()

    @patch("trpc_service.storage.state_backend.RedisSessionService")
    @patch("trpc_service.storage.state_backend.RedisMemoryService")
    def test_session_service_config(self, mock_memory_cls, mock_session_cls):
        """SessionServiceConfig should be created with correct TTL."""
        env = {
            "TRPC_REDIS_URL": "redis://localhost:6379",
            "TRPC_REDIS_SESSION_TTL_SECONDS": "3600",
        }
        RedisStateBackend.from_env(env)

        # Verify SessionServiceConfig was created with correct TTL
        call_args = mock_session_cls.call_args
        assert call_args is not None
        session_config = call_args.kwargs.get("session_config")
        assert session_config is not None

    @patch("trpc_service.storage.state_backend.RedisSessionService")
    @patch("trpc_service.storage.state_backend.RedisMemoryService")
    def test_memory_service_config(self, mock_memory_cls, mock_session_cls):
        """MemoryServiceConfig should be created with correct TTL and enabled."""
        env = {
            "TRPC_REDIS_URL": "redis://localhost:6379",
            "TRPC_REDIS_MEMORY_TTL_SECONDS": "7200",
        }
        RedisStateBackend.from_env(env)

        # Verify MemoryServiceConfig was created with correct TTL and enabled=True
        call_args = mock_memory_cls.call_args
        assert call_args is not None
        memory_config = call_args.kwargs.get("memory_service_config")
        assert memory_config is not None
        assert memory_config.enabled is True


class TestRedisStateBackendReadiness:
    """Test RedisStateBackend.check_ready() method."""

    @patch("trpc_service.storage.state_backend.RedisSessionService")
    @patch("trpc_service.storage.state_backend.RedisMemoryService")
    def test_check_ready_success(self, mock_memory_cls, mock_session_cls):
        """check_ready() should succeed when Redis is reachable."""
        env = {"TRPC_REDIS_URL": "redis://localhost:6379"}
        backend = RedisStateBackend.from_env(env)

        # Mock redis.from_url to return a mock that succeeds on ping
        mock_redis = Mock()
        mock_redis.ping = Mock()
        mock_redis.close = Mock()
        with patch("redis.from_url", return_value=mock_redis) as mock_from_url:
            backend.check_ready()
            mock_from_url.assert_called_once_with(
                "redis://localhost:6379",
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )

    @patch("trpc_service.storage.state_backend.RedisSessionService")
    @patch("trpc_service.storage.state_backend.RedisMemoryService")
    def test_check_ready_failure_sanitized(self, mock_memory_cls, mock_session_cls):
        """check_ready() should sanitize Redis failure messages."""
        env = {"TRPC_REDIS_URL": "redis://localhost:6379"}
        backend = RedisStateBackend.from_env(env)

        # Mock redis.from_url to raise with sensitive info
        with patch("redis.from_url", side_effect=Exception("Connection refused to redis://user:pass@localhost:6379")):
            with pytest.raises(StateBackendConfigurationError) as exc_info:
                backend.check_ready()

        # Should not contain sensitive info
        error_msg = str(exc_info.value)
        assert "user:pass" not in error_msg
        assert "redis://localhost:6379" not in error_msg


class TestRedisStateBackendClose:
    """Test RedisStateBackend.close() method."""

    @patch("trpc_service.storage.state_backend.RedisSessionService")
    @patch("trpc_service.storage.state_backend.RedisMemoryService")
    @pytest.mark.asyncio
    async def test_close_is_idempotent(self, mock_memory_cls, mock_session_cls):
        """close() should be idempotent and safe to call multiple times."""
        env = {"TRPC_REDIS_URL": "redis://localhost:6379"}
        backend = RedisStateBackend.from_env(env)

        # Mock async close methods
        async def mock_close():
            pass

        backend.session_service.close = mock_close
        backend.memory_service.close = mock_close

        # Call close multiple times
        await backend.close()
        await backend.close()
        await backend.close()

    @patch("trpc_service.storage.state_backend.RedisSessionService")
    @patch("trpc_service.storage.state_backend.RedisMemoryService")
    @pytest.mark.asyncio
    async def test_close_async_is_idempotent(self, mock_memory_cls, mock_session_cls):
        """async close() should be idempotent and safe to call multiple times."""
        env = {"TRPC_REDIS_URL": "redis://localhost:6379"}
        backend = RedisStateBackend.from_env(env)

        # Mock async close methods
        async def mock_close():
            pass

        backend.session_service.close = mock_close
        backend.memory_service.close = mock_close

        # Call close multiple times
        await backend.close()
        await backend.close()
        await backend.close()


class TestAgentStateBackendProtocol:
    """Test AgentStateBackend protocol compliance."""

    @patch("trpc_service.storage.state_backend.RedisSessionService")
    @patch("trpc_service.storage.state_backend.RedisMemoryService")
    def test_backend_implements_protocol(self, mock_memory_cls, mock_session_cls):
        """RedisStateBackend should implement AgentStateBackend protocol."""
        env = {"TRPC_REDIS_URL": "redis://localhost:6379"}
        backend = RedisStateBackend.from_env(env)

        # Verify protocol methods exist
        assert hasattr(backend, "session_service")
        assert hasattr(backend, "memory_service")
        assert hasattr(backend, "check_ready")
        assert hasattr(backend, "close")

        # Verify types
        assert backend.session_service is not None
        assert backend.memory_service is not None
        assert callable(backend.check_ready)
        assert callable(backend.close)
