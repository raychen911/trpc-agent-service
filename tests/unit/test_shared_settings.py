from __future__ import annotations
import pytest

from trpc_service.config.settings import (
    ConfigurationError,
    RuntimeProfile,
    load_runtime_settings,
)


def _shared_env() -> dict[str, str]:
    return {
        "TRPC_RUNTIME_PROFILE": "shared",
        "TRPC_NODE_ID": "worker-a",
        "TRPC_SHARED_REDIS_URL": "redis://:runtime-only@127.0.0.1:6379/0",
        "TRPC_SHARED_DATABASE_URL": (
            "postgresql+asyncpg://trpc_agent:runtime-only@127.0.0.1:5432/trpc_agent"
        ),
    }


def test_shared_profile_has_validated_node_and_safe_defaults() -> None:
    settings = load_runtime_settings(_shared_env())
    assert settings.profile == RuntimeProfile.SHARED
    assert settings.node.node_id == "worker-a"
    assert settings.lease.lease_ms == 10_000
    assert settings.lease.heartbeat_ms == 3_000
    assert settings.lease.acquire_wait_ms == 2_000
    assert settings.agent_timeout_seconds == 30
    rendered = repr(settings)
    assert "runtime-only" not in rendered
    assert settings.redis_url.get_secret_value().startswith("redis://")


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("TRPC_NODE_ID", ""),
        ("TRPC_NODE_ID", "not a valid node"),
        ("TRPC_SHARED_REDIS_URL", ""),
        ("TRPC_SHARED_DATABASE_URL", ""),
    ],
)
def test_shared_profile_fails_closed_for_missing_or_invalid_values(
    name: str, value: str
) -> None:
    environ = _shared_env()
    environ[name] = value
    with pytest.raises(ConfigurationError):
        load_runtime_settings(environ)


def test_local_profile_does_not_silently_replace_requested_shared_profile() -> None:
    with pytest.raises(ConfigurationError):
        load_runtime_settings({"TRPC_RUNTIME_PROFILE": "shared"})
