"""Tests for Worker pool configuration and Rendezvous routing (Stage 3C)."""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from trpc_service.transport.models import WorkerTask


def _task(**overrides: Any) -> WorkerTask:
    defaults = {
        "protocol_version": 1,
        "request_id": uuid.uuid4(),
        "tenant_id": "tenant_default",
        "app_id": "app_demo",
        "config_version": 1,
        "user_id": "user_default",
        "channel": "web",
        "session_id": "sess-1",
        "message_id": "msg-1",
        "message": "hello",
    }
    defaults.update(overrides)
    return WorkerTask(**defaults)


# ---------------------------------------------------------------------------
# WorkerEndpoint
# ---------------------------------------------------------------------------


class TestWorkerEndpoint:

    def test_normalizes_trailing_slash(self) -> None:
        from trpc_service.gateway.routing import WorkerEndpoint

        ep = WorkerEndpoint.from_url("http://worker:8001/")
        assert ep.base_url == "http://worker:8001"

    def test_endpoint_id_is_sha256_prefix(self) -> None:
        from trpc_service.gateway.routing import WorkerEndpoint

        ep = WorkerEndpoint.from_url("http://worker:8001")
        assert len(ep.endpoint_id) == 12
        assert all(c in "0123456789abcdef" for c in ep.endpoint_id)

    def test_same_url_produces_same_endpoint_id(self) -> None:
        from trpc_service.gateway.routing import WorkerEndpoint

        ep1 = WorkerEndpoint.from_url("http://worker:8001")
        ep2 = WorkerEndpoint.from_url("http://worker:8001")
        assert ep1.endpoint_id == ep2.endpoint_id

    def test_trailing_slash_does_not_change_endpoint_id(self) -> None:
        from trpc_service.gateway.routing import WorkerEndpoint

        ep1 = WorkerEndpoint.from_url("http://worker:8001")
        ep2 = WorkerEndpoint.from_url("http://worker:8001/")
        assert ep1.endpoint_id == ep2.endpoint_id

    def test_frozen(self) -> None:
        from trpc_service.gateway.routing import WorkerEndpoint

        ep = WorkerEndpoint.from_url("http://worker:8001")
        with pytest.raises(AttributeError):
            ep.base_url = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# WorkerPoolSettings — configuration validation
# ---------------------------------------------------------------------------


class TestWorkerPoolSettings:

    def test_valid_config_with_two_urls(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        settings = WorkerPoolSettings.from_env({
            "TRPC_WORKER_BASE_URLS": "http://a:8001,http://b:8002",
        })
        assert len(settings.endpoints) == 2
        assert settings.health_interval_seconds == 2.0
        assert settings.health_timeout_seconds == 1.0
        assert settings.failure_threshold == 2
        assert settings.recovery_threshold == 2

    def test_missing_env_var_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError, match="TRPC_WORKER_BASE_URLS"):
            WorkerPoolSettings.from_env({})

    def test_empty_string_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError, match="TRPC_WORKER_BASE_URLS"):
            WorkerPoolSettings.from_env({"TRPC_WORKER_BASE_URLS": ""})

    def test_single_url_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError, match="at least two"):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://a:8001",
            })

    def test_duplicate_urls_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError, match="[Dd]uplicate"):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://a:8001,http://a:8001",
            })

    def test_duplicate_after_normalization_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError, match="[Dd]uplicate"):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://a:8001,http://a:8001/",
            })

    def test_empty_entry_in_list_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://a:8001,,http://b:8002",
            })

    def test_non_http_scheme_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError, match="scheme"):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "ftp://a:8001,http://b:8002",
            })

    def test_missing_hostname_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError, match="hostname"):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://,http://b:8002",
            })

    def test_credentials_in_url_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError, match="credentials"):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://user:pass@a:8001,http://b:8002",
            })

    def test_query_in_url_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError, match="query"):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://a:8001?x=1,http://b:8002",
            })

    def test_fragment_in_url_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError, match="fragment"):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://a:8001#frag,http://b:8002",
            })

    def test_custom_health_params(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        settings = WorkerPoolSettings.from_env({
            "TRPC_WORKER_BASE_URLS": "http://a:8001,http://b:8002",
            "TRPC_WORKER_HEALTH_INTERVAL_SECONDS": "5",
            "TRPC_WORKER_HEALTH_TIMEOUT_SECONDS": "3",
            "TRPC_WORKER_HEALTH_FAILURE_THRESHOLD": "5",
            "TRPC_WORKER_HEALTH_RECOVERY_THRESHOLD": "3",
        })
        assert settings.health_interval_seconds == 5.0
        assert settings.health_timeout_seconds == 3.0
        assert settings.failure_threshold == 5
        assert settings.recovery_threshold == 3

    def test_zero_interval_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://a:8001,http://b:8002",
                "TRPC_WORKER_HEALTH_INTERVAL_SECONDS": "0",
            })

    def test_negative_timeout_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://a:8001,http://b:8002",
                "TRPC_WORKER_HEALTH_TIMEOUT_SECONDS": "-1",
            })

    def test_bool_health_param_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://a:8001,http://b:8002",
                "TRPC_WORKER_HEALTH_FAILURE_THRESHOLD": "true",
            })

    def test_float_threshold_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://a:8001,http://b:8002",
                "TRPC_WORKER_HEALTH_FAILURE_THRESHOLD": "2.5",
            })

    def test_zero_failure_threshold_raises(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError):
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "http://a:8001,http://b:8002",
                "TRPC_WORKER_HEALTH_FAILURE_THRESHOLD": "0",
            })

    def test_error_does_not_leak_url(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        with pytest.raises(ValueError) as exc_info:
            WorkerPoolSettings.from_env({
                "TRPC_WORKER_BASE_URLS": "ftp://secret-host:8001,http://b:8002",
            })
        assert "secret-host" not in str(exc_info.value)
        assert "ftp://secret-host:8001" not in str(exc_info.value)

    def test_whitespace_around_urls_trimmed(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        settings = WorkerPoolSettings.from_env({
            "TRPC_WORKER_BASE_URLS": " http://a:8001 , http://b:8002 ",
        })
        assert len(settings.endpoints) == 2

    def test_process_environment_used_when_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        monkeypatch.setenv("TRPC_WORKER_BASE_URLS", "http://a:8001,http://b:8002")
        settings = WorkerPoolSettings.from_env()
        assert len(settings.endpoints) == 2

    def test_order_does_not_affect_endpoints(self) -> None:
        from trpc_service.gateway.routing import WorkerPoolSettings

        s1 = WorkerPoolSettings.from_env({
            "TRPC_WORKER_BASE_URLS": "http://a:8001,http://b:8002",
        })
        s2 = WorkerPoolSettings.from_env({
            "TRPC_WORKER_BASE_URLS": "http://b:8002,http://a:8001",
        })
        urls1 = sorted(ep.base_url for ep in s1.endpoints)
        urls2 = sorted(ep.base_url for ep in s2.endpoints)
        assert urls1 == urls2


# ---------------------------------------------------------------------------
# WorkerRouteKey
# ---------------------------------------------------------------------------


class TestWorkerRouteKey:

    def test_from_task_extracts_fields(self) -> None:
        from trpc_service.gateway.routing import WorkerRouteKey

        task = _task(
            tenant_id="t1",
            app_id="a1",
            config_version=3,
            channel="web",
            user_id="u1",
            session_id="s1",
        )
        key = WorkerRouteKey.from_task(task)
        assert key.tenant_id == "t1"
        assert key.app_id == "a1"
        assert key.config_version == 3
        assert key.channel == "web"
        assert key.user_id == "u1"
        assert key.session_id == "s1"

    def test_same_task_produces_same_key(self) -> None:
        from trpc_service.gateway.routing import WorkerRouteKey

        task = _task()
        k1 = WorkerRouteKey.from_task(task)
        k2 = WorkerRouteKey.from_task(task)
        assert k1 == k2

    def test_different_session_produces_different_key(self) -> None:
        from trpc_service.gateway.routing import WorkerRouteKey

        t1 = _task(session_id="s1")
        t2 = _task(session_id="s2")
        assert WorkerRouteKey.from_task(t1) != WorkerRouteKey.from_task(t2)


# ---------------------------------------------------------------------------
# RendezvousRouter — stability and distribution
# ---------------------------------------------------------------------------


class TestRendezvousRouter:

    def _endpoints(self, urls: list[str]) -> list:
        from trpc_service.gateway.routing import WorkerEndpoint

        return [WorkerEndpoint.from_url(u) for u in urls]

    def test_same_input_same_result(self) -> None:
        from trpc_service.gateway.routing import RendezvousRouter, WorkerRouteKey

        router = RendezvousRouter()
        eps = self._endpoints(["http://a:8001", "http://b:8002"])
        key = WorkerRouteKey("t1", "a1", 1, "web", "u1", "s1")
        r1 = router.rank(key, eps)
        r2 = router.rank(key, eps)
        assert r1 == r2

    def test_order_independent(self) -> None:
        from trpc_service.gateway.routing import RendezvousRouter, WorkerRouteKey

        router = RendezvousRouter()
        eps_ab = self._endpoints(["http://a:8001", "http://b:8002"])
        eps_ba = self._endpoints(["http://b:8002", "http://a:8001"])
        key = WorkerRouteKey("t1", "a1", 1, "web", "u1", "s1")
        assert router.rank(key, eps_ab) == router.rank(key, eps_ba)

    def test_cross_router_consistency(self) -> None:
        from trpc_service.gateway.routing import RendezvousRouter, WorkerRouteKey

        r1 = RendezvousRouter()
        r2 = RendezvousRouter()
        eps = self._endpoints(["http://a:8001", "http://b:8002"])
        key = WorkerRouteKey("t1", "a1", 1, "web", "u1", "s1")
        assert r1.rank(key, eps) == r2.rank(key, eps)

    def test_same_session_stable(self) -> None:
        from trpc_service.gateway.routing import RendezvousRouter, WorkerRouteKey

        router = RendezvousRouter()
        eps = self._endpoints(["http://a:8001", "http://b:8002"])
        key = WorkerRouteKey("t1", "a1", 1, "web", "u1", "stable-session")
        first_choices = [router.rank(key, eps)[0] for _ in range(20)]
        assert len(set(ep.endpoint_id for ep in first_choices)) == 1

    def test_different_sessions_can_distribute(self) -> None:
        from trpc_service.gateway.routing import RendezvousRouter, WorkerRouteKey

        router = RendezvousRouter()
        eps = self._endpoints(["http://a:8001", "http://b:8002"])
        choices: set[str] = set()
        for i in range(50):
            key = WorkerRouteKey("t1", "a1", 1, "web", "u1", f"session-{i}")
            top = router.rank(key, eps)[0]
            choices.add(top.endpoint_id)
        assert len(choices) == 2, "sessions should distribute across both workers"

    def test_removing_one_worker_remaps_minimal(self) -> None:
        from trpc_service.gateway.routing import RendezvousRouter, WorkerRouteKey

        router = RendezvousRouter()
        eps_3 = self._endpoints(["http://a:8001", "http://b:8002", "http://c:8003"])
        eps_2 = self._endpoints(["http://a:8001", "http://b:8002"])
        removed_id = eps_3[2].endpoint_id
        remapped = 0
        total = 50
        for i in range(total):
            key = WorkerRouteKey("t1", "a1", 1, "web", "u1", f"session-{i}")
            choice_3 = router.rank(key, eps_3)[0]
            choice_2 = router.rank(key, eps_2)[0]
            if choice_3.endpoint_id != removed_id:
                if choice_3.endpoint_id != choice_2.endpoint_id:
                    remapped += 1
        assert remapped == 0, "sessions not on removed worker should not be remapped"

    def test_returns_all_endpoints_ranked(self) -> None:
        from trpc_service.gateway.routing import RendezvousRouter, WorkerRouteKey

        router = RendezvousRouter()
        eps = self._endpoints(["http://a:8001", "http://b:8002", "http://c:8003"])
        key = WorkerRouteKey("t1", "a1", 1, "web", "u1", "s1")
        ranked = router.rank(key, eps)
        assert len(ranked) == 3
        ids = [ep.endpoint_id for ep in ranked]
        assert len(set(ids)) == 3

    def test_does_not_use_python_hash(self) -> None:
        """Result must be deterministic across Python processes (no hash randomization)."""
        from trpc_service.gateway.routing import RendezvousRouter, WorkerRouteKey

        router = RendezvousRouter()
        eps = self._endpoints(["http://a:8001", "http://b:8002"])
        key = WorkerRouteKey("t1", "a1", 1, "web", "u1", "s1")
        result = router.rank(key, eps)
        expected_id = result[0].endpoint_id
        assert expected_id in {ep.endpoint_id for ep in eps}

    def test_single_endpoint_returns_it(self) -> None:
        from trpc_service.gateway.routing import RendezvousRouter, WorkerRouteKey

        router = RendezvousRouter()
        eps = self._endpoints(["http://a:8001"])
        key = WorkerRouteKey("t1", "a1", 1, "web", "u1", "s1")
        ranked = router.rank(key, eps)
        assert len(ranked) == 1
        assert ranked[0].base_url == "http://a:8001"
