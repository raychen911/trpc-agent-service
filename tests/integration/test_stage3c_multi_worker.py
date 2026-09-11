"""Stage 3C multi-worker routing integration tests.

These tests start a dedicated Docker Redis, two real Worker ASGI processes,
and a Gateway process. They verify the full dual-Worker topology: stable
routing, health management, session execution coordination, and recovery.

Tests are skipped when Docker is not available.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

import pytest

from .service_topology import (
    ServiceTopology,
    find_session_routing_to_worker,
    http_post_json,
    requires_model_and_docker,
    wait_for_redis,
)


@pytest.fixture(scope="module")
def topology(tmp_path_factory):
    topo = ServiceTopology(tmp_path_factory.mktemp("stage3c"))
    topo.start()
    try:
        yield topo
    finally:
        topo.stop()


@requires_model_and_docker
def test_both_workers_handle_different_sessions(topology):
    """Two Workers both actually process requests (verified via log files)."""
    # Rendezvous hashing is per-session deterministic; a small sample can land
    # entirely on one worker (P ≈ 2·(1/2)^N) purely by chance, which is a test
    # sampling artifact, not a routing failure. 12 sessions keep BOTH
    # "> 0" assertions (unchanged) statistically robust (~1/4096 collision).
    sessions = [f"int-3c-dist-{i}-{uuid.uuid4().hex[:8]}" for i in range(12)]

    for session_id in sessions:
        message_id = f"msg-{session_id}"
        resp = http_post_json(
            f"{topology.gateway_url}/api/chat",
            {
                "session_id": session_id,
                "message_id": message_id,
                "message": "Reply with exactly: OK",
            },
            headers={"X-Tenant-ID": "tenant_default"},
            timeout=30,
        )
        assert resp.get("response"), f"session {session_id} failed: {resp}"

    a_total = sum(topology.count_session_in_log("a", s) for s in sessions)
    b_total = sum(topology.count_session_in_log("b", s) for s in sessions)

    assert a_total > 0, f"Worker A handled no requests (A={a_total}, B={b_total})"
    assert b_total > 0, f"Worker B handled no requests (A={a_total}, B={b_total})"


@requires_model_and_docker
def test_same_session_routes_stably(topology):
    """Same session_id consistently routes to the same Worker (verified via logs)."""
    session_id = f"int-3c-stable-{uuid.uuid4().hex[:8]}"

    for i in range(3):
        message_id = f"msg-{session_id}-{i}"
        resp = http_post_json(
            f"{topology.gateway_url}/api/chat",
            {
                "session_id": session_id,
                "message_id": message_id,
                "message": f"Reply with exactly: MSG{i}",
            },
            headers={"X-Tenant-ID": "tenant_default"},
            timeout=30,
        )
        assert resp.get("response"), f"request {i} failed: {resp}"

    a_count = topology.count_session_in_log("a", session_id)
    b_count = topology.count_session_in_log("b", session_id)

    assert (a_count > 0 and b_count == 0) or (b_count > 0 and a_count == 0), \
        f"Session routed to multiple Workers: A={a_count}, B={b_count}"


@requires_model_and_docker
def test_two_coordinators_same_session_exactly_one_enters(topology):
    """Two independent Redis clients deterministically contend for one lease."""
    from trpc_service.agent.execution_coordinator import (
        RedisSessionExecutionCoordinator,
        SessionBusyError,
        SessionExecutionIdentity,
    )

    async def scenario() -> None:
        first = RedisSessionExecutionCoordinator(
            redis_url=topology.redis_url,
            wait_seconds=0.2,
            lease_seconds=2.0,
            renew_seconds=0.5,
        )
        second = RedisSessionExecutionCoordinator(
            redis_url=topology.redis_url,
            wait_seconds=0.2,
            lease_seconds=2.0,
            renew_seconds=0.5,
        )
        identity = SessionExecutionIdentity(
            tenant_id="tenant_default",
            app_id="app_demo",
            config_version=1,
            sdk_user_id="tenant_default:web:user_default",
            session_id=f"int-3c-concurrent-{uuid.uuid4().hex[:8]}",
        )
        entered = asyncio.Event()
        release = asyncio.Event()
        entry_count = 0

        async def holder() -> None:
            nonlocal entry_count
            async with first.acquire(identity):
                entry_count += 1
                entered.set()
                await release.wait()

        holder_task = asyncio.create_task(holder())
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            with pytest.raises(SessionBusyError):
                async with second.acquire(identity):
                    entry_count += 1
            assert entry_count == 1
        finally:
            release.set()
            await asyncio.wait_for(holder_task, timeout=2)
            await first.close()
            await second.close()

    asyncio.run(scenario())


@requires_model_and_docker
def test_kill_worker_next_request_recovers_session(topology):
    """After killing the current Worker, next request is handled by the other Worker with Session recovery."""
    from .service_topology import complete_model_seed

    session_id = f"int-3c-failover-{uuid.uuid4().hex[:8]}"
    unique_code = f"Failover{uuid.uuid4().hex[:8]}"

    def send(message_id: str, session_id: str) -> dict:
        return http_post_json(
            f"{topology.gateway_url}/api/chat",
            {
                "session_id": session_id,
                "message_id": message_id,
                "message": f"Remember this exact code: {unique_code}. Just acknowledge.",
            },
            headers={"X-Tenant-ID": "tenant_default"},
            timeout=30,
        )

    message_id_1, session_id, resp1 = complete_model_seed(send)
    assert resp1.get("response"), f"initial request failed: {resp1}"

    target = topology.which_worker_handled(session_id)
    assert target is not None, f"Could not determine which Worker handled session {session_id}"

    topology.kill_worker(target)

    # Wait for the Redis lock to expire (2 second TTL + buffer)
    time.sleep(3)

    try:
        message_id_2 = f"msg-{session_id}-2"
        resp2 = http_post_json(
            f"{topology.gateway_url}/api/chat",
            {
                "session_id": session_id,
                "message_id": message_id_2,
                "message": "What code did I ask you to remember? Reply with just the code.",
            },
            headers={"X-Tenant-ID": "tenant_default"},
            timeout=30,
        )

        other = "b" if target == "a" else "a"
        other_count = topology.count_session_in_log(other, session_id)
        assert other_count > 0, f"Other Worker ({other}) did not handle the failover request"

        response_text = resp2.get("response", "")
        assert unique_code in response_text, \
            f"Session not recovered: expected '{unique_code}' in response, got '{response_text[:100]}'"
    finally:
        topology.restart_worker(target)


@requires_model_and_docker
def test_worker_recovery_rejoins(topology):
    """After a Worker is restarted, it rejoins and handles new requests.

    Uses a pre-computed session ID that deterministically routes to the recovered Worker.
    """
    # First, determine which Worker to kill and restart
    probe_session = f"int-3c-rejoin-probe-{uuid.uuid4().hex[:8]}"
    probe_message_id = f"msg-{probe_session}"
    resp = http_post_json(
        f"{topology.gateway_url}/api/chat",
        {
            "session_id": probe_session,
            "message_id": probe_message_id,
            "message": "Reply with exactly: PROBE",
        },
        headers={"X-Tenant-ID": "tenant_default"},
        timeout=30,
    )
    assert resp.get("response"), f"probe request failed: {resp}"

    target = topology.which_worker_handled(probe_session)
    assert target is not None, "Could not determine which Worker handled probe session"

    recovered_url = topology.worker_a_url if target == "a" else topology.worker_b_url
    from trpc_service.gateway.routing import WorkerEndpoint

    endpoint_id = WorkerEndpoint.from_url(recovered_url).endpoint_id
    recovered_log_text = f"endpoint {endpoint_id} recovered"

    def recovery_log_count() -> int:
        if not topology.gateway_log.exists():
            return 0
        return topology.gateway_log.read_text().count(recovered_log_text)

    baseline_recoveries = recovery_log_count()
    topology.kill_worker(target)

    # Route one request to the dead endpoint so the Gateway records a passive
    # connectivity failure and excludes it. The request must not be retried on
    # another Worker.
    outage_session = find_session_routing_to_worker(
        topology.worker_a_url,
        topology.worker_b_url,
        recovered_url,
        prefix="int-3c-rejoin-outage",
    )
    outage_message_id = f"msg-{outage_session}"
    http_post_json(
        f"{topology.gateway_url}/api/chat",
        {
            "session_id": outage_session,
            "message_id": outage_message_id,
            "message": "Reply with exactly: MUST_NOT_RETRY",
        },
        headers={"X-Tenant-ID": "tenant_default"},
        timeout=10,
    )

    topology.restart_worker(target)

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and recovery_log_count() <= baseline_recoveries:
        time.sleep(0.2)
    assert recovery_log_count() > baseline_recoveries, \
        f"Gateway did not mark recovered Worker ({target}) healthy again"

    rejoin_session = find_session_routing_to_worker(
        topology.worker_a_url,
        topology.worker_b_url,
        recovered_url,
        prefix="int-3c-rejoin",
    )

    rejoin_message_id = f"msg-{rejoin_session}"
    resp2 = http_post_json(
        f"{topology.gateway_url}/api/chat",
        {
            "session_id": rejoin_session,
            "message_id": rejoin_message_id,
            "message": "Reply with exactly: REJOINED",
        },
        headers={"X-Tenant-ID": "tenant_default"},
        timeout=60,
    )
    assert resp2.get("response"), f"post-recovery request failed: {resp2}"

    # Verify the RECOVERED worker (not just any healthy worker) handled the request
    recovered_count = topology.count_session_in_log(target, rejoin_session)
    assert recovered_count > 0, \
        f"Recovered Worker ({target}) did not handle the post-recovery request"


@requires_model_and_docker
def test_redis_failure_no_local_fallback(topology):
    """When Redis is down, both Workers return safe error, no fallback to local state."""
    import subprocess
    subprocess.run(
        ["docker", "stop", topology.redis_container],
        capture_output=True,
        timeout=10,
    )
    time.sleep(1)

    try:
        session_id = f"int-3c-outage-{uuid.uuid4().hex[:8]}"
        message_id = f"msg-{session_id}"
        resp = http_post_json(
            f"{topology.gateway_url}/api/chat",
            {
                "session_id": session_id,
                "message_id": message_id,
                "message": "test",
            },
            headers={"X-Tenant-ID": "tenant_default"},
            timeout=15,
        )

        response_text = resp.get("response", "")

        # Public API returns ChatResponse with only session_id and response fields.
        # On error, response contains a fixed safe error text.
        safe_error_texts = [
            "An internal error occurred while talking to the model.",
            "The session is busy. Please try again shortly.",
            "Service is not configured. Set TRPC_MODEL_* environment variables and restart.",
            "Tenant agent configuration is not available.",
        ]
        assert response_text in safe_error_texts, \
            f"Expected one of safe error texts, got: {response_text!r}"

        # Confirm no internal information leakage
        full_response = json.dumps(resp)
        leakage_patterns = [
            "redis://",
            "127.0.0.1",
            topology.redis_url,
            "ConnectionError",
            "TimeoutError",
            "Traceback",
            "File \"",
            "raise ",
        ]
        for pattern in leakage_patterns:
            assert pattern not in full_response, \
                f"Response leaked internal info ({pattern!r}): {full_response}"
    finally:
        subprocess.run(
            ["docker", "start", topology.redis_container],
            capture_output=True,
            timeout=10,
        )
        assert wait_for_redis(topology.redis_url, timeout=15), "Redis did not recover"
