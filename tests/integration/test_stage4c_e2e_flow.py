"""End-to-end integration tests for Stage 4C message idempotency.

Tests the full chain: Gateway → dual Workers → Redis → PostgreSQL.
Uses a real service topology (PostgreSQL + Redis + 2 Workers + Gateway).
"""

from __future__ import annotations

import json
import subprocess
import threading
import time
import urllib
import uuid

import pytest

from .service_topology import (
    ServiceTopology,
    http_post_json,
    requires_model_and_docker,
)


@pytest.fixture(scope="module")
def topology(tmp_path_factory):
    topo = ServiceTopology(tmp_path_factory.mktemp("stage4c-e2e"))
    topo.start()
    try:
        yield topo
    finally:
        topo.stop()


def _pg_query(topology: ServiceTopology, sql: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "exec",
            topology.pg_container,
            "psql",
            "-U",
            topology.pg_user,
            "-d",
            topology.pg_db,
            "-t",
            "-A",
            "-c",
            sql,
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result.stdout.strip()


@requires_model_and_docker
class TestStage4CE2E:
    """Full-chain E2E tests: Gateway → Workers → Redis → PostgreSQL."""

    def test_concurrent_same_key_exactly_one_execution(self, topology: ServiceTopology):
        """Two Workers receiving same business key: exactly one executes."""
        from .service_topology import is_transient_model_error

        max_attempts = 3
        for attempt in range(max_attempts):
            msg_id = f"msg-4c-e2e-concurrent-{uuid.uuid4().hex[:8]}"
            session_id = f"sess-4c-e2e-concurrent-{uuid.uuid4().hex[:8]}"

            task_body = {
                "protocol_version": 1,
                "request_id": str(uuid.uuid4()),
                "tenant_id": "tenant_default",
                "app_id": "app_demo",
                "config_version": 2,
                "user_id": "user_default",
                "channel": "web",
                "session_id": session_id,
                "message_id": msg_id,
                "message": "Reply with exactly: E2E_CONCURRENT",
            }

            body_bytes = json.dumps(task_body).encode()

            req_a = urllib.request.Request(
                f"{topology.worker_a_url}/internal/v1/chat",
                data=body_bytes,
                method="POST",
            )
            req_a.add_header("Content-Type", "application/json")
            req_a.add_header("X-TRPC-Internal-Token", topology._internal_token)

            result_a: dict = {}
            error_a: list = []

            def send_to_a():
                try:
                    with urllib.request.urlopen(req_a, timeout=60) as resp:
                        result_a.update(json.loads(resp.read().decode()))
                except Exception as exc:
                    error_a.append(str(exc))

            t = threading.Thread(target=send_to_a)
            t.start()

            deadline = time.monotonic() + 15
            got_processing = False
            while time.monotonic() < deadline:
                state = _pg_query(
                    topology, f"SELECT state FROM message_receipts "
                    f"WHERE tenant_id='tenant_default' AND message_id='{msg_id}'")
                if state == "processing":
                    got_processing = True
                    break
                time.sleep(0.2)

            if not got_processing:
                t.join(timeout=60)
                if error_a or is_transient_model_error(result_a):
                    if attempt < max_attempts - 1:
                        continue
                pytest.fail(f"Did not observe processing state for receipt. "
                            f"error_a={error_a}, result_a={result_a}")

            resp_b = _worker_chat(topology, "b", {
                **task_body,
                "request_id": str(uuid.uuid4()),
            })

            if resp_b.get("error_code") != "message_in_progress":
                t.join(timeout=60)
                if error_a or is_transient_model_error(result_a):
                    if attempt < max_attempts - 1:
                        continue
                pytest.fail(f"Worker B should return message_in_progress, got: {resp_b}")

            t.join(timeout=60)
            if error_a:
                if is_transient_model_error(result_a):
                    if attempt < max_attempts - 1:
                        continue
                pytest.fail(f"Worker A request failed: {error_a}")

            if "E2E_CONCURRENT" not in result_a.get("response", ""):
                if is_transient_model_error(result_a):
                    if attempt < max_attempts - 1:
                        continue
                pytest.fail(f"Worker A response missing marker: {result_a}")

            final_state = _pg_query(
                topology, f"SELECT state FROM message_receipts "
                f"WHERE tenant_id='tenant_default' AND message_id='{msg_id}'")
            assert final_state == "completed"
            return

        pytest.fail(f"Concurrent test failed after {max_attempts} attempts")

    def test_replay_after_completion_no_extra_calls(self, topology: ServiceTopology):
        """After completion, replay returns cached response with no model calls."""
        from .service_topology import complete_model_seed

        def send(message_id: str, session_id: str) -> dict:
            return http_post_json(
                f"{topology.gateway_url}/api/chat",
                {
                    "session_id": session_id,
                    "message_id": message_id,
                    "message": "Reply with exactly: E2E_REPLAY"
                },
                headers={"X-Tenant-ID": "tenant_default"},
                timeout=60,
            )

        msg_id, session_id, resp1 = complete_model_seed(send)
        assert "E2E_REPLAY" in resp1.get("response", ""), f"First request failed: {resp1}"

        which = topology.which_worker_handled(session_id)
        assert which is not None, f"No worker handled session {session_id}"
        exec_count_before = topology.count_session_in_log(which, session_id)

        resp2 = http_post_json(
            f"{topology.gateway_url}/api/chat",
            {
                "session_id": session_id,
                "message_id": msg_id,
                "message": "Reply with exactly: E2E_REPLAY"
            },
            headers={"X-Tenant-ID": "tenant_default"},
            timeout=60,
        )
        assert "E2E_REPLAY" in resp2.get("response", ""), f"Replay failed: {resp2}"

        exec_count_after = topology.count_session_in_log(which, session_id)
        assert exec_count_after == exec_count_before, (
            f"Replay caused extra execution: {exec_count_before} → {exec_count_after}")

        audit_count = int(
            _pg_query(
                topology, f"SELECT COUNT(*) FROM message_audit_events "
                f"WHERE tenant_id='tenant_default' AND message_id='{msg_id}'"))
        assert audit_count == 2, f"Expected 2 audit events (accepted+completed), got {audit_count}"

    def test_different_body_returns_conflict(self, topology: ServiceTopology):
        """Same message_id but different body returns conflict text."""
        from .service_topology import complete_model_seed

        def send(message_id: str, session_id: str) -> dict:
            return http_post_json(
                f"{topology.gateway_url}/api/chat",
                {
                    "session_id": session_id,
                    "message_id": message_id,
                    "message": "Reply with exactly: E2E_CONFLICT"
                },
                headers={"X-Tenant-ID": "tenant_default"},
                timeout=60,
            )

        successful_msg_id, successful_session_id, _ = complete_model_seed(send)

        # Now send different message with same message_id to trigger conflict
        resp2 = http_post_json(
            f"{topology.gateway_url}/api/chat",
            {
                "session_id": successful_session_id,
                "message_id": successful_msg_id,
                "message": "completely different message"
            },
            headers={"X-Tenant-ID": "tenant_default"},
            timeout=60,
        )
        expected = "This message identifier conflicts with a different message."
        assert resp2.get("response") == expected, (f"Expected conflict text, got: {resp2.get('response')!r}")

    def test_stream_replay_delta_and_done_only(self, topology: ServiceTopology):
        """SSE replay for completed receipt: exactly one delta + done, no tool."""
        from .service_topology import complete_model_seed

        def send(message_id: str, session_id: str) -> dict:
            return http_post_json(
                f"{topology.gateway_url}/api/chat",
                {
                    "session_id": session_id,
                    "message_id": message_id,
                    "message": "Reply with exactly: E2E_SSE"
                },
                headers={"X-Tenant-ID": "tenant_default"},
                timeout=60,
            )

        msg_id, session_id, resp1 = complete_model_seed(send)
        assert "E2E_SSE" in resp1.get("response", ""), f"First request failed: {resp1}"

        sse_raw = _http_get_sse(
            f"{topology.gateway_url}/api/chat/stream",
            {
                "session_id": session_id,
                "message_id": msg_id,
                "message": "Reply with exactly: E2E_SSE"
            },
            headers={"X-Tenant-ID": "tenant_default"},
            timeout=30,
        )

        events = _parse_sse_events(sse_raw)
        event_types = [e["type"] for e in events]

        delta_count = event_types.count("delta")
        done_count = event_types.count("done")
        tool_count = event_types.count("tool")
        error_count = event_types.count("error")

        assert delta_count == 1, f"Expected exactly 1 delta in replay, got {delta_count}"
        assert done_count == 1, f"Expected exactly 1 done, got {done_count}"
        assert tool_count == 0, f"Expected 0 tool events in replay, got {tool_count}"
        assert error_count == 0, f"Expected 0 error events in replay, got {error_count}"

        delta_data = " ".join(e.get("data", "") for e in events if e["type"] == "delta")
        assert "E2E_SSE" in delta_data, f"SSE replay delta missing marker: {delta_data!r}"

    def test_sigkill_processing_receipt_not_retried(self, topology: ServiceTopology):
        """After SIGKILL, processing receipt stays processing, no auto-retry."""
        msg_id = f"msg-4c-e2e-kill-{uuid.uuid4().hex[:8]}"
        session_id = f"sess-4c-e2e-kill-{uuid.uuid4().hex[:8]}"

        result_holder: dict = {}

        def send_slow_request():
            result_holder.update(
                http_post_json(
                    f"{topology.gateway_url}/api/chat",
                    {
                        "session_id": session_id,
                        "message_id": msg_id,
                        "message": "Count from 1 to 50 slowly, one number per line",
                    },
                    headers={"X-Tenant-ID": "tenant_default"},
                    timeout=120,
                ))

        t = threading.Thread(target=send_slow_request)
        t.start()

        deadline = time.monotonic() + 20
        got_processing = False
        while time.monotonic() < deadline:
            state = _pg_query(
                topology, f"SELECT state FROM message_receipts "
                f"WHERE tenant_id='tenant_default' AND message_id='{msg_id}'")
            if state == "processing":
                got_processing = True
                break
            time.sleep(0.3)

        if not got_processing:
            t.join(timeout=120)
            final_state = _pg_query(
                topology, f"SELECT state FROM message_receipts "
                f"WHERE tenant_id='tenant_default' AND message_id='{msg_id}'")
            pytest.fail(f"SIGKILL test could not catch processing state within 20s. "
                        f"Final receipt state: {final_state!r}. "
                        f"Model may have responded too fast or request failed: {result_holder}")

        which = topology.which_worker_handled(session_id)
        assert which is not None, "No worker picked up the session"

        audit_before = _pg_query(
            topology, f"SELECT COUNT(*) FROM message_audit_events "
            f"WHERE tenant_id='tenant_default' AND message_id='{msg_id}'")

        topology.kill_worker(which)

        t.join(timeout=30)

        state_after_kill = _pg_query(
            topology, f"SELECT state FROM message_receipts "
            f"WHERE tenant_id='tenant_default' AND message_id='{msg_id}'")
        assert state_after_kill == "processing", (
            f"Receipt should still be processing after SIGKILL, got: {state_after_kill}")

        topology.restart_worker(which)

        resp_retry = _worker_chat(
            topology, which, {
                "protocol_version": 1,
                "request_id": str(uuid.uuid4()),
                "tenant_id": "tenant_default",
                "app_id": "app_demo",
                "config_version": 2,
                "user_id": "user_default",
                "channel": "web",
                "session_id": session_id,
                "message_id": msg_id,
                "message": "Count from 1 to 50 slowly, one number per line",
            })
        assert resp_retry.get("error_code") == "message_in_progress", (
            f"After SIGKILL, retry should return message_in_progress, got: {resp_retry}")

        audit_after = _pg_query(
            topology, f"SELECT COUNT(*) FROM message_audit_events "
            f"WHERE tenant_id='tenant_default' AND message_id='{msg_id}'")
        assert audit_after == audit_before, (f"SIGKILL caused extra audit events: {audit_before} → {audit_after}")


def _worker_chat(topology: ServiceTopology, which: str, task: dict) -> dict:
    url = topology.worker_a_url if which == "a" else topology.worker_b_url
    body = json.dumps(task).encode()
    req = urllib.request.Request(f"{url}/internal/v1/chat", data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-TRPC-Internal-Token", topology._internal_token)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        return {"error": str(exc)}


def _http_get_sse(url: str, data: dict, headers: dict | None = None, timeout: float = 30.0) -> str:
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode()
    except Exception:
        return ""


def _parse_sse_events(raw: str) -> list[dict]:
    events = []
    for line in raw.splitlines():
        if line.startswith("data: "):
            try:
                events.append(json.loads(line[6:]))
            except json.JSONDecodeError:
                pass
    return events
