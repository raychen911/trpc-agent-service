from __future__ import annotations

import json
import socket
import threading
from pathlib import Path

import httpx
import pytest

from deploy.ambiguous_provider.server import AmbiguousProviderServer
from scripts.ambiguous_provider_acceptance import (
    STAGE_MARKERS,
    _child_report,
    run_acceptance,
)
from scripts.ambiguous_provider_acceptance import (
    main as acceptance_main,
)


def _serve(ledger: Path):
    server = AmbiguousProviderServer(("127.0.0.1", 0), ledger)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_address[1]}"


def _stop(server: AmbiguousProviderServer, thread: threading.Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


@pytest.mark.asyncio
async def test_response_drop_commits_query_and_idempotent_manual_replay(tmp_path: Path) -> None:
    server, thread, url = _serve(tmp_path / "provider.sqlite3")
    try:
        result = await run_acceptance(url)
    finally:
        _stop(server, thread)

    assert result["status"] == "pass"
    assert result["first_attempt_status"] == "ambiguous"
    assert result["first_attempt_provider_code"] == "transport_unknown"
    assert result["first_ledger_status"] == "accepted"
    assert result["first_ledger_receive_count"] == 1
    assert result["manual_confirmation_required"] is True
    assert result["automatic_replay_count"] == 0
    assert result["confirmed_replay_status"] == "pass"
    assert result["final_ledger_receive_count"] == 2
    assert result["provider_ledger"] == {
        "accepted_count": 1,
        "delivery_attempt_count": 2,
        "duplicate_replay_count": 1,
        "side_effect_count": 1,
    }
    assert [marker["name"] for marker in result["stage_markers"]] == list(STAGE_MARKERS)
    assert all(marker["status"] == "pass" for marker in result["stage_markers"])


def test_same_uuid_different_body_is_rejected_without_new_side_effect(tmp_path: Path) -> None:
    server, thread, url = _serve(tmp_path / "provider.sqlite3")
    try:
        with httpx.Client(timeout=2) as client:
            try:
                client.post(
                    f"{url}/v1/send",
                    json={"outbound_id": "ambiguous-id", "body": "first"},
                )
            except httpx.TransportError:
                # The first accepted request deliberately has no HTTP response.
                pass
            else:
                pytest.fail("the first provider response was not dropped")
    except httpx.TransportError:
        # Some httpx versions wrap the transport error at the client context.
        pass
    finally:
        # The server remains live for the query/conflict assertions below.
        pass
    try:
        with httpx.Client(timeout=2) as client:
            state = client.get(f"{url}/state/ambiguous-id")
            assert state.status_code == 200
            assert state.json()["receive_count"] == 1
            conflict = client.post(
                f"{url}/v1/send",
                json={"outbound_id": "ambiguous-id", "body": "different"},
            )
            assert conflict.status_code == 409
            assert client.get(f"{url}/state/ambiguous-id").json()["receive_count"] == 1
    finally:
        _stop(server, thread)


def test_auth_endpoint_waits_for_complete_request_body(tmp_path: Path) -> None:
    server, thread, _url = _serve(tmp_path / "provider.sqlite3")
    body = json.dumps({"app_id": "acceptance", "app_secret": "test-only"}).encode()
    headers = (
        "POST /open-apis/auth/v3/tenant_access_token/internal HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    try:
        with socket.create_connection(server.server_address, timeout=2) as client:
            client.sendall(headers + body[:1])
            client.settimeout(0.1)
            with pytest.raises((TimeoutError, socket.timeout)):
                client.recv(1)
            client.settimeout(2)
            client.sendall(body[1:])
            response = bytearray()
            while chunk := client.recv(4096):
                response.extend(chunk)
        assert b" 200 " in response.split(b"\r\n", 1)[0]
    finally:
        _stop(server, thread)


def test_child_report_is_bound_and_contains_no_body_or_secret() -> None:
    result = {
        "status": "pass",
        "mode": "real_ambiguous_provider_endpoint",
        "manual_confirmation_required": True,
        "automatic_replay_count": 0,
        "confirmed_replay_status": "pass",
        "provider_ledger": {
            "accepted_count": 1,
            "side_effect_count": 1,
        },
        "stage_markers": [
            {"name": name, "status": "pass", "observed_at": "2026-08-25T00:00:00Z"}
            for name in STAGE_MARKERS
        ],
    }
    report = _child_report(
        result,
        run_id="ambiguous-run",
        nonce_sha256="a" * 64,
        started_at="2026-08-25T00:00:00Z",
        ended_at="2026-08-25T00:00:01Z",
    )
    rendered = json.dumps(report, ensure_ascii=True)
    assert report["run_id"] == "ambiguous-run"
    assert report["run_nonce_sha256"] == "a" * 64
    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert report["candidate"]["ambiguous"]["manual_confirmation_required"] is True
    assert "ambiguous provider acceptance" not in rendered
    assert "ambiguous-test-secret" not in rendered
    assert "DSN" not in rendered


def test_acceptance_cli_auto_starts_and_stops_local_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for name in (
        "TRPC_FAULT_AMBIGUOUS_PROVIDER_URL",
        "TRPC_REAL_AMBIGUOUS_PROVIDER_URL",
        "TRPC_REAL_DATABASE_DSN",
        "TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN",
        "TRPC_REAL_TENANT_ID",
        "TRPC_REAL_BINDING_ID",
        "TRPC_REAL_RUN_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    output = tmp_path / "ambiguous-child.json"

    assert acceptance_main(["--timeout-seconds", "3", "--output", str(output)]) == 0
    capsys.readouterr()
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert report["candidate"]["ambiguous"]["provider_ledger"]["side_effect_count"] == 1
