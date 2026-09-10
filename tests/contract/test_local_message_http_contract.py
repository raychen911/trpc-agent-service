from __future__ import annotations

import json

import httpx
import pytest
import secrets
from uuid import UUID

from trpc_service.channels import hmac_auth
from trpc_service.channels.contracts import DeliveryAction, OutboundReply, ReplyStatus
from trpc_service.channels.local_http import reply_envelope
from trpc_service.web.app import create_app
from trpc_service.storage.contracts import AgentExecutionFailed


def _signed_headers(raw: bytes, secret: str, binding: str = "binding-alpha") -> dict[str, str]:
    payload = json.loads(raw)
    timestamp = "1788595200"
    digest = __import__("hashlib").sha256(raw).hexdigest()
    return {
        "content-type": "application/json",
        "x-channel-binding": binding,
        "x-request-timestamp": timestamp,
        "x-signature": hmac_auth.sign_request(secret.encode(), hmac_auth.canonical_string(timestamp, binding, payload["external_message_id"], digest)),
        "x-trace-id": "11111111-1111-4111-8111-111111111111",
    }


def test_processing_envelope_explicitly_allows_safe_retry() -> None:
    reply = OutboundReply(
        status=ReplyStatus.PROCESSING,
        trace_id=UUID(int=1), original_trace_id=UUID(int=2),
        tenant_id="tenant-alpha", platform_session_id="sess_" + "a" * 64,
        external_message_id="message-001", delivery_action=DeliveryAction.NONE,
    )
    assert reply_envelope(reply)["data"]["retryable"] is True


async def test_health_and_valid_message_envelope(runtime_secret_env: dict[str, str]) -> None:
    app = create_app(runtime_secret_env, now=lambda: __import__("tests.support", fromlist=["FIXED_UTC"]).FIXED_UTC)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        health = await client.get("/healthz")
        body = {
            "channel": "local_http",
            "external_message_id": "message-001",
            "external_user_id": "user-001",
            "conversation_type": "direct",
            "external_conversation_id": "conversation-001",
            "text": "Remember validation token ALPHA.",
        }
        raw = json.dumps(body, separators=(",", ":")).encode()
        response = await client.post("/v1/local/messages", content=raw, headers=_signed_headers(raw, runtime_secret_env["TRPC_DEMO_ALPHA_SECRET"]))

    assert health.status_code == 200 and health.json() == {"status": "ok"}
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "succeeded"
    assert payload["data"]["delivery_action"] == "deliver"


async def test_http_validation_auth_and_conflict_have_stable_statuses(runtime_secret_env: dict[str, str]) -> None:
    app = create_app(runtime_secret_env, now=lambda: __import__("tests.support", fromlist=["FIXED_UTC"]).FIXED_UTC)
    transport = httpx.ASGITransport(app=app)
    good = {
        "channel": "local_http",
        "external_message_id": "contract-duplicate",
        "external_user_id": "user-001",
        "conversation_type": "direct",
        "external_conversation_id": "conversation-001",
        "text": "Remember validation token ALPHA.",
    }
    raw = json.dumps(good, separators=(",", ":")).encode()
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        invalid = await client.post("/v1/local/messages", content=b"{}")
        bad_auth = await client.post("/v1/local/messages", content=raw, headers={**_signed_headers(raw, "wrong"), "x-signature": "v1=" + "0" * 64})
        first = await client.post("/v1/local/messages", content=raw, headers=_signed_headers(raw, runtime_secret_env["TRPC_DEMO_ALPHA_SECRET"]))
        changed = {**good, "text": "Recall the validation token."}
        changed_raw = json.dumps(changed, separators=(",", ":")).encode()
        conflict = await client.post("/v1/local/messages", content=changed_raw, headers=_signed_headers(changed_raw, runtime_secret_env["TRPC_DEMO_ALPHA_SECRET"]))
    assert invalid.status_code == 400
    assert bad_auth.status_code == 401 and bad_auth.json()["error"]["code"] == "unauthorized"
    assert first.status_code == 200
    assert conflict.status_code == 409 and conflict.json()["error"]["code"] == "idempotency_conflict"


async def test_unknown_binding_invalid_trace_and_502_503_envelopes(runtime_secret_env, monkeypatch) -> None:
    body = {
        "channel": "local_http", "external_message_id": "matrix-001",
        "external_user_id": "user-001", "conversation_type": "direct",
        "external_conversation_id": "conversation-001", "text": "hello",
    }
    raw = json.dumps(body, separators=(",", ":")).encode()
    app = create_app(runtime_secret_env, now=lambda: __import__("tests.support", fromlist=["FIXED_UTC"]).FIXED_UTC)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        unknown = await client.post("/v1/local/messages", content=raw, headers=_signed_headers(raw, secrets.token_urlsafe(32), "unknown-binding"))
        bad_signature = await client.post("/v1/local/messages", content=raw, headers={**_signed_headers(raw, "wrong"), "x-signature": "v1=" + "0" * 64})
        valid_headers = {**_signed_headers(raw, runtime_secret_env["TRPC_DEMO_ALPHA_SECRET"]), "x-trace-id": "not-a-uuid"}
        invalid_trace = await client.post("/v1/local/messages", content=raw, headers=valid_headers)
    assert unknown.status_code == 401 and unknown.json() == bad_signature.json()
    assert invalid_trace.status_code == 200
    UUID(invalid_trace.json()["trace_id"])
    await app.state.runtime.close()

    class FailingPrepared:
        async def execute(self, **_kwargs):
            raise AgentExecutionFailed("private detail")
    failing_app = create_app(runtime_secret_env, now=lambda: __import__("tests.support", fromlist=["FIXED_UTC"]).FIXED_UTC)
    async def failing_prepare(*_args): return FailingPrepared()
    monkeypatch.setattr(failing_app.state.runtime.worker, "prepare", failing_prepare)
    failed_body = {**body, "external_message_id": "matrix-502"}
    failed_raw = json.dumps(failed_body, separators=(",", ":")).encode()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=failing_app), base_url="http://test") as client:
        failed = await client.post("/v1/local/messages", content=failed_raw, headers=_signed_headers(failed_raw, runtime_secret_env["TRPC_DEMO_ALPHA_SECRET"]))
    assert failed.status_code == 502 and failed.json()["error"]["code"] == "agent_failed"
    assert "private detail" not in failed.text
    await failing_app.state.runtime.close()

    audit_app = create_app(runtime_secret_env, now=lambda: __import__("tests.support", fromlist=["FIXED_UTC"]).FIXED_UTC)
    audit_app.state.runtime.adapters.audit.fail_append = True
    audit_body = {**body, "external_message_id": "matrix-503"}
    audit_raw = json.dumps(audit_body, separators=(",", ":")).encode()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=audit_app), base_url="http://test") as client:
        unavailable = await client.post("/v1/local/messages", content=audit_raw, headers=_signed_headers(audit_raw, runtime_secret_env["TRPC_DEMO_ALPHA_SECRET"]))
    assert unavailable.status_code == 503 and unavailable.json()["error"]["code"] == "audit_unavailable"
    await audit_app.state.runtime.close()


@pytest.mark.parametrize(
    "change",
    [
        {"unexpected": "unsigned-assumption"},
        {"text": "   "},
        {"text": "x" * 4001},
        {"external_user_id": "x" * 129},
    ],
)
async def test_signed_invalid_body_matrix_returns_safe_400(runtime_secret_env, change) -> None:
    app = create_app(runtime_secret_env, now=lambda: __import__("tests.support", fromlist=["FIXED_UTC"]).FIXED_UTC)
    body = {
        "channel": "local_http", "external_message_id": "invalid-matrix",
        "external_user_id": "user-001", "conversation_type": "direct",
        "external_conversation_id": "conversation-001", "text": "hello", **change,
    }
    raw = json.dumps(body, separators=(",", ":")).encode()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/local/messages", content=raw, headers=_signed_headers(raw, runtime_secret_env["TRPC_DEMO_ALPHA_SECRET"]))
    assert response.status_code == 400
    assert response.json()["error"] == {
        "code": "invalid_request", "message": "Request validation failed.",
        "retryable": False, "execution_started": False,
    }
    await app.state.runtime.close()
