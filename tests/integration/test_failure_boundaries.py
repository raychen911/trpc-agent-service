"""Regression tests for untrusted input and pre-execution audit failures."""
import json
import asyncio

import httpx
import pytest

from trpc_service.audit.models import PreAuthScope
from tests.contract.test_local_message_http_contract import _signed_headers
from tests.support import FIXED_UTC, inbound_message_data
from trpc_service.channels.contracts import InboundMessage
from trpc_service.web.app import build_runtime, create_app


@pytest.mark.parametrize("field", ["binding_id", "received_at", "trace_id"])
async def test_client_cannot_supply_server_owned_fields(runtime_secret_env, field):
    app = create_app(runtime_secret_env, now=lambda: FIXED_UTC)
    body = inbound_message_data()
    for name in ("binding_id", "received_at", "trace_id"):
        body.pop(name)
    body[field] = "untrusted"
    raw = json.dumps(body).encode()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/local/messages", content=raw, headers=_signed_headers(raw, runtime_secret_env["TRPC_DEMO_ALPHA_SECRET"]))
    assert response.status_code == 400
    assert response.json()["error"]["execution_started"] is False


async def test_execution_start_audit_failure_does_not_strand_claim(runtime_secret_env):
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    runtime.adapters.audit.fail_on_append_number = 2
    message = InboundMessage(**inbound_message_data(external_message_id="start-audit"))
    first = await runtime.gateway.handle_verified_message_for_test(message)
    assert first.error.code == "audit_unavailable"
    assert first.error.retryable and not first.error.execution_started
    assert runtime.worker.call_count == 0
    recovered = await runtime.gateway.handle_verified_message_for_test(message)
    assert recovered.status.value == "succeeded"
    assert runtime.worker.call_count == 1
    await runtime.close()


async def test_cancelled_prepare_can_retry(runtime_secret_env, monkeypatch):
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    original = runtime.worker.prepare
    entered = asyncio.Event()

    async def wait_prepare(*args):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runtime.worker, "prepare", wait_prepare)
    message = InboundMessage(**inbound_message_data(external_message_id="cancel-prepare"))
    task = asyncio.create_task(runtime.gateway.handle_verified_message_for_test(message))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    monkeypatch.setattr(runtime.worker, "prepare", original)
    recovered = await runtime.gateway.handle_verified_message_for_test(message)
    assert recovered.status.value == "succeeded"
    await runtime.close()


async def test_cancelled_execution_is_nonreplayable(runtime_secret_env, monkeypatch):
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    entered = asyncio.Event()
    calls = 0

    class Prepared:
        async def execute(self, **kwargs):
            nonlocal calls
            calls += 1
            entered.set()
            await asyncio.Event().wait()

    async def prepare(*args):
        return Prepared()

    monkeypatch.setattr(runtime.worker, "prepare", prepare)
    message = InboundMessage(**inbound_message_data(external_message_id="cancel-execute"))
    task = asyncio.create_task(runtime.gateway.handle_verified_message_for_test(message))
    await entered.wait()
    task.cancel()
    reply = await task
    cached = await runtime.gateway.handle_verified_message_for_test(message)
    assert reply.error.code == "outcome_unknown" and not reply.error.retryable
    assert cached.error.code == "outcome_unknown" and cached.delivery_action.value == "suppress"
    assert calls == 1
    await runtime.close()


async def test_duplicate_and_conflict_count_requests_not_deliveries(runtime_secret_env):
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    message = InboundMessage(**inbound_message_data())
    await runtime.gateway.handle_verified_message_for_test(message)
    await runtime.gateway.handle_verified_message_for_test(message)
    await runtime.gateway.handle_verified_message_for_test(message.model_copy(update={"text": "different"}))
    snapshot = runtime.metrics.snapshot(runtime.tenant_scope("tenant-alpha"))
    assert snapshot.request_count == 3
    assert snapshot.error_count == 1
    assert snapshot.channel_delivery_count == 1
    await runtime.close()


async def test_rejected_http_requests_are_preauth_audited_and_metered(runtime_secret_env):
    app = create_app(runtime_secret_env, now=lambda: FIXED_UTC)
    body = {key: value for key, value in inbound_message_data().items() if key not in {"binding_id", "received_at", "trace_id"}}
    raw = json.dumps(body).encode()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        invalid = await client.post("/v1/local/messages", content=b"{}")
        unauthorized = await client.post(
            "/v1/local/messages",
            content=raw,
            headers={**_signed_headers(raw, "incorrect-runtime-value"), "x-signature": "v1=" + "0" * 64},
        )
    records = await app.state.runtime.adapters.audit.list_preauth(PreAuthScope())
    snapshot = app.state.runtime.metrics.snapshot(PreAuthScope())
    assert invalid.status_code == 400 and unauthorized.status_code == 401
    assert [record.decision.value for record in records] == ["invalid_request", "unauthorized"]
    assert all(record.tenant_id is None and record.user_id is None for record in records)
    assert snapshot.request_count == 2 and snapshot.error_count == 2
    await app.state.runtime.close()


async def test_terminal_persistence_uncertainty_is_cached_without_reexecution(runtime_secret_env):
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    runtime.adapters.idempotency.fail_complete_once = True
    message = InboundMessage(**inbound_message_data(external_message_id="uncertain-commit"))
    first = await runtime.gateway.handle_verified_message_for_test(message)
    cached = await runtime.gateway.handle_verified_message_for_test(message)
    assert first.error.code == "outcome_unknown" and first.error.execution_started
    assert cached.error.code == "outcome_unknown" and cached.delivery_action.value == "suppress"
    assert runtime.worker.call_count == 1
    await runtime.close()


async def test_metrics_failure_does_not_change_auth_rejection(runtime_secret_env):
    app = create_app(runtime_secret_env, now=lambda: FIXED_UTC)
    app.state.runtime.metrics.fail_record = True
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/local/messages", content=b"{}")
    assert response.status_code == 400 and response.json()["error"]["code"] == "invalid_request"
    assert app.state.runtime.metrics.operational_events == [
        {"event": "metrics_incomplete", "trace_id": response.json()["trace_id"]}
    ]
    await app.state.runtime.close()
