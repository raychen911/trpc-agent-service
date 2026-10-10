from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

import scripts.im_resilience_gate as im_gate
from scripts.im_resilience_gate import RETRY_AFTER_SECONDS, _repository, _run
from trpc_service.channels.envelopes import DeliveryReceipt, DeliveryStatus, OutboundEnvelope
from trpc_service.channels.feishu import FeishuAdapter, _retry_after_seconds
from trpc_service.channels.wecom import WeComConnector
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.storage.models import OutboxRecord
from trpc_service.tenant.models import Channel, ChannelBinding


def test_documented_direct_invocation_resolves_scripts_package() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/im_resilience_gate.py", "--help"],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "usage: im_resilience_gate.py" in result.stdout


def test_retry_after_accepts_seconds_and_http_date() -> None:
    numeric = httpx.Response(
        429,
        headers={"Retry-After": "0.25"},
        request=httpx.Request("POST", "https://example.test"),
    )
    assert _retry_after_seconds(numeric) == 0.25

    date = httpx.Response(
        429,
        headers={"Retry-After": "Thu, 01 Jan 1970 00:00:02 GMT"},
        request=httpx.Request("POST", "https://example.test"),
    )
    assert _retry_after_seconds(date, now=0) == 2.0

    body = httpx.Response(
        429,
        json={"code": 99991400, "error": {"retry_after": "0.5"}},
        request=httpx.Request("POST", "https://example.test"),
    )
    assert _retry_after_seconds(body) == 0.5


def test_retry_after_receipt_is_bounded() -> None:
    receipt = DeliveryReceipt(
        outbound_id="outbound",
        status=DeliveryStatus.FAILED,
        retryable=True,
        retry_after_seconds=RETRY_AFTER_SECONDS,
    )
    assert receipt.retry_after_seconds is not None
    assert receipt.retry_after_seconds < 3600


@pytest.mark.asyncio
async def test_offline_resilience_gate_passes_but_never_upgrades_production() -> None:
    report = await _run()
    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert report["candidate"]["credentials_used"] is False
    cases = report["candidate"]["cases"]
    assert cases["duplicate_and_delayed_callback"]["unique_inbound_records"] == 1
    assert cases["feishu_429_retry_after"]["recovery_status"] == "delivered"
    assert cases["feishu_429_retry_after"]["retry_after_honored"] is True
    assert cases["wecom_45009_retry_after"]["provider_error_code"] == 45009
    assert cases["wecom_45009_retry_after"]["recovery_status"] == "delivered"
    assert cases["wecom_45009_retry_after"]["retry_after_honored"] is True
    assert cases["wecom_disconnect_lock_takeover"]["old_lock_owner_released"] is True
    assert cases["wecom_disconnect_lock_takeover"]["new_lock_owner_acquired"] is True
    assert cases["long_provider_outage_recovery"]["final_outbox_state"] == "published"
    assert cases["ambiguous_send_requires_manual_replay"]["automatic_replays"] == 0
    assert json.dumps(report, ensure_ascii=False).find("offline acceptance payload") == -1


@pytest.mark.asyncio
async def test_offline_report_binds_current_source_and_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id = "release-20260824-171222-41e6e62a"
    release_nonce = "local-offline-20260824-171222-41e6e62a-7f2d9c4b6a1e"
    monkeypatch.setenv("TRPC_RELEASE_ID", release_id)
    monkeypatch.setenv("TRPC_RELEASE_NONCE", release_nonce)

    report = await _run()

    evidence = report["evidence"]
    assert evidence["kind"] == "current_candidate"
    assert evidence["producer"] == "scripts.im_resilience_gate"
    assert evidence["source_fingerprint"]["status"] == "available"
    assert evidence["release_binding"] == {
        "release_id": release_id,
        "nonce_sha256": hashlib.sha256(release_nonce.encode("utf-8")).hexdigest(),
    }


def test_cli_offline_entry_rejects_missing_current_release_binding(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRPC_RELEASE_ID", raising=False)
    monkeypatch.delenv("TRPC_RELEASE_NONCE", raising=False)
    output = tmp_path / "im-resilience.json"
    monkeypatch.setattr(
        "sys.argv",
        ["im_resilience_gate.py", "--output", str(output)],
    )

    assert im_gate.main() == 1
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "not_run"
    assert any(
        "TRPC_RELEASE_ID and TRPC_RELEASE_NONCE" in reason for reason in report["rejection_reasons"]
    )


class _WeComClient:
    is_connected = True
    is_authenticated = True

    def __init__(self, response: object = None, error: BaseException | None = None) -> None:
        self.response = response
        self.error = error

    async def send_message(self, _target: str, _body: object) -> object:
        if self.error is not None:
            raise self.error
        return self.response


def _feishu_outbound() -> tuple[OutboundEnvelope, ChannelBinding]:
    binding = ChannelBinding(
        binding_id="im-resilience-feishu-binding",
        tenant_id="im-resilience-tenant",
        app_id="im-resilience-app",
        channel=Channel.FEISHU,
        account_id="im-resilience-account",
        secret_refs={"app_secret": SecretRef(uri="literal://offline-app-secret")},
    )
    envelope = OutboundEnvelope(
        outbound_id=str(uuid4()),
        tenant_id=binding.tenant_id,
        binding_id=binding.binding_id,
        channel=binding.channel,
        target_id="offline-user",
        session_id="offline-session",
        text="offline delivery",
    )
    return envelope, binding


@pytest.mark.asyncio
async def test_feishu_transport_error_is_ambiguous_and_invalid_2xx_is_ambiguous() -> None:
    calls = 0

    def transport_error(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
                request=request,
            )
        raise httpx.RemoteProtocolError("connection closed", request=request)

    envelope, binding = _feishu_outbound()
    client = httpx.AsyncClient(transport=httpx.MockTransport(transport_error))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    try:
        transport_receipt = await adapter.send(envelope, binding)
    finally:
        await client.aclose()
    assert transport_receipt.status == DeliveryStatus.AMBIGUOUS
    assert transport_receipt.provider_code == "transport_unknown"

    def invalid_response(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/tenant_access_token/internal"):
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
                request=request,
            )
        return httpx.Response(200, content=b"not-json", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(invalid_response))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    try:
        invalid_receipt = await adapter.send(envelope, binding)
    finally:
        await client.aclose()
    assert invalid_receipt.status == DeliveryStatus.AMBIGUOUS
    assert invalid_receipt.provider_code == "invalid_response"


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [{}, {"data": {}}, {"code": "invalid"}])
async def test_feishu_2xx_missing_or_invalid_code_is_ambiguous(
    payload: dict[str, object],
) -> None:
    def response_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/tenant_access_token/internal"):
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
                request=request,
            )
        return httpx.Response(200, json=payload, request=request)

    envelope, binding = _feishu_outbound()
    client = httpx.AsyncClient(transport=httpx.MockTransport(response_handler))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    try:
        receipt = await adapter.send(envelope, binding)
    finally:
        await client.aclose()
    assert receipt.status == DeliveryStatus.AMBIGUOUS
    assert receipt.provider_code == "invalid_response"


@pytest.mark.asyncio
async def test_feishu_token_transport_error_is_retryable_failure() -> None:
    def transport_error(request: httpx.Request) -> httpx.Response:
        raise httpx.RemoteProtocolError("connection closed", request=request)

    envelope, binding = _feishu_outbound()
    client = httpx.AsyncClient(transport=httpx.MockTransport(transport_error))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    try:
        receipt = await adapter.send(envelope, binding)
    finally:
        await client.aclose()
    assert receipt.status == DeliveryStatus.FAILED
    assert receipt.provider_code == "token_transport"
    assert receipt.retryable


def _wecom_outbound() -> tuple[OutboundEnvelope, ChannelBinding]:
    _repository_value, _runtime, binding, _inbound = _repository()
    binding = binding.model_copy(
        update={"binding_id": "im-resilience-wecom-binding", "channel": Channel.WECOM_AI_BOT}
    )
    return (
        OutboundEnvelope(
            outbound_id=str(uuid4()),
            tenant_id=binding.tenant_id,
            binding_id=binding.binding_id,
            channel=Channel.WECOM_AI_BOT,
            target_id="offline-user",
            session_id="offline-session",
            text="offline delivery",
        ),
        binding,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "status", "provider_code", "retryable"),
    [
        (
            SimpleNamespace(errcode=45009, retry_after=RETRY_AFTER_SECONDS),
            DeliveryStatus.FAILED,
            "rate_limited",
            True,
        ),
        (SimpleNamespace(errcode=40001), DeliveryStatus.FAILED, "ack_40001", False),
        (
            SimpleNamespace(errcode=0, headers={"req_id": "req"}),
            DeliveryStatus.DELIVERED,
            None,
            False,
        ),
        (object(), DeliveryStatus.AMBIGUOUS, "response_unknown", False),
    ],
)
async def test_wecom_ack_and_unknown_response_mapping(
    response: object,
    status: DeliveryStatus,
    provider_code: str | None,
    retryable: bool,
) -> None:
    envelope, binding = _wecom_outbound()
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        SimpleNamespace(),
        owner_id="offline",
    )
    connector._clients[binding.binding_id] = _WeComClient(response=response)
    connector._fenced_bindings.add(binding.binding_id)
    receipt = await connector.send(envelope, binding)
    assert receipt.status == status
    assert receipt.provider_code == provider_code
    assert receipt.retryable is retryable


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "status", "provider_code"),
    [
        (
            RuntimeError("Reply ack error: errcode=40001, errmsg=denied"),
            DeliveryStatus.FAILED,
            "ack_40001",
        ),
        (RuntimeError("socket closed"), DeliveryStatus.AMBIGUOUS, "runtime_unknown"),
        (ValueError("unknown sdk result"), DeliveryStatus.AMBIGUOUS, "runtime_unknown"),
    ],
)
async def test_wecom_sdk_exception_never_becomes_delivered(
    error: BaseException,
    status: DeliveryStatus,
    provider_code: str,
) -> None:
    envelope, binding = _wecom_outbound()
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        SimpleNamespace(),
        owner_id="offline",
    )
    connector._clients[binding.binding_id] = _WeComClient(error=error)
    connector._fenced_bindings.add(binding.binding_id)
    receipt = await connector.send(envelope, binding)
    assert (receipt.status, receipt.provider_code) == (status, provider_code)


@pytest.mark.asyncio
async def test_inmemory_outbox_attempts_persist_across_reclaim() -> None:
    repository, _runtime, binding, _inbound = _repository()
    record = OutboxRecord(
        outbox_id=str(uuid4()),
        tenant_id=binding.tenant_id,
        event_type="outbound.feishu.ready",
        aggregate_id=str(uuid4()),
        payload={"channel": "feishu"},
    )
    repository._outbox[record.outbox_id] = record
    first = await repository.claim_outbox(
        event_type=record.event_type,
        owner_id="worker",
        limit=1,
        lease_for=timedelta(seconds=0),
    )
    await repository.release_outbox(
        binding.tenant_id,
        record.outbox_id,
        owner_id="worker",
        delay=timedelta(seconds=0),
        error_type="retry",
    )
    second = await repository.claim_outbox(
        event_type=record.event_type,
        owner_id="worker",
        limit=1,
        lease_for=timedelta(seconds=0),
    )
    assert first[0].attempts == 1
    assert second[0].attempts == 2
    assert repository._outbox[record.outbox_id].attempts == 2
