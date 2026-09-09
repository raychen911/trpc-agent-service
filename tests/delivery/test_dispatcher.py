"""Security and outcome-boundary tests for fenced outbound delivery."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
from prometheus_client import REGISTRY
from pydantic import SecretStr

from trpc_service.delivery import DeliveryBinding, OutboxDispatcher, TextDeliveryPayload
from trpc_service.delivery.contracts import DispatchState
from trpc_service.reliability.types import (
    OutboxDeliveryClaim,
    OutboxDeliveryOutcome,
    ReplyCredentialRef,
)
from trpc_service.security import EnvelopeCipher
from trpc_service.security.secrets import SecretResolutionError

NOW = datetime(2026, 8, 29, 8, 0, tzinfo=UTC)
TOKEN = "123456:" + "a" * 35
TELEGRAM_ENDPOINT = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
WECOM_URL = "https://qyapi.weixin.qq.com/cgi-bin/aibot/response?response_code=SENSITIVE_ROUTE"


class FakeRepository:
    """Record repository interactions without inspecting credential plaintext."""

    def __init__(
        self,
        claim: OutboxDeliveryClaim | None,
        *,
        renew_results: list[bool] | None = None,
    ) -> None:
        self.claim = claim
        self.renew_results = renew_results or [True] * 10
        self.renew_count = 0
        self.records: list[dict[str, Any]] = []

    async def claim_outbox(
        self,
        tenant_id: str,
        dispatcher_id: str,
    ) -> OutboxDeliveryClaim | None:
        assert tenant_id == "tenant-a"
        assert dispatcher_id == "dispatcher-1"
        return self.claim

    async def renew_outbox_claim(self, claim: OutboxDeliveryClaim) -> bool:
        assert claim is self.claim
        result = self.renew_results[self.renew_count]
        self.renew_count += 1
        return result

    async def record_delivery(
        self,
        claim: OutboxDeliveryClaim,
        outcome: OutboxDeliveryOutcome,
        *,
        external_message_id: str | None = None,
        error_type: str | None = None,
        next_retry_at: datetime | None = None,
    ) -> bool:
        assert claim is self.claim
        self.records.append(
            {
                "outcome": outcome,
                "external_message_id": external_message_id,
                "error_type": error_type,
                "next_retry_at": next_retry_at,
            }
        )
        return True


class FakeBindings:
    def __init__(
        self,
        channel_type: str,
        *,
        tenant_id: str = "tenant-a",
        status: str = "active",
    ) -> None:
        self.channel_type = channel_type
        self.tenant_id = tenant_id
        self.status = status

    async def load(self, tenant_id: str, binding_id: str) -> DeliveryBinding:
        return DeliveryBinding(
            tenant_id=self.tenant_id,
            binding_id=binding_id,
            channel_type=self.channel_type,
            status=self.status,
            secret_refs={"bot_token": "secret://test/telegram"},
        )


class FailingBindings:
    async def load(self, tenant_id: str, binding_id: str) -> DeliveryBinding:
        del tenant_id, binding_id
        raise RuntimeError("must not escape")


class FakeSecrets:
    def resolve(self, reference: str) -> SecretStr:
        assert reference == "secret://test/telegram"
        return SecretStr(TOKEN)


class FailingSecrets:
    def resolve(self, reference: str) -> SecretStr:
        del reference
        raise SecretResolutionError("secret unavailable")


class InvalidTokenSecrets:
    def resolve(self, reference: str) -> SecretStr:
        del reference
        return SecretStr("unsafe/token")


def _claim(
    channel_type: str,
    plaintext_route: str,
    *,
    payload: dict[str, Any] | None = None,
) -> OutboxDeliveryClaim:
    credential_kind = {
        "telegram": "telegram_delivery_context",
        "wecom": "wecom_response_url",
    }[channel_type]
    cipher = EnvelopeCipher("root-key-" * 4)
    ciphertext = cipher.encrypt(
        plaintext_route,
        context={
            "tenant_id": "tenant-a",
            "binding_id": "binding-a",
            "delivery_id": "delivery-a",
            "credential_kind": credential_kind,
        },
    )
    credential = ReplyCredentialRef(
        credential_id="credential-a",
        credential_kind=credential_kind,
        ciphertext=ciphertext,
        ciphertext_hash="a" * 64,
        expires_at=NOW + timedelta(hours=1),
    )
    return OutboxDeliveryClaim(
        tenant_id="tenant-a",
        outbox_id="outbox-a",
        run_id="run-a",
        binding_id="binding-a",
        session_id="session-a",
        delivery_id="delivery-a",
        reply_id="reply-a",
        part_no=0,
        payload=payload or {"schema_version": 1, "kind": "text", "text": "Agent answer"},
        payload_hash="b" * 64,
        dispatcher_id="dispatcher-1",
        delivery_token="opaque-fence-a",  # noqa: S106 - internal fencing token, not a secret
        attempt_no=1,
        claim_expires_at=NOW + timedelta(seconds=20),
        reply_credential=credential,
    )


def _telegram_route(**overrides: Any) -> str:
    value: dict[str, Any] = {
        "chat_id": -100123456,
        "message_thread_id": 42,
        "reply_to_message_id": 77,
    }
    value.update(overrides)
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _dispatcher(
    repository: FakeRepository,
    channel_type: str,
    client: httpx.AsyncClient,
    *,
    sleeper: Any | None = None,
    attempts: int = 3,
) -> OutboxDispatcher:
    async def no_sleep(delay: float) -> None:
        del delay

    return OutboxDispatcher(
        repository,
        FakeBindings(channel_type),
        FakeSecrets(),
        EnvelopeCipher("root-key-" * 4),
        client,
        dispatcher_id="dispatcher-1",
        clock=lambda: NOW,
        sleeper=sleeper or no_sleep,
        telegram_transport_attempts=attempts,
        base_retry_delay=timedelta(seconds=5),
    )


@pytest.mark.asyncio
async def test_no_work_does_not_touch_delivery_state() -> None:
    repository = FakeRepository(None)
    async with httpx.AsyncClient() as client:
        report = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert report.state is DispatchState.NO_WORK
    assert repository.records == []
    assert repository.renew_count == 0


@pytest.mark.asyncio
async def test_dispatcher_constructor_and_payload_validators_fail_closed() -> None:
    repository = FakeRepository(None)
    async with httpx.AsyncClient() as client:
        common = (
            repository,
            FakeBindings("telegram"),
            FakeSecrets(),
            EnvelopeCipher("root-key-" * 4),
            client,
        )
        with pytest.raises(ValueError, match="dispatcher_id"):
            OutboxDispatcher(*common, dispatcher_id="")
        with pytest.raises(ValueError, match="transport_attempts"):
            OutboxDispatcher(*common, dispatcher_id="ok", telegram_transport_attempts=0)
        with pytest.raises(ValueError, match="base_retry_delay"):
            OutboxDispatcher(*common, dispatcher_id="ok", base_retry_delay=timedelta(0))
        with pytest.raises(ValueError, match="wecom_allowed_hosts"):
            OutboxDispatcher(*common, dispatcher_id="ok", wecom_allowed_hosts=frozenset())
    with pytest.raises(ValueError, match="text must not be blank"):
        TextDeliveryPayload.model_validate(
            {"schema_version": 1, "kind": "text", "text": "   "},
            strict=True,
        )


@pytest.mark.asyncio
@respx.mock
async def test_telegram_success_uses_thread_and_reply_without_secret_logging(caplog: Any) -> None:
    claim = _claim("telegram", _telegram_route())
    repository = FakeRepository(claim)
    route = respx.post(TELEGRAM_ENDPOINT).mock(
        return_value=httpx.Response(200, json={"ok": True, "result": {"message_id": 9001}})
    )

    async with httpx.AsyncClient() as client:
        report = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")

    assert report.outcome is OutboxDeliveryOutcome.SENT
    assert repository.records == [
        {
            "outcome": OutboxDeliveryOutcome.SENT,
            "external_message_id": "9001",
            "error_type": None,
            "next_retry_at": None,
        }
    ]
    sent = json.loads(route.calls[0].request.content)
    assert sent == {
        "chat_id": -100123456,
        "message_thread_id": 42,
        "reply_parameters": {
            "allow_sending_without_reply": True,
            "message_id": 77,
        },
        "text": "Agent answer",
    }
    observable = repr(report) + caplog.text
    assert TOKEN not in observable
    assert "-100123456" not in observable
    assert "SENSITIVE_ROUTE" not in observable


@pytest.mark.asyncio
@respx.mock
async def test_telegram_429_honors_bounded_retry_after() -> None:
    repository = FakeRepository(_claim("telegram", _telegram_route()))
    respx.post(TELEGRAM_ENDPOINT).mock(
        return_value=httpx.Response(
            429,
            json={"ok": False, "parameters": {"retry_after": 17}},
        )
    )
    async with httpx.AsyncClient() as client:
        report = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")

    assert report.outcome is OutboxDeliveryOutcome.RETRY_WAIT
    assert repository.records[0]["error_type"] == "telegram_rate_limited"
    assert repository.records[0]["next_retry_at"] == NOW + timedelta(seconds=17)


@pytest.mark.asyncio
@respx.mock
async def test_telegram_read_timeout_is_unknown_and_never_retried() -> None:
    repository = FakeRepository(_claim("telegram", _telegram_route()))
    route = respx.post(TELEGRAM_ENDPOINT).mock(side_effect=httpx.ReadTimeout("unsafe detail"))
    async with httpx.AsyncClient() as client:
        report = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")

    assert report.outcome is OutboxDeliveryOutcome.UNKNOWN
    assert report.error_type == "telegram_result_unknown"
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_telegram_protocol_error_is_unknown() -> None:
    repository = FakeRepository(_claim("telegram", _telegram_route()))
    route = respx.post(TELEGRAM_ENDPOINT).mock(side_effect=httpx.ProtocolError("ambiguous"))
    async with httpx.AsyncClient() as client:
        report = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert report.outcome is OutboxDeliveryOutcome.UNKNOWN
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_telegram_connect_failure_has_finite_retries_then_backoff() -> None:
    repository = FakeRepository(_claim("telegram", _telegram_route()))
    route = respx.post(TELEGRAM_ENDPOINT).mock(side_effect=httpx.ConnectError("unavailable"))
    delays: list[float] = []

    async def capture_sleep(delay: float) -> None:
        delays.append(delay)

    async with httpx.AsyncClient() as client:
        report = await _dispatcher(
            repository,
            "telegram",
            client,
            sleeper=capture_sleep,
        ).dispatch_once("tenant-a")

    assert report.outcome is OutboxDeliveryOutcome.RETRY_WAIT
    assert route.call_count == 3
    assert repository.renew_count == 3
    assert delays == [5.0, 10.0]
    assert repository.records[0]["next_retry_at"] == NOW + timedelta(seconds=20)


@pytest.mark.asyncio
@respx.mock
async def test_telegram_server_failures_retry_but_definite_4xx_does_not() -> None:
    repository = FakeRepository(_claim("telegram", _telegram_route()))
    route = respx.post(TELEGRAM_ENDPOINT).mock(
        side_effect=[
            httpx.Response(503, json={"ok": False, "error_code": 503}),
            httpx.Response(200, json={"ok": True, "result": {"message_id": 8}}),
        ]
    )
    async with httpx.AsyncClient() as client:
        report = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert report.outcome is OutboxDeliveryOutcome.SENT
    assert route.call_count == 2

    repository = FakeRepository(_claim("telegram", _telegram_route()))
    calls_before_rejection = route.call_count
    route = respx.post(TELEGRAM_ENDPOINT).mock(return_value=httpx.Response(403))
    async with httpx.AsyncClient() as client:
        report = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert report.outcome is OutboxDeliveryOutcome.DEAD_LETTER
    assert report.error_type == "telegram_request_rejected"
    assert route.call_count == calls_before_rejection + 1


@pytest.mark.asyncio
@respx.mock
async def test_delivery_outcome_is_recorded_without_high_cardinality_labels() -> None:
    labels = {"tenant": "tenant-a", "channel": "telegram", "outcome": "sent"}
    before = REGISTRY.get_sample_value("agent_platform_delivery_total", labels) or 0.0
    repository = FakeRepository(_claim("telegram", _telegram_route()))
    respx.post(TELEGRAM_ENDPOINT).mock(
        return_value=httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})
    )
    async with httpx.AsyncClient() as client:
        report = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")

    assert report.outcome is OutboxDeliveryOutcome.SENT
    assert REGISTRY.get_sample_value("agent_platform_delivery_total", labels) == before + 1


@pytest.mark.asyncio
@respx.mock
async def test_telegram_exhausted_5xx_and_malformed_success_are_not_sent() -> None:
    repository = FakeRepository(_claim("telegram", _telegram_route()))
    route = respx.post(TELEGRAM_ENDPOINT).mock(return_value=httpx.Response(503))
    async with httpx.AsyncClient() as client:
        report = await _dispatcher(
            repository,
            "telegram",
            client,
            attempts=1,
        ).dispatch_once("tenant-a")
    assert report.outcome is OutboxDeliveryOutcome.UNKNOWN
    assert report.error_type == "telegram_result_unknown"
    assert route.call_count == 1

    repository = FakeRepository(_claim("telegram", _telegram_route(message_thread_id=None)))
    route_calls_before = route.call_count
    route = respx.post(TELEGRAM_ENDPOINT).mock(
        return_value=httpx.Response(200, json={"ok": True, "result": {}})
    )
    async with httpx.AsyncClient() as client:
        report = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert report.outcome is OutboxDeliveryOutcome.UNKNOWN
    assert route.call_count == route_calls_before + 1


@pytest.mark.asyncio
@respx.mock
@pytest.mark.parametrize(
    ("mutator", "expected_error"),
    [
        (
            lambda claim: replace(claim, reply_credential=None),
            "reply_credential_missing",
        ),
        (
            lambda claim: replace(
                claim,
                payload={"schema_version": 1, "kind": "text", "text": "x", "extra": True},
            ),
            "outbox_payload_invalid",
        ),
        (
            lambda claim: replace(
                claim,
                payload={"schema_version": 1, "kind": "text", "text": "x" * 4097},
            ),
            "telegram_text_too_long",
        ),
    ],
)
async def test_fail_closed_before_any_external_request(mutator: Any, expected_error: str) -> None:
    repository = FakeRepository(mutator(_claim("telegram", _telegram_route())))
    async with httpx.AsyncClient() as client:
        report = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert report.outcome is OutboxDeliveryOutcome.DEAD_LETTER
    assert report.error_type == expected_error
    assert repository.renew_count == 0


@pytest.mark.asyncio
@respx.mock
async def test_telegram_route_json_rejects_duplicate_or_unknown_coordinates() -> None:
    duplicate = '{"chat_id":1,"chat_id":2,"message_thread_id":null,"reply_to_message_id":3}'
    repository = FakeRepository(_claim("telegram", duplicate))
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert result.error_type == "telegram_delivery_configuration_invalid"

    repository = FakeRepository(_claim("telegram", "NaN"))
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert result.error_type == "telegram_delivery_configuration_invalid"

    repository = FakeRepository(_claim("telegram", _telegram_route(untrusted="value")))
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert result.error_type == "telegram_delivery_configuration_invalid"


@pytest.mark.asyncio
@respx.mock
async def test_lost_fence_prevents_external_side_effect() -> None:
    repository = FakeRepository(_claim("telegram", _telegram_route()), renew_results=[False])
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert result.state is DispatchState.LEASE_LOST
    assert result.persisted is False
    assert result.error_type == "delivery_lease_lost"


@pytest.mark.asyncio
@respx.mock
async def test_wecom_posts_once_without_redirects() -> None:
    repository = FakeRepository(_claim("wecom", WECOM_URL))
    route = respx.post(WECOM_URL).mock(return_value=httpx.Response(200, json={"errcode": 0}))
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "wecom", client).dispatch_once("tenant-a")

    assert result.outcome is OutboxDeliveryOutcome.SENT
    assert route.call_count == 1
    assert json.loads(route.calls[0].request.content) == {
        "msgtype": "text",
        "text": {"content": "Agent answer"},
    }

    repository = FakeRepository(_claim("wecom", WECOM_URL))
    calls_before_redirect = route.call_count
    route = respx.post(WECOM_URL).mock(
        return_value=httpx.Response(302, headers={"location": "https://attacker.invalid/"})
    )
    async with httpx.AsyncClient(follow_redirects=True) as client:
        result = await _dispatcher(repository, "wecom", client).dispatch_once("tenant-a")
    assert result.outcome is OutboxDeliveryOutcome.DEAD_LETTER
    assert route.call_count == calls_before_redirect + 1


@pytest.mark.asyncio
@respx.mock
async def test_wecom_requires_application_level_success_acknowledgement() -> None:
    repository = FakeRepository(_claim("wecom", WECOM_URL))
    route = respx.post(WECOM_URL).mock(return_value=httpx.Response(200, content=b""))
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "wecom", client).dispatch_once("tenant-a")
    assert result.outcome is OutboxDeliveryOutcome.UNKNOWN
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_wecom_network_ambiguity_is_unknown_and_single_attempt() -> None:
    repository = FakeRepository(_claim("wecom", WECOM_URL))
    route = respx.post(WECOM_URL).mock(side_effect=httpx.WriteTimeout("route is secret"))
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "wecom", client).dispatch_once("tenant-a")
    assert result.outcome is OutboxDeliveryOutcome.UNKNOWN
    assert result.error_type == "wecom_result_unknown"
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_wecom_server_response_is_unknown_without_retry() -> None:
    repository = FakeRepository(_claim("wecom", WECOM_URL))
    route = respx.post(WECOM_URL).mock(return_value=httpx.Response(503))
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "wecom", client).dispatch_once("tenant-a")
    assert result.outcome is OutboxDeliveryOutcome.UNKNOWN
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_wecom_lost_fence_prevents_post() -> None:
    repository = FakeRepository(_claim("wecom", WECOM_URL), renew_results=[False])
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "wecom", client).dispatch_once("tenant-a")
    assert result.state is DispatchState.LEASE_LOST
    assert result.error_type == "delivery_lease_lost"


@pytest.mark.asyncio
@respx.mock
async def test_wecom_route_is_revalidated_after_decryption() -> None:
    repository = FakeRepository(_claim("wecom", "https://attacker.invalid/reply?credential=secret"))
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "wecom", client).dispatch_once("tenant-a")
    assert result.outcome is OutboxDeliveryOutcome.DEAD_LETTER
    assert result.error_type == "wecom_response_route_invalid"
    assert repository.renew_count == 0


@pytest.mark.asyncio
@respx.mock
async def test_binding_and_authenticated_envelope_failures_are_terminal() -> None:
    claim = _claim("telegram", _telegram_route())
    repository = FakeRepository(claim)
    async with httpx.AsyncClient() as client:
        dispatcher = OutboxDispatcher(
            repository,
            FailingBindings(),
            FakeSecrets(),
            EnvelopeCipher("root-key-" * 4),
            client,
            dispatcher_id="dispatcher-1",
        )
        result = await dispatcher.dispatch_once("tenant-a")
    assert result.error_type == "delivery_binding_unavailable"

    assert claim.reply_credential is not None
    corrupted = replace(
        claim,
        reply_credential=replace(claim.reply_credential, ciphertext="v1.invalid"),
    )
    repository = FakeRepository(corrupted)
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert result.error_type == "reply_credential_invalid"

    mismatched = replace(
        claim,
        reply_credential=replace(claim.reply_credential, credential_kind="wecom_response_url"),
    )
    repository = FakeRepository(mismatched)
    async with httpx.AsyncClient() as client:
        result = await _dispatcher(repository, "telegram", client).dispatch_once("tenant-a")
    assert result.error_type == "reply_credential_kind_mismatch"

    repository = FakeRepository(claim)
    async with httpx.AsyncClient() as client:
        dispatcher = OutboxDispatcher(
            repository,
            FakeBindings("telegram"),
            FailingSecrets(),
            EnvelopeCipher("root-key-" * 4),
            client,
            dispatcher_id="dispatcher-1",
        )
        result = await dispatcher.dispatch_once("tenant-a")
    assert result.error_type == "telegram_delivery_configuration_invalid"

    repository = FakeRepository(claim)
    async with httpx.AsyncClient() as client:
        dispatcher = OutboxDispatcher(
            repository,
            FakeBindings("telegram", tenant_id="another-tenant"),
            FakeSecrets(),
            EnvelopeCipher("root-key-" * 4),
            client,
            dispatcher_id="dispatcher-1",
        )
        result = await dispatcher.dispatch_once("tenant-a")
    assert result.error_type == "delivery_binding_mismatch"

    repository = FakeRepository(claim)
    async with httpx.AsyncClient() as client:
        dispatcher = OutboxDispatcher(
            repository,
            FakeBindings("telegram"),
            InvalidTokenSecrets(),
            EnvelopeCipher("root-key-" * 4),
            client,
            dispatcher_id="dispatcher-1",
        )
        result = await dispatcher.dispatch_once("tenant-a")
    assert result.error_type == "telegram_delivery_configuration_invalid"


@pytest.mark.asyncio
@respx.mock
async def test_retry_scheduler_rejects_naive_clock() -> None:
    repository = FakeRepository(_claim("telegram", _telegram_route()))
    respx.post(TELEGRAM_ENDPOINT).mock(return_value=httpx.Response(429, json={"ok": False}))
    async with httpx.AsyncClient() as client:
        dispatcher = OutboxDispatcher(
            repository,
            FakeBindings("telegram"),
            FakeSecrets(),
            EnvelopeCipher("root-key-" * 4),
            client,
            dispatcher_id="dispatcher-1",
            clock=lambda: datetime(2026, 8, 29),
        )
        with pytest.raises(ValueError, match="timezone-aware"):
            await dispatcher.dispatch_once("tenant-a")
