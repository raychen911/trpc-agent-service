from __future__ import annotations

import asyncio
import json
import math

import httpx
import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from scripts.feishu_http_performance import (
    DEFAULT_CONCURRENCY,
    MAX_WARMUP_REQUESTS,
    FeishuHTTPPerformanceOptions,
    _actual_start_rate,
    _latency_histogram,
    _owned_http_client,
    run_feishu_http_performance,
    validate_options,
)
from trpc_service.channels.base import WebhookRequest
from trpc_service.channels.feishu import FeishuAdapter
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.tenant.models import Channel, ChannelBinding

APP_ID = "cli_perf_test"
BINDING_ID = "feishu-perf-binding"
VERIFICATION_TOKEN = "perf-verification-token"
ENCRYPT_KEY = "perf-encrypt-key"


def _options(**overrides: object) -> FeishuHTTPPerformanceOptions:
    values: dict[str, object] = {
        "base_url": "http://testserver",
        "binding_id": BINDING_ID,
        "app_id": APP_ID,
        "verification_token": VERIFICATION_TOKEN,
        "encrypt_key": ENCRYPT_KEY,
        "total_requests": 4,
        "rate_per_second": 200.0,
        "concurrency": 2,
        "timeout_seconds": 1.0,
        "run_id": "run-test",
    }
    values.update(overrides)
    return FeishuHTTPPerformanceOptions(**values)  # type: ignore[arg-type]


def _binding() -> ChannelBinding:
    return ChannelBinding(
        binding_id=BINDING_ID,
        tenant_id="perf-tenant",
        app_id="perf-app",
        channel=Channel.FEISHU,
        account_id=APP_ID,
        secret_refs={
            "verification_token": SecretRef(uri=f"literal://{VERIFICATION_TOKEN}"),
            "encrypt_key": SecretRef(uri=f"literal://{ENCRYPT_KEY}"),
        },
        capabilities=frozenset({"text", "reply"}),
    )


@pytest.mark.asyncio
async def test_mock_transport_requests_are_encrypted_signed_and_adapter_validated() -> None:
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    binding = _binding()
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "POST"
        assert request.url.query == b""
        assert "?" not in str(request.url)
        callback = adapter.verify_and_parse(
            WebhookRequest(
                method=request.method,
                headers=dict(request.headers),
                body=request.content,
            ),
            binding,
        )
        assert callback.envelope is not None
        assert callback.envelope.external_user_id.startswith("ou_perf_run-test-")
        assert callback.envelope.conversation_kind.value == "direct"
        return httpx.Response(200, json={"msg": "success"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run_feishu_http_performance(_options(), client=client)

    assert result.requested == 4
    assert result.accepted == 4
    assert result.failed == 0
    assert result.status_counts == {200: 4}
    assert result.failure_counts == {}
    assert result.max_inflight <= 2
    assert result.submission_span_seconds > 0
    assert result.actual_submission_start_rate_per_second > 0
    assert result.callback_submission_started_at is not None
    assert result.callback_submission_last_started_at is not None
    assert result.callback_submission_last_started_at >= result.callback_submission_started_at
    assert len(result.accepted_external_message_ids) == 4
    assert len(set(result.accepted_external_message_ids)) == 4
    assert len(result.session_identity_inputs) == 4
    assert {item.chat_type for item in result.session_identity_inputs} == {"p2p"}
    assert len({item.external_user_id for item in result.session_identity_inputs}) == 4
    assert len({item.chat_id for item in result.session_identity_inputs}) == 4
    assert all("performance" not in repr(item) for item in result.session_identity_inputs)
    assert all(VERIFICATION_TOKEN not in repr(item) for item in result.session_identity_inputs)
    assert all(ENCRYPT_KEY not in repr(item) for item in result.session_identity_inputs)


@pytest.mark.asyncio
async def test_group_mode_has_unique_chat_and_message_identity() -> None:
    seen: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        # The callback is encrypted; the uniqueness assertions are made from
        # the result identity inputs instead of retaining plaintext payloads.
        assert set(body) == {"encrypt"}
        return httpx.Response(200, json={"msg": "success"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run_feishu_http_performance(
            _options(chat_type="group", total_requests=3), client=client
        )
    seen.extend((item.external_user_id, item.chat_id) for item in result.session_identity_inputs)
    assert result.accepted == 3
    assert len(set(seen)) == 3
    assert {item.chat_type for item in result.session_identity_inputs} == {"group"}


@pytest.mark.asyncio
async def test_warmup_uses_same_encrypted_client_and_is_excluded_from_formal_metrics() -> None:
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    binding = _binding()
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        callback = adapter.verify_and_parse(
            WebhookRequest(
                method=request.method,
                headers=dict(request.headers),
                body=request.content,
            ),
            binding,
        )
        assert callback.envelope is not None
        return httpx.Response(200, json={"msg": "success"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run_feishu_http_performance(
            _options(total_requests=4),
            client=client,
            warmup_requests=2,
            latency_threshold_ms=0.000001,
        )

    assert len(requests) == 6
    assert result.requested == 4
    assert result.accepted == 4
    assert result.failed == 0
    assert result.warmup_requested == 2
    assert result.warmup_accepted == 2
    assert result.warmup_failed == 0
    assert all("-warmup-" not in value for value in result.accepted_external_message_ids)
    assert all("-warmup-" in value for value in result.warmup_accepted_external_message_ids)
    assert "-warmup-" not in result.session_identity_inputs[0].external_user_id
    assert all(
        "-warmup-" in value.external_user_id for value in result.warmup_session_identity_inputs
    )
    assert result.latency_threshold_ms == 0.000001
    assert result.p90_latency_ms is not None
    assert result.p99_latency_ms is not None
    assert result.p99_latency_ms >= result.p90_latency_ms
    assert result.over_threshold_count == 4
    assert sum(result.latency_histogram.values()) == 4
    assert result.latency_histogram == {
        "0_10ms": 4,
        "10_25ms": 0,
        "25_50ms": 0,
        "50_100ms": 0,
        "100_200ms": 0,
        "200_500ms": 0,
        "500_1000ms": 0,
        "gt_1000ms": 0,
    }


@pytest.mark.asyncio
async def test_warmup_failures_are_separate_from_formal_status_and_failure_counts() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, request=request)
        return httpx.Response(200, json={"msg": "success"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run_feishu_http_performance(
            _options(total_requests=3), client=client, warmup_requests=2
        )

    assert calls == 5
    assert result.warmup_requested == 2
    assert result.warmup_accepted == 1
    assert result.warmup_failed == 1
    assert result.accepted == 3
    assert result.failed == 0
    assert result.status_counts == {200: 3}
    assert result.failure_counts == {}
    assert len(result.warmup_accepted_external_message_ids) == 1


@pytest.mark.asyncio
async def test_warmup_timeout_is_counted_without_affecting_formal_requests() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls <= 2:
            await asyncio.sleep(0.05)
        return httpx.Response(200, json={"msg": "success"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run_feishu_http_performance(
            _options(total_requests=2, timeout_seconds=0.001),
            client=client,
            warmup_requests=2,
        )

    assert calls == 4
    assert result.warmup_requested == 2
    assert result.warmup_accepted == 0
    assert result.warmup_failed == 2
    assert result.accepted == 2
    assert result.failed == 0
    assert result.status_counts == {200: 2}
    assert result.failure_counts == {}


@pytest.mark.parametrize("failure_kind", ["invalid_ack", "transport", "client_error"])
@pytest.mark.asyncio
async def test_warmup_client_failures_are_safe_and_separate(failure_kind: str) -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            if failure_kind == "invalid_ack":
                return httpx.Response(200, json={"msg": "not-success"}, request=request)
            if failure_kind == "transport":
                raise httpx.ConnectError("secret-and-user-data", request=request)
            raise RuntimeError("secret-and-user-data")
        return httpx.Response(200, json={"msg": "success"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run_feishu_http_performance(
            _options(total_requests=1), client=client, warmup_requests=1
        )

    assert result.warmup_requested == 1
    assert result.warmup_accepted == 0
    assert result.warmup_failed == 1
    assert result.accepted == 1
    assert result.failed == 0
    assert result.status_counts == {200: 1}
    assert result.failure_counts == {}
    rendered = repr(result)
    assert "secret-and-user-data" not in rendered


def test_latency_evidence_histogram_is_fixed_and_bounded() -> None:
    histogram = _latency_histogram(
        [
            0.0,
            10.0,
            10.01,
            25.0,
            50.0,
            100.0,
            200.0,
            500.0,
            1_000.0,
            1_000.01,
            math.nan,
        ]
    )
    assert tuple(histogram) == (
        "0_10ms",
        "10_25ms",
        "25_50ms",
        "50_100ms",
        "100_200ms",
        "200_500ms",
        "500_1000ms",
        "gt_1000ms",
    )
    assert sum(histogram.values()) == 11
    assert all(isinstance(value, int) and value >= 0 for value in histogram.values())


@pytest.mark.asyncio
async def test_invalid_ack_http_status_and_transport_are_counted_without_response_content() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, json={"msg": "not-success"}, request=request)
        if calls == 2:
            return httpx.Response(503, text="provider-secret-and-user-text", request=request)
        raise httpx.ConnectError("token-and-user-text-must-not-be-reported", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run_feishu_http_performance(_options(total_requests=3), client=client)
    assert result.accepted == 0
    assert result.failed == 3
    assert result.status_counts == {200: 1, 503: 1}
    assert result.failure_counts == {"http_status": 1, "invalid_ack": 1, "transport": 1}
    safe = repr(result)
    assert "provider-secret" not in safe
    assert "token-and-user-text" not in safe
    assert VERIFICATION_TOKEN not in safe
    assert ENCRYPT_KEY not in safe


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("total_requests", 2_001),
        ("rate_per_second", 200.1),
        ("concurrency", 65),
        ("timeout_seconds", 0.0),
    ],
)
def test_hard_limits_are_checked_before_network_activity(field: str, value: object) -> None:
    options = _options(**{field: value})
    with pytest.raises(ValueError):
        validate_options(options)


def test_rate_schedule_has_a_bounded_total_window() -> None:
    with pytest.raises(ValueError, match="bounded performance-run window"):
        validate_options(_options(total_requests=2, rate_per_second=0.0001))


@pytest.mark.parametrize(
    ("warmup_requests", "latency_threshold_ms"),
    [
        (-1, 200.0),
        (MAX_WARMUP_REQUESTS + 1, 200.0),
        (0, 0.0),
        (0, math.nan),
    ],
)
@pytest.mark.asyncio
async def test_run_evidence_limits_are_checked_before_network_activity(
    warmup_requests: int, latency_threshold_ms: float
) -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"msg": "success"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ValueError):
            await run_feishu_http_performance(
                _options(),
                client=client,
                warmup_requests=warmup_requests,
                latency_threshold_ms=latency_threshold_ms,
            )
    assert calls == 0


@pytest.mark.asyncio
async def test_default_run_evidence_fields_remain_compatible() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"msg": "success"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run_feishu_http_performance(_options(total_requests=2), client=client)

    assert result.warmup_requested == 0
    assert result.warmup_accepted == 0
    assert result.warmup_failed == 0
    assert result.latency_threshold_ms == 200.0
    assert sum(result.latency_histogram.values()) == 2


def test_query_credentials_and_url_data_are_rejected() -> None:
    with pytest.raises(ValueError, match="query"):
        validate_options(_options(base_url="https://example.test/callback?secret=leak"))


def test_options_repr_does_not_expose_callback_secrets() -> None:
    rendered = repr(_options())
    assert VERIFICATION_TOKEN not in rendered
    assert ENCRYPT_KEY not in rendered


def test_padding_and_aes_protocol_shape_is_cbc_compatible() -> None:
    # This is a protocol sanity check for the same primitives used by the
    # adapter; the network runner tests the end-to-end verification above.
    plaintext = b"synthetic"
    padder = padding.PKCS7(128).padder()
    padded = padder.update(plaintext) + padder.finalize()
    iv = bytes(range(16))
    key = __import__("hashlib").sha256(ENCRYPT_KEY.encode()).digest()
    encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
    encrypted = encryptor.update(padded) + encryptor.finalize()
    assert len(encrypted) % 16 == 0


def test_actual_submission_start_rate_uses_inter_start_span() -> None:
    assert _actual_start_rate([]) == 0.0
    assert _actual_start_rate([1.0]) == 0.0
    assert _actual_start_rate([1.0, 1.5, 2.0]) == 2.0


def test_owned_http_client_pool_matches_the_bounded_concurrency() -> None:
    client = _owned_http_client(_options(concurrency=32, timeout_seconds=7.0))
    try:
        pool = client._transport._pool  # type: ignore[attr-defined]
        assert pool._max_connections == 32
        assert pool._max_keepalive_connections == 32
        assert client.trust_env is False
    finally:
        import asyncio

        asyncio.run(client.aclose())


def test_formal_http_default_concurrency_matches_performance_gate() -> None:
    options = FeishuHTTPPerformanceOptions(
        base_url="http://testserver",
        binding_id=BINDING_ID,
        app_id=APP_ID,
        verification_token=VERIFICATION_TOKEN,
        encrypt_key=ENCRYPT_KEY,
    )

    assert DEFAULT_CONCURRENCY == 64
    assert options.concurrency == DEFAULT_CONCURRENCY
