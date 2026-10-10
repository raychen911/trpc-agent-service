#!/usr/bin/env python3
"""Generate an opt-in, bounded Feishu HTTP callback performance workload.

The module is deliberately a library rather than a command-line load runner.
Importing it and constructing a workload never opens a socket.  Callers supply
an already configured ``httpx.AsyncClient`` when they need a local MockTransport
or an explicitly approved real gateway.  The request body is encrypted using
the same AES-CBC and SHA-256 signature rules as :class:`FeishuAdapter`.

Only aggregate metrics and synthetic identifiers are returned.  Secrets,
request bodies, response bodies, message text and URL query strings are never
included in the result or in exception messages produced by this module.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import os
import time
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal
from urllib.parse import quote
from uuid import uuid4

import httpx
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

MAX_TOTAL_REQUESTS = 2_000
MAX_RATE_PER_SECOND = 200.0
MAX_CONCURRENCY = 64
MAX_TIMEOUT_SECONDS = 600.0
MAX_SCHEDULE_SECONDS = 1_800.0
SCHEDULE_GRACE_SECONDS = 5.0
MAX_RUN_SECONDS = MAX_SCHEDULE_SECONDS + MAX_TIMEOUT_SECONDS + SCHEDULE_GRACE_SECONDS
DEFAULT_TOTAL_REQUESTS = 200
DEFAULT_RATE_PER_SECOND = 100.0
# The formal acceptance run offers 105 callbacks/s.  Keep the default HTTP
# client cap aligned with the performance gate's load cap so request latency
# cannot turn the scheduler into an accidental 32-request bottleneck.
DEFAULT_CONCURRENCY = 64
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_LATENCY_THRESHOLD_MS = 200.0
MAX_WARMUP_REQUESTS = MAX_TOTAL_REQUESTS
_CHAT_TYPES = {"p2p", "group"}
_SAFE_RUN_ID = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
_LATENCY_HISTOGRAM_BUCKETS: tuple[tuple[str, float | None], ...] = (
    ("0_10ms", 10.0),
    ("10_25ms", 25.0),
    ("25_50ms", 50.0),
    ("50_100ms", 100.0),
    ("100_200ms", 200.0),
    ("200_500ms", 500.0),
    ("500_1000ms", 1_000.0),
    ("gt_1000ms", None),
)


@dataclass(frozen=True, slots=True)
class FeishuHTTPPerformanceOptions:
    """Bounded options for one synthetic callback run.

    ``verification_token`` and ``encrypt_key`` are accepted only to construct
    requests in memory.  They are intentionally not represented in result
    objects and must not be logged by callers.
    """

    base_url: str
    binding_id: str
    app_id: str
    verification_token: str = field(repr=False)
    encrypt_key: str = field(repr=False)
    total_requests: int = DEFAULT_TOTAL_REQUESTS
    rate_per_second: float = DEFAULT_RATE_PER_SECOND
    concurrency: int = DEFAULT_CONCURRENCY
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    chat_type: Literal["p2p", "group"] = "p2p"
    run_id: str | None = None


@dataclass(frozen=True, slots=True)
class FeishuSessionIdentityInput:
    """Synthetic external identifiers used by the runtime session resolver."""

    binding_id: str
    app_id: str
    external_user_id: str
    chat_type: Literal["p2p", "group"]
    chat_id: str


@dataclass(frozen=True, slots=True)
class FeishuHTTPPerformanceResult:
    """Safe aggregate output from one bounded callback run."""

    run_id: str
    requested: int
    accepted: int
    failed: int
    status_counts: dict[int, int]
    failure_counts: dict[str, int]
    elapsed_ms: float
    p50_latency_ms: float | None
    p95_latency_ms: float | None
    max_latency_ms: float | None
    offered_rate_per_second: float
    observed_rate_per_second: float
    submission_span_seconds: float
    actual_submission_start_rate_per_second: float
    callback_submission_started_at: str | None
    callback_submission_last_started_at: str | None
    max_inflight: int
    accepted_external_message_ids: tuple[str, ...]
    session_identity_inputs: tuple[FeishuSessionIdentityInput, ...]
    timed_out: bool = False
    warmup_requested: int = 0
    warmup_accepted: int = 0
    warmup_failed: int = 0
    warmup_accepted_external_message_ids: tuple[str, ...] = ()
    warmup_session_identity_inputs: tuple[FeishuSessionIdentityInput, ...] = ()
    p90_latency_ms: float | None = None
    p99_latency_ms: float | None = None
    latency_threshold_ms: float = DEFAULT_LATENCY_THRESHOLD_MS
    over_threshold_count: int = 0
    latency_histogram: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _CallbackInput:
    index: int
    event_id: str
    message_id: str
    external_user_id: str
    chat_id: str
    chat_type: Literal["p2p", "group"]
    identity: FeishuSessionIdentityInput


@dataclass(frozen=True, slots=True)
class _ResponseOutcome:
    index: int
    message_id: str
    identity: FeishuSessionIdentityInput
    status_code: int | None
    success: bool
    failure_kind: str | None
    latency_ms: float


def validate_options(options: FeishuHTTPPerformanceOptions) -> None:
    """Validate options before any URL construction or network activity."""

    if not options.base_url or not options.binding_id or not options.app_id:
        raise ValueError("base URL, binding ID and app ID are required")
    if not options.verification_token or not options.encrypt_key:
        raise ValueError("Feishu callback secrets are required")
    if not isinstance(options.total_requests, int) or isinstance(options.total_requests, bool):
        raise ValueError("total requests must be an integer")
    if not 1 <= options.total_requests <= MAX_TOTAL_REQUESTS:
        raise ValueError(f"total requests must be between 1 and {MAX_TOTAL_REQUESTS}")
    if (
        not math.isfinite(options.rate_per_second)
        or options.rate_per_second <= 0
        or options.rate_per_second > MAX_RATE_PER_SECOND
    ):
        raise ValueError(f"rate per second must be between 0 and {MAX_RATE_PER_SECOND:g}")
    schedule_seconds = (options.total_requests - 1) / options.rate_per_second
    if schedule_seconds > MAX_SCHEDULE_SECONDS:
        raise ValueError(
            "request schedule exceeds the bounded performance-run window "
            f"of {MAX_SCHEDULE_SECONDS:g} seconds"
        )
    if not 1 <= options.concurrency <= MAX_CONCURRENCY:
        raise ValueError(f"concurrency must be between 1 and {MAX_CONCURRENCY}")
    if (
        not math.isfinite(options.timeout_seconds)
        or options.timeout_seconds <= 0
        or options.timeout_seconds > MAX_TIMEOUT_SECONDS
    ):
        raise ValueError(f"timeout must be between 0 and {MAX_TIMEOUT_SECONDS:g} seconds")
    if options.chat_type not in _CHAT_TYPES:
        raise ValueError("chat type must be p2p or group")
    if options.run_id is not None and (
        not 1 <= len(options.run_id) <= 64
        or any(char not in _SAFE_RUN_ID for char in options.run_id)
    ):
        raise ValueError("run ID contains unsupported characters")
    _callback_url(options.base_url, options.binding_id)


def _validate_run_evidence_options(warmup_requests: int, latency_threshold_ms: float) -> None:
    if (
        not isinstance(warmup_requests, int)
        or isinstance(warmup_requests, bool)
        or not 0 <= warmup_requests <= MAX_WARMUP_REQUESTS
    ):
        raise ValueError(f"warmup requests must be between 0 and {MAX_WARMUP_REQUESTS}")
    if (
        not isinstance(latency_threshold_ms, (int, float))
        or isinstance(latency_threshold_ms, bool)
        or not math.isfinite(float(latency_threshold_ms))
        or latency_threshold_ms <= 0
    ):
        raise ValueError("latency threshold must be a finite positive number of milliseconds")


def _latency_histogram(values: list[float]) -> dict[str, int]:
    """Return a fixed-cardinality, non-sensitive histogram of formal latencies."""

    histogram = {label: 0 for label, _ in _LATENCY_HISTOGRAM_BUCKETS}
    for value in values:
        safe_value = value if math.isfinite(value) and value >= 0 else math.inf
        for label, upper_bound in _LATENCY_HISTOGRAM_BUCKETS:
            if upper_bound is None or safe_value <= upper_bound:
                histogram[label] += 1
                break
    return histogram


def _callback_url(base_url: str, binding_id: str) -> str:
    """Build the callback route while rejecting caller-supplied query data."""

    try:
        parsed = httpx.URL(base_url)
    except (TypeError, ValueError):
        raise ValueError("base URL is invalid") from None
    if parsed.scheme not in {"http", "https"} or parsed.host is None:
        raise ValueError("base URL must be an HTTP(S) URL with a host")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError("base URL must not contain query, fragment or credentials")
    base_path = parsed.path.rstrip("/")
    route = f"{base_path}/v1/channels/feishu/{quote(binding_id, safe='')}/callback"
    return str(parsed.copy_with(path=route))


def _run_id(value: str | None) -> str:
    return value or f"perf-{uuid4().hex}"


def _callback_inputs(
    options: FeishuHTTPPerformanceOptions, run_id: str, *, count: int | None = None
) -> tuple[_CallbackInput, ...]:
    result: list[_CallbackInput] = []
    request_count = options.total_requests if count is None else count
    for index in range(request_count):
        suffix = f"{run_id}-{index:05d}"
        user_id = f"ou_perf_{suffix}"
        chat_id = f"oc_perf_{suffix}"
        identity = FeishuSessionIdentityInput(
            binding_id=options.binding_id,
            app_id=options.app_id,
            external_user_id=user_id,
            chat_type=options.chat_type,
            chat_id=chat_id,
        )
        result.append(
            _CallbackInput(
                index=index,
                event_id=f"evt_perf_{suffix}",
                message_id=f"om_perf_{suffix}",
                external_user_id=user_id,
                chat_id=chat_id,
                chat_type=options.chat_type,
                identity=identity,
            )
        )
    return tuple(result)


def _payload(item: _CallbackInput, verification_token: str, app_id: str) -> dict[str, object]:
    """Return one synthetic Feishu event payload, including no real user data."""

    now_ms = str(int(time.time() * 1000))
    return {
        "schema": "2.0",
        "header": {
            "event_id": item.event_id,
            "event_type": "im.message.receive_v1",
            "create_time": now_ms,
            "token": verification_token,
            "app_id": app_id,
            "tenant_key": "perf-tenant",
        },
        "event": {
            "sender": {
                "sender_id": {
                    "open_id": item.external_user_id,
                    "user_id": f"u_perf_{item.index:05d}",
                    "union_id": f"on_perf_{item.index:05d}",
                },
                "sender_type": "user",
                "tenant_key": "perf-tenant",
            },
            "message": {
                "message_id": item.message_id,
                "root_id": item.message_id,
                "parent_id": item.message_id,
                "create_time": now_ms,
                "chat_id": item.chat_id,
                "thread_id": f"omt_perf_{item.index:05d}",
                "chat_type": item.chat_type,
                "message_type": "text",
                "content": json.dumps({"text": "performance"}, separators=(",", ":")),
                "mentions": [],
            },
        },
    }


def _signed_request(
    item: _CallbackInput,
    *,
    options: FeishuHTTPPerformanceOptions,
    url: str,
    timestamp: int | None = None,
    nonce: str | None = None,
) -> httpx.Request:
    """Create one encrypted, signed POST request without sending it."""

    plaintext = json.dumps(
        _payload(item, options.verification_token, options.app_id),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded = padder.update(plaintext) + padder.finalize()
    iv = os.urandom(16)
    key = hashlib.sha256(options.encrypt_key.encode("utf-8")).digest()
    encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
    encrypted = base64.b64encode(iv + encryptor.update(padded) + encryptor.finalize()).decode(
        "ascii"
    )
    body = json.dumps({"encrypt": encrypted}, separators=(",", ":")).encode("ascii")
    request_timestamp = str(int(time.time()) if timestamp is None else timestamp)
    request_nonce = nonce or uuid4().hex
    signature = hashlib.sha256(
        request_timestamp.encode()
        + request_nonce.encode()
        + options.encrypt_key.encode("utf-8")
        + body
    ).hexdigest()
    return httpx.Request(
        "POST",
        url,
        headers={
            "Content-Type": "application/json",
            "X-Lark-Request-Timestamp": request_timestamp,
            "X-Lark-Request-Nonce": request_nonce,
            "X-Lark-Signature": signature,
        },
        content=body,
    )


def _owned_http_client(options: FeishuHTTPPerformanceOptions) -> httpx.AsyncClient:
    """Create a deterministic client whose pool cannot undercut the load cap."""

    return httpx.AsyncClient(
        timeout=httpx.Timeout(options.timeout_seconds),
        limits=httpx.Limits(
            max_connections=options.concurrency,
            max_keepalive_connections=options.concurrency,
            keepalive_expiry=5.0,
        ),
        trust_env=False,
    )


async def run_feishu_http_performance(
    options: FeishuHTTPPerformanceOptions,
    *,
    client: httpx.AsyncClient | None = None,
    warmup_requests: int = 0,
    latency_threshold_ms: float = DEFAULT_LATENCY_THRESHOLD_MS,
) -> FeishuHTTPPerformanceResult:
    """Send a bounded, rate-limited callback workload through a Feishu gateway.

    The caller must opt into real network activity by calling this function.
    Tests should pass an ``httpx.AsyncClient`` using ``MockTransport``.  If no
    client is supplied, this function owns and closes a client before return.
    Optional warmup requests use the same client and encrypted callback route,
    but their independent identities and observations are kept separate from
    the formal workload metrics.
    """

    validate_options(options)
    _validate_run_evidence_options(warmup_requests, latency_threshold_ms)
    run_id = _run_id(options.run_id)
    url = _callback_url(options.base_url, options.binding_id)
    inputs = _callback_inputs(options, run_id)
    warmup_inputs = _callback_inputs(options, f"{run_id}-warmup", count=warmup_requests)
    own_client = client is None
    http = client or _owned_http_client(options)
    semaphore = asyncio.Semaphore(options.concurrency)
    warmup_semaphore = asyncio.Semaphore(max(1, min(options.concurrency, warmup_requests)))
    warmup_outcomes: list[bool | None] = [None] * warmup_requests

    async def send_warmup(item: _CallbackInput) -> None:
        try:
            request = _signed_request(item, options=options, url=url)
            async with warmup_semaphore:
                async with asyncio.timeout(options.timeout_seconds):
                    response = await http.send(request)
            if not 200 <= response.status_code < 300:
                return
            try:
                acknowledgement = response.json()
            except (TypeError, ValueError):
                return
            if isinstance(acknowledgement, Mapping) and acknowledgement.get("msg") == "success":
                warmup_outcomes[item.index] = True
        except TimeoutError:
            return
        except httpx.HTTPError:
            return
        except Exception:
            # Keep provider/client exception text out of the report.
            return

    warmup_tasks: list[asyncio.Task[None]] = []
    if warmup_requests:
        warmup_tasks = [asyncio.create_task(send_warmup(item)) for item in warmup_inputs]
        warmup_batches = math.ceil(warmup_requests / options.concurrency)
        warmup_timeout = min(
            MAX_RUN_SECONDS,
            max(
                options.timeout_seconds,
                (warmup_batches * options.timeout_seconds) + SCHEDULE_GRACE_SECONDS,
            ),
        )
        try:
            await asyncio.wait_for(asyncio.gather(*warmup_tasks), timeout=warmup_timeout)
        except TimeoutError:
            for task in warmup_tasks:
                task.cancel()
            await asyncio.gather(*warmup_tasks, return_exceptions=True)

    warmup_accepted = tuple(item for item in warmup_inputs if warmup_outcomes[item.index] is True)
    outcomes: list[_ResponseOutcome | None] = [None] * options.total_requests
    inflight = 0
    max_inflight = 0
    submission_start_times: list[float] = []
    submission_started_at: list[str] = []
    inflight_lock = asyncio.Lock()
    started = time.perf_counter()
    schedule_started = asyncio.get_running_loop().time()
    schedule_seconds = (options.total_requests - 1) / options.rate_per_second
    overall_timeout = min(
        MAX_RUN_SECONDS,
        max(
            options.timeout_seconds,
            schedule_seconds + options.timeout_seconds + SCHEDULE_GRACE_SECONDS,
        ),
    )

    async def send_one(item: _CallbackInput) -> None:
        nonlocal inflight, max_inflight
        due = schedule_started + (item.index / options.rate_per_second)
        delay = due - asyncio.get_running_loop().time()
        if delay > 0:
            await asyncio.sleep(delay)
        async with semaphore:
            async with inflight_lock:
                inflight += 1
                max_inflight = max(max_inflight, inflight)
                submission_start_times.append(time.perf_counter())
                submission_started_at.append(datetime.now(UTC).isoformat().replace("+00:00", "Z"))
            request = _signed_request(item, options=options, url=url)
            request_started = time.perf_counter()
            status_code: int | None = None
            failure_kind: str | None = None
            success = False
            try:
                async with asyncio.timeout(options.timeout_seconds):
                    response = await http.send(request)
                status_code = response.status_code
                if 200 <= status_code < 300:
                    try:
                        acknowledgement = response.json()
                    except (TypeError, ValueError):
                        failure_kind = "invalid_ack"
                    else:
                        success = (
                            isinstance(acknowledgement, Mapping)
                            and acknowledgement.get("msg") == "success"
                        )
                        if not success:
                            failure_kind = "invalid_ack"
                else:
                    failure_kind = "http_status"
            except TimeoutError:
                failure_kind = "timeout"
            except httpx.HTTPError:
                failure_kind = "transport"
            except Exception:
                # Keep provider/client exception text out of the report.
                failure_kind = "client_error"
            finally:
                async with inflight_lock:
                    inflight -= 1
            outcomes[item.index] = _ResponseOutcome(
                index=item.index,
                message_id=item.message_id,
                identity=item.identity,
                status_code=status_code,
                success=success,
                failure_kind=failure_kind,
                latency_ms=(time.perf_counter() - request_started) * 1000.0,
            )

    timed_out = False
    tasks = [asyncio.create_task(send_one(item)) for item in inputs]
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=overall_timeout)
    except TimeoutError:
        timed_out = True
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        if own_client:
            await http.aclose()
    elapsed = max(time.perf_counter() - started, 1e-9)
    completed = tuple(outcome for outcome in outcomes if outcome is not None)
    latencies = sorted(outcome.latency_ms for outcome in completed)
    status_counts = Counter(
        outcome.status_code for outcome in completed if outcome.status_code is not None
    )
    failure_counts = Counter(
        outcome.failure_kind for outcome in completed if outcome.failure_kind is not None
    )
    accepted = tuple(outcome for outcome in completed if outcome.success)
    threshold = float(latency_threshold_ms)
    submission_span = (
        submission_start_times[-1] - submission_start_times[0]
        if len(submission_start_times) > 1
        else 0.0
    )
    return FeishuHTTPPerformanceResult(
        run_id=run_id,
        requested=options.total_requests,
        accepted=len(accepted),
        failed=options.total_requests - len(accepted),
        status_counts=dict(sorted(status_counts.items())),
        failure_counts=dict(sorted(failure_counts.items())),
        elapsed_ms=elapsed * 1000.0,
        p50_latency_ms=_percentile(latencies, 0.50),
        p95_latency_ms=_percentile(latencies, 0.95),
        max_latency_ms=max(latencies) if latencies else None,
        offered_rate_per_second=options.rate_per_second,
        observed_rate_per_second=len(completed) / elapsed,
        submission_span_seconds=submission_span,
        actual_submission_start_rate_per_second=_actual_start_rate(submission_start_times),
        callback_submission_started_at=(
            submission_started_at[0] if submission_started_at else None
        ),
        callback_submission_last_started_at=(
            submission_started_at[-1] if submission_started_at else None
        ),
        max_inflight=max_inflight,
        accepted_external_message_ids=tuple(outcome.message_id for outcome in accepted),
        session_identity_inputs=tuple(outcome.identity for outcome in accepted),
        timed_out=timed_out,
        warmup_requested=warmup_requests,
        warmup_accepted=len(warmup_accepted),
        warmup_failed=warmup_requests - len(warmup_accepted),
        warmup_accepted_external_message_ids=tuple(item.message_id for item in warmup_accepted),
        warmup_session_identity_inputs=tuple(item.identity for item in warmup_accepted),
        p90_latency_ms=_percentile(latencies, 0.90),
        p99_latency_ms=_percentile(latencies, 0.99),
        latency_threshold_ms=threshold,
        over_threshold_count=sum(latency > threshold for latency in latencies),
        latency_histogram=_latency_histogram(latencies),
    )


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    position = (len(values) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] + (values[upper] - values[lower]) * weight


def _actual_start_rate(start_times: list[float]) -> float:
    if len(start_times) <= 1:
        return 0.0
    span = start_times[-1] - start_times[0]
    return (len(start_times) - 1) / span if span > 0 else 0.0


__all__ = [
    "DEFAULT_CONCURRENCY",
    "DEFAULT_LATENCY_THRESHOLD_MS",
    "DEFAULT_RATE_PER_SECOND",
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_TOTAL_REQUESTS",
    "MAX_CONCURRENCY",
    "MAX_RATE_PER_SECOND",
    "MAX_RUN_SECONDS",
    "MAX_SCHEDULE_SECONDS",
    "MAX_TIMEOUT_SECONDS",
    "MAX_TOTAL_REQUESTS",
    "MAX_WARMUP_REQUESTS",
    "SCHEDULE_GRACE_SECONDS",
    "FeishuHTTPPerformanceOptions",
    "FeishuHTTPPerformanceResult",
    "FeishuSessionIdentityInput",
    "run_feishu_http_performance",
    "validate_options",
]
