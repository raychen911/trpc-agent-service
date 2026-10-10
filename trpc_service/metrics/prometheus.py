"""Low-cardinality service metrics.

Metric labels in this module are deliberately bounded.  In particular, the
SessionReady metrics describe transport/claim state (not tenant, session, or
event identifiers), so a busy multi-tenant deployment cannot create an
unbounded Prometheus time-series set.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Coroutine
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from prometheus_client import Counter, Gauge, Histogram

CALLBACKS = Counter(
    "trpc_im_callbacks_total",
    "Verified IM callbacks accepted or rejected.",
    ("channel", "outcome"),
)
CALLBACK_LATENCY = Histogram(
    "trpc_im_callback_seconds",
    "IM callback acknowledgement latency.",
    ("channel",),
    buckets=(0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2, 5),
)
TURN_LATENCY = Histogram(
    "trpc_agent_turn_seconds",
    "End-to-end agent turn latency.",
    ("outcome",),
)
DELIVERIES = Counter(
    "trpc_im_deliveries_total",
    "Outbound delivery attempts.",
    ("channel", "outcome"),
)
QUEUE_DEPTH = Gauge("trpc_queue_depth", "Pending work by queue.", ("queue",))
LEASE_CONFLICTS = Counter("trpc_session_lease_conflicts_total", "Rejected session leases.")
TOKENS = Counter("trpc_model_tokens_total", "Model token consumption.", ("direction",))
TENANT_COST = Counter(
    "trpc_tenant_cost_units_total",
    "Attributed cost units by opaque tenant identifier.",
    ("tenant",),
)

# SessionReady is a wake-up transport.  These counters measure the bounded
# receive/ack/reclaim state machine; PostgreSQL remains authoritative for
# business state and the actual turn duration is recorded by TURN_LATENCY.
SESSION_READY_RECEIVES = Counter(
    "trpc_session_ready_receives_total",
    "SessionReady Redis receive calls by bounded outcome.",
    ("outcome",),
)
SESSION_READY_RECEIVE_LATENCY = Histogram(
    "trpc_session_ready_receive_seconds",
    "SessionReady Redis receive latency.",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1, 5),
)
SESSION_READY_ACKS = Counter(
    "trpc_session_ready_acks_total",
    "SessionReady Redis ACK calls by bounded outcome.",
    ("outcome",),
)
SESSION_READY_ACK_LATENCY = Histogram(
    "trpc_session_ready_ack_seconds",
    "SessionReady Redis ACK latency.",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1),
)
SESSION_READY_RECLAIMS = Counter(
    "trpc_session_ready_reclaims_total",
    "SessionReady Redis XAUTOCLAIM calls by bounded outcome.",
    ("outcome",),
)
SESSION_READY_RECLAIM_LATENCY = Histogram(
    "trpc_session_ready_reclaim_seconds",
    "SessionReady Redis XAUTOCLAIM latency.",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1, 5),
)

# PostgreSQL is the source of truth for all v2 scheduling state.  Decorators
# below are used at the repository boundary so every claim/renew/commit/retry
# attempt is measured, including validation and fencing failures.
SESSION_READY_CLAIMS = Counter(
    "trpc_session_ready_claims_total",
    "PostgreSQL SessionReady claim results by bounded status.",
    ("status",),
)
SESSION_READY_CLAIM_LATENCY = Histogram(
    "trpc_session_ready_claim_seconds",
    "PostgreSQL SessionReady claim latency by bounded status.",
    ("status",),
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1, 5, 15),
)
SESSION_READY_LEASE_RENEWS = Counter(
    "trpc_session_ready_lease_renewals_total",
    "PostgreSQL SessionReady lease renewals by bounded outcome.",
    ("outcome",),
)
SESSION_READY_LEASE_RENEW_LATENCY = Histogram(
    "trpc_session_ready_lease_renew_seconds",
    "PostgreSQL SessionReady lease renewal latency.",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1, 5),
)
SESSION_READY_COMMITS = Counter(
    "trpc_session_ready_commits_total",
    "PostgreSQL SessionReady turn commits by bounded outcome.",
    ("outcome",),
)
SESSION_READY_COMMIT_LATENCY = Histogram(
    "trpc_session_ready_commit_seconds",
    "PostgreSQL SessionReady turn commit latency.",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1, 5, 15),
)
SESSION_READY_RETRIES = Counter(
    "trpc_session_ready_retries_total",
    "PostgreSQL SessionReady retry transitions by bounded outcome.",
    ("outcome",),
)
SESSION_READY_RETRY_LATENCY = Histogram(
    "trpc_session_ready_retry_seconds",
    "PostgreSQL SessionReady retry transition latency.",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1, 5),
)
SESSION_READY_FAILURES = Counter(
    "trpc_session_ready_failures_total",
    "PostgreSQL SessionReady permanent-failure transitions by bounded outcome.",
    ("outcome",),
)
SESSION_READY_FAILURE_LATENCY = Histogram(
    "trpc_session_ready_failure_seconds",
    "PostgreSQL SessionReady permanent-failure transition latency.",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1, 5),
)
SESSION_READY_RECOVERY_HEALTH = Gauge(
    "trpc_session_recovery_health",
    "Latest health of each PostgreSQL SessionReady recovery component (1=ok).",
    ("component",),
)

P = ParamSpec("P")
T = TypeVar("T")


def _status_label(value: Any, *, fallback: str = "success") -> str:
    """Convert a result status to a small, stable label."""

    status = getattr(value, "status", None)
    raw = getattr(status, "value", status)
    if isinstance(raw, str) and raw:
        return raw.lower()
    return fallback


def _error_label(error: BaseException) -> str:
    """Classify expected fencing separately without importing storage code."""

    return "fencing_conflict" if type(error).__name__ == "FencingConflict" else "error"


def _observe_async(
    counter: Counter,
    histogram: Histogram,
    function: Callable[P, Awaitable[T]],
    *,
    result_status: bool,
    label_name: str,
) -> Callable[P, Coroutine[Any, Any, T]]:
    @wraps(function)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        started = time.perf_counter()
        label = "error"
        try:
            result = await function(*args, **kwargs)
        except asyncio.CancelledError:
            label = "cancelled"
            raise
        except Exception as error:
            label = _error_label(error)
            raise
        else:
            label = _status_label(result) if result_status else "success"
            return result
        finally:
            counter.labels(**{label_name: label}).inc()
            if result_status:
                histogram.labels(status=label).observe(time.perf_counter() - started)
            else:
                histogram.observe(time.perf_counter() - started)

    return wrapped


def observe_session_ready_claim(
    function: Callable[P, Awaitable[T]],
) -> Callable[P, Coroutine[Any, Any, T]]:
    """Instrument a repository SessionReady claim and its returned status."""

    return _observe_async(
        SESSION_READY_CLAIMS,
        SESSION_READY_CLAIM_LATENCY,
        function,
        result_status=True,
        label_name="status",
    )


def observe_session_ready_lease_renewal(
    function: Callable[P, Awaitable[T]],
) -> Callable[P, Coroutine[Any, Any, T]]:
    """Instrument a repository SessionReady lease renewal."""

    return _observe_async(
        SESSION_READY_LEASE_RENEWS,
        SESSION_READY_LEASE_RENEW_LATENCY,
        function,
        result_status=False,
        label_name="outcome",
    )


def observe_session_ready_commit(
    function: Callable[P, Awaitable[T]],
) -> Callable[P, Coroutine[Any, Any, T]]:
    """Instrument a repository SessionReady turn commit."""

    return _observe_async(
        SESSION_READY_COMMITS,
        SESSION_READY_COMMIT_LATENCY,
        function,
        result_status=False,
        label_name="outcome",
    )


def observe_session_ready_retry(
    function: Callable[P, Awaitable[T]],
) -> Callable[P, Coroutine[Any, Any, T]]:
    """Instrument a repository SessionReady retry transition."""

    return _observe_async(
        SESSION_READY_RETRIES,
        SESSION_READY_RETRY_LATENCY,
        function,
        result_status=False,
        label_name="outcome",
    )


def observe_session_ready_failure(
    function: Callable[P, Awaitable[T]],
) -> Callable[P, Coroutine[Any, Any, T]]:
    """Instrument a repository SessionReady permanent-failure transition."""

    return _observe_async(
        SESSION_READY_FAILURES,
        SESSION_READY_FAILURE_LATENCY,
        function,
        result_status=False,
        label_name="outcome",
    )


__all__ = [
    "CALLBACKS",
    "CALLBACK_LATENCY",
    "DELIVERIES",
    "LEASE_CONFLICTS",
    "QUEUE_DEPTH",
    "SESSION_READY_ACKS",
    "SESSION_READY_ACK_LATENCY",
    "SESSION_READY_CLAIMS",
    "SESSION_READY_CLAIM_LATENCY",
    "SESSION_READY_COMMITS",
    "SESSION_READY_COMMIT_LATENCY",
    "SESSION_READY_FAILURES",
    "SESSION_READY_FAILURE_LATENCY",
    "SESSION_READY_LEASE_RENEWS",
    "SESSION_READY_LEASE_RENEW_LATENCY",
    "SESSION_READY_RECEIVES",
    "SESSION_READY_RECEIVE_LATENCY",
    "SESSION_READY_RECLAIMS",
    "SESSION_READY_RECLAIM_LATENCY",
    "SESSION_READY_RECOVERY_HEALTH",
    "SESSION_READY_RETRIES",
    "SESSION_READY_RETRY_LATENCY",
    "TENANT_COST",
    "TOKENS",
    "TURN_LATENCY",
    "observe_session_ready_claim",
    "observe_session_ready_commit",
    "observe_session_ready_failure",
    "observe_session_ready_lease_renewal",
    "observe_session_ready_retry",
]
