"""Worker pool configuration and Rendezvous (highest-random-weight) routing."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit

from trpc_service.transport.models import WorkerApprovalTask, WorkerTask

_WORKER_BASE_URLS_ENV = "TRPC_WORKER_BASE_URLS"
_HEALTH_INTERVAL_ENV = "TRPC_WORKER_HEALTH_INTERVAL_SECONDS"
_HEALTH_TIMEOUT_ENV = "TRPC_WORKER_HEALTH_TIMEOUT_SECONDS"
_FAILURE_THRESHOLD_ENV = "TRPC_WORKER_HEALTH_FAILURE_THRESHOLD"
_RECOVERY_THRESHOLD_ENV = "TRPC_WORKER_HEALTH_RECOVERY_THRESHOLD"

_DEFAULT_HEALTH_INTERVAL = 2.0
_DEFAULT_HEALTH_TIMEOUT = 1.0
_DEFAULT_FAILURE_THRESHOLD = 2
_DEFAULT_RECOVERY_THRESHOLD = 2


@dataclass(frozen=True)
class WorkerEndpoint:
    """A single Worker endpoint with a stable opaque identity."""

    base_url: str
    endpoint_id: str

    @classmethod
    def from_url(cls, raw_url: str) -> WorkerEndpoint:
        normalized = _normalize_url(raw_url)
        endpoint_id = _sha256_prefix(normalized)
        return cls(base_url=normalized, endpoint_id=endpoint_id)


@dataclass(frozen=True)
class WorkerPoolSettings:
    """Validated Worker URL list and health-check parameters."""

    endpoints: tuple[WorkerEndpoint, ...]
    health_interval_seconds: float
    health_timeout_seconds: float
    failure_threshold: int
    recovery_threshold: int

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> WorkerPoolSettings:
        if environ is None:
            import os
            environ = os.environ

        raw_urls = environ.get(_WORKER_BASE_URLS_ENV, "").strip()
        if not raw_urls:
            raise ValueError(f"{_WORKER_BASE_URLS_ENV} is required and must not be empty")

        parts = raw_urls.split(",")
        seen_urls: set[str] = set()
        endpoints: list[WorkerEndpoint] = []
        for part in parts:
            stripped = part.strip()
            if not stripped:
                raise ValueError("Worker URL list contains an empty entry")
            normalized = _normalize_url(stripped)
            if normalized in seen_urls:
                raise ValueError("Worker URL list contains duplicate entries")
            seen_urls.add(normalized)
            endpoints.append(WorkerEndpoint.from_url(stripped))

        if len(endpoints) < 2:
            raise ValueError("TRPC_WORKER_BASE_URLS must contain at least two distinct URLs")

        health_interval = _parse_positive_float(
            environ.get(_HEALTH_INTERVAL_ENV, "").strip(),
            _HEALTH_INTERVAL_ENV,
            default=_DEFAULT_HEALTH_INTERVAL,
        )
        health_timeout = _parse_positive_float(
            environ.get(_HEALTH_TIMEOUT_ENV, "").strip(),
            _HEALTH_TIMEOUT_ENV,
            default=_DEFAULT_HEALTH_TIMEOUT,
        )
        failure_threshold = _parse_positive_int(
            environ.get(_FAILURE_THRESHOLD_ENV, "").strip(),
            _FAILURE_THRESHOLD_ENV,
            default=_DEFAULT_FAILURE_THRESHOLD,
        )
        recovery_threshold = _parse_positive_int(
            environ.get(_RECOVERY_THRESHOLD_ENV, "").strip(),
            _RECOVERY_THRESHOLD_ENV,
            default=_DEFAULT_RECOVERY_THRESHOLD,
        )

        return cls(
            endpoints=tuple(endpoints),
            health_interval_seconds=health_interval,
            health_timeout_seconds=health_timeout,
            failure_threshold=failure_threshold,
            recovery_threshold=recovery_threshold,
        )


@dataclass(frozen=True)
class WorkerRouteKey:
    """Identity fields used for stable session-to-Worker routing."""

    tenant_id: str
    app_id: str
    config_version: int
    channel: str
    user_id: str
    session_id: str

    @classmethod
    def from_task(cls, task: "WorkerTask | WorkerApprovalTask") -> WorkerRouteKey:
        return cls(
            tenant_id=task.tenant_id,
            app_id=task.app_id,
            config_version=task.config_version,
            channel=task.channel,
            user_id=task.user_id,
            session_id=task.session_id,
        )

    def _canonical(self) -> str:
        return json.dumps(
            [self.tenant_id, self.app_id, self.config_version, self.channel, self.user_id, self.session_id],
            separators=(",", ":"),
            ensure_ascii=False,
        )


class RendezvousRouter:
    """Rendezvous (highest random weight) hashing for stable Worker selection."""

    def rank(
        self,
        route_key: WorkerRouteKey,
        endpoints: Collection[WorkerEndpoint],
    ) -> tuple[WorkerEndpoint, ...]:
        key_str = route_key._canonical()
        scored: list[tuple[str, WorkerEndpoint]] = []
        for ep in endpoints:
            score_input = json.dumps([key_str, ep.base_url], separators=(",", ":"), ensure_ascii=False)
            score = hashlib.sha256(score_input.encode()).hexdigest()
            scored.append((score, ep))
        scored.sort(key=lambda pair: pair[0], reverse=True)
        return tuple(ep for _, ep in scored)


def _normalize_url(raw: str) -> str:
    parsed = urlsplit(raw)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Worker URL scheme must be http or https, got {parsed.scheme!r}")
    if not parsed.hostname:
        raise ValueError("Worker URL must include a hostname")
    if parsed.username or parsed.password:
        raise ValueError("Worker URL must not contain credentials")
    if parsed.query:
        raise ValueError("Worker URL must not contain query parameters")
    if parsed.fragment:
        raise ValueError("Worker URL must not contain a fragment")
    base = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        base += f":{parsed.port}"
    if parsed.path and parsed.path != "/":
        base += parsed.path.rstrip("/")
    return base


def _sha256_prefix(value: str, length: int = 12) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:length]


def _parse_positive_float(raw: str, name: str, *, default: float) -> float:
    if not raw:
        return default
    try:
        value = float(raw)
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a positive finite number") from None
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return value


def _parse_positive_int(raw: str, name: str, *, default: int) -> int:
    if not raw:
        return default
    try:
        value = int(raw)
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a positive integer") from None
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


__all__ = [
    "RendezvousRouter",
    "WorkerEndpoint",
    "WorkerPoolSettings",
    "WorkerRouteKey",
]
