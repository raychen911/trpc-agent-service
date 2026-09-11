"""Strict environment configuration for Stage 6B1 tracing (design §配置).

Everything is disabled unless ``TRPC_TRACE_ENABLED=true`` is explicitly set
together with a safe ``TRPC_TRACE_OTLP_ENDPOINT``.  Malformed values are a
hard configuration error (never a silent fallback), so a typo cannot put the
service in an unpredictable observability state.  The endpoint validator
rejects credentials, query strings and fragments so secrets can never ride
into exporter configuration or outbound connection strings.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Final, Mapping
from urllib.parse import urlsplit

TRPC_TRACE_ENABLED_ENV: Final[str] = "TRPC_TRACE_ENABLED"
TRPC_TRACE_OTLP_ENDPOINT_ENV: Final[str] = "TRPC_TRACE_OTLP_ENDPOINT"
TRPC_TRACE_SAMPLE_RATIO_ENV: Final[str] = "TRPC_TRACE_SAMPLE_RATIO"
TRPC_TRACE_EXPORT_TIMEOUT_SECONDS_ENV: Final[str] = "TRPC_TRACE_EXPORT_TIMEOUT_SECONDS"

_TRUE_VALUES: Final[frozenset[str]] = frozenset({"true"})
_FALSE_VALUES: Final[frozenset[str]] = frozenset({"false"})
_ALLOWED_SCHEMES: Final[frozenset[str]] = frozenset({"http", "https"})


class TelemetryConfigurationError(ValueError):
    """Invalid tracing configuration; raised verbatim, message is fixed-ish.

    Messages describe the offending *shape* of the value only — never the
    value itself — because endpoints/URLs may carry credentials or tokens.
    """


def _parse_bool(name: str, raw: str) -> bool:
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    raise TelemetryConfigurationError(f"{name} must be 'true' or 'false'")


def _parse_positive_float(name: str, raw: str, *, allow_zero: bool, upper_bound: float | None) -> float:
    if not raw.strip():
        raise TelemetryConfigurationError(f"{name} must not be empty")
    try:
        value = float(raw.strip())
    except (TypeError, ValueError):
        raise TelemetryConfigurationError(f"{name} must be a number") from None
    if not math.isfinite(value):
        raise TelemetryConfigurationError(f"{name} must be finite")
    in_lower = value < 0.0 if allow_zero else value <= 0.0
    in_upper = upper_bound is not None and value > upper_bound
    if in_lower or in_upper:
        bound = "0 <= value <= 1" if allow_zero else "value > 0"
        raise TelemetryConfigurationError(f"{name} out of range ({bound})")
    return value


def _validate_endpoint(name: str, raw: str) -> str:
    endpoint = raw.strip()
    if not endpoint:
        raise TelemetryConfigurationError(f"{name} must not be empty when tracing is enabled")
    try:
        parsed = urlsplit(endpoint)
    except ValueError:
        raise TelemetryConfigurationError(f"{name} is not a valid URL") from None
    if parsed.scheme not in _ALLOWED_SCHEMES:
        raise TelemetryConfigurationError(f"{name} scheme must be http or https")
    if not parsed.hostname:
        raise TelemetryConfigurationError(f"{name} must include a host")
    if parsed.username is not None or parsed.password is not None:
        raise TelemetryConfigurationError(f"{name} must not contain credentials")
    if parsed.query:
        raise TelemetryConfigurationError(f"{name} must not contain a query string")
    if parsed.fragment:
        raise TelemetryConfigurationError(f"{name} must not contain a fragment")
    return endpoint


@dataclass(frozen=True, slots=True)
class TelemetrySettings:
    """Immutable, validated tracing configuration for one service."""

    enabled: bool
    service_name: str
    otlp_endpoint: str | None
    sample_ratio: float
    export_timeout_seconds: float

    @classmethod
    def from_env(cls, service_name: str, environ: Mapping[str, str] | None = None) -> "TelemetrySettings":
        if not isinstance(service_name, str) or not service_name.strip():
            raise TelemetryConfigurationError("service_name must be a non-empty string")
        env = os.environ if environ is None else environ

        raw_enabled = env.get(TRPC_TRACE_ENABLED_ENV)
        enabled = _parse_bool(TRPC_TRACE_ENABLED_ENV, "false" if raw_enabled is None else str(raw_enabled))

        raw_endpoint = str(env.get(TRPC_TRACE_OTLP_ENDPOINT_ENV, "") or "")
        endpoint = _validate_endpoint(TRPC_TRACE_OTLP_ENDPOINT_ENV, raw_endpoint) if raw_endpoint.strip() else None
        if enabled and endpoint is None:
            raise TelemetryConfigurationError(f"{TRPC_TRACE_OTLP_ENDPOINT_ENV} is required when tracing is enabled")

        raw_ratio = env.get(TRPC_TRACE_SAMPLE_RATIO_ENV)
        sample_ratio = _parse_positive_float(TRPC_TRACE_SAMPLE_RATIO_ENV,
                                             "1.0" if raw_ratio is None else str(raw_ratio),
                                             allow_zero=True,
                                             upper_bound=1.0)

        raw_timeout = env.get(TRPC_TRACE_EXPORT_TIMEOUT_SECONDS_ENV)
        timeout = _parse_positive_float(TRPC_TRACE_EXPORT_TIMEOUT_SECONDS_ENV,
                                        "5.0" if raw_timeout is None else str(raw_timeout),
                                        allow_zero=False,
                                        upper_bound=None)

        return cls(
            enabled=enabled,
            service_name=service_name.strip(),
            otlp_endpoint=endpoint,
            sample_ratio=sample_ratio,
            export_timeout_seconds=timeout,
        )


__all__ = [
    "TRPC_TRACE_ENABLED_ENV",
    "TRPC_TRACE_EXPORT_TIMEOUT_SECONDS_ENV",
    "TRPC_TRACE_OTLP_ENDPOINT_ENV",
    "TRPC_TRACE_SAMPLE_RATIO_ENV",
    "TelemetryConfigurationError",
    "TelemetrySettings",
]
