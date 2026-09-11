"""Framework-independent observability building blocks.

HTTP handlers, outbox records and stream messages use the same W3C-compatible trace
context.  Optional framework instrumentation can wrap these primitives; the local demo
still receives useful trace IDs and JSON logs without an OTEL collector installed.
"""

from __future__ import annotations

import contextlib
import contextvars
import hashlib
import json
import logging
import re
import secrets
import time
from collections import Counter, defaultdict
from collections.abc import Iterator, Mapping, MutableMapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

_TRACE_CONTEXT: contextvars.ContextVar[TraceContext | None] = contextvars.ContextVar(
    "trpc_trace_context", default=None
)
_SECRET_KEY = re.compile(
    r"(?:authorization|token|api[_-]?key|secret|password|credential|cookie)", re.I
)
_PII_KEY = re.compile(
    r"(?:email|phone|mobile|id[_-]?(?:number|card)|prompt|message|content|text)", re.I
)
_BEARER = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/-]+=*", re.I)
_EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
_PHONE = re.compile(r"(?<!\d)(?:\+?\d[\d -]{7,}\d)(?!\d)")


@dataclass(frozen=True, slots=True)
class TraceContext:
    trace_id: str
    span_id: str
    request_id: str
    trace_flags: str = "01"

    @property
    def traceparent(self) -> str:
        return f"00-{self.trace_id}-{self.span_id}-{self.trace_flags}"


def new_trace_context(request_id: str | None = None, trace_id: str | None = None) -> TraceContext:
    return TraceContext(
        trace_id=trace_id or secrets.token_hex(16),
        span_id=secrets.token_hex(8),
        request_id=request_id or f"req_{secrets.token_hex(12)}",
    )


def extract_trace_context(
    headers: Mapping[str, str], request_id: str | None = None
) -> TraceContext:
    """Create a local child context from a valid W3C ``traceparent`` header."""

    traceparent = next((v for k, v in headers.items() if k.lower() == "traceparent"), "")
    parts = traceparent.split("-")
    if (
        len(parts) == 4
        and parts[0] == "00"
        and len(parts[1]) == 32
        and len(parts[2]) == 16
        and all(c in "0123456789abcdefABCDEF" for c in parts[1] + parts[2])
    ):
        return TraceContext(
            trace_id=parts[1].lower(),
            span_id=secrets.token_hex(8),
            request_id=request_id
            or _header(headers, "x-request-id")
            or f"req_{secrets.token_hex(12)}",
            trace_flags=parts[3] if len(parts[3]) == 2 else "01",
        )
    return new_trace_context(request_id=request_id or _header(headers, "x-request-id"))


def _header(headers: Mapping[str, str], name: str) -> str | None:
    return next((value for key, value in headers.items() if key.lower() == name), None)


def inject_trace_context(
    headers: MutableMapping[str, str], context: TraceContext | None = None
) -> None:
    current = context or current_trace_context()
    headers["traceparent"] = current.traceparent
    headers["x-request-id"] = current.request_id


def current_trace_context() -> TraceContext:
    return _TRACE_CONTEXT.get() or new_trace_context()


@contextlib.contextmanager
def bind_trace_context(context: TraceContext) -> Iterator[TraceContext]:
    token = _TRACE_CONTEXT.set(context)
    try:
        yield context
    finally:
        _TRACE_CONTEXT.reset(token)


@contextlib.contextmanager
def traced(
    name: str, *, logger: logging.Logger | None = None, **attributes: object
) -> Iterator[TraceContext]:
    """Create a real OTEL span when configured and retain a safe log fallback."""

    parent = current_trace_context()
    context = TraceContext(
        parent.trace_id, secrets.token_hex(8), parent.request_id, parent.trace_flags
    )
    started = time.perf_counter()
    span_context = contextlib.nullcontext(None)
    try:
        from opentelemetry import trace
        from opentelemetry.trace import (
            NonRecordingSpan,
            SpanContext,
            TraceFlags,
            set_span_in_context,
        )

        parent_span = SpanContext(
            trace_id=int(parent.trace_id, 16),
            span_id=int(parent.span_id, 16),
            is_remote=True,
            trace_flags=TraceFlags(int(parent.trace_flags, 16)),
        )
        otel_parent = set_span_in_context(NonRecordingSpan(parent_span))
        span_context = trace.get_tracer("trpc_service").start_as_current_span(name, context=otel_parent)
    except (ImportError, ValueError):
        # A dependency-free local run still receives the structured trace
        # context and log timing below.
        pass
    with span_context as span, bind_trace_context(context):
        if span is not None:
            for key, value in attributes.items():
                if isinstance(value, (str, bool, int, float)):
                    span.set_attribute(str(key), value)
            span.set_attribute("trpc.request_id", context.request_id)
        try:
            yield context
        except Exception as exc:
            if span is not None:
                span.record_exception(exc)
                span.set_status(trace.Status(trace.StatusCode.ERROR, type(exc).__name__))
            if logger:
                logger.exception(
                    "span_failed",
                    extra={"span": name, "error_type": type(exc).__name__, **attributes},
                )
            raise
        finally:
            if logger:
                logger.info(
                    "span_finished",
                    extra={
                        "span": name,
                        "latency_ms": round((time.perf_counter() - started) * 1000, 3),
                        **attributes,
                    },
                )


class Redactor:
    """Centralized, defensive redaction for logs and telemetry attributes."""

    def __init__(self, sensitive_fields: tuple[str, ...] = ()) -> None:
        self._custom = {field.lower() for field in sensitive_fields}

    def redact(self, value: Any, *, key: str | None = None) -> Any:
        if key and (key.lower() in self._custom or _SECRET_KEY.search(key) or _PII_KEY.search(key)):
            return "[REDACTED]"
        if isinstance(value, Mapping):
            return {str(k): self.redact(v, key=str(k)) for k, v in value.items()}
        if isinstance(value, tuple):
            return tuple(self.redact(item) for item in value)
        if isinstance(value, list):
            return [self.redact(item) for item in value]
        if isinstance(value, str):
            return _PHONE.sub(
                "[REDACTED_PHONE]",
                _EMAIL.sub("[REDACTED_EMAIL]", _BEARER.sub("Bearer [REDACTED]", value)),
            )
        return value


class JsonFormatter(logging.Formatter):
    """A small JSON formatter that automatically includes safe request identifiers."""

    _standard = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}

    def __init__(self, *, redactor: Redactor | None = None) -> None:
        super().__init__()
        self.redactor = redactor or Redactor()

    def format(self, record: logging.LogRecord) -> str:
        context = current_trace_context()
        data: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": context.trace_id,
            "request_id": context.request_id,
        }
        for key, value in record.__dict__.items():
            if key not in self._standard and not key.startswith("_"):
                data[key] = value
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        return json.dumps(
            self.redactor.redact(data), default=str, ensure_ascii=False, separators=(",", ":")
        )


def configure_logging(
    level: str = "INFO", *, sensitive_fields: tuple[str, ...] = ()
) -> logging.Logger:
    root = logging.getLogger()
    root.setLevel(level.upper())
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(redactor=Redactor(sensitive_fields)))
    root.handlers[:] = [handler]
    return root


def tenant_bucket(tenant_id: str, buckets: int = 64) -> str:
    """Bound tenant metric cardinality while retaining a stable cost bucket."""

    if buckets <= 0:
        raise ValueError("buckets must be positive")
    digest = hashlib.sha256(tenant_id.encode("utf-8")).digest()
    return f"t{int.from_bytes(digest[:4], 'big') % buckets:02d}"


class InMemoryMetrics:
    """Metrics facade used by tests and local mode.

    Production can bridge calls from this narrow API to OpenTelemetry metrics.  Labels are
    restricted to bounded dimensions; tenant IDs are converted to buckets.
    """

    def __init__(self) -> None:
        self._counters: Counter[tuple[str, tuple[tuple[str, str], ...]]] = Counter()
        self._histograms: defaultdict[tuple[str, tuple[tuple[str, str], ...]], list[float]] = (
            defaultdict(list)
        )

    @staticmethod
    def _labels(labels: Mapping[str, object] | None) -> tuple[tuple[str, str], ...]:
        normalized = dict(labels or {})
        if "tenant_id" in normalized:
            normalized["tenant_bucket"] = tenant_bucket(str(normalized.pop("tenant_id")))
        return tuple(sorted((str(key), str(value)) for key, value in normalized.items()))

    def increment(
        self, name: str, value: int = 1, *, labels: Mapping[str, object] | None = None
    ) -> None:
        self._counters[(name, self._labels(labels))] += value

    def observe(
        self, name: str, value: float, *, labels: Mapping[str, object] | None = None
    ) -> None:
        self._histograms[(name, self._labels(labels))].append(value)

    def snapshot(self) -> dict[str, object]:
        return {
            "counters": {
                f"{name}{labels}": value for (name, labels), value in self._counters.items()
            },
            "histograms": {
                f"{name}{labels}": list(values)
                for (name, labels), values in self._histograms.items()
            },
        }


def configure_opentelemetry(
    *,
    service_name: str = "trpc-agent-service",
    otlp_endpoint: str | None = None,
    fastapi_app: object | None = None,
    sqlalchemy_engine: object | None = None,
) -> bool:
    """Install OTEL exporters/instrumentors when optional packages are present.

    Returning ``False`` intentionally selects the JSON-log/trace-context console
    fallback for a zero-credential local run rather than failing application
    startup.  Explicit OTLP configuration remains observable to operators.
    """

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import (
            BatchSpanProcessor,
            ConsoleSpanExporter,
            SimpleSpanProcessor,
        )
    except ImportError:
        return False

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    if otlp_endpoint:
        # ``OTEL_EXPORTER_OTLP_ENDPOINT`` conventionally accepts an HTTP URL,
        # while the gRPC exporter requires a host:port target.  Supporting
        # both forms keeps local Compose and a standard OTEL deployment from
        # silently retrying against an invalid target.
        parsed_endpoint = urlsplit(otlp_endpoint)
        exporter_endpoint = (
            parsed_endpoint.netloc
            if parsed_endpoint.scheme in {"http", "https"} and parsed_endpoint.netloc
            else otlp_endpoint
        )
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=exporter_endpoint, insecure=True))
        )
    else:
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
    HTTPXClientInstrumentor().instrument()
    if fastapi_app is not None:
        FastAPIInstrumentor.instrument_app(fastapi_app)
    if sqlalchemy_engine is not None:
        SQLAlchemyInstrumentor().instrument(engine=sqlalchemy_engine)
    return True
