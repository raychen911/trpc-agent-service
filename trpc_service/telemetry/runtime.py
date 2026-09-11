"""Application-scoped tracer runtime with exporter failure safety.

Design guarantees (Stage 6B1):

- The process-global ``TracerProvider`` is **never** set or replaced: every
  runtime owns its provider instance (``shutdown_on_exit=False`` so the SDK
  atexit hook can never block service shutdown).
- Disabled or closed runtimes hand out a shared ``NoOpTracer``; no network
  component is ever constructed.
- A trace pipeline plus (Stage 6C) one low-cardinality metrics pipeline:
  the meter provider is built in ``start()`` behind the same enable switch,
  exported through a fail-safe OTLP metric exporter and shut down inside
  the same bounded ``close()`` budget.  Disabled/closed runtimes hand out
  ``NoopMetricsRecorder`` — no network object is ever constructed.
- Every span is created through :func:`safe_span`, which records NOTHING but
  fixed attribute keys and never calls ``record_exception()``/
  ``set_status_on_exception`` (those serialize ``str(exc)`` and stack frames
  into span events).
- The exporter is wrapped in :class:`SafeSpanExporter`: export/flush/shutdown
  failures (including non-success results) are converted into fixed
  ``telemetry.*`` safe-log events and reported to the SDK as SUCCESS, so the
  SDK's own unredacted error logging never fires and business code is never
  affected.
- :meth:`TelemetryRuntime.close` bounds provider shutdown by the configured
  export timeout using a daemon thread; an overrun only logs
  ``telemetry.shutdown_timeout``.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
from contextlib import contextmanager
from typing import Final

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExportResult
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import NoOpTracer, SpanKind, StatusCode

from trpc_service.log.safe import safe_log

from .settings import TelemetrySettings

# --- Fixed, low-cardinality span names ------------------------------------
SPAN_GATEWAY_REQUEST: Final[str] = "gateway.request"
SPAN_WORKER_REQUEST: Final[str] = "worker.request"
SPAN_AGENT_TURN: Final[str] = "agent.turn"
SPAN_STATE_SESSION: Final[str] = "state.session"
SPAN_STATE_MEMORY: Final[str] = "state.memory"
SPAN_TOOL_EXECUTE: Final[str] = "tool.execute"
SPAN_CHANNEL_RECEIVE: Final[str] = "channel.receive"
SPAN_CHANNEL_REPLY: Final[str] = "channel.reply"

_DURATION_OPERATION_BY_SPAN: Final[dict[str, str]] = {
    SPAN_AGENT_TURN: "agent_turn",
    SPAN_TOOL_EXECUTE: "tool_execute",
    SPAN_STATE_SESSION: "session_backend",
    SPAN_STATE_MEMORY: "memory_backend",
}

# --- Fixed attribute keys ---------------------------------------------------
ATTR_STATUS: Final[str] = "status"
ATTR_ERROR_CODE: Final[str] = "error_code"
ATTR_EXCEPTION_TYPE: Final[str] = "exception.type"
ATTR_OPERATION: Final[str] = "operation"
ATTR_RESULT: Final[str] = "result"
ATTR_REPLY_COUNT: Final[str] = "reply_count"

STATUS_OK: Final[str] = "ok"
STATUS_ERROR: Final[str] = "error"

_TELEMETRY_LOGGER = logging.getLogger("trpc_service.telemetry")

_NOOP_TRACER: Final[NoOpTracer] = NoOpTracer()


class _DeferredTracer:
    """Stable tracer handle that becomes active only after runtime.start()."""

    def __init__(self, runtime: "TelemetryRuntime", name: str) -> None:
        self._runtime = runtime
        self._name = name

    def __getattr__(self, name: str):
        return getattr(self._runtime._active_tracer(self._name), name)

    def observe_duration(self, span_name: str, duration_ms: float, result: str) -> None:
        self._runtime._observe_span_duration(span_name, duration_ms, result)


def _safe_event(event: str) -> None:
    """Emit one fixed telemetry safe-log event; never raise."""
    try:
        safe_log(_TELEMETRY_LOGGER, logging.WARNING, event)
    except Exception:
        return None


class SafeSpanExporter:
    """Decorator turning any span exporter into a fail-safe one.

    Failures — exceptions *and* non-success results — are logged as fixed
    ``telemetry.*`` events and reported to the SDK as
    ``SpanExportResult.SUCCESS``, because the SDK's own handling of failures
    logs unredacted exception text/stack traces.  Nothing user-controlled
    ever enters the log line.
    """

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def export(self, spans) -> SpanExportResult:
        try:
            result = self._inner.export(spans)  # type: ignore[attr-defined]
        except Exception:
            _safe_event("telemetry.export_failed")
            return SpanExportResult.SUCCESS
        if result != SpanExportResult.SUCCESS:
            _safe_event("telemetry.export_failed")
        return SpanExportResult.SUCCESS

    def force_flush(self, timeout_millis: int | None = None) -> bool:
        try:
            inner_flush = getattr(self._inner, "force_flush", None)
            if inner_flush is not None:
                if _accepts_kwarg(inner_flush, "timeout_millis") and timeout_millis is not None:
                    inner_flush(timeout_millis=timeout_millis)
                else:
                    inner_flush()
        except Exception:
            _safe_event("telemetry.flush_failed")
        return True

    def shutdown(self, timeout_millis: int | None = None) -> bool:
        try:
            inner_shutdown = getattr(self._inner, "shutdown", None)
            if inner_shutdown is not None:
                if _accepts_kwarg(inner_shutdown, "timeout_millis") and timeout_millis is not None:
                    inner_shutdown(timeout_millis=timeout_millis)
                else:
                    inner_shutdown()
        except Exception:
            _safe_event("telemetry.shutdown_failed")
        return True


class SafeMetricExporter:
    """Fail-safe decorator for the OTLP metric exporter (Stage 6C).

    Identical contract as :class:`SafeSpanExporter`: export failures (any
    exception or non-success result) become fixed ``telemetry.*`` safe-log
    events and are reported to the SDK as SUCCESS so the SDK never logs
    unredacted transport errors; flush/shutdown never raise.
    """

    def __init__(self, inner: object) -> None:
        self._inner = inner

    @property
    def preferred_temporality(self):  # pragma: no cover - passthrough
        return getattr(self._inner, "preferred_temporality", {})

    @property
    def _preferred_temporality(self):  # read by the SDK reader constructor
        return getattr(self._inner, "_preferred_temporality", {})

    @property
    def preferred_aggregation(self):  # pragma: no cover - passthrough
        return getattr(self._inner, "preferred_aggregation", {})

    @property
    def _preferred_aggregation(self):
        return getattr(self._inner, "_preferred_aggregation", {})

    def export(self, metrics_data, timeout_millis: float = 10_000, **kwargs) -> object:
        from opentelemetry.sdk.metrics.export import MetricExportResult

        try:
            inner_export = self._inner.export  # type: ignore[attr-defined]
            if _accepts_kwarg(inner_export, "timeout_millis"):
                result = inner_export(metrics_data, timeout_millis=timeout_millis, **kwargs)
            else:
                result = inner_export(metrics_data)
        except Exception:
            _safe_event("telemetry.export_failed")
            return MetricExportResult.SUCCESS
        if result != MetricExportResult.SUCCESS:
            _safe_event("telemetry.export_failed")
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: int | None = None) -> bool:
        try:
            inner_flush = getattr(self._inner, "force_flush", None)
            if inner_flush is not None:
                if timeout_millis is not None and _accepts_kwarg(inner_flush, "timeout_millis"):
                    inner_flush(timeout_millis=timeout_millis)
                else:
                    inner_flush()
        except Exception:
            _safe_event("telemetry.flush_failed")
        return True

    def shutdown(self, timeout_millis: int | None = None) -> bool:
        try:
            inner_shutdown = getattr(self._inner, "shutdown", None)
            if inner_shutdown is not None:
                if timeout_millis is not None and _accepts_kwarg(inner_shutdown, "timeout_millis"):
                    inner_shutdown(timeout_millis=timeout_millis)
                else:
                    inner_shutdown()
        except Exception:
            _safe_event("telemetry.shutdown_failed")
        return True


def _accepts_kwarg(func: object, name: str) -> bool:
    try:
        spec = inspect.getfullargspec(func)  # type: ignore[arg-type]
    except TypeError:
        return False
    return name in (spec.args or ()) or bool(spec.varkw)


class TelemetryRuntime:
    """Owns one app-scoped ``TracerProvider``; safe when disabled/closed."""

    def __init__(self,
                 settings: TelemetrySettings,
                 *,
                 exporter: object | None = None,
                 metric_exporter: object | None = None) -> None:
        self._settings = settings
        self._exporter_override = exporter
        self._metric_exporter_override = metric_exporter
        self._provider: TracerProvider | None = None
        self._meter_provider = None
        self._metrics_reader = None
        self._metrics_recorder = None
        self._build_failed = False
        self._started = False
        self._closed = False
        self._lock = threading.Lock()
        self._tracers: dict[str, _DeferredTracer] = {}

    @property
    def settings(self) -> TelemetrySettings:
        return self._settings

    @property
    def enabled(self) -> bool:
        return self._settings.enabled and not self._closed and not self._build_failed

    # ------------------------------------------------------------------ build
    def _build(self) -> TracerProvider | None:
        if self._provider is not None or self._build_failed:
            return self._provider
        try:
            resource = Resource.create({"service.name": self._settings.service_name})
            provider = TracerProvider(
                sampler=ParentBased(TraceIdRatioBased(self._settings.sample_ratio)),
                resource=resource,
                shutdown_on_exit=False,
            )
            inner = self._exporter_override
            if inner is None:
                inner = OTLPSpanExporter(
                    endpoint=self._settings.otlp_endpoint,
                    timeout=self._settings.export_timeout_seconds,
                )
            processor = BatchSpanProcessor(SafeSpanExporter(inner))
            provider.add_span_processor(processor)
            self._provider = provider
            self._build_metrics(resource)
        except Exception:
            # A broken collector URL or SDK init failure must never break the
            # service: disable tracing for this process with one fixed event.
            self._build_failed = True
            self._provider = None
            _safe_event("telemetry.setup_failed")
        return self._provider

    def _build_metrics(self, resource) -> None:
        """Build the Stage 6C low-cardinality metrics pipeline.

        A metric pipeline failure only degrades observability (fixed event),
        never tracing or the business path.
        """
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        inner = self._metric_exporter_override
        if inner is None:
            endpoint = self._settings.otlp_endpoint or ""
            if endpoint.endswith("/traces"):
                metrics_url = endpoint[:-len("/traces")] + "/metrics"
            else:
                metrics_url = endpoint.rstrip("/") + "/v1/metrics"
            inner = OTLPMetricExporter(endpoint=metrics_url, timeout=self._settings.export_timeout_seconds)
        safe = SafeMetricExporter(inner)
        reader = PeriodicExportingMetricReader(
            safe,
            export_interval_millis=max(1000, int(self._settings.export_timeout_seconds * 1000)),
            export_timeout_millis=max(500, int(self._settings.export_timeout_seconds * 1000)),
        )
        provider = MeterProvider(metric_readers=[reader], resource=resource)
        self._metrics_reader = reader
        self._meter_provider = provider
        self._metrics_recorder = None

    def metrics_recorder(self):
        """Validated metrics facade; Noop while disabled/closed/unbuilt."""
        from .metrics import MetricsRecorder, NoopMetricsRecorder

        if not self.enabled or not self._started:
            return NoopMetricsRecorder()
        provider = self._meter_provider
        if provider is None:
            return NoopMetricsRecorder()
        if self._metrics_recorder is not None:
            return self._metrics_recorder
        try:
            meter = provider.get_meter("trpc_service.metrics")
        except Exception:
            return NoopMetricsRecorder()
        self._metrics_recorder = MetricsRecorder(meter, self._settings.service_name)
        return self._metrics_recorder

    def _observe_span_duration(self, span_name: str, duration_ms: float, result: str) -> None:
        operation = _DURATION_OPERATION_BY_SPAN.get(span_name)
        if operation is None:
            return
        try:
            from .metrics import METRIC_DURATION_MS

            self.metrics_recorder().observe(
                METRIC_DURATION_MS,
                duration_ms,
                operation=operation,
                result=result,
            )
        except Exception:
            return None

    def tracer(self, name: str):
        """Return a stable lazy tracer without constructing the provider."""
        if not self._settings.enabled or self._closed:
            return _NOOP_TRACER
        with self._lock:
            tracer = self._tracers.get(name)
            if tracer is None:
                tracer = _DeferredTracer(self, name)
                self._tracers[name] = tracer
            return tracer

    def _active_tracer(self, name: str):
        """Resolve a deferred handle against the currently started provider."""
        with self._lock:
            provider = self._provider if self._started and not self._closed else None
        if provider is None:
            return _NOOP_TRACER
        try:
            return provider.get_tracer(name)
        except Exception:
            return _NOOP_TRACER

    # -------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Pre-build the provider.  Never raises."""
        if not self._settings.enabled or self._closed:
            return None
        try:
            with self._lock:
                self._started = True
                self._build()
        except Exception:
            _safe_event("telemetry.setup_failed")
        return None

    async def close(self) -> None:
        """Bounded, idempotent, never-raising shutdown of the provider."""
        if self._closed:
            return None
        with self._lock:
            self._closed = True
            self._started = False
            provider = self._provider
            self._provider = None
            meter_provider = self._meter_provider
            self._meter_provider = None
            self._metrics_reader = None
            self._metrics_recorder = None
        if provider is None and meter_provider is None:
            return None
        budget = self._settings.export_timeout_seconds
        finished = threading.Event()

        def _shutdown() -> None:
            try:
                # Metrics flush/shutdown failures are swallowed by the safe
                # exporter; order: export last points, then traces.
                if meter_provider is not None:
                    meter_provider.shutdown()
            except Exception:
                _safe_event("telemetry.shutdown_failed")
            try:
                if provider is not None:
                    provider.shutdown()
            except Exception:
                _safe_event("telemetry.shutdown_failed")
            finally:
                finished.set()

        worker = threading.Thread(target=_shutdown, name="telemetry-shutdown", daemon=True)
        try:
            worker.start()
            loop = asyncio.get_running_loop()
            deadline = loop.time() + budget
            while not finished.is_set():
                remaining = deadline - loop.time()
                if remaining <= 0:
                    _safe_event("telemetry.shutdown_timeout")
                    return None
                await asyncio.sleep(min(0.01, remaining))
            worker.join(timeout=0)
        except Exception:
            _safe_event("telemetry.shutdown_failed")
        return None


@contextmanager
def safe_span(tracer, name: str, *, kind: SpanKind = SpanKind.INTERNAL, attributes=None, context=None):
    """Context manager for a fixed-name span with sanitized error handling.

    Yields the started span (already current in context) or ``None`` if the
    tracer could not start one.  On any exception the span records only the
    exception **type name** and an ``error`` status — never ``record_exception``
    (which would serialize the message and full stack into a span event).

    ``GeneratorExit`` is generator-close protocol, not a business failure:
    an ``aclosing()`` teardown after a consumer returned at a terminal event
    is the *golden path* of every streamed reply, so the span ends without
    error attributes or status (and still ends — cancellation rule).
    """
    if tracer is None:
        yield None
        return
    started_at = time.monotonic()
    try:
        span = tracer.start_span(name, context=context, kind=kind, attributes=attributes)
    except Exception:
        try:
            yield None
        finally:
            _observe_duration(tracer, name, started_at, STATUS_OK)
        return
    token = otel_context.attach(trace.set_span_in_context(span))
    ok = False
    metric_result = STATUS_OK
    try:
        yield span
        ok = True
    except GeneratorExit:
        # Stream closed by the consumer (early return / aclose after the
        # terminal event): end the span silently, no error labelling.
        raise
    except BaseException as exc:
        metric_result = STATUS_ERROR
        try:
            span.set_attribute(ATTR_EXCEPTION_TYPE, type(exc).__name__)
            span.set_attribute(ATTR_STATUS, STATUS_ERROR)
            span.set_status(StatusCode.ERROR)
        except Exception:
            pass
        raise
    finally:
        if ok:
            try:
                span.set_attribute(ATTR_STATUS, STATUS_OK)
            except Exception:
                pass
        try:
            otel_context.detach(token)
        except Exception:
            # Cross-context detach (async generators) must never break the
            # business path; module-level detach already swallows this.
            pass
        try:
            span.end()
        except Exception:
            pass
        _observe_duration(tracer, name, started_at, metric_result)


def _observe_duration(tracer, span_name: str, started_at: float, result: str) -> None:
    """Record one fixed operation duration without exposing span attributes."""
    try:
        observe = getattr(tracer, "observe_duration", None)
        if observe is not None:
            observe(span_name, max(0.0, (time.monotonic() - started_at) * 1000), result)
    except Exception:
        return None


__all__ = [
    "ATTR_ERROR_CODE",
    "ATTR_EXCEPTION_TYPE",
    "ATTR_OPERATION",
    "ATTR_REPLY_COUNT",
    "ATTR_RESULT",
    "ATTR_STATUS",
    "SafeMetricExporter",
    "SafeSpanExporter",
    "SPAN_AGENT_TURN",
    "SPAN_CHANNEL_RECEIVE",
    "SPAN_CHANNEL_REPLY",
    "SPAN_GATEWAY_REQUEST",
    "SPAN_STATE_MEMORY",
    "SPAN_STATE_SESSION",
    "SPAN_TOOL_EXECUTE",
    "SPAN_WORKER_REQUEST",
    "STATUS_ERROR",
    "STATUS_OK",
    "TelemetryRuntime",
    "safe_span",
]
