"""Sanitized process telemetry and durable queue measurements.

Only fixed operation/outcome labels reach metrics. Message bodies, tool arguments,
SQL text, connection URLs and exception messages are never exported here.
"""

import functools
import os
import time
import uuid
from contextlib import contextmanager
from datetime import timezone

from opentelemetry import metrics, trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from sqlalchemy import func, select

from .storage import MetricsRegistry

registry = MetricsRegistry()
_tracer = trace.get_tracer("trpc_service")
_meter = metrics.get_meter("trpc_service")
_providers = []
_instruments = {}


def configure(service, database=None, *, span_exporter=None, metric_reader=None, sdk_traces=False):
    """Own platform providers; CLI processes opt into sanitized native SDK spans."""
    global _tracer, _meter
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    directory = os.environ.get("TRPC_TRACE_DIRECTORY")
    if _providers or not (endpoint or directory or span_exporter or metric_reader):
        return
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

    resource = Resource.create({"service.name": service, "service.instance.id": uuid.uuid4().hex})
    provider = TracerProvider(resource=resource)
    if span_exporter is None and directory:
        from .file_export import FileSpanExporter
        span_exporter = FileSpanExporter(directory, service)
    if span_exporter is None and endpoint:
        span_exporter = OTLPSpanExporter(endpoint=endpoint.rstrip("/") + "/v1/traces", timeout=5)
    if span_exporter is not None:
        provider.add_span_processor((SimpleSpanProcessor if directory else BatchSpanProcessor)(span_exporter))
    _tracer = provider.get_tracer("trpc_service")
    if metric_reader is None and endpoint:
        metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=endpoint.rstrip("/") + "/v1/metrics",
                                                                         timeout=5),
                                                      export_interval_millis=10000)
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader] if metric_reader else [])
    _meter = meter_provider.get_meter("trpc_service")
    _instruments.clear()
    _providers.extend([provider, meter_provider])
    if sdk_traces:
        # OTel permits installing its global provider once per process. Only CLI
        # entrypoints opt in; embedded hosts keep control of their global provider.
        from .sdk_tracing import SanitizedSDKExporter
        sdk_provider = TracerProvider(resource=resource)
        if directory:
            from .file_export import FileSpanExporter
            sdk_exporter = FileSpanExporter(directory, service + "-sdk")
        elif endpoint:
            sdk_exporter = OTLPSpanExporter(endpoint=endpoint.rstrip("/") + "/v1/traces", timeout=5)
        else:
            raise ValueError("SDK tracing requires a configured file or OTLP destination")
        sdk_provider.add_span_processor(BatchSpanProcessor(SanitizedSDKExporter(sdk_exporter)))
        trace.set_tracer_provider(sdk_provider)
        _providers.append(sdk_provider)
    if database is not None:

        def observe_queues(options):
            try:
                return [
                    metrics.Observation(value, {
                        "queue": queue,
                        "measure": measure
                    }) for queue, values in queue_snapshot(database).items() for measure, value in values.items()
                ]
            except Exception:
                return [metrics.Observation(1, {"queue": "database", "measure": "unavailable"})]

        _meter.create_observable_gauge("trpc.queue", callbacks=[observe_queues])

        def observe_pool(options):
            pool = database.engine.pool
            return [
                metrics.Observation(getattr(pool, method)(), {"state": method})
                for method in ("checkedin", "checkedout", "overflow") if hasattr(pool, method)
            ]

        _meter.create_observable_gauge("trpc.sql.pool", callbacks=[observe_pool])


def shutdown():
    global _tracer, _meter
    for provider in _providers:
        provider.shutdown()
    _providers.clear()
    _instruments.clear()
    _tracer = trace.get_tracer("trpc_service")
    _meter = metrics.get_meter("trpc_service")


def count(name, value=1, **labels):
    registry.increment(name, value, **labels)
    key = (name, "count")
    if key not in _instruments:
        _instruments[key] = _meter.create_counter(name)
    instrument = _instruments[key]
    instrument.add(value, labels)


def observe(name, value, **labels):
    registry.observe(name, value, **labels)
    key = (name, "histogram")
    if key not in _instruments:
        _instruments[key] = _meter.create_histogram(name)
    instrument = _instruments[key]
    instrument.record(value, labels)


def carrier():
    result = {}
    TraceContextTextMapPropagator().inject(result)
    return result


def current_trace_id():
    """Persist the active trace identity instead of an unrelated random identifier."""
    context = trace.get_current_span().get_span_context()
    return f"{context.trace_id:032x}" if context.is_valid else uuid.uuid4().hex


@contextmanager
def operation(name, *, parent=None, attributes=None):
    from .logging import context as log_context, emit
    started, outcome = time.perf_counter(), "ok"
    context = TraceContextTextMapPropagator().extract(parent) if parent else None
    with _tracer.start_as_current_span(name, context=context, record_exception=False,
                                       set_status_on_exception=False) as span:
        for key, value in (attributes or {}).items():
            span.set_attribute(key, value)
        with log_context(**{**(attributes or {}), "stage": name}):
            error_type = (attributes or {}).get("error.type")
            if error_type:
                outcome = "error"
                span.set_status(trace.Status(trace.StatusCode.ERROR))
            try:
                yield span
            except BaseException as error:
                outcome = "error"
                error_type = type(error).__name__
                span.set_attribute("error.type", error_type)
                span.set_status(trace.Status(trace.StatusCode.ERROR))
                raise
            finally:
                elapsed = (time.perf_counter() - started) * 1000
                count("trpc.operation.calls", operation=name, outcome=outcome)
                observe("trpc.operation.duration_ms", elapsed, operation=name, outcome=outcome)
                emit("operation." + outcome, level=40 if error_type else 20, latency_ms=elapsed, error_type=error_type)


def measured(name):

    def decorate(function):

        @functools.wraps(function)
        async def invoke(*args, **kwargs):
            with operation(name):
                return await function(*args, **kwargs)

        return invoke

    return decorate


def queue_snapshot(database):
    from trpc_service.persistence.models import InboundMessageRow, OutboxMessageRow, PostTurnTaskRow, utcnow
    from trpc_service.persistence.models import PendingActionRow, ActionJobRow, ActionNoticeRow
    now = utcnow()
    result = {}
    for name, model, terminal in (("inbox", InboundMessageRow, {"succeeded", "failed_final", "unknown_outcome"}),
                                  ("outbox", OutboxMessageRow, {"delivered",
                                                                "dead_letter"}), ("post_turn", PostTurnTaskRow,
                                                                                  {"succeeded", "failed"}),
                                  ("actions", PendingActionRow,
                                   {"succeeded", "failed", "cancelled", "rejected", "expired",
                                    "unknown_outcome"}), ("action_jobs", ActionJobRow,
                                                          {"succeeded", "failed", "cancelled",
                                                           "unknown_outcome"}), ("action_notices", ActionNoticeRow, {
                                                               "delivered", "superseded", "dead_letter",
                                                               "unknown_outcome", "authorization_revoked"
                                                           })):
        with database.sessions() as session:
            rows = session.execute(
                select(model.status, func.count(), func.min(model.created_at)).group_by(model.status))
            values = {"pending": 0, "oldest_age_seconds": 0.0, "failed": 0, "active": 0}
            for status, total, oldest in rows:
                if status not in terminal:
                    values["pending"] += total
                    age = (now - oldest.replace(tzinfo=timezone.utc)).total_seconds()
                    values["oldest_age_seconds"] = max(values["oldest_age_seconds"], age)
                elif status not in {"succeeded", "delivered", "superseded", "rejected", "cancelled", "expired"}:
                    values["failed"] += total
                if status in {"running", "processing", "dispatching", "executing", "sending"}:
                    values["active"] += total
            result[name] = values
    return result
