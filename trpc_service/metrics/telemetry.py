"""Low-cardinality platform metrics and OpenTelemetry lifecycle."""

from contextlib import AbstractContextManager
from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import metrics as otel_metrics
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import Span
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from trpc_service.log.redaction import SensitiveDataRedactor

_TRACE_ATTRIBUTES = frozenset({
    "agent.name",
    "channel.type",
    "config.version",
    "error.type",
    "model.provider",
    "request.id",
    "result",
    "retry.count",
    "session.id_hash",
    "storage.operation",
    "tenant.id",
    "tool.name",
})


class PlatformTelemetry:
    """Own process-local instruments behind a provider-neutral project API."""

    def __init__(
        self,
        *,
        service_name: str,
        environment: str,
        node_role: str,
        otlp_endpoint: str | None,
    ) -> None:
        resource = Resource.create({
            "service.name": service_name,
            "deployment.environment.name": environment,
            "service.instance.role": node_role,
        })
        self._redactor = SensitiveDataRedactor()
        self._trace_provider = TracerProvider(resource=resource)
        self._meter_provider: MeterProvider | None = None
        self._otel_governance = None
        self._otel_agent_requests = None
        self._otel_agent_duration = None
        self._otel_model_tokens = None
        self._otel_active_executions = None
        self._otel_tool_calls = None
        self._otel_tool_duration = None
        self._otel_storage_operations = None
        self._otel_storage_duration = None
        self._otel_im_deliveries = None
        self._otel_im_duration = None
        self._otel_http_requests = None
        self._otel_http_duration = None
        if otlp_endpoint is not None:
            base_endpoint = otlp_endpoint.rstrip("/")
            self._trace_provider.add_span_processor(
                BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{base_endpoint}/v1/traces")))
            metric_reader = PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=f"{base_endpoint}/v1/metrics"),
                export_interval_millis=5_000,
            )
            self._meter_provider = MeterProvider(
                resource=resource,
                metric_readers=(metric_reader, ),
            )
            meter = self._meter_provider.get_meter("trpc_service")
            self._otel_governance = meter.create_counter("trpc_governance_decisions")
            self._otel_agent_requests = meter.create_counter("trpc_agent_requests")
            self._otel_agent_duration = meter.create_histogram(
                "trpc_agent_execution_duration",
                unit="s",
            )
            self._otel_model_tokens = meter.create_counter("trpc_model_tokens")
            self._otel_active_executions = meter.create_up_down_counter(
                "trpc_agent_active_executions")
            self._otel_tool_calls = meter.create_counter("trpc_tool_calls")
            self._otel_tool_duration = meter.create_histogram(
                "trpc_tool_call_duration",
                unit="s",
            )
            self._otel_storage_operations = meter.create_counter("trpc_storage_operations")
            self._otel_storage_duration = meter.create_histogram(
                "trpc_storage_operation_duration",
                unit="s",
            )
            self._otel_im_deliveries = meter.create_counter("trpc_im_deliveries")
            self._otel_im_duration = meter.create_histogram(
                "trpc_im_delivery_duration",
                unit="s",
            )
            self._otel_http_requests = meter.create_counter("trpc_http_requests")
            self._otel_http_duration = meter.create_histogram(
                "trpc_http_request_duration",
                unit="s",
            )
            # tRPC-Agent-Python uses the global API. Configure it once at process
            # composition so its model and Tool spans share this exporter.
            trace.set_tracer_provider(self._trace_provider)
            otel_metrics.set_meter_provider(self._meter_provider)
        self._tracer = self._trace_provider.get_tracer("trpc_service")
        self._registry = CollectorRegistry(auto_describe=True)
        self._runtime = Gauge(
            "trpc_runtime_info",
            "Static identity of this service process.",
            ("service_name", "environment", "node_role"),
            registry=self._registry,
        )
        self._runtime.labels(service_name, environment, node_role).set(1)
        self._governance = Counter(
            "trpc_governance_decisions_total",
            "Tenant governance decisions.",
            ("action", "reason_code"),
            registry=self._registry,
        )
        self._agent_requests = Counter(
            "trpc_agent_requests_total",
            "Agent execution outcomes.",
            ("channel_type", "result"),
            registry=self._registry,
        )
        self._agent_duration = Histogram(
            "trpc_agent_execution_duration_seconds",
            "End-to-end Agent execution duration.",
            ("result", "model_provider"),
            registry=self._registry,
        )
        self._model_tokens = Counter(
            "trpc_model_tokens_total",
            "Model tokens reported by the provider.",
            ("direction", "model_provider"),
            registry=self._registry,
        )
        self._active_executions = Gauge(
            "trpc_agent_active_executions",
            "Agent executions currently owned by this process.",
            registry=self._registry,
        )
        self._http_requests = Counter(
            "trpc_http_requests_total",
            "HTTP requests handled by the Gateway.",
            ("method", "status_class"),
            registry=self._registry,
        )
        self._http_duration = Histogram(
            "trpc_http_request_duration_seconds",
            "Gateway HTTP request duration.",
            ("method", "status_class"),
            registry=self._registry,
        )
        self._tool_calls = Counter(
            "trpc_tool_calls_total",
            "Governed Tool invocation outcomes.",
            ("tool_name", "result"),
            registry=self._registry,
        )
        self._tool_duration = Histogram(
            "trpc_tool_call_duration_seconds",
            "Governed Tool invocation duration.",
            ("tool_name", "result"),
            registry=self._registry,
        )
        self._storage_operations = Counter(
            "trpc_storage_operations_total",
            "Storage adapter operation outcomes.",
            ("operation", "result"),
            registry=self._registry,
        )
        self._storage_duration = Histogram(
            "trpc_storage_operation_duration_seconds",
            "Storage adapter operation duration.",
            ("operation", "result"),
            registry=self._registry,
        )
        self._im_deliveries = Counter(
            "trpc_im_deliveries_total",
            "IM delivery outcomes.",
            ("channel_type", "result"),
            registry=self._registry,
        )
        self._im_duration = Histogram(
            "trpc_im_delivery_duration_seconds",
            "IM delivery duration.",
            ("channel_type", "result"),
            registry=self._registry,
        )

    def record_governance(self, action: str, reason_code: str) -> None:
        """Count a stable policy result without tenant or request labels."""

        self._governance.labels(action, reason_code).inc()
        if self._otel_governance is not None:
            self._otel_governance.add(1, {"action": action, "reason_code": reason_code})

    def record_agent_execution(
        self,
        *,
        channel_type: str,
        model_provider: str,
        result: str,
        duration_seconds: float,
        input_tokens: int = 0,
        output_tokens: int = 0,
    ) -> None:
        """Record one terminal execution and provider-reported token usage."""

        self._agent_requests.labels(channel_type, result).inc()
        self._agent_duration.labels(result, model_provider).observe(max(duration_seconds, 0))
        execution_attributes = {
            "channel.type": channel_type,
            "result": result,
            "model.provider": model_provider,
        }
        if self._otel_agent_requests is not None:
            self._otel_agent_requests.add(1, execution_attributes)
        if self._otel_agent_duration is not None:
            self._otel_agent_duration.record(max(duration_seconds, 0), execution_attributes)
        if input_tokens > 0:
            self._model_tokens.labels("input", model_provider).inc(input_tokens)
            if self._otel_model_tokens is not None:
                self._otel_model_tokens.add(input_tokens, {
                    "direction": "input",
                    "model.provider": model_provider,
                })
        if output_tokens > 0:
            self._model_tokens.labels("output", model_provider).inc(output_tokens)
            if self._otel_model_tokens is not None:
                self._otel_model_tokens.add(output_tokens, {
                    "direction": "output",
                    "model.provider": model_provider,
                })

    def execution_started(self) -> None:
        self._active_executions.inc()
        if self._otel_active_executions is not None:
            self._otel_active_executions.add(1)

    def execution_finished(self) -> None:
        self._active_executions.dec()
        if self._otel_active_executions is not None:
            self._otel_active_executions.add(-1)

    def record_http(self, method: str, status_code: int, duration_seconds: float) -> None:
        """Record bounded HTTP dimensions; paths and identities are excluded."""

        status_class = f"{max(status_code, 0) // 100}xx"
        normalized_method = method.upper()
        duration = max(duration_seconds, 0)
        self._http_requests.labels(normalized_method, status_class).inc()
        self._http_duration.labels(normalized_method, status_class).observe(duration)
        attributes = {
            "http.request.method": normalized_method,
            "http.response.status_class": status_class
        }
        if self._otel_http_requests is not None:
            self._otel_http_requests.add(1, attributes)
        if self._otel_http_duration is not None:
            self._otel_http_duration.record(duration, attributes)

    def record_tool(
        self,
        *,
        tool_name: str,
        result: str,
        duration_seconds: float,
    ) -> None:
        """Record a configured Tool name and stable result, never its arguments."""

        duration = max(duration_seconds, 0)
        self._tool_calls.labels(tool_name, result).inc()
        self._tool_duration.labels(tool_name, result).observe(duration)
        attributes = {"tool.name": tool_name, "result": result}
        if self._otel_tool_calls is not None:
            self._otel_tool_calls.add(1, attributes)
        if self._otel_tool_duration is not None:
            self._otel_tool_duration.record(duration, attributes)

    def record_storage(
        self,
        *,
        operation: str,
        result: str,
        duration_seconds: float,
    ) -> None:
        """Record one storage boundary without backend addresses or identities."""

        duration = max(duration_seconds, 0)
        self._storage_operations.labels(operation, result).inc()
        self._storage_duration.labels(operation, result).observe(duration)
        attributes = {"storage.operation": operation, "result": result}
        if self._otel_storage_operations is not None:
            self._otel_storage_operations.add(1, attributes)
        if self._otel_storage_duration is not None:
            self._otel_storage_duration.record(duration, attributes)

    def record_im_delivery(
        self,
        *,
        channel_type: str,
        result: str,
        duration_seconds: float,
    ) -> None:
        """Record a channel delivery with a configured, bounded channel type."""

        duration = max(duration_seconds, 0)
        self._im_deliveries.labels(channel_type, result).inc()
        self._im_duration.labels(channel_type, result).observe(duration)
        attributes = {"channel.type": channel_type, "result": result}
        if self._otel_im_deliveries is not None:
            self._otel_im_deliveries.add(1, attributes)
        if self._otel_im_duration is not None:
            self._otel_im_duration.record(duration, attributes)

    def render_prometheus(self) -> bytes:
        """Render this process registry for the loopback-local scraper."""

        return generate_latest(self._registry)

    @property
    def prometheus_content_type(self) -> str:
        return CONTENT_TYPE_LATEST

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, object] | None = None,
        context: otel_context.Context | None = None,
    ) -> AbstractContextManager[Span]:
        """Start a span after dropping attributes outside the safe schema."""

        safe: dict[str, Any] = {}
        for key, value in (attributes or {}).items():
            if key in _TRACE_ATTRIBUTES and isinstance(value, (str, bool, int, float)):
                safe[key] = (self._redactor.redact_text(value, redact_pii=True) if isinstance(
                    value, str) else value)
        return self._tracer.start_as_current_span(
            name,
            context=context,
            attributes=safe,
            # Automatic exception events contain the original message and
            # stack. Callers record only stable error.type/result attributes.
            record_exception=False,
            set_status_on_exception=False,
        )

    @staticmethod
    def inject_context() -> dict[str, str]:
        """Serialize W3C propagation fields for a durable queue snapshot."""

        carrier: dict[str, str] = {}
        propagate.inject(carrier)
        return carrier

    @staticmethod
    def extract_context(carrier: dict[str, str]) -> otel_context.Context:
        """Validate and extract W3C fields on the consuming Worker."""

        return propagate.extract(carrier)

    @staticmethod
    def current_trace_id() -> str:
        """Return the active OTel trace ID as fixed-width lowercase hex."""

        trace_id = trace.get_current_span().get_span_context().trace_id
        return f"{trace_id:032x}"

    def shutdown(self) -> None:
        """Flush pending spans without coupling callers to the OTel SDK."""

        self._trace_provider.shutdown()
        if self._meter_provider is not None:
            self._meter_provider.shutdown()
