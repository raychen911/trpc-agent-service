"""Stage 6B1 minimal cross-process tracing (OpenTelemetry).

Public surface used by product wiring:

- :class:`TelemetrySettings` / :class:`TelemetryRuntime` — strict config and
  one app-scoped provider per service; nothing global is replaced.
- :func:`inject_traceparent` / :func:`extract_traceparent` — W3C
  ``traceparent`` only (no baggage).
- :class:`TraceRequestMiddleware` — fixed-name SERVER span per HTTP request
  with ``/health`` excluded.
- :func:`instrument_session_service` / :func:`instrument_memory_service` —
  transparent proxies creating ``state.session`` / ``state.memory`` spans.
- :func:`traced_tool_function` / :func:`traced_tool_execution` — the shared
  ``tool.execute`` boundary for normal and approved tool runs.

All span names and attribute keys are fixed, low-cardinality constants
exported below; secrets/body text never belong in them.
"""

from .asgi import TraceRequestMiddleware
from .propagation import extract_traceparent, inject_traceparent
from .runtime import (
    ATTR_ERROR_CODE,
    ATTR_EXCEPTION_TYPE,
    ATTR_OPERATION,
    ATTR_REPLY_COUNT,
    ATTR_RESULT,
    ATTR_STATUS,
    SPAN_AGENT_TURN,
    SPAN_CHANNEL_RECEIVE,
    SPAN_CHANNEL_REPLY,
    SPAN_GATEWAY_REQUEST,
    SPAN_STATE_MEMORY,
    SPAN_STATE_SESSION,
    SPAN_TOOL_EXECUTE,
    SPAN_WORKER_REQUEST,
    STATUS_ERROR,
    STATUS_OK,
    SafeSpanExporter,
    TelemetryRuntime,
    safe_span,
)
from .sdk_services import instrument_memory_service, instrument_session_service
from .settings import (
    TRPC_TRACE_ENABLED_ENV,
    TRPC_TRACE_EXPORT_TIMEOUT_SECONDS_ENV,
    TRPC_TRACE_OTLP_ENDPOINT_ENV,
    TRPC_TRACE_SAMPLE_RATIO_ENV,
    TelemetryConfigurationError,
    TelemetrySettings,
)
from .tool import (
    ERROR_CODE_TENANT_CONFIGURATION,
    traced_tool_execution,
    traced_tool_function,
    tracer_for,
)

__all__ = [
    "ATTR_ERROR_CODE",
    "ATTR_EXCEPTION_TYPE",
    "ATTR_OPERATION",
    "ATTR_REPLY_COUNT",
    "ATTR_RESULT",
    "ATTR_STATUS",
    "ERROR_CODE_TENANT_CONFIGURATION",
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
    "TRPC_TRACE_ENABLED_ENV",
    "TRPC_TRACE_EXPORT_TIMEOUT_SECONDS_ENV",
    "TRPC_TRACE_OTLP_ENDPOINT_ENV",
    "TRPC_TRACE_SAMPLE_RATIO_ENV",
    "SafeSpanExporter",
    "TelemetryConfigurationError",
    "TelemetryRuntime",
    "TelemetrySettings",
    "TraceRequestMiddleware",
    "extract_traceparent",
    "inject_traceparent",
    "instrument_memory_service",
    "instrument_session_service",
    "safe_span",
    "traced_tool_execution",
    "traced_tool_function",
    "tracer_for",
]
