"""Reuse SDK-created spans through public OTel APIs with a content-free export."""

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter
from opentelemetry.trace import Status

_operations = {"run_runner", "run_runner_cancelled", "run_agent", "execute_tool", "call_llm"}
_tokens = {
    "gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens", "gen_ai.usage.cache_read_input_tokens",
    "gen_ai.usage.cache_creation_input_tokens"
}


class SanitizedSDKExporter(SpanExporter):
    """Strip SDK bodies, state, event/exception text and dynamic span names before I/O."""

    def __init__(self, downstream):
        self.downstream = downstream

    def export(self, spans):
        safe = []
        for span in spans:
            scope = span.instrumentation_scope
            if scope is None or scope.name != "trpc.python.agent":
                continue
            attrs = span.attributes or {}
            operation = attrs.get("gen_ai.operation.name")
            operation = operation if operation in _operations else "internal"
            fields = {"gen_ai.operation.name": operation}
            fields.update({
                key: value
                for key, value in attrs.items() if key in _tokens and isinstance(value, int) and value >= 0
            })
            safe.append(
                ReadableSpan(name="sdk." + operation,
                             context=span.context,
                             parent=span.parent,
                             resource=span.resource,
                             attributes=fields,
                             events=(),
                             links=(),
                             kind=span.kind,
                             status=Status(span.status.status_code),
                             start_time=span.start_time,
                             end_time=span.end_time,
                             instrumentation_scope=scope))
        return self.downstream.export(safe)

    def shutdown(self):
        self.downstream.shutdown()

    def force_flush(self, timeout_millis=30000):
        return self.downstream.force_flush(timeout_millis)
