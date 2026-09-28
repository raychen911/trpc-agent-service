import json
from pathlib import Path

import httpx
import pytest

from trpc_service.config import Settings
from trpc_service.metrics import PlatformTelemetry
from trpc_service.metrics import telemetry as telemetry_module
from tests.conftest import create_test_app


def test_grafana_dashboard_keeps_sparse_tool_calls_visible() -> None:
    dashboard_path = (Path(__file__).parents[1] / "trpc_service" / "config" / "observability" /
                      "grafana" / "dashboards" / "agent-platform.json")
    dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
    panels = {panel["title"]: panel for panel in dashboard["panels"]}

    assert {
        "核心健康状态",
        "Agent 与模型",
        "Tool、MCP 与治理",
        "通道与基础设施",
    }.issubset(panels)
    assert all(panels[title]["type"] == "row" for title in {
        "核心健康状态",
        "Agent 与模型",
        "Tool、MCP 与治理",
        "通道与基础设施",
    })
    assert "Tool 调用明细（当前时间范围）" in panels
    tool_expressions = [target["expr"] for target in panels["Tool 调用明细（当前时间范围）"]["targets"]]
    assert any("increase(trpc_tool_calls_total[$__range])" in expr for expr in tool_expressions)
    assert all("[5m]" not in expr for expr in tool_expressions)
    success_expression = panels["请求成功率"]["targets"][0]["expr"]
    assert "or vector(0)" in success_expression
    assert {
        "请求成功率",
        "当前执行中",
        "Tool 调用总数",
        "Tool P95 延迟",
        "模型 Token 消耗",
        "IM 投递结果",
    }.issubset(panels)


def test_platform_metrics_expose_low_cardinality_governance_and_agent_results() -> None:
    telemetry = PlatformTelemetry(
        service_name="trpc-agent-service",
        environment="test",
        node_role="worker",
        otlp_endpoint=None,
    )

    telemetry.record_governance("deny", "PRINCIPAL_DENIED")
    telemetry.record_agent_execution(
        channel_type="wecom",
        model_provider="bailian_openai",
        result="succeeded",
        duration_seconds=0.25,
        input_tokens=12,
        output_tokens=4,
    )
    output = telemetry.render_prometheus().decode()

    assert ('trpc_governance_decisions_total{action="deny",'
            'reason_code="PRINCIPAL_DENIED"} 1.0' in output)
    assert ('trpc_agent_requests_total{channel_type="wecom",'
            'result="succeeded"} 1.0' in output)
    assert ('trpc_model_tokens_total{direction="input",'
            'model_provider="bailian_openai"} 12.0' in output)
    assert "tenant_id" not in output
    assert "session_id" not in output


def test_trace_context_propagates_without_sensitive_attributes() -> None:
    telemetry = PlatformTelemetry(
        service_name="trpc-agent-service",
        environment="test",
        node_role="api",
        otlp_endpoint=None,
    )

    with telemetry.start_span(
            "channel.callback",
            attributes={
                "tenant.id": "tenant-1",
                "channel.type": "wecom",
                "agent.name": "agent-sk-must-not-enter-trace",
                "api_key": "sk-must-not-enter-trace",
            },
    ) as parent:
        carrier = telemetry.inject_context()
        parent_trace_id = parent.get_span_context().trace_id

    with telemetry.start_span(
            "worker.execute",
            context=telemetry.extract_context(carrier),
    ) as child:
        assert child.get_span_context().trace_id == parent_trace_id
        assert "api_key" not in child.attributes

    assert parent.attributes["agent.name"] == "agent-[REDACTED_SECRET]"

    assert carrier["traceparent"].startswith("00-")
    assert "sk-must-not-enter-trace" not in str(carrier)

    with pytest.raises(RuntimeError):
        with telemetry.start_span("safe.error") as failed:
            raise RuntimeError("provider leaked sk-example-secret-123456")
    assert failed.events == ()


def test_platform_metrics_cover_tool_storage_im_and_gateway_boundaries() -> None:
    """Every main execution boundary exposes bounded operational dimensions."""

    telemetry = PlatformTelemetry(
        service_name="trpc-agent-service",
        environment="test",
        node_role="worker",
        otlp_endpoint=None,
    )

    telemetry.record_tool(tool_name="ticket.read", result="success", duration_seconds=0.01)
    telemetry.record_storage(
        operation="session.load",
        result="success",
        duration_seconds=0.02,
    )
    telemetry.record_im_delivery(
        channel_type="wecom",
        result="success",
        duration_seconds=0.03,
    )
    telemetry.record_http("post", 202, 0.04)
    output = telemetry.render_prometheus().decode()

    assert 'trpc_tool_calls_total{result="success",tool_name="ticket.read"} 1.0' in output
    assert ('trpc_storage_operations_total{operation="session.load",result="success"} 1.0'
            in output)
    assert ('trpc_im_deliveries_total{channel_type="wecom",result="success"} 1.0' in output)
    assert 'trpc_http_requests_total{method="POST",status_class="2xx"} 1.0' in output


@pytest.mark.anyio
async def test_metrics_endpoint_exposes_application_registry() -> None:
    app = create_test_app(
        Settings(
            _env_file=None,
            database_url="sqlite+aiosqlite:///:memory:",
            auto_create_schema=True,
        ))
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/metrics")

    await app.state.engine.dispose()
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "trpc_runtime_info" in response.text


def test_platform_telemetry_forwards_every_signal_to_configured_otlp(
    monkeypatch: pytest.MonkeyPatch, ) -> None:
    """An OTLP configuration exports the same bounded facts as Prometheus."""

    class Instrument:

        def __init__(self) -> None:
            self.values: list[tuple[object, object]] = []

        def add(self, value: object, attributes: object = None) -> None:
            self.values.append((value, attributes))

        def record(self, value: object, attributes: object = None) -> None:
            self.values.append((value, attributes))

    class Meter:

        def __init__(self) -> None:
            self.instruments: list[Instrument] = []

        def _create(self, *args: object, **kwargs: object) -> Instrument:
            del args, kwargs
            instrument = Instrument()
            self.instruments.append(instrument)
            return instrument

        def create_counter(self, *args: object, **kwargs: object) -> Instrument:
            return self._create(*args, **kwargs)

        def create_histogram(self, *args: object, **kwargs: object) -> Instrument:
            return self._create(*args, **kwargs)

        def create_up_down_counter(self, *args: object, **kwargs: object) -> Instrument:
            return self._create(*args, **kwargs)

    class MeterProvider:

        def __init__(self, **kwargs: object) -> None:
            self.options = kwargs
            self.meter = Meter()
            self.closed = False
            meter_providers.append(self)

        def get_meter(self, name: str) -> Meter:
            assert name == "trpc_service"
            return self.meter

        def shutdown(self) -> None:
            self.closed = True

    class TraceProvider:

        def __init__(self, **kwargs: object) -> None:
            self.options = kwargs
            self.processors: list[object] = []
            self.closed = False
            trace_providers.append(self)

        def add_span_processor(self, processor: object) -> None:
            self.processors.append(processor)

        def get_tracer(self, name: str) -> object:
            assert name == "trpc_service"
            return object()

        def shutdown(self) -> None:
            self.closed = True

    meter_providers: list[MeterProvider] = []
    trace_providers: list[TraceProvider] = []

    monkeypatch.setattr(telemetry_module, "TracerProvider", TraceProvider)
    monkeypatch.setattr(telemetry_module, "MeterProvider", MeterProvider)
    monkeypatch.setattr(telemetry_module, "BatchSpanProcessor", lambda exporter: exporter)
    monkeypatch.setattr(telemetry_module, "OTLPSpanExporter", lambda **kwargs: kwargs)
    monkeypatch.setattr(telemetry_module, "OTLPMetricExporter", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        telemetry_module,
        "PeriodicExportingMetricReader",
        lambda exporter, **kwargs: (exporter, kwargs),
    )
    monkeypatch.setattr(telemetry_module.trace, "set_tracer_provider", lambda provider: None)
    monkeypatch.setattr(telemetry_module.otel_metrics, "set_meter_provider", lambda provider: None)

    telemetry = PlatformTelemetry(
        service_name="trpc-agent-service",
        environment="test",
        node_role="worker",
        otlp_endpoint="http://collector/",
    )
    telemetry.record_governance("allow", "POLICY_ALLOWED")
    telemetry.record_agent_execution(
        channel_type="feishu",
        model_provider="bailian_openai",
        result="succeeded",
        duration_seconds=-1,
        input_tokens=3,
        output_tokens=2,
    )
    telemetry.record_agent_execution(
        channel_type="feishu",
        model_provider="bailian_openai",
        result="empty",
        duration_seconds=0,
    )
    telemetry.execution_started()
    telemetry.execution_finished()
    telemetry.record_http("get", -1, -1)
    telemetry.record_tool(tool_name="time.now", result="success", duration_seconds=-1)
    telemetry.record_storage(operation="session.load", result="success", duration_seconds=-1)
    telemetry.record_im_delivery(channel_type="feishu", result="success", duration_seconds=-1)
    telemetry.shutdown()

    assert meter_providers[0].closed
    assert trace_providers[0].closed
    assert all(instrument.values for instrument in meter_providers[0].meter.instruments)
