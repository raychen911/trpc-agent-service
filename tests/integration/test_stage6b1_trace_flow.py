"""Stage 6B1 Task 2 Step 4 — cross-application trace proof over the real stack.

Two scenarios, both exporting through the REAL OTLP/HTTP exporter path into a
minimal in-test collector (protobuf decode, no vendor server):

A. Console full chain across real OS processes: the ServiceTopology Gateway
   and Worker subprocesses run with ``TRPC_TRACE_*`` enabled; one console
   request must produce one trace id containing
   ``gateway.request -> worker.request (CLIENT, real TCP) -> worker.request
   (SERVER) -> agent.turn -> state.session -> tool.execute`` through the real
   model, with ``get_current_time`` actually invoked.

B. WeCom channel: the gateway-side tracing runtime lives in the test process
   (``channel.receive``/``channel.reply`` plus the worker CLIENT span with
   injected ``traceparent``) while the downstream spans come from the real
   Worker subprocess — same trace id across the process boundary.

Finally every span the collector ever received is scanned: sentinel tokens
embedded in user-visible payloads (message text and external user/message
ids) must appear in no span name or attribute, span names and attribute keys
must stay inside the fixed low-cardinality sets, and /health traffic must
have produced nothing.
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import AsyncMock, MagicMock
from urllib.request import Request, urlopen

import pytest
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2
from sqlalchemy.ext.asyncio import create_async_engine

from trpc_service.channels.wecom.service import WeComAibotService
from trpc_service.channels.wecom.settings import WeComSettings
from trpc_service.channels.binding import ChannelBinding
from trpc_service.gateway.channel_service import ChannelIngressService
from trpc_service.gateway.client import HttpWorkerClient
from trpc_service.storage.tenant_repository import SqlTenantConfigRepository
from trpc_service.telemetry import TelemetryRuntime, TelemetrySettings, tracer_for
from trpc_service.transport.auth import InternalToken

from .service_topology import ServiceTopology, http_post_json, requires_model_and_docker

pytestmark = requires_model_and_docker


def _wecom_binding() -> ChannelBinding:
    return ChannelBinding(
        binding_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        tenant_id="tenant_default",
        app_id="app_demo",
        channel="wecom",
        external_account_id="integration-bot",
        secret_ref="env:TRPC_WECOM_BOT_SECRET",
        enabled=True,
        version=1,
    )


# OTLP enum values (avoid importing the SDK enum into a proto-side decoder).
_KIND_INTERNAL = 1
_KIND_SERVER = 2
_KIND_CLIENT = 3

KNOWN_SPAN_NAMES = frozenset({
    "gateway.request",
    "worker.request",
    "agent.turn",
    "state.session",
    "state.memory",
    "tool.execute",
    "channel.receive",
    "channel.reply",
})
KNOWN_ATTR_KEYS = frozenset({
    "status",
    "error_code",
    "exception.type",
    "operation",
    "result",
    "reply_count",
})

# ---------------------------------------------------------------------------
# Minimal OTLP/HTTP collector
# ---------------------------------------------------------------------------


def _anyvalue(value) -> object:
    which = value.WhichOneof("value")
    if which == "string_value":
        return value.string_value
    if which == "bool_value":
        return value.bool_value
    if which == "int_value":
        return value.int_value
    if which == "double_value":
        return value.double_value
    # bytes/array/kvlist span attributes are forbidden by design; surface the
    # oneof name so the whitelist assertion reports it instead of hiding it.
    return f"<{which}>"


class _Collector:
    """Threaded OTLP/HTTP receiver collecting plain-dict spans."""

    def __init__(self) -> None:
        self._spans: list[dict] = []
        self._lock = threading.Lock()
        outer = self

        class _Handler(BaseHTTPRequestHandler):

            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                payload = self.rfile.read(length)
                if self.path == "/v1/traces":
                    request = trace_service_pb2.ExportTraceServiceRequest()
                    response = trace_service_pb2.ExportTraceServiceResponse()
                    try:
                        request.ParseFromString(payload)
                    except Exception:
                        self.send_response(400)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    for resource_spans in request.resource_spans:
                        for scope_spans in resource_spans.scope_spans:
                            for span in scope_spans.spans:
                                outer._store(span)
                elif self.path == "/v1/metrics":
                    request = metrics_service_pb2.ExportMetricsServiceRequest()
                    response = metrics_service_pb2.ExportMetricsServiceResponse()
                    try:
                        request.ParseFromString(payload)
                    except Exception:
                        self.send_response(400)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                else:
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                body = response.SerializeToString()
                self.send_response(200)
                self.send_header("Content-Type", "application/x-protobuf")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):  # silence stderr noise
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    @property
    def endpoint(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/v1/traces"

    def _store(self, span) -> None:
        attributes = {kv.key: _anyvalue(kv.value) for kv in span.attributes}
        parent = span.parent_span_id.hex()
        with self._lock:
            self._spans.append({
                "name": span.name,
                "kind": span.kind,
                "trace_id": span.trace_id.hex(),
                "span_id": span.span_id.hex(),
                "parent_id": parent or None,
                "attributes": attributes,
            })

    def spans(self) -> list[dict]:
        with self._lock:
            return [dict(s) for s in self._spans]


def _wait_for_trace(collector: _Collector, names: list[str], timeout: float) -> tuple[str, list[dict]]:
    """Poll until one trace id contains every expected span name."""
    deadline = time.monotonic() + timeout
    last_seen: set = set()
    while time.monotonic() < deadline:
        by_trace: dict[str, list[dict]] = {}
        for span in collector.spans():
            by_trace.setdefault(span["trace_id"], []).append(span)
        for trace_id, spans in by_trace.items():
            present = {s["name"] for s in spans}
            last_seen |= present
            if set(names) <= present:
                return trace_id, spans
        time.sleep(0.5)
    raise AssertionError(f"no single trace held {sorted(names)} yet (span names seen so far: {sorted(last_seen)})")


def _assert_sanitized(collector: _Collector, sentinels: list[str]) -> None:
    """Every exported span: fixed names/keys, no sentinel token anywhere."""
    spans = collector.spans()
    assert spans, "collector received no spans at all"
    for span in spans:
        assert span["name"] in KNOWN_SPAN_NAMES, f"unexpected span name {span['name']!r}"
        assert set(span["attributes"]) <= KNOWN_ATTR_KEYS, f"unexpected attribute keys in {span['name']}: " \
                                                           f"{set(span['attributes']) - KNOWN_ATTR_KEYS}"
        blob = span["name"] + " " + " ".join(f"{k}={v}" for k, v in span["attributes"].items())
        for token in sentinels:
            assert token not in blob, f"sentinel leaked into span {span['name']}"


def test_collector_keeps_otlp_metrics_out_of_span_store(collector):
    """Metrics share the OTLP HTTP server but must never decode as spans."""
    request = Request(
        collector.endpoint.replace("/v1/traces", "/v1/metrics"),
        data=metrics_service_pb2.ExportMetricsServiceRequest().SerializeToString(),
        headers={"Content-Type": "application/x-protobuf"},
        method="POST",
    )
    with urlopen(request, timeout=5) as response:  # noqa: S310 - local test collector only
        assert response.status == 200
    assert collector.spans() == []


# ---------------------------------------------------------------------------
# Traced subprocess topology
# ---------------------------------------------------------------------------


class _TracedTopology(ServiceTopology):
    """Same stack, but Gateway/Workers export traces to the test collector."""

    trace_endpoint: str = ""

    def _build_env(self):
        env = super()._build_env()
        env["TRPC_TRACE_ENABLED"] = "true"
        env["TRPC_TRACE_OTLP_ENDPOINT"] = self.trace_endpoint
        env["TRPC_TRACE_SAMPLE_RATIO"] = "1.0"
        env["TRPC_TRACE_EXPORT_TIMEOUT_SECONDS"] = "5.0"
        return env


@pytest.fixture(scope="module")
def collector():
    receiver = _Collector()
    receiver.start()
    yield receiver
    receiver.stop()


@pytest.fixture(scope="module")
def topology(collector, tmp_path_factory):
    topo = _TracedTopology(tmp_path_factory.mktemp("stage6b1-trace"))
    topo.trace_endpoint = collector.endpoint
    topo.start()
    try:
        yield topo
    finally:
        topo.stop()


# ---------------------------------------------------------------------------
# Scenario A — real Console → Gateway → Worker (two processes), one trace
# ---------------------------------------------------------------------------


def test_console_chain_shares_one_trace_across_processes(collector, topology):
    token = uuid.uuid4().hex[:12]
    sentinels = [token]
    url = topology.gateway_url + "/api/console/messages"
    headers = {"X-Tenant-ID": "tenant_default"}
    expected = ["gateway.request", "worker.request", "agent.turn", "state.session", "tool.execute"]

    trace_id = spans = None
    attempts = []
    for attempt in range(3):
        body = {
            "tenant_id": "tenant_default",
            "user_id": f"intg-user-{token}",
            "conversation_id": f"intg-conv-{token}",
            "message_id": f"intg-msg-{attempt}-{token}",
            # the sentinel token rides inside the user-visible text so the
            # leak scan covers request bodies, model output and receipts.
            "message": f"现在几点了？必须调用 get_current_time 工具查询后再回答，不要凭记忆或猜测。SENTINEL-{token}",
        }
        attempts.append(body["message_id"])
        http_post_json(url, body, headers, timeout=120.0)
        try:
            trace_id, spans = _wait_for_trace(collector, expected, timeout=60.0)
            break
        except AssertionError:
            if attempt == 2:
                raise

    assert trace_id is not None and spans is not None
    by_name: dict[str, list[dict]] = {}
    for span in spans:
        by_name.setdefault(span["name"], []).append(span)

    # all spans in this trace share the single trace id (guaranteed by
    # _wait_for_trace) — and the structural W3C linkage holds:
    gw_root = by_name["gateway.request"][0]
    assert gw_root["kind"] == _KIND_SERVER
    assert gw_root["parent_id"] is None, "console request must root the trace"

    client_spans = [s for s in by_name["worker.request"] if s["kind"] == _KIND_CLIENT]
    server_spans = [s for s in by_name["worker.request"] if s["kind"] == _KIND_SERVER]
    assert client_spans and server_spans, "expected both CLIENT and SERVER worker spans"
    # CLIENT span hangs under the gateway SERVER span (in-process parenting)
    assert any(c["parent_id"] == gw_root["span_id"] for c in client_spans)
    # SERVER span joined the trace across the real TCP boundary: its parent
    # is the CLIENT span id that left the gateway process in traceparent.
    client_ids = {c["span_id"] for c in client_spans}
    assert any(s["parent_id"] in client_ids for s in server_spans), \
        f"worker SERVER span not parented by the gateway CLIENT span: {spans}"
    turn = by_name["agent.turn"][0]
    assert any(turn["parent_id"] == s["span_id"] for s in server_spans), "agent.turn nests under worker request"
    for session_span in by_name["state.session"]:
        assert session_span["kind"] == _KIND_INTERNAL
        assert session_span["attributes"].get("status") == "ok"
    tool_span = by_name["tool.execute"][0]
    assert tool_span["attributes"].get("status") == "ok"
    assert not any("exception.type" in s["attributes"] for s in spans), f"span errors present: {spans}"

    _assert_sanitized(collector, sentinels)


# ---------------------------------------------------------------------------
# Scenario B — WeCom channel.receive/reply joined to the real Worker
# ---------------------------------------------------------------------------


def _wecom_frame(content: str, msgid: str, userid: str) -> dict:
    return {
        "cmd": "aibot_msg_callback",
        "headers": {
            "req_id": f"req-{uuid.uuid4().hex[:8]}"
        },
        "body": {
            "msgtype": "text",
            "text": {
                "content": content
            },
            "chattype": "single",
            "msgid": msgid,
            "from": {
                "userid": userid
            },
        },
    }


def test_wecom_channel_trace_joins_worker_process(collector, topology):
    token = uuid.uuid4().hex[:12]

    async def _drive() -> int:
        settings = WeComSettings.from_env({
            "TRPC_WECOM_BOT_ID": "integration-bot",
            "TRPC_WECOM_BOT_SECRET": "integration-secret-not-a-real-one",
        })
        assert settings is not None
        runtime = TelemetryRuntime(
            TelemetrySettings(
                enabled=True,
                service_name="test-gateway-side",
                otlp_endpoint=collector.endpoint,
                sample_ratio=1.0,
                export_timeout_seconds=5.0,
            ))
        await runtime.start()
        tracer = tracer_for(runtime, "trpc-service.gateway")
        engine = create_async_engine(topology.pg_url)
        repo = SqlTenantConfigRepository(engine)
        client = HttpWorkerClient(
            base_url=topology.worker_a_url,
            internal_token=InternalToken(topology._internal_token),
            tracer=tracer,
        )
        ingress = ChannelIngressService(tenant_repository=repo, worker_client=client)
        sdk = MagicMock()
        sdk.reply_stream = AsyncMock()
        service = WeComAibotService(
            settings=settings,
            client=sdk,
            ingress=ingress,
            tracer=tracer,
            binding=_wecom_binding(),
        )
        try:
            await service.handle_text_frame(
                _wecom_frame(
                    f"现在几点了？必须调用 get_current_time 工具查询后再回答，不要凭记忆或猜测。CHSENTINEL-{token}",
                    f"chmsg-{token}",
                    f"chuser-{token}",
                ))
            return sdk.reply_stream.await_count
        finally:
            await client.close()
            await repo.close()
            await engine.dispose()
            await runtime.close()  # flushes channel.receive/reply spans

    writes = asyncio.run(_drive())
    assert writes >= 1, "reply chain must have written back through the SDK"

    expected = ["channel.receive", "channel.reply", "worker.request", "agent.turn", "state.session", "tool.execute"]
    trace_id, spans = _wait_for_trace(collector, expected, timeout=90.0)

    by_name: dict[str, list[dict]] = {}
    for span in spans:
        by_name.setdefault(span["name"], []).append(span)

    receive = by_name["channel.receive"][0]
    assert receive["parent_id"] is None, "IM callback roots its own trace"
    reply = by_name["channel.reply"][0]
    assert reply["parent_id"] == receive["span_id"]
    assert reply["attributes"].get("result") == "done"
    assert int(reply["attributes"].get("reply_count", 0)) >= 1

    server_spans = [s for s in by_name["worker.request"] if s["kind"] == _KIND_SERVER]
    assert server_spans, "worker SERVER span missing — traceparent did not cross the TCP boundary"
    # the in-process CLIENT span joins the same trace as the subprocess spans
    client_ids = {s["span_id"] for s in by_name["worker.request"] if s["kind"] == _KIND_CLIENT}
    assert client_ids, "gateway-side CLIENT span missing"
    assert any(s["parent_id"] in client_ids for s in server_spans)
    assert all(s["trace_id"] == trace_id for s in spans)
    assert not any("exception.type" in s["attributes"] for s in spans), f"span errors present: {spans}"

    _assert_sanitized(collector, [token])
