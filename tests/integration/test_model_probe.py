"""Official SDK wire contract for the single bounded model probe."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading

import pytest
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.configs import ModelRetryConfig

from trpc_service.agent.model_probe import check, MODEL
from trpc_service.telemetry.sdk_logging import configure_sdk_logging


@pytest.mark.asyncio
async def test_model_probe_sends_one_bounded_request_through_official_sdk():
    received = []

    class Handler(BaseHTTPRequestHandler):

        def log_message(self, *_):
            pass

        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            body = json.dumps({
                "id":
                "synthetic-response",
                "object":
                "chat.completion",
                "created":
                1,
                "model":
                MODEL,
                "choices": [{
                    "index": 0,
                    "finish_reason": "stop",
                    "message": {
                        "role": "assistant",
                        "content": "模型连接成功"
                    }
                }],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15
                }
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    configure_sdk_logging()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        model = OpenAIModel(model_name=MODEL,
                            api_key="synthetic-model-secret",
                            base_url=f"http://127.0.0.1:{server.server_port}/api/v1",
                            model_retry_config=ModelRetryConfig(num_retries=0),
                            client_args={
                                "timeout": 5
                            })
        result = await check(model)
        assert result["status"] == "text_received" and result["input_tokens"] == 10
        assert len(received) == 1
        body = received[0]
        assert body["max_tokens"] == 32 and body["max_completion_tokens"] == 32
        assert body["provider"]["allow_fallbacks"] is False and body["reasoning"]["enabled"] is False
        assert "tools" not in body and body["stream"] is False
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
