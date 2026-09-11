from __future__ import annotations

import asyncio
import importlib.util
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest

from trpc_service.agent import AgentFactory, FunctionTool, ImmutableRelease, TrpcAgentSdkRunner
from trpc_service.config import AppSettings

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("trpc_agent_sdk") is None,
    reason="the optional tRPC-Agent SDK is not installed in this environment",
)


class _OpenAICompatibleHandler(BaseHTTPRequestHandler):
    requests: list[dict] = []

    def do_POST(self) -> None:  # noqa: N802 - stdlib HTTP handler API
        length = int(self.headers["Content-Length"])
        type(self).requests.append(json.loads(self.rfile.read(length)))
        request = type(self).requests[-1]
        if len(type(self).requests) == 1 and request.get("tools"):
            chunks = [
                {
                    "id": "sdk-tool-call",
                    "object": "chat.completion.chunk",
                    "model": "gpt-sdk-test",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "role": "assistant",
                                "tool_calls": [
                                    {
                                        "index": 0,
                                        "id": "call-ticket-42",
                                        "type": "function",
                                        "function": {
                                            "name": "ticket.lookup",
                                            "arguments": '{"ticket_id":"42"}',
                                        },
                                    }
                                ],
                            },
                            "finish_reason": None,
                        }
                    ],
                },
                {
                    "id": "sdk-tool-call",
                    "object": "chat.completion.chunk",
                    "model": "gpt-sdk-test",
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}],
                },
            ]
        else:
            chunks = [
                {
                    "id": "sdk-test-response",
                    "object": "chat.completion.chunk",
                    "model": "gpt-sdk-test",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"role": "assistant", "content": "SDK runner reply"},
                            "finish_reason": None,
                        }
                    ],
                },
                {
                    "id": "sdk-test-response",
                    "object": "chat.completion.chunk",
                    "model": "gpt-sdk-test",
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                    "usage": {
                        "prompt_tokens": 7,
                        "completion_tokens": 3,
                        "total_tokens": 10,
                    },
                },
            ]
        body = "".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks) + "data: [DONE]\n\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(body.encode())))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body.encode())
        self.close_connection = True

    def log_message(self, *_: object) -> None:
        return


def test_real_trpc_agent_sdk_runner_executes_openai_model_with_a_hard_output_cap() -> None:
    _OpenAICompatibleHandler.requests.clear()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _OpenAICompatibleHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        release = ImmutableRelease(
            tenant_id="tenant-sdk",
            agent_id="support",
            config_version=1,
            app_config={"system_prompt": "Answer concisely."},
            model_config={
                "mode": "trpc_agent",
                "model_name": "gpt-sdk-test",
                "base_url": f"http://127.0.0.1:{server.server_port}/v1",
                "max_output_tokens": 13,
            },
            tool_policy={},
        )
        runtime = asyncio.run(
            AgentFactory(AppSettings(trpc_agent_api_key="test-sdk-key")).build(release)
        )
        assert isinstance(runtime.runner, TrpcAgentSdkRunner)

        result = asyncio.run(runtime.run("hello", {"summary": "durable context"}))

        assert result.reply.text() == "SDK runner reply"
        assert result.usage == {"input_tokens": 7, "output_tokens": 3}
        assert _OpenAICompatibleHandler.requests[-1]["max_tokens"] == 13
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_real_trpc_agent_sdk_runner_invokes_platform_ledger_tool() -> None:
    _OpenAICompatibleHandler.requests.clear()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _OpenAICompatibleHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    seen_arguments: list[dict] = []

    async def lookup_ticket(arguments: dict) -> dict:
        seen_arguments.append(dict(arguments))
        return {"ticket_id": arguments["ticket_id"], "status": "open"}

    try:
        release = ImmutableRelease(
            tenant_id="tenant-sdk",
            agent_id="support",
            config_version=1,
            app_config={"system_prompt": "Use the ticket tool when needed."},
            model_config={
                "mode": "trpc_agent",
                "model_name": "gpt-sdk-test",
                "base_url": f"http://127.0.0.1:{server.server_port}/v1",
                "max_output_tokens": 13,
            },
            tool_policy={"allow": ["ticket.lookup"]},
        )
        runtime = asyncio.run(
            AgentFactory(AppSettings(trpc_agent_api_key="test-sdk-key")).build(
                release,
                tools=(
                    FunctionTool(
                        name="ticket.lookup",
                        description="Look up a support ticket.",
                        parameters={
                            "type": "object",
                            "properties": {"ticket_id": {"type": "string"}},
                            "required": ["ticket_id"],
                        },
                        handler=lookup_ticket,
                    ),
                ),
            )
        )

        result = asyncio.run(runtime.run("Please inspect ticket 42", {}))

        assert result.reply.text() == "SDK runner reply"
        assert seen_arguments == [{"ticket_id": "42"}]
        assert len(_OpenAICompatibleHandler.requests) == 2
        assert _OpenAICompatibleHandler.requests[0]["tools"][0]["function"]["name"] == "ticket.lookup"
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
