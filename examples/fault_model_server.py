"""Small OpenAI-compatible server for local failure and shutdown tests.

This server uses only the Python standard library.  It never calls a real
model and must not be used as a production model endpoint.
"""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class FaultModelHandler(BaseHTTPRequestHandler):
    server_version = "TRPCFaultModel/1.0"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path == "/healthz":
            self._send_json(200, {"status": "ok", "mode": self.server.mode})
            return
        self._send_json(404, {"error": {"message": "not found"}})

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length:
            self.rfile.read(content_length)

        if self.server.mode == "rate-limit":
            self._send_json(
                429,
                {"error": {
                    "message": "rate limited",
                    "type": "rate_limit_error",
                    "code": "rate_limit"
                }},
                {"Retry-After": str(self.server.retry_after)},
            )
            return

        time.sleep(self.server.delay)
        if self.server.mode == "timeout":
            self._send_json(504, {"error": {"message": "delayed response", "type": "timeout_test"}})
            return

        chunks = [
            {
                "id":
                "chatcmpl-local-fault-test",
                "object":
                "chat.completion.chunk",
                "created":
                0,
                "model":
                "local-fault-model",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "role": "assistant",
                        "content": self.server.reply
                    },
                    "finish_reason": None,
                }],
            },
            {
                "id": "chatcmpl-local-fault-test",
                "object": "chat.completion.chunk",
                "created": 0,
                "model": "local-fault-model",
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "stop"
                }],
            },
        ]
        body = "".join(f"data: {json.dumps(chunk, separators=(',', ':'))}\n\n" for chunk in chunks)
        body += "data: [DONE]\n\n"
        payload = body.encode("utf-8")
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _send_json(self, status: int, body: object, headers: dict[str, str] | None = None) -> None:
        payload = _json_bytes(body)
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, format: str, *args: object) -> None:
        print(format % args, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Local OpenAI-compatible fault model server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=18093)
    parser.add_argument("--mode", choices=("delayed-success", "rate-limit", "timeout"), default="delayed-success")
    parser.add_argument("--delay", type=float, default=8.0)
    parser.add_argument("--reply", default="graceful-ok")
    parser.add_argument("--retry-after", type=int, default=2)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), FaultModelHandler)
    server.mode = args.mode
    server.delay = max(0.0, args.delay)
    server.reply = args.reply
    server.retry_after = max(0, args.retry_after)
    print(f"fault model listening on http://{args.host}:{args.port} mode={args.mode}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
