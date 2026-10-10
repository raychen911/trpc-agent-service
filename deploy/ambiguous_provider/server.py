#!/usr/bin/env python3
"""Small, local-only provider endpoint for ambiguous-delivery acceptance.

The send endpoint commits an idempotent receipt to SQLite and then closes the
TCP connection without writing an HTTP response.  A caller therefore observes
a transport-unknown result even though the provider has durably accepted the
request.  The read-only state endpoint is intentionally separate so an
operator can confirm acceptance before explicitly replaying the delivery.

This endpoint is a test dependency, not an application API.  Bind it to
loopback or an isolated acceptance network and do not expose it publicly.
Only the outbound id, a body hash, and counters are persisted; message
content and authorization headers are never stored or logged.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import socket
import sqlite3
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

MAX_REQUEST_BYTES = 64 * 1024
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
AUTH_PATH = "/open-apis/auth/v3/tenant_access_token/internal"
MESSAGE_PATH = "/open-apis/im/v1/messages"
GENERIC_SEND_PATH = "/v1/send"
STATE_PREFIX = "/state/"


class LedgerConflict(ValueError):
    """Raised when one idempotency key is reused for a different request."""


class DeliveryLedger:
    """Durable, content-free ledger for provider acceptance observations."""

    def __init__(self, path: Path) -> None:
        self.path = path.expanduser().absolute()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS deliveries (
                    outbound_id TEXT PRIMARY KEY,
                    request_sha256 TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    receive_count INTEGER NOT NULL CHECK (receive_count >= 1)
                )
                """
            )
            self._connection.commit()

    def record(self, outbound_id: str, body: bytes) -> dict[str, Any]:
        digest = hashlib.sha256(body).hexdigest()
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM deliveries WHERE outbound_id = ?",
                (outbound_id,),
            ).fetchone()
            if row is None:
                self._connection.execute(
                    """
                    INSERT INTO deliveries (
                        outbound_id, request_sha256, first_seen_at, last_seen_at, receive_count
                    ) VALUES (?, ?, ?, ?, 1)
                    """,
                    (outbound_id, digest, now, now),
                )
                count = 1
                first_seen_at = now
            else:
                if row["request_sha256"] != digest:
                    raise LedgerConflict("outbound id was reused with a different request")
                count = int(row["receive_count"]) + 1
                first_seen_at = str(row["first_seen_at"])
                self._connection.execute(
                    """
                    UPDATE deliveries
                       SET last_seen_at = ?, receive_count = ?
                     WHERE outbound_id = ?
                    """,
                    (now, count, outbound_id),
                )
            self._connection.commit()
        return {
            "status": "accepted",
            "outbound_id": outbound_id,
            "request_sha256": digest,
            "first_seen_at": first_seen_at,
            "last_seen_at": now,
            "receive_count": count,
            "duplicate": count > 1,
        }

    def get(self, outbound_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM deliveries WHERE outbound_id = ?",
                (outbound_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "status": "accepted",
            "outbound_id": str(row["outbound_id"]),
            "request_sha256": str(row["request_sha256"]),
            "first_seen_at": str(row["first_seen_at"]),
            "last_seen_at": str(row["last_seen_at"]),
            "receive_count": int(row["receive_count"]),
            "duplicate": int(row["receive_count"]) > 1,
        }

    def close(self) -> None:
        with self._lock:
            self._connection.close()


def _strict_json(raw: bytes) -> dict[str, Any]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"invalid JSON constant: {value}")

    value = json.loads(raw, parse_constant=reject_constant)
    if not isinstance(value, dict):
        raise ValueError("request body must be a JSON object")
    return value


def _outbound_id(payload: dict[str, Any]) -> str:
    # Feishu uses ``uuid`` for the provider idempotency key.  The generic path
    # accepts the application name as well, which keeps the endpoint useful for
    # a small provider adapter without introducing a second protocol.
    value = payload.get("uuid", payload.get("outbound_id"))
    if not isinstance(value, str) or SAFE_ID_RE.fullmatch(value) is None:
        raise ValueError("request id is invalid")
    return value


class _Handler(BaseHTTPRequestHandler):
    server: AmbiguousProviderServer

    def log_message(self, _format: str, *_args: object) -> None:
        # Request paths can contain caller-controlled identifiers.  The
        # endpoint intentionally has no access log so no message/body can leak.
        return

    def _json_response(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> bytes:
        raw_length = self.headers.get("Content-Length")
        try:
            length = int(raw_length or "0")
        except ValueError as error:
            raise ValueError("content length is invalid") from error
        if length <= 0 or length > MAX_REQUEST_BYTES:
            raise ValueError("request body size is invalid")
        body = self.rfile.read(length)
        if len(body) != length:
            raise ValueError("request body is incomplete")
        return body

    def _drop_response(self) -> None:
        # The ledger transaction has committed before this method is called.
        # Closing both directions prevents BaseHTTPRequestHandler from writing
        # a default response after the handler returns.
        self.close_connection = True
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.connection.close()
        except OSError:
            pass

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path == "/health":
            self._json_response(200, {"status": "ok"})
            return
        if not path.startswith(STATE_PREFIX):
            self._json_response(404, {"status": "not_found"})
            return
        outbound_id = unquote(path.removeprefix(STATE_PREFIX))
        if SAFE_ID_RE.fullmatch(outbound_id) is None:
            self._json_response(400, {"status": "invalid_request"})
            return
        record = self.server.ledger.get(outbound_id)
        self._json_response(200 if record is not None else 404, record or {"status": "not_found"})

    def do_POST(self) -> None:
        path = urlsplit(self.path).path
        if path == AUTH_PATH:
            # FeishuAdapter needs a token before it reaches the response-drop
            # message route.  Drain the complete bounded request before
            # replying: closing an HTTP/1.0 connection with unread request
            # bytes can reset the socket and make a successful token response
            # appear as a transport failure on Windows.
            try:
                _strict_json(self._read_body())
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
                self._json_response(400, {"status": "invalid_request"})
                return
            # The token is test-only and carries no credential.
            self._json_response(
                200,
                {"code": 0, "tenant_access_token": "ambiguous-test-token", "expire": 3600},
            )
            return
        if path not in {MESSAGE_PATH, GENERIC_SEND_PATH}:
            self._json_response(404, {"status": "not_found"})
            return
        try:
            body = self._read_body()
            payload = _strict_json(body)
            outbound_id = _outbound_id(payload)
            record = self.server.ledger.record(outbound_id, body)
        except LedgerConflict:
            self._json_response(409, {"status": "idempotency_conflict"})
            return
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            self._json_response(400, {"status": "invalid_request"})
            return
        # The first attempt models a provider that accepted the side effect but
        # lost its response.  A later request with the same idempotency key is
        # an explicit/manual replay: acknowledge it without creating another
        # ledger row or side effect.
        if record["duplicate"]:
            if path == MESSAGE_PATH:
                self._json_response(
                    200,
                    {
                        "code": 0,
                        "data": {"message_id": outbound_id},
                        "duplicate": True,
                    },
                )
            else:
                self._json_response(
                    200,
                    {"status": "accepted", "outbound_id": outbound_id, "duplicate": True},
                )
            return
        self._drop_response()


class AmbiguousProviderServer(ThreadingHTTPServer):
    """Threaded endpoint with a durable acceptance ledger."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: tuple[str, int], ledger_path: Path) -> None:
        self.ledger = DeliveryLedger(ledger_path)
        try:
            super().__init__(address, _Handler)
        except BaseException:
            self.ledger.close()
            raise

    def server_close(self) -> None:
        super().server_close()
        self.ledger.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8791)
    parser.add_argument(
        "--ledger",
        type=Path,
        default=Path("runs/multitenant/ambiguous-provider.sqlite3"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.host not in {"127.0.0.1", "::1"}:
        raise SystemExit("ambiguous provider endpoint must bind to loopback")
    if not 0 <= args.port <= 65535:
        raise SystemExit("port is outside the valid range")
    server = AmbiguousProviderServer((args.host, args.port), args.ledger)
    print(f"ambiguous_provider_listening={server.server_address[0]}:{server.server_address[1]}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
