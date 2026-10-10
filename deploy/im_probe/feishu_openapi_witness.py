#!/usr/bin/env python3
"""Independent, content-free witness for Feishu outbound OpenAPI acknowledgements."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import ipaddress
import json
import math
import os
import re
import socket
import ssl
import stat
import sys
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any, cast

MAX_REQUEST_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = 1024 * 1024
MAX_CONTROL_BYTES = 16 * 1024
MAX_RECEIPTS = 512
MAX_DROP_TTL_SECONDS = 30.0
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8752
AUTH_PATH = "/open-apis/auth/v3/tenant_access_token/internal"
MESSAGE_PREFIX = "/open-apis/im/v1/messages"
NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{16,256}$")
_AF_UNIX = cast(int | None, getattr(socket, "AF_UNIX", None))


class WitnessError(RuntimeError):
    """A fail-closed configuration, request, or upstream failure."""


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value} is forbidden")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key is forbidden")
        result[key] = value
    return result


def _strict_json(raw: str | bytes) -> Any:
    return json.loads(
        raw,
        parse_constant=_reject_constant,
        object_pairs_hook=_reject_duplicate_keys,
    )


def _sha256(domain: str, value: bytes) -> str:
    return hashlib.sha256(domain.encode() + b"\x00" + value).hexdigest()


def _provider_code(body: bytes) -> int | None:
    try:
        value = _strict_json(body)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None
    if not isinstance(value, dict):
        return None
    code = value.get("code")
    if isinstance(code, bool) or not isinstance(code, int):
        return None
    return code


def _retry_after(headers: Mapping[str, str]) -> float | None:
    value = next(
        (item for key, item in headers.items() if key.casefold() == "retry-after"),
        None,
    )
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) and 0 <= result <= 3600 else None


@dataclass(frozen=True, slots=True)
class WitnessReceipt:
    sequence: int
    path_sha256: str
    body_sha256: str
    provider_status: int
    provider_code: int | None
    provider_request_id_sha256: str | None
    retry_after_seconds: float | None
    provider_acknowledged: bool
    downstream_response_dropped: bool
    observed_at: str


@dataclass(frozen=True, slots=True)
class ForwardedResponse:
    status: int
    headers: dict[str, str]
    body: bytes


Forwarder = Callable[[str, Mapping[str, str], bytes], ForwardedResponse]


class FeishuOpenAPIWitness:
    """Forward approved Feishu calls and retain only bounded acknowledgement hashes."""

    def __init__(self, forwarder: Forwarder, *, clock: Callable[[], float] = time.time) -> None:
        self._forwarder = forwarder
        self._clock = clock
        self._lock = Lock()
        self._receipts: deque[WitnessReceipt] = deque(maxlen=MAX_RECEIPTS)
        self._next_sequence = 1
        self._drop_nonce: str | None = None
        self._drop_expires_at = 0.0

    def arm_drop_next_ack(self, run_nonce: str, ttl_seconds: float) -> None:
        if NONCE_RE.fullmatch(run_nonce) is None:
            raise WitnessError("run nonce is invalid")
        if not math.isfinite(ttl_seconds) or not 0.001 <= ttl_seconds <= MAX_DROP_TTL_SECONDS:
            raise WitnessError("drop TTL is invalid")
        with self._lock:
            self._drop_nonce = run_nonce
            self._drop_expires_at = self._clock() + ttl_seconds

    def forward(
        self, path: str, headers: Mapping[str, str], body: bytes
    ) -> tuple[ForwardedResponse, bool]:
        if path != AUTH_PATH and not path.startswith(MESSAGE_PREFIX):
            raise WitnessError("OpenAPI path is not allowed")
        if len(body) > MAX_REQUEST_BYTES:
            raise WitnessError("request body is too large")
        response = self._forwarder(path, headers, body)
        if not 100 <= response.status <= 599 or len(response.body) > MAX_RESPONSE_BYTES:
            raise WitnessError("upstream response is invalid")
        if path == AUTH_PATH:
            return response, False

        code = _provider_code(response.body)
        acknowledged = 200 <= response.status < 300 and code == 0
        dropped = False
        now = self._clock()
        with self._lock:
            if self._drop_nonce is not None:
                if now <= self._drop_expires_at and acknowledged:
                    dropped = True
                self._drop_nonce = None
                self._drop_expires_at = 0.0
            request_id = next(
                (
                    value
                    for key, value in response.headers.items()
                    if key.casefold() in {"x-request-id", "x-tt-logid"}
                ),
                None,
            )
            receipt = WitnessReceipt(
                sequence=self._next_sequence,
                path_sha256=_sha256("trpc.feishu.openapi.path.v1", path.encode()),
                body_sha256=_sha256("trpc.feishu.openapi.body.v1", body),
                provider_status=response.status,
                provider_code=code,
                provider_request_id_sha256=(
                    _sha256("trpc.feishu.openapi.request-id.v1", request_id.encode())
                    if request_id
                    else None
                ),
                retry_after_seconds=_retry_after(response.headers),
                provider_acknowledged=acknowledged,
                downstream_response_dropped=dropped,
                observed_at=datetime.fromtimestamp(now, UTC).isoformat(),
            )
            self._next_sequence += 1
            self._receipts.append(receipt)
        return response, dropped

    def query(self, after_sequence: int, limit: int) -> list[dict[str, object]]:
        if isinstance(after_sequence, bool) or after_sequence < 0:
            raise WitnessError("after_sequence is invalid")
        if isinstance(limit, bool) or not 1 <= limit <= 100:
            raise WitnessError("limit is invalid")
        with self._lock:
            return [
                cast(dict[str, object], asdict(receipt))
                for receipt in self._receipts
                if receipt.sequence > after_sequence
            ][:limit]


def _upstream_forward(path: str, headers: Mapping[str, str], body: bytes) -> ForwardedResponse:
    forwarded_headers = {
        key: value
        for key, value in headers.items()
        if key.casefold() in {"authorization", "content-type", "user-agent"}
    }
    forwarded_headers["Content-Length"] = str(len(body))
    connection = http.client.HTTPSConnection(
        "open.feishu.cn",
        443,
        timeout=15,
        context=ssl.create_default_context(),
    )
    try:
        connection.request("POST", path, body=body, headers=forwarded_headers)
        response = connection.getresponse()
        raw = response.read(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise WitnessError("upstream response is too large")
        return ForwardedResponse(
            status=response.status,
            headers={key: value for key, value in response.getheaders()},
            body=raw,
        )
    except (OSError, http.client.HTTPException) as error:
        raise WitnessError("upstream request failed") from error
    finally:
        connection.close()


class _WitnessHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], witness: FeishuOpenAPIWitness) -> None:
        self.witness = witness
        super().__init__(address, _WitnessHandler)


class _WitnessHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:
        try:
            raw_length = self.headers.get("Content-Length")
            length = int(raw_length or "-1")
            if length < 0 or length > MAX_REQUEST_BYTES:
                raise WitnessError("request size is invalid")
            body = self.rfile.read(length)
            if len(body) != length:
                raise WitnessError("request body is incomplete")
            headers = {
                name: self.headers.get(name, "")
                for name in ("Authorization", "Content-Type", "User-Agent")
            }
            response, dropped = cast(_WitnessHTTPServer, self.server).witness.forward(
                self.path, headers, body
            )
            if dropped:
                self.close_connection = True
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                return
            self.send_response(response.status)
            for key, value in response.headers.items():
                if key.casefold() in {
                    "content-type",
                    "retry-after",
                    "x-request-id",
                    "x-tt-logid",
                }:
                    self.send_header(key, value)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(response.body)))
            self.end_headers()
            self.wfile.write(response.body)
        except (OSError, ValueError, WitnessError):
            self.send_error(502)

    def do_GET(self) -> None:
        self.send_error(405)

    def log_message(self, _format: str, *args: object) -> None:
        return


def create_http_server(host: str, port: int, witness: FeishuOpenAPIWitness) -> ThreadingHTTPServer:
    try:
        address = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)[0][4][0]
        if not ipaddress.ip_address(address).is_loopback:
            raise ValueError
    except (OSError, ValueError):
        raise WitnessError("HTTP listener must be loopback") from None
    return _WitnessHTTPServer((host, port), witness)


def control_request(raw: bytes, witness: FeishuOpenAPIWitness) -> bytes:
    response: dict[str, object] = {"status": "not_run", "error_code": "request_invalid"}
    if raw and len(raw) <= MAX_CONTROL_BYTES:
        try:
            request = _strict_json(raw)
            if not isinstance(request, dict) or request.get("schema_version") != 1:
                raise WitnessError("request schema is invalid")
            action = request.get("action")
            if action == "query" and set(request) == {
                "schema_version",
                "action",
                "after_sequence",
                "limit",
            }:
                receipts = witness.query(request["after_sequence"], request["limit"])
                response = {"status": "pass", "receipts": receipts}
            elif action == "arm_drop_next_ack" and set(request) == {
                "schema_version",
                "action",
                "run_nonce",
                "ttl_seconds",
            }:
                witness.arm_drop_next_ack(request["run_nonce"], request["ttl_seconds"])
                response = {"status": "pass"}
        except (TypeError, ValueError, WitnessError):
            pass
    return json.dumps(response, sort_keys=True, separators=(",", ":")).encode() + b"\n"


class UnixControlServer:
    """Small AF_UNIX control server kept independent of the HTTP listener."""

    def __init__(self, path: Path, witness: FeishuOpenAPIWitness) -> None:
        if _AF_UNIX is None:
            raise WitnessError("Unix sockets are unavailable")
        self.witness = witness
        self._stopping = Event()
        self._socket = socket.socket(_AF_UNIX, socket.SOCK_STREAM)
        self._socket.bind(str(path))
        self._socket.listen(16)
        self._socket.settimeout(0.25)

    def serve_forever(self) -> None:
        while not self._stopping.is_set():
            try:
                connection, _address = self._socket.accept()
            except TimeoutError:
                continue
            except OSError:
                if self._stopping.is_set():
                    break
                raise
            Thread(target=self._handle, args=(connection,), daemon=True).start()

    def _handle(self, connection: socket.socket) -> None:
        with connection:
            connection.settimeout(2)
            received = bytearray()
            try:
                while len(received) <= MAX_CONTROL_BYTES:
                    chunk = connection.recv(min(4096, MAX_CONTROL_BYTES + 1 - len(received)))
                    if not chunk:
                        break
                    received.extend(chunk)
                    if b"\n" in chunk:
                        break
                raw = bytes(received).split(b"\n", 1)[0]
                connection.sendall(control_request(raw, self.witness))
            except OSError:
                return

    def shutdown(self) -> None:
        self._stopping.set()

    def server_close(self) -> None:
        self._socket.close()


def create_control_server(path: Path, witness: FeishuOpenAPIWitness) -> UnixControlServer:
    if os.name == "nt" or _AF_UNIX is None:
        raise WitnessError("Unix sockets are unavailable")
    if not path.is_absolute() or path.is_symlink():
        raise WitnessError("control socket path is invalid")
    path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    if path.exists():
        if not stat.S_ISSOCK(path.stat().st_mode):
            raise WitnessError("control socket path is occupied")
        path.unlink()
    server = UnixControlServer(path, witness)
    os.chmod(path, 0o660)
    return server


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--control-socket",
        type=Path,
        default=Path("/run/trpc-im-probe/feishu-openapi-witness.sock"),
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    witness = FeishuOpenAPIWitness(_upstream_forward)
    try:
        http_server = create_http_server(args.host, args.port, witness)
        control_server = create_control_server(args.control_socket, witness)
        if args.check:
            http_server.server_close()
            control_server.server_close()
            args.control_socket.unlink(missing_ok=True)
            print('{"status":"ready"}')
            return 0
    except (OSError, WitnessError):
        print('{"status":"not_ready"}')
        return 2
    thread = Thread(target=control_server.serve_forever, daemon=True)
    thread.start()
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        http_server.shutdown()
        http_server.server_close()
        control_server.shutdown()
        control_server.server_close()
        thread.join(timeout=5)
        try:
            args.control_socket.unlink(missing_ok=True)
        except OSError:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = [
    "AUTH_PATH",
    "FeishuOpenAPIWitness",
    "ForwardedResponse",
    "WitnessError",
    "WitnessReceipt",
    "control_request",
    "create_control_server",
    "create_http_server",
]
