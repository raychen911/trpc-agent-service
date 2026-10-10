#!/usr/bin/env python3
"""Content-free, independent observer for mirrored Feishu callbacks.

Nginx mirrors the callback body to the loopback-only HTTP listener.  A valid
callback is authenticated and decrypted independently of ``trpc_service`` and
reduced to domain-separated hashes.  The observer never persists raw callback
headers, bodies, identifiers, message content, account IDs, or secrets.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import hmac
import ipaddress
import json
import os
import re
import socket
import stat
import sys
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any, cast

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

MIRROR_PATH = "/mirror"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8751
MAX_BODY_BYTES = 2 * 1024 * 1024
MAX_QUERY_BYTES = 8 * 1024
MAX_HEADER_BYTES = 8 * 1024
DEFAULT_TTL_SECONDS = 24 * 60 * 60
DEFAULT_CAPACITY = 4096
MAX_SIGNATURE_AGE_SECONDS = 5 * 60
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_AF_UNIX = cast(int | None, getattr(socket, "AF_UNIX", None))


class CallbackRejected(ValueError):
    """Safe callback rejection whose message contains no request material."""


class ObserverConfigurationError(RuntimeError):
    """Safe startup error whose message contains no secret material."""


def _strict_json(raw: str | bytes) -> Any:
    def reject_constant(_value: str) -> None:
        raise ValueError("non-finite JSON is forbidden")

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key is forbidden")
            result[key] = value
        return result

    return json.loads(
        raw,
        parse_constant=reject_constant,
        object_pairs_hook=reject_duplicates,
    )


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _domain_hash(domain: str, value: str | bytes) -> str:
    encoded = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(domain.encode("ascii") + b"\x00" + encoded).hexdigest()


def marker_sha256(marker: str) -> str:
    """Hash a marker exactly as the observer hashes a text-message body."""

    if not isinstance(marker, str) or not marker:
        raise ValueError("marker must be a non-empty string")
    return _domain_hash("trpc.feishu.callback.marker.v1", marker)


def profile_sha256(*, account_id: str, event_type: str, message_type: str, chat_type: str) -> str:
    """Hash the non-secret callback profile used to scope receipt queries."""

    profile = {
        "account_id": account_id,
        "chat_type": chat_type,
        "event_type": event_type,
        "message_type": message_type,
    }
    if not all(isinstance(value, str) and value for value in profile.values()):
        raise ValueError("profile values must be non-empty strings")
    return _domain_hash("trpc.feishu.callback.profile.v1", _canonical_json(profile))


@dataclass(frozen=True, slots=True)
class CallbackReceipt:
    receipt_sha256: str
    event_id_sha256: str
    message_id_sha256: str
    provider_time_sha256: str
    marker_sha256: str
    profile_sha256: str
    media_locator_sha256: tuple[str, ...]
    observed_at: str

    def as_dict(self) -> dict[str, object]:
        return {
            "event_id_sha256": self.event_id_sha256,
            "marker_sha256": self.marker_sha256,
            "media_locator_sha256": list(self.media_locator_sha256),
            "message_id_sha256": self.message_id_sha256,
            "observed_at": self.observed_at,
            "profile_sha256": self.profile_sha256,
            "provider_time_sha256": self.provider_time_sha256,
            "receipt_sha256": self.receipt_sha256,
        }


class ReceiptStore:
    """Bounded, process-local receipt cache with deterministic expiry."""

    def __init__(
        self,
        *,
        ttl_seconds: float = DEFAULT_TTL_SECONDS,
        capacity: int = DEFAULT_CAPACITY,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("receipt TTL must be positive")
        if capacity <= 0:
            raise ValueError("receipt capacity must be positive")
        self._ttl_seconds = ttl_seconds
        self._capacity = capacity
        self._clock = clock
        self._items: OrderedDict[str, tuple[float, CallbackReceipt]] = OrderedDict()
        self._lock = Lock()

    def put(self, receipt: CallbackReceipt) -> CallbackReceipt:
        with self._lock:
            now = self._clock()
            self._prune(now)
            existing = self._items.get(receipt.receipt_sha256)
            if existing is not None:
                return existing[1]
            self._items[receipt.receipt_sha256] = (
                now + self._ttl_seconds,
                receipt,
            )
            while len(self._items) > self._capacity:
                self._items.popitem(last=False)
            return receipt

    def query(self, marker_hash: str, profile_hash: str) -> CallbackReceipt | None:
        with self._lock:
            self._prune(self._clock())
            for _expires_at, receipt in reversed(self._items.values()):
                if receipt.marker_sha256 == marker_hash and receipt.profile_sha256 == profile_hash:
                    return receipt
        return None

    def _prune(self, now: float) -> None:
        expired = [key for key, (expires_at, _receipt) in self._items.items() if expires_at <= now]
        for key in expired:
            self._items.pop(key, None)

    def __len__(self) -> int:
        with self._lock:
            self._prune(self._clock())
            return len(self._items)


class FeishuCallbackObserver:
    def __init__(
        self,
        *,
        verification_token: str,
        encrypt_key: str,
        account_id: str,
        store: ReceiptStore | None = None,
        wall_clock: Callable[[], float] = time.time,
        max_signature_age_seconds: float = MAX_SIGNATURE_AGE_SECONDS,
    ) -> None:
        if not verification_token or not encrypt_key or not account_id:
            raise ObserverConfigurationError("observer credentials are incomplete")
        if max_signature_age_seconds <= 0:
            raise ObserverConfigurationError("signature age limit must be positive")
        self._verification_token = verification_token
        self._encrypt_key = encrypt_key
        self._account_id = account_id
        self._wall_clock = wall_clock
        self._max_signature_age_seconds = max_signature_age_seconds
        self.store = store if store is not None else ReceiptStore()

    def observe(self, headers: Mapping[str, str], body: bytes) -> CallbackReceipt | None:
        """Authenticate one mirrored callback and retain only a safe receipt."""

        if not body or len(body) > MAX_BODY_BYTES:
            raise CallbackRejected("callback is invalid")
        self._verify_signature(headers, body)
        payload = self._decrypt(body)
        self._verify_token(payload)

        event_type = _event_type(payload)
        if event_type == "url_verification" or event_type != "im.message.receive_v1":
            return None
        header = _mapping(payload.get("header"))
        app_id = _text(header.get("app_id"))
        if not hmac.compare_digest(app_id, self._account_id):
            raise CallbackRejected("callback is invalid")
        event = _mapping(payload.get("event"))
        sender = _mapping(event.get("sender"))
        if sender.get("sender_type") in {"app", "bot"}:
            return None
        message = _mapping(event.get("message"))

        event_id = _text(header.get("event_id"))
        message_id = _text(message.get("message_id"))
        provider_time = _text(message.get("create_time") or header.get("create_time"))
        message_type = _text(message.get("message_type"))
        chat_type = _text(message.get("chat_type"))
        content_raw = _text(message.get("content"))
        content = _strict_object(content_raw)
        marker_value, media_locators = _content_fingerprint_input(message_type, content)

        event_hash = _domain_hash("trpc.feishu.callback.event-id.v1", event_id)
        message_hash = _domain_hash("trpc.feishu.callback.message-id.v1", message_id)
        provider_time_hash = _domain_hash("trpc.feishu.callback.provider-time.v1", provider_time)
        marker_hash = marker_sha256(marker_value)
        callback_profile_hash = profile_sha256(
            account_id=app_id,
            event_type=event_type,
            message_type=message_type,
            chat_type=chat_type,
        )
        media_hashes = tuple(
            _domain_hash("trpc.feishu.callback.media-locator.v1", locator)
            for locator in media_locators
        )
        receipt_hash = _domain_hash(
            "trpc.feishu.callback.receipt.v1",
            event_hash + ":" + message_hash,
        )
        observed_at = datetime.fromtimestamp(self._wall_clock(), UTC).isoformat()
        return self.store.put(
            CallbackReceipt(
                receipt_sha256=receipt_hash,
                event_id_sha256=event_hash,
                message_id_sha256=message_hash,
                provider_time_sha256=provider_time_hash,
                marker_sha256=marker_hash,
                profile_sha256=callback_profile_hash,
                media_locator_sha256=media_hashes,
                observed_at=observed_at,
            )
        )

    def _verify_signature(self, headers: Mapping[str, str], body: bytes) -> None:
        timestamp = _header(headers, "X-Lark-Request-Timestamp")
        nonce = _header(headers, "X-Lark-Request-Nonce")
        supplied = _header(headers, "X-Lark-Signature")
        if not timestamp or not nonce or not supplied:
            raise CallbackRejected("callback is invalid")
        if any(
            len(value.encode("utf-8")) > MAX_HEADER_BYTES for value in (timestamp, nonce, supplied)
        ):
            raise CallbackRejected("callback is invalid")
        try:
            signed_at = int(timestamp)
        except ValueError:
            raise CallbackRejected("callback is invalid") from None
        if abs(self._wall_clock() - signed_at) > self._max_signature_age_seconds:
            raise CallbackRejected("callback is invalid")
        expected = hashlib.sha256(
            timestamp.encode() + nonce.encode() + self._encrypt_key.encode() + body
        ).hexdigest()
        if not hmac.compare_digest(expected, supplied.lower()):
            raise CallbackRejected("callback is invalid")

    def _decrypt(self, body: bytes) -> dict[str, Any]:
        outer = _strict_object(body)
        encrypted = outer.get("encrypt")
        if not isinstance(encrypted, str) or not encrypted:
            raise CallbackRejected("callback is invalid")
        try:
            ciphertext = base64.b64decode(encrypted, validate=True)
            if len(ciphertext) < 32 or len(ciphertext) % 16 != 0:
                raise ValueError
            iv, encrypted_body = ciphertext[:16], ciphertext[16:]
            key = hashlib.sha256(self._encrypt_key.encode()).digest()
            decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
            padded = decryptor.update(encrypted_body) + decryptor.finalize()
            unpadder = padding.PKCS7(128).unpadder()
            plaintext = unpadder.update(padded) + unpadder.finalize()
            return _strict_object(plaintext)
        except (ValueError, binascii.Error, UnicodeError):
            raise CallbackRejected("callback is invalid") from None

    def _verify_token(self, payload: Mapping[str, Any]) -> None:
        header = payload.get("header")
        supplied = header.get("token") if isinstance(header, Mapping) else payload.get("token")
        if not isinstance(supplied, str) or not hmac.compare_digest(
            supplied, self._verification_token
        ):
            raise CallbackRejected("callback is invalid")


def _header(headers: Mapping[str, str], wanted: str) -> str | None:
    folded = wanted.casefold()
    for key, value in headers.items():
        if key.casefold() == folded:
            return value
    return None


def _strict_object(raw: str | bytes) -> dict[str, Any]:
    try:
        value = _strict_json(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise CallbackRejected("callback is invalid") from None
    if not isinstance(value, dict):
        raise CallbackRejected("callback is invalid")
    return value


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CallbackRejected("callback is invalid")
    return value


def _text(value: object) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > MAX_BODY_BYTES:
        raise CallbackRejected("callback is invalid")
    return value


def _event_type(payload: Mapping[str, Any]) -> str | None:
    header = payload.get("header")
    value = header.get("event_type") if isinstance(header, Mapping) else payload.get("type")
    return value if isinstance(value, str) else None


def _content_fingerprint_input(
    message_type: str, content: Mapping[str, object]
) -> tuple[str, tuple[str, ...]]:
    if message_type == "text":
        marker = content.get("text")
        if not isinstance(marker, str) or not marker:
            raise CallbackRejected("callback is invalid")
    else:
        try:
            marker = _canonical_json(content)
        except (TypeError, ValueError):
            raise CallbackRejected("callback is invalid") from None
    locators: list[str] = []
    for key in ("image_key", "file_key", "media_id", "resource_key"):
        value = content.get(key)
        if isinstance(value, str) and value:
            locators.append(value)
    return marker, tuple(locators)


def query_receipt(raw: bytes, store: ReceiptStore) -> bytes:
    """Answer one strict, content-free receipt query."""

    response: dict[str, object] = {"status": "invalid"}
    if raw and len(raw) <= MAX_QUERY_BYTES:
        try:
            request = _strict_json(raw)
            if isinstance(request, dict) and set(request) == {
                "marker_sha256",
                "profile_sha256",
            }:
                marker_hash = request["marker_sha256"]
                profile_hash = request["profile_sha256"]
                if (
                    isinstance(marker_hash, str)
                    and isinstance(profile_hash, str)
                    and HASH_RE.fullmatch(marker_hash)
                    and HASH_RE.fullmatch(profile_hash)
                ):
                    receipt = store.query(marker_hash, profile_hash)
                    response = (
                        {"receipt": receipt.as_dict(), "status": "found"}
                        if receipt is not None
                        else {"status": "not_found"}
                    )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            pass
    return json.dumps(response, sort_keys=True, separators=(",", ":")).encode() + b"\n"


class _MirrorServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        observer: FeishuCallbackObserver,
    ) -> None:
        self.observer = observer
        super().__init__(server_address, _MirrorHandler)


class _MirrorHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:
        if self.path != MIRROR_PATH:
            self.send_error(404)
            return
        try:
            self.connection.settimeout(2)
            raw_length = self.headers.get("Content-Length")
            if raw_length is None:
                raise CallbackRejected("callback is invalid")
            length = int(raw_length)
            if length <= 0 or length > MAX_BODY_BYTES:
                raise CallbackRejected("callback is invalid")
            body = self.rfile.read(length)
            if len(body) != length:
                raise CallbackRejected("callback is invalid")
            headers = {
                name: self.headers.get(name, "")
                for name in (
                    "X-Lark-Request-Timestamp",
                    "X-Lark-Request-Nonce",
                    "X-Lark-Signature",
                )
            }
            cast(_MirrorServer, self.server).observer.observe(headers, body)
        except Exception:
            # Mirror processing is deliberately fail-isolated from the main
            # callback.  The observer emits no error details and always lets
            # nginx discard a successful, empty subrequest response.
            self.close_connection = True
        self.send_response_only(204)
        self.send_header("Connection", "close")
        self.end_headers()

    def do_GET(self) -> None:
        self.send_error(404)

    def log_message(self, _format: str, *args: object) -> None:
        return


def create_mirror_server(
    host: str, port: int, observer: FeishuCallbackObserver
) -> ThreadingHTTPServer:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        raise ValueError("mirror listener must use a literal loopback address") from None
    if not address.is_loopback:
        raise ValueError("mirror listener must use a loopback address")
    if not 0 <= port <= 65535:
        raise ValueError("mirror port is invalid")
    if address.version == 6:

        class _IPv6MirrorServer(_MirrorServer):
            address_family = socket.AF_INET6

        return _IPv6MirrorServer((host, port), observer)
    return _MirrorServer((host, port), observer)


class UnixQueryServer:
    """Small read-only AF_UNIX server kept independent of the HTTP listener."""

    def __init__(self, path: Path, store: ReceiptStore) -> None:
        if _AF_UNIX is None:
            raise ObserverConfigurationError("Unix query sockets are unavailable")
        self.store = store
        self._path = path
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
                while len(received) <= MAX_QUERY_BYTES:
                    chunk = connection.recv(min(4096, MAX_QUERY_BYTES + 1 - len(received)))
                    if not chunk:
                        break
                    received.extend(chunk)
                    if b"\n" in chunk:
                        break
                connection.sendall(query_receipt(bytes(received).split(b"\n", 1)[0], self.store))
            except OSError:
                return

    def shutdown(self) -> None:
        self._stopping.set()

    def server_close(self) -> None:
        self._socket.close()


def create_query_server(path: Path, store: ReceiptStore) -> UnixQueryServer:
    if os.name == "nt" or _AF_UNIX is None:
        raise ObserverConfigurationError("Unix query sockets are unavailable")
    if not path.is_absolute() or path.is_symlink():
        raise ObserverConfigurationError("query socket path is invalid")
    path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    if path.exists():
        try:
            if not stat.S_ISSOCK(path.stat().st_mode):
                raise ObserverConfigurationError("query socket path is occupied")
            path.unlink()
        except OSError as error:
            raise ObserverConfigurationError("query socket path is unavailable") from error
    server = UnixQueryServer(path, store)
    os.chmod(path, 0o660)
    return server


def _read_secret_file(value: str | None, *, label: str) -> str:
    if not value:
        raise ObserverConfigurationError(f"{label} file is not configured")
    path = Path(value)
    if (
        not path.is_absolute()
        or path.is_symlink()
        or any(parent.is_symlink() for parent in path.parents)
    ):
        raise ObserverConfigurationError(f"{label} file is unavailable")
    try:
        resolved = path.resolve(strict=True)
        mode = resolved.stat().st_mode
        if not stat.S_ISREG(mode) or (os.name != "nt" and mode & 0o027):
            raise ObserverConfigurationError(f"{label} file permissions are invalid")
        raw = resolved.read_bytes()
    except OSError as error:
        raise ObserverConfigurationError(f"{label} file is unavailable") from error
    if not raw or len(raw) > MAX_HEADER_BYTES or b"\x00" in raw:
        raise ObserverConfigurationError(f"{label} file is invalid")
    try:
        secret = raw.decode("utf-8").strip()
    except UnicodeDecodeError:
        raise ObserverConfigurationError(f"{label} file is invalid") from None
    if not secret:
        raise ObserverConfigurationError(f"{label} file is invalid")
    return secret


def _observer_from_environment() -> FeishuCallbackObserver:
    account_id = os.environ.get("TRPC_FEISHU_OBSERVER_APP_ID", "")
    if not account_id:
        raise ObserverConfigurationError("observer app ID is not configured")
    return FeishuCallbackObserver(
        verification_token=_read_secret_file(
            os.environ.get("TRPC_FEISHU_OBSERVER_VERIFICATION_TOKEN_FILE"),
            label="verification token",
        ),
        encrypt_key=_read_secret_file(
            os.environ.get("TRPC_FEISHU_OBSERVER_ENCRYPT_KEY_FILE"),
            label="encrypt key",
        ),
        account_id=account_id,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--query-socket",
        type=Path,
        default=Path("/run/trpc-im-probe/feishu-observer.sock"),
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    try:
        observer = _observer_from_environment()
        mirror = create_mirror_server(args.host, args.port, observer)
        if args.check:
            mirror.server_close()
            print('{"status":"ready"}')
            return 0
        query = create_query_server(args.query_socket, observer.store)
    except (ObserverConfigurationError, OSError, ValueError):
        print('{"status":"not_ready"}')
        return 2

    query_thread = Thread(target=query.serve_forever, daemon=True)
    query_thread.start()
    try:
        mirror.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        mirror.server_close()
        query.shutdown()
        query.server_close()
        query_thread.join(timeout=5)
        try:
            args.query_socket.unlink(missing_ok=True)
        except OSError:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = [
    "CallbackReceipt",
    "CallbackRejected",
    "FeishuCallbackObserver",
    "ReceiptStore",
    "create_mirror_server",
    "create_query_server",
    "marker_sha256",
    "profile_sha256",
    "query_receipt",
]
