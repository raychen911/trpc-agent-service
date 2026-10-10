#!/usr/bin/python3
"""Independent, fail-closed Feishu provider-evidence driver.

The driver does not implement provider actions and never manufactures an
observation.  It asks a separately deployed control broker to execute each
fixed action, then verifies the broker's callback claim against the
content-free receipt produced by the independent Feishu callback observer.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import socket
import stat
import struct
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO, cast

MAX_INPUT_BYTES = 64 * 1024
MAX_PROFILE_BYTES = 64 * 1024
MAX_BROKER_RESPONSE_BYTES = 256 * 1024
MAX_OBSERVER_RESPONSE_BYTES = 16 * 1024
MAX_WITNESS_RESPONSE_BYTES = 256 * 1024
TOTAL_TIMEOUT_SECONDS = 175.0
TIMESTAMP_SKEW_SECONDS = 5.0

REQUIRED_CASES = (
    "round_trip",
    "idempotency",
    "media",
    "reconnect",
    "rate_limit_retry_after",
    "credential_rotation",
    "prolonged_outage",
    "ambiguous",
)
BROKER_ACTIONS = {
    "round_trip": "feishu_round_trip",
    "idempotency": "feishu_idempotency",
    "media": "feishu_media",
    "reconnect": "feishu_reconnect",
    "rate_limit_retry_after": "feishu_rate_limit_retry_after",
    "credential_rotation": "feishu_credential_rotation",
    "prolonged_outage": "feishu_prolonged_outage",
    "ambiguous": "feishu_ambiguous",
}
ACK_CASES = frozenset(
    {
        "round_trip",
        "reconnect",
        "rate_limit_retry_after",
        "credential_rotation",
        "prolonged_outage",
        "ambiguous",
    }
)
REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "channel",
        "run_id",
        "run_nonce",
        "expected_image_digest",
        "release_id",
        "release_nonce_sha256",
        "source_fingerprint",
        "control_profile_sha256",
        "cases",
    }
)
PROFILE_FIELDS = frozenset(
    {
        "schema_version",
        "channel",
        "account_id_sha256",
        "observer_socket",
        "observer_profile_sha256",
        "openapi_witness_socket",
        "broker_actions",
    }
)
BROKER_RESPONSE_FIELDS = frozenset({"status", "result"})
BROKER_RESULT_FIELDS = frozenset({"observation", "callback_query", "callback_expected"})
BROKER_ACK_RESULT_FIELDS = BROKER_RESULT_FIELDS | {"openapi_witness"}
CALLBACK_QUERY_FIELDS = frozenset({"marker_sha256", "profile_sha256"})
CALLBACK_EXPECTED_FIELDS = frozenset({"event_id_sha256", "message_id_sha256"})
OBSERVER_RESPONSE_FIELDS = frozenset({"status", "receipt"})
RECEIPT_FIELDS = frozenset(
    {
        "event_id_sha256",
        "marker_sha256",
        "media_locator_sha256",
        "message_id_sha256",
        "observed_at",
        "profile_sha256",
        "provider_time_sha256",
        "receipt_sha256",
    }
)
WITNESS_BINDING_FIELDS = frozenset({"after_sequence", "path_sha256", "body_sha256"})
WITNESS_RESPONSE_FIELDS = frozenset({"status", "receipts"})
WITNESS_RECEIPT_FIELDS = frozenset(
    {
        "sequence",
        "path_sha256",
        "body_sha256",
        "provider_status",
        "provider_code",
        "provider_request_id_sha256",
        "retry_after_seconds",
        "provider_acknowledged",
        "downstream_response_dropped",
        "observed_at",
    }
)
COMMON_OBSERVATION_FIELDS = frozenset({"status", "run_nonce", "provider_event_id", "observed_at"})
OBSERVATION_FIELDS = {
    "round_trip": COMMON_OBSERVATION_FIELDS
    | {"callback_event_id", "outbound_request_id", "provider_code"},
    "idempotency": COMMON_OBSERVATION_FIELDS
    | {
        "duplicate_event_id",
        "unique_inbound_id",
        "duplicate_count",
        "original_event_id",
        "provider_delivery_count",
    },
    "media": COMMON_OBSERVATION_FIELDS | {"media_id_hash", "sha256", "bytes"},
    "reconnect": COMMON_OBSERVATION_FIELDS
    | {
        "failed_endpoint_id",
        "replacement_endpoint_id",
        "endpoint_set_observed",
        "received_after_failover_event_id",
        "outbound_request_id",
        "acknowledged_request_id",
        "ready_endpoint_count",
        "unready_endpoint_count",
        "terminating_endpoint_count",
    },
    "rate_limit_retry_after": COMMON_OBSERVATION_FIELDS
    | {
        "provider_error_code",
        "retry_after_seconds",
        "retry_request_id",
        "retry_attempts",
        "retry_elapsed_seconds",
    },
    "credential_rotation": COMMON_OBSERVATION_FIELDS
    | {
        "old_credential_event_id",
        "new_credential_event_id",
        "post_rotation_event_id",
        "old_credential_rejected",
    },
    "prolonged_outage": COMMON_OBSERVATION_FIELDS
    | {"outage_event_id", "recovery_event_id", "outage_seconds"},
    "ambiguous": COMMON_OBSERVATION_FIELDS
    | {
        "ambiguous_event_id",
        "manual_review_id",
        "drop_response_observed",
        "auto_replay_count",
    },
}

PROFILE_ENV = "TRPC_IM_PROBE_FEISHU_CONTROL_PROFILE_FILE"
ACCOUNT_ENV = "TRPC_IM_PROBE_FEISHU_APP_ID"
BROKER_SOCKET_ENV = "TRPC_IM_PROBE_CONTROL_SOCKET"
BROKER_UID_ENV = "TRPC_IM_PROBE_BROKER_UID"
BROKER_GID_ENV = "TRPC_IM_PROBE_BROKER_GID"
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{16,256}$")
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
FEISHU_RATE_LIMIT_CODES = frozenset({429, 99991400, 99991401, 99991402, 99991672})
_AF_UNIX = cast(int | None, getattr(socket, "AF_UNIX", None))

SocketValidator = Callable[[str, str], Path]
Exchange = Callable[[Path, bytes, float, int], bytes]
BrokerExchange = Callable[[Path, bytes, float, int, int, int], bytes]


class DriverError(RuntimeError):
    """Content-free driver failure."""


@dataclass(frozen=True, slots=True)
class ControlProfile:
    raw_sha256: str
    account_id_sha256: str
    observer_socket: Path
    observer_profile_sha256: str
    openapi_witness_socket: Path


@dataclass(frozen=True, slots=True)
class WitnessBinding:
    after_sequence: int
    path_sha256: str
    body_sha256: str


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


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()


def _safe_hash(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or HASH_RE.fullmatch(value) is None
        or value in {"0" * 64, "f" * 64}
    ):
        raise DriverError(f"{label} is invalid")
    return value


def account_id_sha256(account_id: str) -> str:
    if (
        not isinstance(account_id, str)
        or not account_id
        or len(account_id.encode()) > 4096
        or "\x00" in account_id
    ):
        raise DriverError("account binding is invalid")
    return hashlib.sha256(account_id.encode()).hexdigest()


def _parse_request(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != REQUEST_FIELDS:
        raise DriverError("request schema is invalid")
    if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        raise DriverError("request schema is invalid")
    if value.get("channel") != "feishu":
        raise DriverError("request channel is invalid")
    run_id = value.get("run_id")
    run_nonce = value.get("run_nonce")
    digest = value.get("expected_image_digest")
    release_id = value.get("release_id")
    if not isinstance(run_id, str) or SAFE_ID_RE.fullmatch(run_id) is None:
        raise DriverError("request run binding is invalid")
    if not isinstance(run_nonce, str) or NONCE_RE.fullmatch(run_nonce) is None:
        raise DriverError("request nonce is invalid")
    if not isinstance(digest, str) or IMAGE_RE.fullmatch(digest) is None:
        raise DriverError("request image digest is invalid")
    if not isinstance(release_id, str) or SAFE_ID_RE.fullmatch(release_id) is None:
        raise DriverError("request release id is invalid")
    release_nonce_sha256 = _safe_hash(
        value.get("release_nonce_sha256"), label="request release nonce hash"
    )
    deployed_source_fingerprint = _safe_hash(
        value.get("source_fingerprint"), label="request source fingerprint"
    )
    profile_hash = _safe_hash(
        value.get("control_profile_sha256"), label="request control profile hash"
    )
    cases = value.get("cases")
    if (
        not isinstance(cases, list)
        or any(type(case) is not str for case in cases)
        or cases != list(REQUIRED_CASES)
    ):
        raise DriverError("request cases are invalid")
    return {
        "schema_version": 1,
        "channel": "feishu",
        "run_id": run_id,
        "run_nonce": run_nonce,
        "expected_image_digest": digest,
        "release_id": release_id,
        "release_nonce_sha256": release_nonce_sha256,
        "source_fingerprint": deployed_source_fingerprint,
        "control_profile_sha256": profile_hash,
        "cases": list(REQUIRED_CASES),
    }


def _profile_path(environment: Mapping[str, str]) -> Path:
    raw = environment.get(PROFILE_ENV, "").strip()
    if not raw:
        raise DriverError("control profile is not configured")
    path = Path(raw)
    if (
        not path.is_absolute()
        or path.is_symlink()
        or any(parent.is_symlink() for parent in path.parents)
    ):
        raise DriverError("control profile path is invalid")
    try:
        resolved = path.resolve(strict=True)
        mode = resolved.stat().st_mode
    except (OSError, RuntimeError) as error:
        raise DriverError("control profile is unavailable") from error
    if not stat.S_ISREG(mode) or (os.name != "nt" and mode & 0o027):
        raise DriverError("control profile permissions are invalid")
    return resolved


def _read_profile(path: Path) -> tuple[bytes, dict[str, Any]]:
    try:
        with path.open("rb") as stream:
            raw = stream.read(MAX_PROFILE_BYTES + 1)
    except OSError as error:
        raise DriverError("control profile is unavailable") from error
    if not raw or len(raw) > MAX_PROFILE_BYTES:
        raise DriverError("control profile size is invalid")
    try:
        value = _strict_json(raw)
    except (UnicodeError, ValueError, json.JSONDecodeError):
        raise DriverError("control profile schema is invalid") from None
    if not isinstance(value, dict) or set(value) != PROFILE_FIELDS:
        raise DriverError("control profile schema is invalid")
    return raw, value


def _load_profile(
    environment: Mapping[str, str],
    *,
    expected_sha256: str | None,
    socket_validator: SocketValidator,
) -> ControlProfile:
    raw, value = _read_profile(_profile_path(environment))
    observed_hash = hashlib.sha256(raw).hexdigest()
    if expected_sha256 is not None and observed_hash != expected_sha256:
        raise DriverError("control profile hash mismatch")
    if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        raise DriverError("control profile schema is invalid")
    if value.get("channel") != "feishu":
        raise DriverError("control profile channel is invalid")
    actions = value.get("broker_actions")
    if not isinstance(actions, dict) or actions != BROKER_ACTIONS:
        raise DriverError("control profile broker actions are invalid")
    configured_account_hash = _safe_hash(
        value.get("account_id_sha256"), label="control profile account hash"
    )
    account_id = environment.get(ACCOUNT_ENV, "")
    if configured_account_hash != account_id_sha256(account_id):
        raise DriverError("control profile account binding mismatch")
    observer_profile_hash = _safe_hash(
        value.get("observer_profile_sha256"),
        label="control profile observer hash",
    )
    observer_socket_raw = value.get("observer_socket")
    witness_socket_raw = value.get("openapi_witness_socket")
    if not isinstance(observer_socket_raw, str) or not isinstance(witness_socket_raw, str):
        raise DriverError("control profile observer socket is invalid")
    observer_socket = socket_validator(observer_socket_raw, "observer socket")
    witness_socket = socket_validator(witness_socket_raw, "OpenAPI witness socket")
    return ControlProfile(
        raw_sha256=observed_hash,
        account_id_sha256=configured_account_hash,
        observer_socket=observer_socket,
        observer_profile_sha256=observer_profile_hash,
        openapi_witness_socket=witness_socket,
    )


def _validate_unix_socket(raw: str, label: str) -> Path:
    if _AF_UNIX is None:
        raise DriverError(f"{label} is unavailable")
    path = Path(raw)
    if (
        not path.is_absolute()
        or path.is_symlink()
        or any(parent.is_symlink() for parent in path.parents)
    ):
        raise DriverError(f"{label} is invalid")
    try:
        resolved = path.resolve(strict=True)
        mode = resolved.stat().st_mode
    except (OSError, RuntimeError) as error:
        raise DriverError(f"{label} is unavailable") from error
    if not stat.S_ISSOCK(mode):
        raise DriverError(f"{label} is invalid")
    return resolved


def _broker_socket(environment: Mapping[str, str], socket_validator: SocketValidator) -> Path:
    raw = environment.get(BROKER_SOCKET_ENV, "").strip()
    if not raw:
        raise DriverError("broker socket is not configured")
    return socket_validator(raw, "broker socket")


def _broker_identity(environment: Mapping[str, str]) -> tuple[int, int]:
    try:
        uid = int(environment.get(BROKER_UID_ENV, ""))
        gid = int(environment.get(BROKER_GID_ENV, ""))
    except ValueError as error:
        raise DriverError("broker identity is invalid") from error
    if uid < 0 or gid < 0:
        raise DriverError("broker identity is invalid")
    if sys.platform.startswith("linux"):
        current_euid = getattr(os, "geteuid", None)
        if current_euid is None or uid == current_euid():
            raise DriverError("control broker must use a dedicated uid")
        if not hasattr(socket, "SO_PEERCRED"):
            raise DriverError("Linux SO_PEERCRED is unavailable")
    return uid, gid


def _verify_connected_broker_peer(
    connection: socket.socket,
    *,
    expected_uid: int,
    expected_gid: int,
) -> None:
    if not sys.platform.startswith("linux"):
        return
    try:
        raw = connection.getsockopt(
            socket.SOL_SOCKET,
            socket.SO_PEERCRED,
            struct.calcsize("3i"),
        )
        _pid, uid, gid = struct.unpack("3i", raw)
    except (OSError, struct.error) as error:
        raise DriverError("control broker peer identity is unavailable") from error
    if uid != expected_uid or gid != expected_gid:
        raise DriverError("control broker peer identity does not match")


def _unix_exchange(path: Path, payload: bytes, timeout: float, limit: int) -> bytes:
    if _AF_UNIX is None:
        raise DriverError("control socket is unavailable")
    if timeout <= 0:
        raise DriverError("driver time budget is exhausted")
    connection = socket.socket(_AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(timeout)
    received = bytearray()
    try:
        connection.connect(str(path))
        connection.sendall(payload + b"\n")
        connection.shutdown(socket.SHUT_WR)
        while len(received) <= limit:
            chunk = connection.recv(min(4096, limit + 1 - len(received)))
            if not chunk:
                break
            received.extend(chunk)
    except OSError as error:
        raise DriverError("control socket exchange failed") from error
    finally:
        connection.close()
    return bytes(received).split(b"\n", 1)[0]


def _authenticated_broker_exchange(
    path: Path,
    payload: bytes,
    timeout: float,
    limit: int,
    expected_uid: int,
    expected_gid: int,
) -> bytes:
    if _AF_UNIX is None:
        raise DriverError("control socket is unavailable")
    if timeout <= 0:
        raise DriverError("driver time budget is exhausted")
    connection = socket.socket(_AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(timeout)
    received = bytearray()
    try:
        connection.connect(str(path))
        _verify_connected_broker_peer(
            connection,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
        )
        connection.sendall(payload + b"\n")
        connection.shutdown(socket.SHUT_WR)
        while len(received) <= limit:
            chunk = connection.recv(min(4096, limit + 1 - len(received)))
            if not chunk:
                break
            received.extend(chunk)
    except DriverError:
        raise
    except OSError as error:
        raise DriverError("control socket exchange failed") from error
    finally:
        connection.close()
    return bytes(received).split(b"\n", 1)[0]


def _decode_object(raw: bytes, *, limit: int, label: str) -> dict[str, Any]:
    if not raw or len(raw) > limit:
        raise DriverError(f"{label} response is too large")
    try:
        value = _strict_json(raw)
    except (UnicodeError, ValueError, json.JSONDecodeError):
        raise DriverError(f"{label} response schema is invalid") from None
    if not isinstance(value, dict):
        raise DriverError(f"{label} response schema is invalid")
    return value


def _validate_observation(case: str, value: object, *, run_nonce: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != OBSERVATION_FIELDS[case]:
        raise DriverError("broker observation schema is invalid")
    if value.get("status") != "pass" or value.get("run_nonce") != run_nonce:
        raise DriverError("broker observation run binding is invalid")
    return {key: value[key] for key in OBSERVATION_FIELDS[case]}


def _parse_broker_response(
    raw: bytes,
    *,
    case: str,
    run_nonce: str,
    observer_profile_sha256: str,
) -> tuple[
    dict[str, Any],
    dict[str, str],
    dict[str, str],
    WitnessBinding | None,
]:
    response = _decode_object(raw, limit=MAX_BROKER_RESPONSE_BYTES, label="broker")
    if set(response) != BROKER_RESPONSE_FIELDS or response.get("status") != "pass":
        raise DriverError("broker response schema is invalid")
    result = response.get("result")
    expected_result_fields = BROKER_ACK_RESULT_FIELDS if case in ACK_CASES else BROKER_RESULT_FIELDS
    if not isinstance(result, dict) or set(result) != expected_result_fields:
        raise DriverError("broker response schema is invalid")
    observation = _validate_observation(case, result.get("observation"), run_nonce=run_nonce)
    query = result.get("callback_query")
    expected = result.get("callback_expected")
    if not isinstance(query, dict) or set(query) != CALLBACK_QUERY_FIELDS:
        raise DriverError("broker callback query is invalid")
    if not isinstance(expected, dict) or set(expected) != CALLBACK_EXPECTED_FIELDS:
        raise DriverError("broker callback expectation is invalid")
    marker_hash = _safe_hash(query.get("marker_sha256"), label="broker marker hash")
    profile_hash = _safe_hash(query.get("profile_sha256"), label="broker profile hash")
    if profile_hash != observer_profile_sha256:
        raise DriverError("observer receipt profile mismatch")
    event_hash = _safe_hash(expected.get("event_id_sha256"), label="broker event hash")
    message_hash = _safe_hash(expected.get("message_id_sha256"), label="broker message hash")
    witness_binding: WitnessBinding | None = None
    if case in ACK_CASES:
        witness = result.get("openapi_witness")
        if not isinstance(witness, dict) or set(witness) != WITNESS_BINDING_FIELDS:
            raise DriverError("broker OpenAPI witness binding is invalid")
        after_sequence = witness.get("after_sequence")
        if (
            isinstance(after_sequence, bool)
            or not isinstance(after_sequence, int)
            or after_sequence < 0
        ):
            raise DriverError("broker OpenAPI witness sequence is invalid")
        witness_binding = WitnessBinding(
            after_sequence=after_sequence,
            path_sha256=_safe_hash(witness.get("path_sha256"), label="broker OpenAPI path hash"),
            body_sha256=_safe_hash(witness.get("body_sha256"), label="broker OpenAPI body hash"),
        )
    return (
        observation,
        {"marker_sha256": marker_hash, "profile_sha256": profile_hash},
        {"event_id_sha256": event_hash, "message_id_sha256": message_hash},
        witness_binding,
    )


def _parse_observed_at(value: object, *, earliest: datetime) -> datetime:
    if not isinstance(value, str) or not 20 <= len(value) <= 64:
        raise DriverError("observer receipt timestamp is invalid")
    try:
        observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DriverError("observer receipt timestamp is invalid") from None
    if observed.tzinfo is None:
        raise DriverError("observer receipt timestamp is invalid")
    observed = observed.astimezone(UTC)
    now = datetime.now(UTC)
    if observed < earliest - timedelta(seconds=TIMESTAMP_SKEW_SECONDS):
        raise DriverError("observer receipt is stale")
    if observed > now + timedelta(seconds=TIMESTAMP_SKEW_SECONDS):
        raise DriverError("observer receipt timestamp is invalid")
    return observed


def _validate_receipt(
    raw: bytes,
    *,
    query: Mapping[str, str],
    expected: Mapping[str, str],
    earliest: datetime,
) -> None:
    response = _decode_object(raw, limit=MAX_OBSERVER_RESPONSE_BYTES, label="observer")
    if set(response) != OBSERVER_RESPONSE_FIELDS or response.get("status") != "found":
        raise DriverError("observer receipt is missing")
    receipt = response.get("receipt")
    if not isinstance(receipt, dict) or set(receipt) != RECEIPT_FIELDS:
        raise DriverError("observer receipt schema is invalid")
    hashes = {
        field: _safe_hash(receipt.get(field), label="observer receipt hash")
        for field in (
            "event_id_sha256",
            "marker_sha256",
            "message_id_sha256",
            "profile_sha256",
            "provider_time_sha256",
            "receipt_sha256",
        )
    }
    media_hashes = receipt.get("media_locator_sha256")
    if not isinstance(media_hashes, list):
        raise DriverError("observer receipt media hashes are invalid")
    for value in media_hashes:
        _safe_hash(value, label="observer receipt media hash")
    _parse_observed_at(receipt.get("observed_at"), earliest=earliest)
    if (
        hashes["marker_sha256"] != query["marker_sha256"]
        or hashes["profile_sha256"] != query["profile_sha256"]
        or hashes["event_id_sha256"] != expected["event_id_sha256"]
        or hashes["message_id_sha256"] != expected["message_id_sha256"]
    ):
        raise DriverError("observer receipt hash mismatch")


def _witness_receipt(value: object, *, earliest: datetime) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != WITNESS_RECEIPT_FIELDS:
        raise DriverError("OpenAPI witness receipt schema is invalid")
    sequence = value.get("sequence")
    provider_status = value.get("provider_status")
    provider_code = value.get("provider_code")
    retry_after = value.get("retry_after_seconds")
    acknowledged = value.get("provider_acknowledged")
    dropped = value.get("downstream_response_dropped")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        raise DriverError("OpenAPI witness receipt sequence is invalid")
    if (
        isinstance(provider_status, bool)
        or not isinstance(provider_status, int)
        or not 100 <= provider_status <= 599
    ):
        raise DriverError("OpenAPI witness provider status is invalid")
    if provider_code is not None and (
        isinstance(provider_code, bool) or not isinstance(provider_code, int)
    ):
        raise DriverError("OpenAPI witness provider code is invalid")
    if retry_after is not None and (
        isinstance(retry_after, bool)
        or not isinstance(retry_after, (int, float))
        or not math.isfinite(float(retry_after))
        or not 0 <= float(retry_after) <= 3600
    ):
        raise DriverError("OpenAPI witness Retry-After is invalid")
    if type(acknowledged) is not bool or type(dropped) is not bool:
        raise DriverError("OpenAPI witness acknowledgement is invalid")
    path_hash = _safe_hash(value.get("path_sha256"), label="OpenAPI witness path hash")
    body_hash = _safe_hash(value.get("body_sha256"), label="OpenAPI witness body hash")
    request_id = value.get("provider_request_id_sha256")
    if request_id is not None:
        request_id = _safe_hash(request_id, label="OpenAPI witness request ID hash")
    observed_at = _parse_observed_at(value.get("observed_at"), earliest=earliest)
    return {
        "sequence": sequence,
        "path_sha256": path_hash,
        "body_sha256": body_hash,
        "provider_status": provider_status,
        "provider_code": provider_code,
        "provider_request_id_sha256": request_id,
        "retry_after_seconds": retry_after,
        "provider_acknowledged": acknowledged,
        "downstream_response_dropped": dropped,
        "observed_at": observed_at,
    }


def _provider_code_number(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _validate_openapi_witness(
    raw: bytes,
    *,
    binding: WitnessBinding,
    case: str,
    observation: Mapping[str, Any],
    earliest: datetime,
) -> None:
    response = _decode_object(raw, limit=MAX_WITNESS_RESPONSE_BYTES, label="OpenAPI witness")
    if set(response) != WITNESS_RESPONSE_FIELDS or response.get("status") != "pass":
        raise DriverError("OpenAPI witness response schema is invalid")
    raw_receipts = response.get("receipts")
    if not isinstance(raw_receipts, list) or not 1 <= len(raw_receipts) <= 100:
        raise DriverError("OpenAPI witness receipt is missing")
    receipts = [_witness_receipt(value, earliest=earliest) for value in raw_receipts]
    sequences = [cast(int, receipt["sequence"]) for receipt in receipts]
    if sequences != sorted(set(sequences)) or any(
        sequence <= binding.after_sequence for sequence in sequences
    ):
        raise DriverError("OpenAPI witness sequence boundary is invalid")
    matching = [
        receipt
        for receipt in receipts
        if receipt["path_sha256"] == binding.path_sha256
        and receipt["body_sha256"] == binding.body_sha256
    ]
    if not matching:
        raise DriverError("OpenAPI witness receipt is missing")

    if case == "rate_limit_retry_after":
        expected_code = _provider_code_number(observation.get("provider_error_code"))
        retry_after = observation.get("retry_after_seconds")
        retry_attempts = observation.get("retry_attempts")
        if (
            expected_code not in FEISHU_RATE_LIMIT_CODES
            or isinstance(retry_after, bool)
            or not isinstance(retry_after, (int, float))
            or isinstance(retry_attempts, bool)
            or not isinstance(retry_attempts, int)
            or not 2 <= retry_attempts <= 100
        ):
            raise DriverError("OpenAPI witness rate-limit binding is invalid")
        if len(matching) != retry_attempts:
            raise DriverError("OpenAPI witness rate-limit receipts are incomplete")
        limited = matching[:-1]
        acknowledged_receipt = matching[-1]
        if (
            acknowledged_receipt["provider_acknowledged"] is not True
            or not 200 <= cast(int, acknowledged_receipt["provider_status"]) < 300
            or acknowledged_receipt["provider_code"] != 0
            or acknowledged_receipt["downstream_response_dropped"] is not False
        ):
            raise DriverError("OpenAPI witness rate-limit success ACK is invalid")
        for receipt in limited:
            if (
                receipt["provider_acknowledged"] is not False
                or (
                    receipt["provider_status"] != expected_code
                    and receipt["provider_code"] != expected_code
                )
                or receipt["retry_after_seconds"] is None
                or receipt["downstream_response_dropped"] is not False
            ):
                raise DriverError("OpenAPI witness rate-limit receipt is invalid")
        limited_receipt = limited[-1]
        witnessed_retry_after = cast(float | int, limited_receipt["retry_after_seconds"])
        if abs(float(witnessed_retry_after) - float(retry_after)) > 0.001:
            raise DriverError("OpenAPI witness Retry-After mismatch")
        for index in range(len(matching) - 1):
            before = matching[index]
            after = matching[index + 1]
            required_delay = before["retry_after_seconds"]
            if required_delay is None:
                continue
            elapsed = (
                cast(datetime, after["observed_at"]) - cast(datetime, before["observed_at"])
            ).total_seconds()
            if elapsed < float(cast(float | int, required_delay)) * 0.9:
                raise DriverError("OpenAPI witness retry did not honor Retry-After")
        for receipt in matching:
            if receipt["provider_request_id_sha256"] is None:
                raise DriverError("OpenAPI witness request ID hash is missing")
        return

    if len(matching) != 1:
        raise DriverError("OpenAPI witness provider acknowledgement is invalid")
    receipt = matching[0]
    if (
        receipt["provider_acknowledged"] is not True
        or not 200 <= cast(int, receipt["provider_status"]) < 300
        or receipt["provider_code"] != 0
    ):
        raise DriverError("OpenAPI witness provider acknowledgement is invalid")
    if receipt["provider_request_id_sha256"] is None:
        raise DriverError("OpenAPI witness request ID hash is missing")
    expected_drop = case == "ambiguous"
    if receipt["downstream_response_dropped"] is not expected_drop:
        raise DriverError("OpenAPI witness response-drop evidence is invalid")
    if case == "round_trip" and _provider_code_number(observation.get("provider_code")) != 0:
        raise DriverError("OpenAPI witness provider code mismatch")


def _remaining(deadline: float, monotonic: Callable[[], float]) -> float:
    value = deadline - monotonic()
    if value <= 0:
        raise DriverError("driver time budget is exhausted")
    return min(value, TOTAL_TIMEOUT_SECONDS)


def run_driver(
    request_value: object,
    environment: Mapping[str, str],
    *,
    exchange: Exchange = _unix_exchange,
    broker_exchange: BrokerExchange | None = None,
    socket_validator: SocketValidator = _validate_unix_socket,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    request = _parse_request(request_value)
    broker_socket = _broker_socket(environment, socket_validator)
    broker_uid, broker_gid = _broker_identity(environment)
    if broker_exchange is None:
        if exchange is _unix_exchange:
            broker_exchange = _authenticated_broker_exchange
        else:

            def injected_broker_exchange(
                path: Path,
                payload: bytes,
                timeout: float,
                limit: int,
                _uid: int,
                _gid: int,
            ) -> bytes:
                return exchange(path, payload, timeout, limit)

            broker_exchange = injected_broker_exchange
    profile = _load_profile(
        environment,
        expected_sha256=cast(str, request["control_profile_sha256"]),
        socket_validator=socket_validator,
    )
    started_at = datetime.now(UTC)
    deadline = monotonic() + TOTAL_TIMEOUT_SECONDS
    observations: dict[str, dict[str, Any]] = {}
    for case in REQUIRED_CASES:
        broker_request = {
            "schema_version": 1,
            "channel": "feishu",
            "action": BROKER_ACTIONS[case],
            "run_id": request["run_id"],
            "run_nonce": request["run_nonce"],
            "control_profile_sha256": profile.raw_sha256,
            "payload": {
                "case": case,
                "expected_image_digest": request["expected_image_digest"],
                "account_id_sha256": profile.account_id_sha256,
                "observer_profile_sha256": profile.observer_profile_sha256,
            },
        }
        broker_raw = broker_exchange(
            broker_socket,
            _canonical_json(broker_request),
            _remaining(deadline, monotonic),
            MAX_BROKER_RESPONSE_BYTES,
            broker_uid,
            broker_gid,
        )
        observation, callback_query, callback_expected, witness_binding = _parse_broker_response(
            broker_raw,
            case=case,
            run_nonce=cast(str, request["run_nonce"]),
            observer_profile_sha256=profile.observer_profile_sha256,
        )
        observer_raw = exchange(
            profile.observer_socket,
            _canonical_json(callback_query),
            _remaining(deadline, monotonic),
            MAX_OBSERVER_RESPONSE_BYTES,
        )
        _validate_receipt(
            observer_raw,
            query=callback_query,
            expected=callback_expected,
            earliest=started_at,
        )
        if case in ACK_CASES:
            if witness_binding is None:
                raise DriverError("OpenAPI witness binding is missing")
            witness_query = {
                "schema_version": 1,
                "action": "query",
                "after_sequence": witness_binding.after_sequence,
                "limit": 100,
            }
            witness_raw = exchange(
                profile.openapi_witness_socket,
                _canonical_json(witness_query),
                _remaining(deadline, monotonic),
                MAX_WITNESS_RESPONSE_BYTES,
            )
            _validate_openapi_witness(
                witness_raw,
                binding=witness_binding,
                case=case,
                observation=observation,
                earliest=started_at,
            )
        observations[case] = observation
    return {"schema_version": 1, "observations": observations}


def check_configuration(
    environment: Mapping[str, str],
    *,
    socket_validator: SocketValidator = _validate_unix_socket,
) -> dict[str, str]:
    _broker_socket(environment, socket_validator)
    _broker_identity(environment)
    _load_profile(
        environment,
        expected_sha256=None,
        socket_validator=socket_validator,
    )
    return {"status": "ready"}


def _input_stream() -> BinaryIO:
    return cast(BinaryIO, getattr(sys.stdin, "buffer", sys.stdin))


def _read_request() -> object:
    raw = _input_stream().read(MAX_INPUT_BYTES + 1)
    if isinstance(raw, str):
        raw = raw.encode()
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_INPUT_BYTES:
        raise DriverError("request is too large")
    try:
        return _strict_json(raw)
    except (UnicodeError, ValueError, json.JSONDecodeError):
        raise DriverError("request schema is invalid") from None


def _run_from_environment() -> dict[str, object]:
    return run_driver(_read_request(), os.environ)


def _write_status(value: Mapping[str, object]) -> None:
    sys.stdout.write(json.dumps(dict(value), sort_keys=True, separators=(",", ":")) + "\n")


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments == ["--check"]:
        try:
            result: Mapping[str, object] = check_configuration(os.environ)
            code = 0
        except Exception:
            result = {"status": "not_ready"}
            code = 2
        _write_status(result)
        return code
    if arguments:
        _write_status({"status": "not_run"})
        return 2
    try:
        result = _run_from_environment()
    except Exception:
        _write_status({"status": "not_run"})
        return 2
    _write_status(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = [
    "ACK_CASES",
    "BROKER_ACTIONS",
    "REQUIRED_CASES",
    "DriverError",
    "account_id_sha256",
    "check_configuration",
    "run_driver",
]
