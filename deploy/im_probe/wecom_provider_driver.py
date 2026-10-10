#!/usr/bin/python3
"""Independent, broker-only WeCom provider evidence driver.

The driver never connects to the provider transport.  Each required case is
delegated to one host-allowlisted control-broker action.  Broker observations
are checked against a hash-only Admin snapshot and all raw correlation IDs are
replaced with context-bound opaque identifiers before stdout is written.
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
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO, cast

MAX_INPUT_BYTES = 64 * 1024
MAX_OUTPUT_BYTES = 64 * 1024
MAX_PROFILE_BYTES = 64 * 1024
MAX_BROKER_BYTES = 64 * 1024
BROKER_TIMEOUT_SECONDS = 185.0
MAX_RETRY_SECONDS = 3600.0
MAX_RETRY_ATTEMPTS = 100
MIN_OUTAGE_SECONDS = 60.0
MAX_OUTAGE_SECONDS = 7 * 24 * 60 * 60.0
HASH_RE = re.compile(r"^[0-9a-f]{64}$")

PROFILE_ENV = "TRPC_IM_PROBE_WECOM_CONTROL_PROFILE_FILE"
SOCKET_ENV = "TRPC_IM_PROBE_CONTROL_SOCKET"
ACCOUNT_ENV = "TRPC_IM_PROBE_WECOM_BOT_ID"
BROKER_UID_ENV = "TRPC_IM_PROBE_BROKER_UID"
BROKER_GID_ENV = "TRPC_IM_PROBE_BROKER_GID"

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
        "tenant_id",
        "binding_id",
        "account_id_sha256",
        "actions",
    }
)
BROKER_REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "channel",
        "action",
        "run_id",
        "run_nonce",
        "control_profile_sha256",
        "payload",
    }
)
BROKER_RESPONSE_FIELDS = frozenset({"status", "result"})
BROKER_RESULT_FIELDS = frozenset({"observation", "wecom_snapshot"})
SNAPSHOT_FIELDS = frozenset(
    {
        "schema_version",
        "channel",
        "case",
        "tenant_id",
        "binding_id",
        "account_id_sha256",
        "provider_event_hash",
        "lifecycle",
    }
)
LIFECYCLE_FIELDS = frozenset({"state", "epoch"})
LIFECYCLE_STATES = frozenset(
    {"connected", "disconnected", "released", "acquired", "takeover", "authenticated"}
)
GRACEFUL_HANDOFF_LIFECYCLE = ("disconnected", "released", "acquired", "authenticated")
HARD_FAILOVER_LIFECYCLE = ("takeover", "authenticated")

IDEMPOTENCY_FIELDS = (
    "duplicate_event_id",
    "unique_inbound_id",
    "duplicate_count",
    "duplicate_source",
    "original_event_id",
    "replayed_event_id",
)
RECONNECT_FIELDS = (
    "disconnect_event_id",
    "reconnect_event_id",
    "received_after_reconnect_event_id",
    "lock_takeover_event_id",
    "old_lock_owner_released",
    "new_lock_owner_acquired",
    "lock_epoch",
    "outbound_request_id",
    "acknowledged_request_id",
    "provider_code",
)
OUTAGE_FIELDS = (
    "outage_event_id",
    "recovery_event_id",
    "outage_seconds",
    "outage_mode",
    "failed_instance_id",
    "takeover_instance_id",
    "old_lock_owner_released",
    "new_lock_owner_acquired",
    "connection_epoch",
    "event_during_outage_id",
    "reply_for_event_id",
    "outbound_request_id",
    "acknowledged_request_id",
    "reply_count",
    "ack_count",
    "pending_count",
    "dlq_count",
)
OBSERVATION_FIELDS = {
    "round_trip": ("callback_event_id", "outbound_request_id", "provider_code"),
    "idempotency": IDEMPOTENCY_FIELDS,
    "media": ("media_id_hash", "sha256", "bytes"),
    "reconnect": RECONNECT_FIELDS,
    "rate_limit_retry_after": (
        "provider_error_code",
        "retry_after_seconds",
        "retry_request_id",
        "retry_attempts",
        "retry_elapsed_seconds",
    ),
    "credential_rotation": (
        "old_credential_event_id",
        "new_credential_event_id",
        "post_rotation_event_id",
        "old_credential_rejected",
        "outbound_request_id",
        "acknowledged_request_id",
        "provider_code",
    ),
    "prolonged_outage": OUTAGE_FIELDS,
    "ambiguous": (
        "ambiguous_event_id",
        "manual_review_id",
        "drop_response_observed",
        "auto_replay_count",
    ),
}
COMMON_OBSERVATION_FIELDS = ("provider_event_id", "observed_at")
SPECIAL_ID_FIELDS = frozenset(
    {
        "unique_inbound_id",
        "event_during_outage_id",
        "reply_for_event_id",
        "outbound_request_id",
        "acknowledged_request_id",
    }
)
RATE_LIMIT_CODES = frozenset({429, 45009, 45011})

SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{16,256}$")
HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")
IMAGE_RE = re.compile(r"^sha256:[0-9a-fA-F]{64}$")


class DriverError(RuntimeError):
    """Fail-closed driver protocol or evidence error."""


@dataclass(frozen=True)
class DriverRequest:
    channel: str
    run_id: str
    run_nonce: str
    expected_image_digest: str
    release_id: str
    release_nonce_sha256: str
    source_fingerprint: str
    control_profile_sha256: str


@dataclass(frozen=True)
class ControlProfile:
    tenant_id: str
    binding_id: str
    account_id_sha256: str
    actions: dict[str, str]


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value} is forbidden")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key {key!r}")
        value[key] = item
    return value


def _strict_json(raw: str) -> Any:
    return json.loads(
        raw,
        object_pairs_hook=_unique_object,
        parse_constant=_reject_constant,
    )


def _safe_identifier(value: object) -> str | None:
    if not isinstance(value, str) or SAFE_ID_RE.fullmatch(value) is None:
        return None
    return value


def _safe_name(value: object) -> str | None:
    if not isinstance(value, str) or SAFE_NAME_RE.fullmatch(value) is None:
        return None
    return value


def _finite_number(value: object, *, minimum: float, maximum: float) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isfinite(float(value)) and minimum <= float(value) <= maximum


def _provider_code(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        return None
    return parsed.astimezone(UTC)


def _secure_file(path: Path) -> bytes:
    if not path.is_absolute():
        raise DriverError("control profile path must be absolute")
    try:
        metadata = path.lstat()
    except OSError as error:
        raise DriverError("control profile is unavailable") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise DriverError("control profile must be a non-symlink regular file")
    if os.name == "posix" and metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise DriverError("control profile permissions are unsafe")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise DriverError("control profile cannot be opened") from error
    try:
        opened_metadata = os.fstat(descriptor)
        if not stat.S_ISREG(opened_metadata.st_mode):
            raise DriverError("control profile changed type")
        if os.name == "posix" and opened_metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise DriverError("control profile permissions are unsafe")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(MAX_PROFILE_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(raw) > MAX_PROFILE_BYTES:
        raise DriverError("control profile is too large")
    return raw


def _parse_request(value: object) -> DriverRequest:
    if not isinstance(value, dict) or set(value) != REQUEST_FIELDS:
        raise DriverError("request schema is invalid")
    if type(value.get("schema_version")) is not int or value.get("schema_version") != 1:
        raise DriverError("request schema version is invalid")
    if value.get("channel") != "wecom":
        raise DriverError("request channel is invalid")
    run_id = _safe_identifier(value.get("run_id"))
    run_nonce = value.get("run_nonce")
    image_digest = value.get("expected_image_digest")
    release_id = _safe_identifier(value.get("release_id"))
    release_nonce_sha256 = value.get("release_nonce_sha256")
    deployed_source_fingerprint = value.get("source_fingerprint")
    profile_hash = value.get("control_profile_sha256")
    if run_id is None:
        raise DriverError("request run ID is invalid")
    if not isinstance(run_nonce, str) or NONCE_RE.fullmatch(run_nonce) is None:
        raise DriverError("request nonce is invalid")
    if not isinstance(image_digest, str) or IMAGE_RE.fullmatch(image_digest) is None:
        raise DriverError("request image digest is invalid")
    if release_id is None:
        raise DriverError("request release id is invalid")
    if (
        not isinstance(release_nonce_sha256, str)
        or HEX64_RE.fullmatch(release_nonce_sha256) is None
        or not isinstance(deployed_source_fingerprint, str)
        or HEX64_RE.fullmatch(deployed_source_fingerprint) is None
    ):
        raise DriverError("request release binding is invalid")
    if not isinstance(profile_hash, str) or HEX64_RE.fullmatch(profile_hash) is None:
        raise DriverError("request control profile hash is invalid")
    if value.get("cases") != list(REQUIRED_CASES):
        raise DriverError("request cases are invalid")
    return DriverRequest(
        channel="wecom",
        run_id=run_id,
        run_nonce=run_nonce,
        expected_image_digest=image_digest.lower(),
        release_id=release_id,
        release_nonce_sha256=release_nonce_sha256.lower(),
        source_fingerprint=deployed_source_fingerprint.lower(),
        control_profile_sha256=profile_hash.lower(),
    )


def _load_profile(path: Path, expected_hash: str, account_id: str) -> ControlProfile:
    raw = _secure_file(path)
    actual_hash = hashlib.sha256(raw).hexdigest()
    if actual_hash != expected_hash.lower():
        raise DriverError("control profile hash does not match")
    try:
        value = _strict_json(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise DriverError("control profile is not strict JSON") from error
    if not isinstance(value, dict) or set(value) != PROFILE_FIELDS:
        raise DriverError("control profile schema is invalid")
    if type(value.get("schema_version")) is not int or value.get("schema_version") != 1:
        raise DriverError("control profile schema version is invalid")
    if value.get("channel") != "wecom":
        raise DriverError("control profile channel is invalid")
    tenant_id = _safe_identifier(value.get("tenant_id"))
    binding_id = _safe_identifier(value.get("binding_id"))
    account_hash = value.get("account_id_sha256")
    if tenant_id is None or binding_id is None:
        raise DriverError("control profile tenant or binding is invalid")
    if not isinstance(account_hash, str) or HEX64_RE.fullmatch(account_hash) is None:
        raise DriverError("control profile account hash is invalid")
    if hashlib.sha256(account_id.encode("utf-8")).hexdigest() != account_hash.lower():
        raise DriverError("control profile account does not match")
    actions_value = value.get("actions")
    if not isinstance(actions_value, dict) or set(actions_value) != set(REQUIRED_CASES):
        raise DriverError("control profile actions are invalid")
    actions: dict[str, str] = {}
    for case in REQUIRED_CASES:
        action = _safe_name(actions_value.get(case))
        if action is None:
            raise DriverError("control profile action name is invalid")
        actions[case] = action
    if len(set(actions.values())) != len(REQUIRED_CASES):
        raise DriverError("control profile action names must be unique")
    return ControlProfile(
        tenant_id=tenant_id,
        binding_id=binding_id,
        account_id_sha256=account_hash.lower(),
        actions=actions,
    )


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _validate_socket_path(path: Path) -> None:
    if not path.is_absolute():
        raise DriverError("control socket path must be absolute")
    try:
        metadata = path.lstat()
    except OSError as error:
        raise DriverError("control socket is unavailable") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISSOCK(metadata.st_mode):
        raise DriverError("control socket must be a non-symlink socket")


def _verify_connected_broker_peer(
    connection: socket.socket,
    *,
    expected_uid: int,
    expected_gid: int,
) -> None:
    if not sys.platform.startswith("linux"):
        return
    if not hasattr(socket, "SO_PEERCRED"):
        raise DriverError("Linux SO_PEERCRED is unavailable")
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


def _broker_call(
    socket_path: Path,
    request: dict[str, Any],
    expected_uid: int,
    expected_gid: int,
) -> dict[str, Any]:
    _validate_socket_path(socket_path)
    unix_family = getattr(socket, "AF_UNIX", None)
    if unix_family is None:
        raise DriverError("control broker transport is unavailable")
    raw_request = _canonical_bytes(request)
    if len(raw_request) > MAX_BROKER_BYTES + 1:
        raise DriverError("broker request is too large")
    received = bytearray()
    try:
        with socket.socket(unix_family, socket.SOCK_STREAM) as connection:
            connection.settimeout(BROKER_TIMEOUT_SECONDS)
            connection.connect(str(socket_path))
            _verify_connected_broker_peer(
                connection,
                expected_uid=expected_uid,
                expected_gid=expected_gid,
            )
            connection.sendall(raw_request)
            connection.shutdown(socket.SHUT_WR)
            while len(received) <= MAX_BROKER_BYTES + 1:
                chunk = connection.recv(min(4096, MAX_BROKER_BYTES + 2 - len(received)))
                if not chunk:
                    break
                received.extend(chunk)
                if b"\n" in chunk:
                    break
    except OSError as error:
        raise DriverError("control broker is unavailable") from error
    raw = bytes(received)
    if len(raw) > MAX_BROKER_BYTES + 1:
        raise DriverError("control broker response is too large")
    if not raw.endswith(b"\n") or b"\n" in raw[:-1] or b"\r" in raw[:-1]:
        raise DriverError("control broker response is not one line")
    try:
        value = _strict_json(raw[:-1].decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise DriverError("control broker response is not strict JSON") from error
    if not isinstance(value, dict):
        raise DriverError("control broker response is invalid")
    return value


def _opaque_id(profile: ControlProfile, case: str, label: str, value: str) -> str:
    digest = hashlib.sha256(
        "\0".join(
            (
                "trpc-wecom-evidence-v1",
                "observation-id",
                profile.tenant_id,
                profile.binding_id,
                case,
                label,
                value,
            )
        ).encode("utf-8")
    ).hexdigest()
    return "wecom-" + digest


def _required_fields(case: str) -> frozenset[str]:
    return frozenset(
        {
            "status",
            "run_nonce",
            *COMMON_OBSERVATION_FIELDS,
            *OBSERVATION_FIELDS[case],
        }
    )


def _handoff_mode(observation: dict[str, Any]) -> str:
    released = observation.get("old_lock_owner_released")
    acquired = observation.get("new_lock_owner_acquired")
    if type(released) is not bool or acquired is not True:
        raise DriverError("broker observation lock ownership is invalid")
    return "graceful" if released else "hard"


def _validate_observation(case: str, value: object, request: DriverRequest) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != _required_fields(case):
        raise DriverError("broker observation schema is invalid")
    if value.get("status") != "pass" or value.get("run_nonce") != request.run_nonce:
        raise DriverError("broker observation is not bound to this run")
    provider_event_id = _safe_identifier(value.get("provider_event_id"))
    observed_at = _parse_timestamp(value.get("observed_at"))
    if provider_event_id is None:
        raise DriverError("broker observation provider event is invalid")
    if observed_at is None or (observed_at - datetime.now(UTC)).total_seconds() > 5:
        raise DriverError("broker observation timestamp is invalid")

    for field in OBSERVATION_FIELDS[case]:
        if (
            field.endswith("_event_id")
            or field.endswith("_request_id")
            or field.endswith("_review_id")
            or field.endswith("_instance_id")
            or field in SPECIAL_ID_FIELDS
        ) and _safe_identifier(value.get(field)) is None:
            raise DriverError(f"broker observation {field} is invalid")

    if case == "round_trip":
        code = value.get("provider_code")
        if (
            isinstance(code, bool)
            or not isinstance(code, (str, int))
            or SAFE_CODE_RE.fullmatch(str(code)) is None
        ):
            raise DriverError("broker observation provider code is invalid")
    if case == "idempotency":
        count = value.get("duplicate_count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise DriverError("broker observation duplicate count is invalid")
        if value.get("duplicate_source") != "service_replay_of_provider_event":
            raise DriverError("broker observation duplicate source is invalid")
        if value.get("original_event_id") == value.get("replayed_event_id"):
            raise DriverError("broker observation processing IDs must differ")
    if case == "media":
        if any(
            not isinstance(value.get(field), str)
            or HEX64_RE.fullmatch(cast(str, value.get(field))) is None
            for field in ("media_id_hash", "sha256")
        ):
            raise DriverError("broker observation media hash is invalid")
        size = value.get("bytes")
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise DriverError("broker observation media size is invalid")
    if case == "reconnect":
        _handoff_mode(value)
        epoch = value.get("lock_epoch")
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 2:
            raise DriverError("broker observation reconnect epoch is invalid")
    if case == "rate_limit_retry_after":
        if _provider_code(value.get("provider_error_code")) not in RATE_LIMIT_CODES:
            raise DriverError("broker observation rate-limit code is invalid")
        retry_after = value.get("retry_after_seconds")
        elapsed = value.get("retry_elapsed_seconds")
        attempts = value.get("retry_attempts")
        if not _finite_number(retry_after, minimum=0.001, maximum=MAX_RETRY_SECONDS):
            raise DriverError("broker observation retry delay is invalid")
        if (
            isinstance(attempts, bool)
            or not isinstance(attempts, int)
            or not 2 <= attempts <= MAX_RETRY_ATTEMPTS
        ):
            raise DriverError("broker observation retry attempts are invalid")
        if not _finite_number(elapsed, minimum=0.001, maximum=MAX_RETRY_SECONDS):
            raise DriverError("broker observation retry elapsed time is invalid")
        if float(cast(float, elapsed)) < float(cast(float, retry_after)) * 0.9:
            raise DriverError("broker observation did not honor retry delay")
    if case == "credential_rotation" and value.get("old_credential_rejected") is not True:
        raise DriverError("broker observation credential rotation is invalid")
    if case in {"reconnect", "credential_rotation"}:
        code = value.get("provider_code")
        if (
            value.get("acknowledged_request_id") != value.get("outbound_request_id")
            or isinstance(code, bool)
            or not isinstance(code, (str, int))
            or str(code) not in {"0", "200"}
        ):
            raise DriverError("broker observation acknowledgement is invalid")
    if case == "prolonged_outage":
        if not _finite_number(
            value.get("outage_seconds"),
            minimum=MIN_OUTAGE_SECONDS,
            maximum=MAX_OUTAGE_SECONDS,
        ):
            raise DriverError("broker observation outage duration is invalid")
        if value.get("outage_mode") != "service_failover":
            raise DriverError("broker observation outage mode is invalid")
        if value.get("failed_instance_id") == value.get("takeover_instance_id"):
            raise DriverError("broker observation failover instances must differ")
        _handoff_mode(value)
        epoch = value.get("connection_epoch")
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 2:
            raise DriverError("broker observation outage epoch is invalid")
        if value.get("reply_for_event_id") != value.get("event_during_outage_id"):
            raise DriverError("broker observation outage reply does not match event")
        if value.get("acknowledged_request_id") != value.get("outbound_request_id"):
            raise DriverError("broker observation outage acknowledgement does not match request")
        for field, expected in (
            ("reply_count", 1),
            ("ack_count", 1),
            ("pending_count", 0),
            ("dlq_count", 0),
        ):
            count = value.get(field)
            if isinstance(count, bool) or not isinstance(count, int) or count != expected:
                raise DriverError(f"broker observation {field} is invalid")
    if case == "ambiguous" and (
        value.get("drop_response_observed") is not True
        or type(value.get("auto_replay_count")) is not int
        or value.get("auto_replay_count") != 0
    ):
        raise DriverError("broker observation ambiguous handling is invalid")
    return value


def _validate_snapshot(
    case: str,
    value: object,
    observation: dict[str, Any],
    profile: ControlProfile,
) -> str:
    if not isinstance(value, dict) or set(value) != SNAPSHOT_FIELDS:
        raise DriverError("wecom snapshot schema is invalid")
    if (
        type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("channel") != "wecom"
        or value.get("case") != case
        or value.get("tenant_id") != profile.tenant_id
        or value.get("binding_id") != profile.binding_id
        or value.get("account_id_sha256") != profile.account_id_sha256
    ):
        raise DriverError("wecom snapshot identity is invalid")
    provider_hash = value.get("provider_event_hash")
    if not isinstance(provider_hash, str) or HASH_RE.fullmatch(provider_hash) is None:
        raise DriverError("wecom snapshot provider event hash does not match")
    lifecycle = value.get("lifecycle")
    if not isinstance(lifecycle, list) or not lifecycle or len(lifecycle) > 16:
        raise DriverError("wecom snapshot lifecycle is invalid")
    parsed_lifecycle: list[tuple[str, int]] = []
    for entry in lifecycle:
        if not isinstance(entry, dict) or set(entry) != LIFECYCLE_FIELDS:
            raise DriverError("wecom snapshot lifecycle is invalid")
        state = entry.get("state")
        epoch = entry.get("epoch")
        if (
            state not in LIFECYCLE_STATES
            or isinstance(epoch, bool)
            or not isinstance(epoch, int)
            or epoch < 1
        ):
            raise DriverError("wecom snapshot lifecycle is invalid")
        parsed_lifecycle.append((cast(str, state), epoch))
    if case in {"reconnect", "prolonged_outage"}:
        states = tuple(state for state, _epoch in parsed_lifecycle)
        mode = _handoff_mode(observation)
        expected_lifecycle = (
            GRACEFUL_HANDOFF_LIFECYCLE if mode == "graceful" else HARD_FAILOVER_LIFECYCLE
        )
        if states != expected_lifecycle:
            raise DriverError("wecom snapshot lifecycle is incomplete")
        if mode == "graceful":
            old_epoch = parsed_lifecycle[0][1]
            if parsed_lifecycle[1][1] != old_epoch:
                raise DriverError("wecom snapshot lifecycle release epoch is invalid")
            new_epoch = parsed_lifecycle[2][1]
            if new_epoch <= old_epoch or parsed_lifecycle[3][1] != new_epoch:
                raise DriverError("wecom snapshot lifecycle epoch did not increment")
        else:
            new_epoch = parsed_lifecycle[0][1]
            if new_epoch < 2 or parsed_lifecycle[1][1] != new_epoch:
                raise DriverError("wecom snapshot lifecycle epoch did not increment")
        observation_epoch = observation["lock_epoch" if case == "reconnect" else "connection_epoch"]
        if observation_epoch != new_epoch:
            raise DriverError("wecom snapshot lifecycle epoch does not match observation")
    return provider_hash


def _correlation_label(case: str, field: str) -> str:
    if case == "idempotency" and field in {"original_event_id", "replayed_event_id"}:
        return "processing-action"
    if case == "prolonged_outage":
        if field in {"event_during_outage_id", "reply_for_event_id"}:
            return "outage-inbound"
        if field in {"outbound_request_id", "acknowledged_request_id"}:
            return "outage-outbound"
        if field in {"failed_instance_id", "takeover_instance_id"}:
            return "connector-instance"
    if case in {"reconnect", "credential_rotation"} and field in {
        "outbound_request_id",
        "acknowledged_request_id",
    }:
        return f"{case}-outbound"
    return field


def _sanitize_observation(
    case: str,
    observation: dict[str, Any],
    provider_hash: str,
    profile: ControlProfile,
) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    id_fields = {
        field
        for field in OBSERVATION_FIELDS[case]
        if field.endswith("_event_id")
        or field.endswith("_request_id")
        or field.endswith("_review_id")
        or field.endswith("_instance_id")
        or field in SPECIAL_ID_FIELDS
    }
    for field in _required_fields(case):
        value = observation[field]
        if field == "provider_event_id":
            sanitized[field] = "wecom-provider-" + provider_hash
        elif field in id_fields:
            sanitized[field] = _opaque_id(
                profile,
                case,
                _correlation_label(case, field),
                cast(str, value),
            )
        else:
            sanitized[field] = value
    return sanitized


def _validate_broker_response(
    case: str,
    response: object,
    request: DriverRequest,
    profile: ControlProfile,
) -> dict[str, Any]:
    if not isinstance(response, dict) or set(response) != BROKER_RESPONSE_FIELDS:
        raise DriverError("control broker response schema is invalid")
    if response.get("status") != "pass":
        raise DriverError("control broker did not pass")
    result = response.get("result")
    if not isinstance(result, dict) or set(result) != BROKER_RESULT_FIELDS:
        raise DriverError("control broker result schema is invalid")
    observation = _validate_observation(case, result.get("observation"), request)
    provider_hash = _validate_snapshot(
        case,
        result.get("wecom_snapshot"),
        observation,
        profile,
    )
    return _sanitize_observation(case, observation, provider_hash, profile)


def _broker_request(
    case: str,
    request: DriverRequest,
    profile: ControlProfile,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "channel": "wecom",
        "action": profile.actions[case],
        "run_id": request.run_id,
        "run_nonce": request.run_nonce,
        "control_profile_sha256": request.control_profile_sha256,
        "payload": {
            "case": case,
            "expected_image_digest": request.expected_image_digest,
            "tenant_id": profile.tenant_id,
            "binding_id": profile.binding_id,
            "account_id_sha256": profile.account_id_sha256,
        },
    }


def _run(
    request: DriverRequest,
    profile: ControlProfile,
    socket_path: Path,
    broker_uid: int,
    broker_gid: int,
) -> dict[str, Any]:
    observations: dict[str, dict[str, Any]] = {}
    provider_ids: set[str] = set()
    for case in REQUIRED_CASES:
        response = _broker_call(
            socket_path,
            _broker_request(case, request, profile),
            broker_uid,
            broker_gid,
        )
        observation = _validate_broker_response(case, response, request, profile)
        provider_id = cast(str, observation["provider_event_id"])
        if provider_id in provider_ids:
            raise DriverError("provider event identifiers must be unique")
        provider_ids.add(provider_id)
        observations[case] = observation
    return {"schema_version": 1, "observations": observations}


def _input_stream() -> BinaryIO:
    return cast(BinaryIO, getattr(sys.stdin, "buffer", sys.stdin))


def _read_request() -> DriverRequest:
    raw = _input_stream().read(MAX_INPUT_BYTES + 1)
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    if not isinstance(raw, bytes) or len(raw) > MAX_INPUT_BYTES:
        raise DriverError("request is too large")
    try:
        value = _strict_json(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise DriverError("request is not strict JSON") from error
    return _parse_request(value)


def _configuration(request: DriverRequest) -> tuple[ControlProfile, Path, int, int]:
    profile_value = os.environ.get(PROFILE_ENV, "").strip()
    socket_value = os.environ.get(SOCKET_ENV, "").strip()
    account_id = os.environ.get(ACCOUNT_ENV, "")
    uid_value = os.environ.get(BROKER_UID_ENV, "").strip()
    gid_value = os.environ.get(BROKER_GID_ENV, "").strip()
    if (
        not profile_value
        or not socket_value
        or not account_id
        or "\x00" in account_id
        or not uid_value
        or not gid_value
    ):
        raise DriverError("driver configuration is incomplete")
    if len(account_id.encode("utf-8")) > 4096:
        raise DriverError("driver account is invalid")
    profile = _load_profile(Path(profile_value), request.control_profile_sha256, account_id)
    socket_path = Path(socket_value)
    _validate_socket_path(socket_path)
    try:
        broker_uid = int(uid_value)
        broker_gid = int(gid_value)
    except ValueError as error:
        raise DriverError("broker identity is invalid") from error
    if broker_uid < 0 or broker_gid < 0:
        raise DriverError("broker identity is invalid")
    if sys.platform.startswith("linux"):
        current_euid = getattr(os, "geteuid", None)
        if current_euid is None or broker_uid == current_euid():
            raise DriverError("control broker must use a dedicated uid")
    return profile, socket_path, broker_uid, broker_gid


def main() -> int:
    try:
        request = _read_request()
        profile, socket_path, broker_uid, broker_gid = _configuration(request)
        result = _run(request, profile, socket_path, broker_uid, broker_gid)
        raw = _canonical_bytes(result)
        if len(raw) > MAX_OUTPUT_BYTES:
            raise DriverError("driver output is too large")
    except DriverError:
        return 1
    sys.stdout.buffer.write(raw)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
