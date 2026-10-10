#!/usr/bin/python3
"""Fail-closed WeCom control action backed by control and Admin evidence APIs."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO, NoReturn, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    Request,
    build_opener,
)

MAX_INPUT_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 64 * 1024
MAX_CONFIG_BYTES = 64 * 1024
MAX_TOKEN_BYTES = 4096
ACTION_ENV = "TRPC_IM_CONTROL_ACTION"

CASES = (
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
        "action",
        "run_id",
        "run_nonce",
        "control_profile_sha256",
        "payload",
    }
)
PAYLOAD_FIELDS = frozenset(
    {"case", "expected_image_digest", "tenant_id", "binding_id", "account_id_sha256"}
)
CONFIG_FIELDS = frozenset(
    {
        "schema_version",
        "channel",
        "tenant_id",
        "binding_id",
        "account_id_sha256",
        "admin_base_url",
        "admin_token_file",
        "control_base_url",
        "control_token_file",
        "timeout_seconds",
    }
)
HOOK_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "channel",
        "case",
        "run_binding_sha256",
        "expected_image_digest",
        "tenant_id",
        "binding_id",
        "account_id_sha256",
        "ack_provider_event_hash",
        "observation",
        "provider_witness",
    }
)
WITNESS_FIELDS = frozenset({"status", "source", "provider_event_hash", "observed_at"})
STATE_FIELDS = frozenset(
    {
        "owner_hash",
        "epoch",
        "phase",
        "acquired_at",
        "authenticated_at",
        "disconnected_at",
        "released_at",
        "last_provider_event_hash",
        "last_provider_event_at",
        "updated_at",
    }
)
EVENT_FIELDS = frozenset(
    {
        "event_id",
        "connection_epoch",
        "event_type",
        "owner_hash",
        "provider_event_hash",
        "occurred_at",
    }
)
EVENT_EVIDENCE_FIELDS = frozenset(
    {
        "schema_version",
        "tenant_id",
        "binding_id",
        "channel",
        "requested_run_id_sha256",
        "run_binding_sha256",
        "provider_event_hash",
        "correlation",
        "outbounds",
        "artifact",
    }
)
RUN_REGISTRATION_FIELDS = frozenset(
    {
        "schema_version",
        "tenant_id",
        "binding_id",
        "channel",
        "run_id_sha256",
        "run_binding_sha256",
        "created_at",
        "expires_at",
    }
)
CORRELATION_FIELDS = frozenset(
    {"availability", "inbound_id_sha256", "status", "delivery_count", "accepted_at"}
)
OUTBOUNDS_FIELDS = frozenset({"count", "truncated", "items"})
OUTBOUND_ITEM_FIELDS = frozenset(
    {
        "outbound_id_sha256",
        "delivery_status",
        "provider_message_id_sha256",
        "attempt_count",
        "attempts",
        "pending_count",
        "dlq_count",
        "created_at",
        "updated_at",
    }
)
ATTEMPT_FIELDS = frozenset(
    {
        "attempt_number",
        "status",
        "provider_code",
        "retry_after_seconds",
        "started_at",
        "completed_at",
    }
)
ARTIFACT_FIELDS = frozenset({"availability", "count", "items"})
ARTIFACT_ITEM_FIELDS = frozenset({"sha256", "bytes", "status", "created_at"})

COMMON_OBSERVATION_FIELDS = frozenset({"provider_event_id", "observed_at"})
CASE_OBSERVATION_FIELDS = {
    "round_trip": frozenset({"callback_event_id", "outbound_request_id", "provider_code"}),
    "idempotency": frozenset(
        {
            "duplicate_event_id",
            "unique_inbound_id",
            "duplicate_count",
            "duplicate_source",
            "original_event_id",
            "replayed_event_id",
        }
    ),
    "media": frozenset({"media_id_hash", "sha256", "bytes"}),
    "reconnect": frozenset(
        {
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
        }
    ),
    "rate_limit_retry_after": frozenset(
        {
            "provider_error_code",
            "retry_after_seconds",
            "retry_request_id",
            "retry_attempts",
            "retry_elapsed_seconds",
        }
    ),
    "credential_rotation": frozenset(
        {
            "old_credential_event_id",
            "new_credential_event_id",
            "post_rotation_event_id",
            "old_credential_rejected",
            "outbound_request_id",
            "acknowledged_request_id",
            "provider_code",
        }
    ),
    "prolonged_outage": frozenset(
        {
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
        }
    ),
    "ambiguous": frozenset(
        {"ambiguous_event_id", "manual_review_id", "drop_response_observed", "auto_replay_count"}
    ),
}
RATE_LIMIT_CODES = frozenset({"429", "45009", "45011"})
SAFE_PROVIDER_CODES = frozenset(
    {"0", "200", "429", "45009", "45011", "99991400", "99991401", "99991402", "99991672"}
)
LIFECYCLE_STATES = frozenset(
    {"connected", "disconnected", "released", "acquired", "takeover", "authenticated"}
)

SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{16,256}$")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
OPAQUE_ID_RE = re.compile(r"^wcctl_[0-9a-f]{64}$")


class ActionNotRun(RuntimeError):
    """Expected, content-free inability to prove one control action."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ActionNotRun("invalid_arguments")


@dataclass(frozen=True)
class ActionRequest:
    action: str
    case: str
    run_id: str
    run_nonce: str
    profile_sha256: str
    expected_image_digest: str
    tenant_id: str
    binding_id: str
    account_id_sha256: str


@dataclass(frozen=True)
class ActionConfig:
    tenant_id: str
    binding_id: str
    account_id_sha256: str
    admin_base_url: str
    admin_token_file: Path
    control_base_url: str
    control_token_file: Path
    timeout_seconds: float


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite constant {value!r}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _strict_json(raw: bytes) -> Any:
    return json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_unique_object,
        parse_constant=_reject_constant,
    )


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _validate_root_owned_parent_chain(path: Path) -> None:
    if os.name != "posix":
        return
    current = path.parent
    while True:
        try:
            metadata = current.lstat()
        except OSError as error:
            raise ActionNotRun("configuration_unavailable") from error
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            or (metadata.st_uid != 0 and metadata.st_mode & stat.S_IWUSR)
        ):
            raise ActionNotRun("configuration_unavailable")
        if current.parent == current:
            return
        current = current.parent


def _secure_read(path: Path, *, limit: int, executable: bool = False) -> bytes:
    if not path.is_absolute():
        raise ActionNotRun("configuration_unavailable")
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ActionNotRun("configuration_unavailable") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ActionNotRun("configuration_unavailable")
    if os.name == "posix":
        _validate_root_owned_parent_chain(path)
        if metadata.st_uid != 0 or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise ActionNotRun("configuration_unavailable")
    if executable and os.name == "posix" and metadata.st_mode & 0o111 == 0:
        raise ActionNotRun("configuration_unavailable")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ActionNotRun("configuration_unavailable") from error
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise ActionNotRun("configuration_unavailable")
        if os.name == "posix" and (
            opened.st_uid != 0 or opened.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        ):
            raise ActionNotRun("configuration_unavailable")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(limit + 1)
    finally:
        os.close(descriptor)
    if len(raw) > limit:
        raise ActionNotRun("configuration_unavailable")
    return raw


def _safe_name(value: object) -> str | None:
    return value if isinstance(value, str) and SAFE_NAME_RE.fullmatch(value) else None


def _safe_url(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return None
    return value.rstrip("/")


def _finite(value: object, *, minimum: float, maximum: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and minimum <= float(value) <= maximum
    )


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _parse_request(value: object) -> ActionRequest:
    if not isinstance(value, dict) or set(value) != REQUEST_FIELDS:
        raise ActionNotRun("invalid_request")
    payload = value.get("payload")
    if (
        type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("channel") != "wecom"
        or not isinstance(payload, dict)
        or set(payload) != PAYLOAD_FIELDS
    ):
        raise ActionNotRun("invalid_request")
    case = payload.get("case")
    action = value.get("action")
    run_id = _safe_name(value.get("run_id"))
    run_nonce = value.get("run_nonce")
    profile_hash = value.get("control_profile_sha256")
    image = payload.get("expected_image_digest")
    tenant = _safe_name(payload.get("tenant_id"))
    binding = _safe_name(payload.get("binding_id"))
    account_hash = payload.get("account_id_sha256")
    if (
        case not in CASES
        or action != f"wecom_{case}"
        or run_id is None
        or not isinstance(run_nonce, str)
        or NONCE_RE.fullmatch(run_nonce) is None
        or not isinstance(profile_hash, str)
        or HEX64_RE.fullmatch(profile_hash) is None
        or not isinstance(image, str)
        or IMAGE_RE.fullmatch(image) is None
        or tenant is None
        or binding is None
        or not isinstance(account_hash, str)
        or HEX64_RE.fullmatch(account_hash) is None
    ):
        raise ActionNotRun("invalid_request")
    return ActionRequest(
        action=cast(str, action),
        case=cast(str, case),
        run_id=run_id,
        run_nonce=run_nonce,
        profile_sha256=profile_hash,
        expected_image_digest=image,
        tenant_id=tenant,
        binding_id=binding,
        account_id_sha256=account_hash,
    )


def _load_config(path: Path) -> ActionConfig:
    try:
        value = _strict_json(_secure_read(path, limit=MAX_CONFIG_BYTES))
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise ActionNotRun("configuration_unavailable") from error
    if not isinstance(value, dict) or set(value) != CONFIG_FIELDS:
        raise ActionNotRun("configuration_unavailable")
    tenant = _safe_name(value.get("tenant_id"))
    binding = _safe_name(value.get("binding_id"))
    account_hash = value.get("account_id_sha256")
    admin_url = _safe_url(value.get("admin_base_url"))
    control_url = _safe_url(value.get("control_base_url"))
    timeout = value.get("timeout_seconds")
    if (
        value.get("schema_version") != 1
        or value.get("channel") != "wecom"
        or tenant is None
        or binding is None
        or not isinstance(account_hash, str)
        or HEX64_RE.fullmatch(account_hash) is None
        or admin_url is None
        or control_url is None
        or not _finite(timeout, minimum=0.1, maximum=175.0)
    ):
        raise ActionNotRun("configuration_unavailable")
    admin_token = value.get("admin_token_file")
    control_token = value.get("control_token_file")
    if not isinstance(admin_token, str) or not isinstance(control_token, str):
        raise ActionNotRun("configuration_unavailable")
    config = ActionConfig(
        tenant_id=tenant,
        binding_id=binding,
        account_id_sha256=account_hash,
        admin_base_url=admin_url,
        admin_token_file=Path(admin_token),
        control_base_url=control_url,
        control_token_file=Path(control_token),
        timeout_seconds=float(cast(float, timeout)),
    )
    _read_token(config.admin_token_file)
    _read_token(config.control_token_file)
    return config


def _read_token(path: Path) -> str:
    raw = _secure_read(path, limit=MAX_TOKEN_BYTES)
    try:
        token = raw.decode("utf-8").strip()
    except UnicodeError as error:
        raise ActionNotRun("configuration_unavailable") from error
    if not token or any(character in token for character in "\x00\r\n"):
        raise ActionNotRun("configuration_unavailable")
    return token


def _request_json(
    method: str,
    url: str,
    *,
    token_file: Path,
    timeout_seconds: float,
    body: object | None = None,
) -> object:
    token = _read_token(token_file)
    data = _canonical_bytes(body) if body is not None else None
    # Both configured bases and all derived URLs are restricted to HTTPS.
    request = Request(  # noqa: S310
        url,
        data=data,
        method=method,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            **({"Content-Type": "application/json"} if data is not None else {}),
        },
    )
    try:
        with build_opener(_NoRedirect()).open(request, timeout=timeout_seconds) as response:
            if response.status < 200 or response.status >= 300:
                raise ActionNotRun("external_evidence_unavailable")
            raw = response.read(MAX_RESPONSE_BYTES + 1)
    except ActionNotRun:
        raise
    except (HTTPError, URLError, OSError, TimeoutError, ValueError) as error:
        raise ActionNotRun("external_evidence_unavailable") from error
    finally:
        token = ""
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ActionNotRun("external_evidence_unavailable")
    try:
        return _strict_json(raw)
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise ActionNotRun("external_evidence_invalid") from error


def _run_binding_hash(request: ActionRequest) -> str:
    material = "\x00".join(
        (
            "trpc-wecom-control-action-v1",
            request.tenant_id,
            request.binding_id,
            request.case,
            request.run_id,
            request.run_nonce,
            request.expected_image_digest,
        )
    )
    return hashlib.sha256(material.encode()).hexdigest()


def _acceptance_hash(domain: str, request: ActionRequest, *values: str) -> str:
    material = "\x00".join(
        (
            "trpc-im-acceptance-evidence-v1",
            domain,
            request.tenant_id,
            request.binding_id,
            *values,
        )
    )
    return hashlib.sha256(material.encode()).hexdigest()


def _acceptance_run_id(request: ActionRequest) -> str:
    material = "\x00".join(
        (
            "trpc-im-acceptance-run-v1",
            "wecom_ai_bot",
            request.tenant_id,
            request.binding_id,
            request.run_id,
            request.case,
        )
    )
    return "im-" + hashlib.sha256(material.encode()).hexdigest()


def _acceptance_run_binding_hash(request: ActionRequest) -> str:
    run_hash = _acceptance_hash("run", request, _acceptance_run_id(request))
    nonce_hash = _acceptance_hash("run-nonce", request, request.run_nonce)
    return _acceptance_hash("run-binding", request, "wecom_ai_bot", run_hash, nonce_hash)


def _control_request(request: ActionRequest) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "channel": "wecom",
        "case": request.case,
        "run_id": request.run_id,
        "run_nonce": request.run_nonce,
        "run_binding_sha256": _run_binding_hash(request),
        "expected_image_digest": request.expected_image_digest,
        "tenant_id": request.tenant_id,
        "binding_id": request.binding_id,
        "account_id_sha256": request.account_id_sha256,
    }


def _validate_observation(case: str, value: object) -> dict[str, Any]:
    expected = COMMON_OBSERVATION_FIELDS | CASE_OBSERVATION_FIELDS[case]
    if not isinstance(value, dict) or set(value) != expected:
        raise ActionNotRun("control_evidence_invalid")
    observed = _timestamp(value.get("observed_at"))
    if observed is None or abs((datetime.now(UTC) - observed).total_seconds()) > 300:
        raise ActionNotRun("control_evidence_stale")
    for field, item in value.items():
        if (
            field == "provider_event_id"
            or field.endswith("_event_id")
            or field.endswith("_request_id")
            or field.endswith("_review_id")
            or field.endswith("_instance_id")
            or field in {"unique_inbound_id", "event_during_outage_id", "reply_for_event_id"}
        ) and (not isinstance(item, str) or OPAQUE_ID_RE.fullmatch(item) is None):
            raise ActionNotRun("control_evidence_contains_raw_identifier")
    if case == "round_trip" and str(value.get("provider_code")) not in {"0", "200"}:
        raise ActionNotRun("control_evidence_invalid")
    if case == "idempotency" and (
        type(value.get("duplicate_count")) is not int
        or value.get("duplicate_count", 0) < 1
        or value.get("duplicate_source") != "service_replay_of_provider_event"
        or value.get("original_event_id") == value.get("replayed_event_id")
    ):
        raise ActionNotRun("control_evidence_invalid")
    if case == "media" and (
        not isinstance(value.get("media_id_hash"), str)
        or HEX64_RE.fullmatch(cast(str, value.get("media_id_hash"))) is None
        or not isinstance(value.get("sha256"), str)
        or HEX64_RE.fullmatch(cast(str, value.get("sha256"))) is None
        or type(value.get("bytes")) is not int
        or value.get("bytes", 0) <= 0
    ):
        raise ActionNotRun("control_evidence_invalid")
    if case == "rate_limit_retry_after" and (
        str(value.get("provider_error_code")) not in RATE_LIMIT_CODES
        or not _finite(value.get("retry_after_seconds"), minimum=0.001, maximum=3600)
        or type(value.get("retry_attempts")) is not int
        or not 2 <= value.get("retry_attempts", 0) <= 100
        or not _finite(value.get("retry_elapsed_seconds"), minimum=0.001, maximum=3600)
        or float(cast(float, value.get("retry_elapsed_seconds")))
        < 0.9 * float(cast(float, value.get("retry_after_seconds")))
    ):
        raise ActionNotRun("control_evidence_invalid")
    if case == "credential_rotation" and value.get("old_credential_rejected") is not True:
        raise ActionNotRun("control_evidence_invalid")
    if case in {"reconnect", "credential_rotation"} and (
        value.get("acknowledged_request_id") != value.get("outbound_request_id")
        or str(value.get("provider_code")) not in {"0", "200"}
    ):
        raise ActionNotRun("control_evidence_invalid")
    if case in {"reconnect", "prolonged_outage"} and (
        type(value.get("old_lock_owner_released")) is not bool
        or value.get("new_lock_owner_acquired") is not True
    ):
        raise ActionNotRun("control_evidence_invalid")
    if case == "reconnect" and (
        type(value.get("lock_epoch")) is not int or value.get("lock_epoch", 0) < 2
    ):
        raise ActionNotRun("control_evidence_invalid")
    if case == "prolonged_outage" and (
        not _finite(value.get("outage_seconds"), minimum=60, maximum=7 * 24 * 3600)
        or value.get("outage_mode") != "service_failover"
        or value.get("failed_instance_id") == value.get("takeover_instance_id")
        or type(value.get("connection_epoch")) is not int
        or value.get("connection_epoch", 0) < 2
        or value.get("reply_for_event_id") != value.get("event_during_outage_id")
        or value.get("acknowledged_request_id") != value.get("outbound_request_id")
        or any(
            value.get(field) != expected_count
            for field, expected_count in {
                "reply_count": 1,
                "ack_count": 1,
                "pending_count": 0,
                "dlq_count": 0,
            }.items()
        )
    ):
        raise ActionNotRun("control_evidence_invalid")
    if case == "ambiguous" and (
        value.get("drop_response_observed") is not True
        or type(value.get("auto_replay_count")) is not int
        or value.get("auto_replay_count") != 0
    ):
        raise ActionNotRun("control_evidence_invalid")
    return value


def _validate_hook(value: object, request: ActionRequest) -> tuple[dict[str, Any], str]:
    if not isinstance(value, dict) or set(value) != HOOK_FIELDS:
        raise ActionNotRun("control_evidence_invalid")
    if (
        value.get("schema_version") != 1
        or value.get("status") != "pass"
        or value.get("channel") != "wecom"
        or value.get("case") != request.case
        or value.get("run_binding_sha256") != _run_binding_hash(request)
        or value.get("expected_image_digest") != request.expected_image_digest
        or value.get("tenant_id") != request.tenant_id
        or value.get("binding_id") != request.binding_id
        or value.get("account_id_sha256") != request.account_id_sha256
    ):
        raise ActionNotRun("control_evidence_mismatch")
    ack_hash = value.get("ack_provider_event_hash")
    if not isinstance(ack_hash, str) or HEX64_RE.fullmatch(ack_hash) is None:
        raise ActionNotRun("control_evidence_invalid")
    observation = _validate_observation(request.case, value.get("observation"))
    witness = value.get("provider_witness")
    if (
        not isinstance(witness, dict)
        or set(witness) != WITNESS_FIELDS
        or witness.get("status") != "pass"
        or witness.get("source") != "wecom_provider_control"
        or witness.get("provider_event_hash") != ack_hash
        or _timestamp(witness.get("observed_at")) is None
    ):
        raise ActionNotRun("provider_witness_unavailable")
    return observation, ack_hash


def _admin_snapshot_url(config: ActionConfig, request: ActionRequest) -> str:
    return (
        f"{config.admin_base_url}/v1/tenants/{quote(request.tenant_id, safe='')}"
        f"/bindings/{quote(request.binding_id, safe='')}/im-acceptance/wecom?limit=200"
    )


def _admin_event_evidence_url(config: ActionConfig, request: ActionRequest) -> str:
    return (
        f"{config.admin_base_url}/v1/tenants/{quote(request.tenant_id, safe='')}"
        f"/bindings/{quote(request.binding_id, safe='')}"
        "/im-acceptance/event-evidence"
    )


def _admin_run_registration_url(config: ActionConfig, request: ActionRequest) -> str:
    return (
        f"{config.admin_base_url}/v1/tenants/{quote(request.tenant_id, safe='')}"
        f"/bindings/{quote(request.binding_id, safe='')}"
        "/im-acceptance/runs"
    )


def _validate_run_registration(value: object, request: ActionRequest) -> datetime:
    if not isinstance(value, dict) or set(value) != RUN_REGISTRATION_FIELDS:
        raise ActionNotRun("admin_run_registration_invalid")
    created_at = _timestamp(value.get("created_at"))
    expires_at = _timestamp(value.get("expires_at"))
    if (
        value.get("schema_version") != 1
        or value.get("tenant_id") != request.tenant_id
        or value.get("binding_id") != request.binding_id
        or value.get("channel") != "wecom_ai_bot"
        or value.get("run_id_sha256")
        != _acceptance_hash("run", request, _acceptance_run_id(request))
        or value.get("run_binding_sha256") != _acceptance_run_binding_hash(request)
        or created_at is None
        or expires_at is None
        or expires_at <= created_at
        or expires_at <= datetime.now(UTC)
    ):
        raise ActionNotRun("admin_run_registration_mismatch")
    return created_at


def _lifecycle(
    snapshot: object,
    request: ActionRequest,
    ack_hash: str,
    observation: dict[str, Any],
    run_started_at: datetime,
) -> list[dict[str, Any]]:
    if not isinstance(snapshot, dict) or set(snapshot) != {"state", "events"}:
        raise ActionNotRun("admin_evidence_invalid")
    state = snapshot.get("state")
    events = snapshot.get("events")
    if state is not None and (not isinstance(state, dict) or set(state) != STATE_FIELDS):
        raise ActionNotRun("admin_evidence_invalid")
    if not isinstance(events, list) or len(events) > 200:
        raise ActionNotRun("admin_evidence_invalid")
    parsed_events: list[dict[str, Any]] = []
    provider_seen = False
    for event in events:
        if not isinstance(event, dict) or set(event) != EVENT_FIELDS:
            raise ActionNotRun("admin_evidence_invalid")
        epoch = event.get("connection_epoch")
        event_type = event.get("event_type")
        if (
            type(epoch) is not int
            or epoch < 1
            or event_type not in LIFECYCLE_STATES | {"provider_event"}
        ):
            raise ActionNotRun("admin_evidence_invalid")
        occurred_at = _timestamp(event.get("occurred_at"))
        if occurred_at is None:
            raise ActionNotRun("admin_evidence_invalid")
        if occurred_at < run_started_at - timedelta(seconds=5):
            continue
        if event.get("provider_event_hash") == ack_hash:
            provider_seen = True
        parsed_events.append({"state": event_type, "epoch": epoch, "occurred_at": occurred_at})
    if not provider_seen:
        raise ActionNotRun("admin_provider_event_unavailable")

    if request.case not in {"reconnect", "prolonged_outage"}:
        if not isinstance(state, dict):
            raise ActionNotRun("admin_lifecycle_unavailable")
        phase = state.get("phase")
        epoch = state.get("epoch")
        if phase not in LIFECYCLE_STATES or type(epoch) is not int or epoch < 1:
            raise ActionNotRun("admin_lifecycle_unavailable")
        return [{"state": phase, "epoch": epoch}]

    epoch_field = "lock_epoch" if request.case == "reconnect" else "connection_epoch"
    new_epoch = cast(int, observation[epoch_field])
    graceful = observation["old_lock_owner_released"] is True
    expected = (
        (
            ("disconnected", new_epoch - 1),
            ("released", new_epoch - 1),
            ("acquired", new_epoch),
            ("authenticated", new_epoch),
        )
        if graceful
        else (("takeover", new_epoch), ("authenticated", new_epoch))
    )
    ordered = sorted(parsed_events, key=lambda item: cast(datetime, item["occurred_at"]))
    position = 0
    for event in ordered:
        if position < len(expected) and (event["state"], event["epoch"]) == expected[position]:
            position += 1
    if position != len(expected):
        raise ActionNotRun("admin_lifecycle_unavailable")
    return [{"state": state_name, "epoch": epoch} for state_name, epoch in expected]


def _validated_attempts(item: object) -> list[dict[str, Any]]:
    if not isinstance(item, dict) or set(item) != OUTBOUND_ITEM_FIELDS:
        raise ActionNotRun("admin_event_evidence_invalid")
    outbound_hash = item.get("outbound_id_sha256")
    provider_hash = item.get("provider_message_id_sha256")
    attempts = item.get("attempts")
    if (
        not isinstance(outbound_hash, str)
        or HEX64_RE.fullmatch(outbound_hash) is None
        or (
            provider_hash is not None
            and (not isinstance(provider_hash, str) or HEX64_RE.fullmatch(provider_hash) is None)
        )
        or not isinstance(attempts, list)
        or type(item.get("attempt_count")) is not int
        or item.get("attempt_count") != len(attempts)
        or type(item.get("pending_count")) is not int
        or item.get("pending_count", -1) < 0
        or type(item.get("dlq_count")) is not int
        or item.get("dlq_count", -1) < 0
        or _timestamp(item.get("created_at")) is None
        or _timestamp(item.get("updated_at")) is None
    ):
        raise ActionNotRun("admin_event_evidence_invalid")
    validated: list[dict[str, Any]] = []
    for attempt in attempts:
        if not isinstance(attempt, dict) or set(attempt) != ATTEMPT_FIELDS:
            raise ActionNotRun("admin_event_evidence_invalid")
        code = attempt.get("provider_code")
        retry_after = attempt.get("retry_after_seconds")
        if (
            type(attempt.get("attempt_number")) is not int
            or attempt.get("attempt_number", 0) < 1
            or _safe_name(attempt.get("status")) is None
            or (code is not None and str(code) not in SAFE_PROVIDER_CODES)
            or (retry_after is not None and not _finite(retry_after, minimum=0.0, maximum=3600.0))
            or _timestamp(attempt.get("started_at")) is None
            or _timestamp(attempt.get("completed_at")) is None
        ):
            raise ActionNotRun("admin_event_evidence_invalid")
        validated.append(attempt)
    if [attempt["attempt_number"] for attempt in validated] != sorted(
        attempt["attempt_number"] for attempt in validated
    ):
        raise ActionNotRun("admin_event_evidence_invalid")
    return validated


def _validate_event_evidence(
    value: object,
    request: ActionRequest,
    provider_event_hash: str,
    observation: dict[str, Any],
) -> None:
    if not isinstance(value, dict) or set(value) != EVENT_EVIDENCE_FIELDS:
        raise ActionNotRun("admin_event_evidence_invalid")
    expected_run_hash = _acceptance_hash("run", request, _acceptance_run_id(request))
    if (
        value.get("schema_version") != 1
        or value.get("tenant_id") != request.tenant_id
        or value.get("binding_id") != request.binding_id
        or value.get("channel") != "wecom_ai_bot"
        or value.get("requested_run_id_sha256") != expected_run_hash
        or value.get("run_binding_sha256") != _acceptance_run_binding_hash(request)
        or value.get("provider_event_hash") != provider_event_hash
    ):
        raise ActionNotRun("admin_event_evidence_mismatch")

    correlation = value.get("correlation")
    if not isinstance(correlation, dict) or set(correlation) != CORRELATION_FIELDS:
        raise ActionNotRun("admin_event_correlation_unavailable")
    delivery_count = correlation.get("delivery_count")
    if (
        correlation.get("availability") != "available"
        or not isinstance(correlation.get("inbound_id_sha256"), str)
        or HEX64_RE.fullmatch(cast(str, correlation.get("inbound_id_sha256"))) is None
        or correlation.get("status") != "committed"
        or type(delivery_count) is not int
        or delivery_count < 1
        or _timestamp(correlation.get("accepted_at")) is None
    ):
        raise ActionNotRun("admin_event_correlation_unavailable")
    if request.case == "idempotency" and delivery_count != observation["duplicate_count"] + 1:
        raise ActionNotRun("admin_idempotency_evidence_mismatch")

    outbounds = value.get("outbounds")
    if not isinstance(outbounds, dict) or set(outbounds) != OUTBOUNDS_FIELDS:
        raise ActionNotRun("admin_event_evidence_invalid")
    items = outbounds.get("items")
    if (
        type(outbounds.get("count")) is not int
        or not isinstance(items, list)
        or outbounds.get("count") != len(items)
        or outbounds.get("truncated") is not False
    ):
        raise ActionNotRun("admin_event_evidence_truncated")
    attempts_by_outbound = [_validated_attempts(item) for item in items]

    artifact = value.get("artifact")
    if not isinstance(artifact, dict) or set(artifact) != ARTIFACT_FIELDS:
        raise ActionNotRun("admin_event_evidence_invalid")
    artifact_items = artifact.get("items")
    if (
        artifact.get("availability") not in {"available", "not_found"}
        or type(artifact.get("count")) is not int
        or not isinstance(artifact_items, list)
        or artifact.get("count") != len(artifact_items)
    ):
        raise ActionNotRun("admin_event_evidence_invalid")
    for item in artifact_items:
        if (
            not isinstance(item, dict)
            or set(item) != ARTIFACT_ITEM_FIELDS
            or not isinstance(item.get("sha256"), str)
            or HEX64_RE.fullmatch(cast(str, item.get("sha256"))) is None
            or type(item.get("bytes")) is not int
            or item.get("bytes", 0) <= 0
            or _safe_name(item.get("status")) is None
            or _timestamp(item.get("created_at")) is None
        ):
            raise ActionNotRun("admin_event_evidence_invalid")

    if request.case in {"round_trip", "idempotency"}:
        if len(items) != 1:
            raise ActionNotRun("admin_ack_evidence_mismatch")
        attempts = attempts_by_outbound[0]
        expected_code = observation.get("provider_code") if request.case == "round_trip" else None
        if (
            items[0].get("delivery_status") != "delivered"
            or items[0].get("pending_count") != 0
            or items[0].get("dlq_count") != 0
            or not attempts
            or attempts[-1].get("status") != "delivered"
            or (
                expected_code is not None
                and str(expected_code)
                not in {str(attempt.get("provider_code")) for attempt in attempts}
            )
        ):
            raise ActionNotRun("admin_ack_evidence_mismatch")
    elif request.case in {"reconnect", "credential_rotation"}:
        if len(items) != 1:
            raise ActionNotRun("admin_ack_evidence_mismatch")
        codes = [attempt.get("provider_code") for attempt in attempts_by_outbound[0]]
        if (
            items[0].get("delivery_status") != "delivered"
            or items[0].get("pending_count") != 0
            or items[0].get("dlq_count") != 0
            or str(observation.get("provider_code")) not in codes
        ):
            raise ActionNotRun("admin_ack_evidence_mismatch")
    elif request.case == "media":
        matched = [
            item
            for item in artifact_items
            if item.get("status") == "available"
            and item.get("sha256") == observation.get("sha256")
            and item.get("bytes") == observation.get("bytes")
        ]
        if artifact.get("availability") != "available" or len(matched) != 1:
            raise ActionNotRun("admin_artifact_evidence_unavailable")
    elif request.case == "rate_limit_retry_after":
        if len(items) != 1:
            raise ActionNotRun("admin_retry_evidence_mismatch")
        attempts = attempts_by_outbound[0]
        matching = [
            attempt
            for attempt in attempts
            if str(attempt.get("provider_code")) == str(observation["provider_error_code"])
            and attempt.get("retry_after_seconds") == observation["retry_after_seconds"]
        ]
        if (
            items[0].get("delivery_status") != "delivered"
            or items[0].get("pending_count") != 0
            or items[0].get("dlq_count") != 0
            or len(attempts) != observation["retry_attempts"]
            or len(matching) != 1
        ):
            raise ActionNotRun("admin_retry_evidence_mismatch")
    elif request.case == "prolonged_outage":
        if (
            len(items) != observation["reply_count"]
            or any(item.get("delivery_status") != "delivered" for item in items)
            or sum(len(attempts) for attempts in attempts_by_outbound) != observation["ack_count"]
            or sum(cast(int, item.get("pending_count")) for item in items) != 0
            or sum(cast(int, item.get("dlq_count")) for item in items) != 0
        ):
            raise ActionNotRun("admin_outage_evidence_mismatch")
    elif request.case == "ambiguous":
        if (
            len(items) != 1
            or items[0].get("delivery_status") != "ambiguous"
            or len(attempts_by_outbound[0]) != 1
            or items[0].get("pending_count") != 0
        ):
            raise ActionNotRun("admin_ambiguous_evidence_mismatch")


def _execute(request: ActionRequest, config: ActionConfig) -> dict[str, Any]:
    if (
        request.tenant_id != config.tenant_id
        or request.binding_id != config.binding_id
        or request.account_id_sha256 != config.account_id_sha256
    ):
        raise ActionNotRun("configured_identity_mismatch")
    acceptance_run_id = _acceptance_run_id(request)
    registration = _request_json(
        "POST",
        _admin_run_registration_url(config, request),
        token_file=config.admin_token_file,
        timeout_seconds=config.timeout_seconds,
        body={
            "channel": "wecom_ai_bot",
            "run_id": acceptance_run_id,
            "run_nonce": request.run_nonce,
            "expires_in_seconds": 300,
        },
    )
    run_started_at = _validate_run_registration(registration, request)
    action_url = f"{config.control_base_url}/v1/wecom/control/actions/{quote(request.case)}"
    hook = _request_json(
        "POST",
        action_url,
        token_file=config.control_token_file,
        timeout_seconds=config.timeout_seconds,
        body=_control_request(request),
    )
    observation, ack_hash = _validate_hook(hook, request)
    snapshot = _request_json(
        "GET",
        _admin_snapshot_url(config, request),
        token_file=config.admin_token_file,
        timeout_seconds=config.timeout_seconds,
    )
    lifecycle = _lifecycle(snapshot, request, ack_hash, observation, run_started_at)
    event_evidence = _request_json(
        "POST",
        _admin_event_evidence_url(config, request),
        token_file=config.admin_token_file,
        timeout_seconds=config.timeout_seconds,
        body={
            "channel": "wecom_ai_bot",
            "run_id": acceptance_run_id,
            "run_nonce": request.run_nonce,
            "provider_event_hash": ack_hash,
        },
    )
    _validate_event_evidence(event_evidence, request, ack_hash, observation)
    return {
        "observation": {
            "status": "pass",
            "run_nonce": request.run_nonce,
            **observation,
        },
        "wecom_snapshot": {
            "schema_version": 1,
            "channel": "wecom",
            "case": request.case,
            "tenant_id": request.tenant_id,
            "binding_id": request.binding_id,
            "account_id_sha256": request.account_id_sha256,
            "provider_event_hash": ack_hash,
            "lifecycle": lifecycle,
        },
    }


def _input_stream() -> BinaryIO:
    return cast(BinaryIO, getattr(sys.stdin, "buffer", sys.stdin))


def _read_request() -> ActionRequest:
    raw = _input_stream().read(MAX_INPUT_BYTES + 1)
    if isinstance(raw, str):
        raw = raw.encode()
    if not isinstance(raw, bytes) or len(raw) > MAX_INPUT_BYTES:
        raise ActionNotRun("invalid_request")
    try:
        return _parse_request(_strict_json(raw))
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise ActionNotRun("invalid_request") from error


def _emit(value: object) -> None:
    raw = _canonical_bytes(value)
    stream = cast(BinaryIO, getattr(sys.stdout, "buffer", sys.stdout))
    stream.write(raw)
    stream.flush()


def main(argv: list[str] | None = None) -> int:
    parser = _ArgumentParser(add_help=False)
    parser.add_argument("--config", type=Path, required=True)
    try:
        args = parser.parse_args(argv)
        request = _read_request()
        if os.getenv(ACTION_ENV) != request.action:
            raise ActionNotRun("action_context_mismatch")
        result = _execute(request, _load_config(args.config))
    except ActionNotRun as error:
        _emit({"schema_version": 1, "status": "not_run", "reason": error.reason})
        return 1
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        _emit({"schema_version": 1, "status": "not_run", "reason": "internal_error"})
        return 1
    _emit(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
