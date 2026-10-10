#!/usr/bin/python3
"""Fail-closed Feishu action client for a separately reviewed control plane.

The executable accepts only the canonical broker request on stdin.  A fixed
``--config`` argument selects an action-specific HTTPS hook and an absolute
private token file.  It never implements or simulates provider behavior.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import math
import os
import re
import ssl
import stat
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, cast
from urllib.parse import SplitResult, quote, urlsplit

MAX_INPUT_BYTES = 64 * 1024
MAX_CONFIG_BYTES = 64 * 1024
MAX_TOKEN_BYTES = 4 * 1024
MAX_RESPONSE_BYTES = 256 * 1024
MAX_TIMEOUT_SECONDS = 180.0

ACTION_ENV = "TRPC_IM_CONTROL_ACTION"
ACTION_CASES = {
    "feishu_round_trip": "round_trip",
    "feishu_idempotency": "idempotency",
    "feishu_media": "media",
    "feishu_reconnect": "reconnect",
    "feishu_rate_limit_retry_after": "rate_limit_retry_after",
    "feishu_credential_rotation": "credential_rotation",
    "feishu_prolonged_outage": "prolonged_outage",
    "feishu_ambiguous": "ambiguous",
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

CONFIG_FIELDS = frozenset(
    {
        "schema_version",
        "channel",
        "tenant_id",
        "binding_id",
        "control_token_file",
        "evidence_base_url",
        "evidence_token_file",
        "hooks",
    }
)
HOOK_FIELDS = frozenset({"url", "timeout_seconds"})
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
    {"case", "expected_image_digest", "account_id_sha256", "observer_profile_sha256"}
)
PASS_ENVELOPE_FIELDS = frozenset({"schema_version", "status", "provider_event_hash", "evidence"})
NOT_RUN_ENVELOPE_FIELDS = frozenset({"schema_version", "status", "error_code"})
RESULT_FIELDS = frozenset({"observation", "callback_query", "callback_expected"})
ACK_RESULT_FIELDS = RESULT_FIELDS | {"openapi_witness"}
CALLBACK_QUERY_FIELDS = frozenset({"marker_sha256", "profile_sha256"})
CALLBACK_EXPECTED_FIELDS = frozenset({"event_id_sha256", "message_id_sha256"})
WITNESS_FIELDS = frozenset({"after_sequence", "path_sha256", "body_sha256"})
ACK_RESPONSE_FIELDS = frozenset(
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
ACK_CORRELATION_FIELDS = frozenset(
    {"availability", "inbound_id_sha256", "status", "delivery_count", "accepted_at"}
)
ACK_OUTBOUNDS_FIELDS = frozenset({"count", "truncated", "items"})
ACK_OUTBOUND_FIELDS = frozenset(
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
ACK_ATTEMPT_FIELDS = frozenset(
    {
        "attempt_number",
        "status",
        "provider_code",
        "retry_after_seconds",
        "started_at",
        "completed_at",
    }
)
ACK_ARTIFACT_FIELDS = frozenset({"availability", "count", "items"})
ACK_ARTIFACT_ITEM_FIELDS = frozenset({"sha256", "bytes", "status", "created_at"})
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

SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{16,256}$")
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
OPAQUE_ID_RE = re.compile(r"^imref-[0-9a-f]{64}$")
SAFE_PROVIDER_CODES = frozenset(
    {
        "0",
        "200",
        "429",
        "45009",
        "45011",
        "99991400",
        "99991401",
        "99991402",
        "99991672",
    }
)
SAFE_STATUS_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


class ActionError(RuntimeError):
    """Content-free action failure."""

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class HookConfig:
    action: str
    url: str
    target: SplitResult
    timeout_seconds: float


@dataclass(frozen=True, slots=True)
class ControlConfig:
    tenant_id: str
    binding_id: str
    control_token_file: Path
    evidence_base_url: str
    evidence_token_file: Path
    hooks: dict[str, HookConfig]


HookInvoker = Callable[[HookConfig, str, bytes], bytes]


def _not_run(error_code: str) -> dict[str, object]:
    return {"schema_version": 1, "status": "not_run", "error_code": error_code}


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
        object_pairs_hook=reject_duplicates,
        parse_constant=reject_constant,
    )


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _validate_root_owned_parent_chain(path: Path) -> None:
    if os.name != "posix":
        return
    current = path.parent
    while True:
        try:
            metadata = current.lstat()
        except OSError as error:
            raise ActionError("config_invalid") from error
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            or (metadata.st_uid != 0 and metadata.st_mode & stat.S_IWUSR)
        ):
            raise ActionError("config_invalid")
        if current.parent == current:
            return
        current = current.parent


def _read_private_file(path: Path, *, limit: int) -> bytes:
    if not path.is_absolute():
        raise ActionError("config_invalid")
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ActionError("config_invalid") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ActionError("config_invalid")
    if os.name == "posix":
        _validate_root_owned_parent_chain(path)
        if metadata.st_uid != 0 or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise ActionError("config_invalid")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ActionError("config_invalid") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ActionError("config_invalid")
        if os.name == "posix" and (
            metadata.st_uid != 0 or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        ):
            raise ActionError("config_invalid")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            contents = stream.read(limit + 1)
    finally:
        os.close(descriptor)
    if len(contents) > limit:
        raise ActionError("config_invalid")
    return contents


def _parse_hook(action_name: str, value: object) -> HookConfig:
    if not isinstance(value, dict) or set(value) != HOOK_FIELDS:
        raise ActionError("config_invalid")
    url = value.get("url")
    timeout = value.get("timeout_seconds")
    if not isinstance(url, str) or not 1 <= len(url) <= 2048:
        raise ActionError("config_invalid")
    try:
        target = urlsplit(url)
        port = target.port
    except ValueError as error:
        raise ActionError("config_invalid") from error
    expected_path = f"/v1/im/feishu/{ACTION_CASES[action_name]}"
    if (
        target.scheme != "https"
        or not target.hostname
        or target.username is not None
        or target.password is not None
        or target.query
        or target.fragment
        or target.path != expected_path
        or "//" in target.path
        or any(character.isspace() for character in url)
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise ActionError("config_invalid")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(float(timeout))
        or not 0 < float(timeout) <= MAX_TIMEOUT_SECONDS
    ):
        raise ActionError("config_invalid")
    return HookConfig(
        action=action_name,
        url=url,
        target=target,
        timeout_seconds=float(timeout),
    )


def _evidence_base_url(value: object) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 2048:
        raise ActionError("config_invalid")
    try:
        target = urlsplit(value)
        port = target.port
    except ValueError as error:
        raise ActionError("config_invalid") from error
    if (
        target.scheme != "https"
        or not target.hostname
        or target.username is not None
        or target.password is not None
        or target.query
        or target.fragment
        or target.path not in {"", "/"}
        or any(character.isspace() for character in value)
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise ActionError("config_invalid")
    return value.rstrip("/")


def _load_config(path: Path) -> ControlConfig:
    raw = _read_private_file(path, limit=MAX_CONFIG_BYTES)
    try:
        value = _strict_json(raw)
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise ActionError("config_invalid") from error
    if (
        not isinstance(value, dict)
        or set(value) != CONFIG_FIELDS
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("channel") != "feishu"
    ):
        raise ActionError("config_invalid")
    tenant_id = value.get("tenant_id")
    binding_id = value.get("binding_id")
    control_token_value = value.get("control_token_file")
    evidence_token_value = value.get("evidence_token_file")
    if (
        not isinstance(tenant_id, str)
        or SAFE_ID_RE.fullmatch(tenant_id) is None
        or not isinstance(binding_id, str)
        or SAFE_ID_RE.fullmatch(binding_id) is None
        or not isinstance(control_token_value, str)
        or not Path(control_token_value).is_absolute()
        or not isinstance(evidence_token_value, str)
        or not Path(evidence_token_value).is_absolute()
    ):
        raise ActionError("config_invalid")
    hooks_value = value.get("hooks")
    if not isinstance(hooks_value, dict) or any(name not in ACTION_CASES for name in hooks_value):
        raise ActionError("config_invalid")
    hooks = {name: _parse_hook(name, hook) for name, hook in hooks_value.items()}
    return ControlConfig(
        tenant_id=tenant_id,
        binding_id=binding_id,
        control_token_file=Path(control_token_value),
        evidence_base_url=_evidence_base_url(value.get("evidence_base_url")),
        evidence_token_file=Path(evidence_token_value),
        hooks=hooks,
    )


def _load_token(path: Path) -> str:
    try:
        raw = _read_private_file(path, limit=MAX_TOKEN_BYTES)
        token = raw.decode("utf-8")
    except (ActionError, UnicodeError) as error:
        raise ActionError("token_invalid") from error
    if token.endswith("\r\n"):
        token = token[:-2]
    elif token.endswith("\n"):
        token = token[:-1]
    if (
        not 16 <= len(token) <= MAX_TOKEN_BYTES
        or not token
        or any(not 0x21 <= ord(character) <= 0x7E for character in token)
    ):
        raise ActionError("token_invalid")
    return token


def _parse_request(value: object, *, action_name: str) -> dict[str, Any]:
    if (
        action_name not in ACTION_CASES
        or not isinstance(value, dict)
        or set(value) != REQUEST_FIELDS
    ):
        raise ActionError("input_invalid")
    payload = value.get("payload")
    expected_case = ACTION_CASES[action_name]
    if (
        type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("channel") != "feishu"
        or value.get("action") != action_name
        or not isinstance(value.get("run_id"), str)
        or SAFE_ID_RE.fullmatch(cast(str, value.get("run_id"))) is None
        or not isinstance(value.get("run_nonce"), str)
        or NONCE_RE.fullmatch(cast(str, value.get("run_nonce"))) is None
        or not isinstance(value.get("control_profile_sha256"), str)
        or HASH_RE.fullmatch(cast(str, value.get("control_profile_sha256"))) is None
        or not isinstance(payload, dict)
        or set(payload) != PAYLOAD_FIELDS
        or payload.get("case") != expected_case
        or not isinstance(payload.get("expected_image_digest"), str)
        or IMAGE_RE.fullmatch(cast(str, payload.get("expected_image_digest"))) is None
        or any(
            not isinstance(payload.get(field), str)
            or HASH_RE.fullmatch(cast(str, payload.get(field))) is None
            for field in ("account_id_sha256", "observer_profile_sha256")
        )
    ):
        raise ActionError("input_invalid")
    return {key: value[key] for key in REQUEST_FIELDS}


def _require_fields(value: object, expected: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ActionError("evidence_invalid")
    actual = set(value)
    if actual != expected:
        code = "evidence_incomplete" if actual < expected else "evidence_invalid"
        raise ActionError(code)
    return value


def _safe_hash(value: object) -> str:
    if not isinstance(value, str) or HASH_RE.fullmatch(value) is None:
        raise ActionError("evidence_invalid")
    return value


def _validate_evidence(
    value: object,
    *,
    case: str,
    run_nonce: str,
    observer_profile_sha256: str,
) -> dict[str, Any]:
    expected_result = ACK_RESULT_FIELDS if case in ACK_CASES else RESULT_FIELDS
    evidence = _require_fields(value, expected_result)
    observation = _require_fields(evidence.get("observation"), OBSERVATION_FIELDS[case])
    if (
        observation.get("status") != "pass"
        or observation.get("run_nonce") != run_nonce
        or not isinstance(observation.get("observed_at"), str)
        or not 20 <= len(cast(str, observation.get("observed_at"))) <= 64
    ):
        raise ActionError("evidence_invalid")
    for field, item in observation.items():
        if field.endswith("_id") and (
            not isinstance(item, str) or OPAQUE_ID_RE.fullmatch(item) is None
        ):
            raise ActionError("evidence_invalid")

    query = _require_fields(evidence.get("callback_query"), CALLBACK_QUERY_FIELDS)
    expected = _require_fields(evidence.get("callback_expected"), CALLBACK_EXPECTED_FIELDS)
    if _safe_hash(query.get("profile_sha256")) != observer_profile_sha256:
        raise ActionError("evidence_invalid")
    _safe_hash(query.get("marker_sha256"))
    for field in CALLBACK_EXPECTED_FIELDS:
        _safe_hash(expected.get(field))

    if case in ACK_CASES:
        witness = _require_fields(evidence.get("openapi_witness"), WITNESS_FIELDS)
        after_sequence = witness.get("after_sequence")
        if (
            isinstance(after_sequence, bool)
            or not isinstance(after_sequence, int)
            or after_sequence < 0
        ):
            raise ActionError("evidence_invalid")
        _safe_hash(witness.get("path_sha256"))
        _safe_hash(witness.get("body_sha256"))
    return evidence


def _invoke_https_hook(hook: HookConfig, token: str, payload: bytes) -> bytes:
    target = hook.target
    host = cast(str, target.hostname)
    port = target.port or 443
    connection = http.client.HTTPSConnection(
        host,
        port,
        timeout=hook.timeout_seconds,
        context=ssl.create_default_context(),
    )
    try:
        connection.request(
            "POST",
            target.path,
            body=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "trpc-im-feishu-control-action/1",
                "X-TRPC-IM-Action": hook.action,
            },
        )
        response = connection.getresponse()
        raw = response.read(MAX_RESPONSE_BYTES + 1)
        content_type = response.getheader("Content-Type", "")
    except (OSError, http.client.HTTPException, ssl.SSLError) as error:
        raise ActionError("hook_unavailable") from error
    finally:
        connection.close()
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ActionError("evidence_invalid")
    if response.status != 200 or not content_type.lower().startswith("application/json"):
        raise ActionError("hook_rejected")
    return raw


def _parse_hook_response(
    raw: bytes,
    *,
    request: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    if not raw or len(raw) > MAX_RESPONSE_BYTES:
        raise ActionError("evidence_invalid")
    try:
        value = _strict_json(raw)
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise ActionError("evidence_invalid") from error
    if not isinstance(value, dict):
        raise ActionError("evidence_invalid")
    if set(value) == NOT_RUN_ENVELOPE_FIELDS:
        if (
            type(value.get("schema_version")) is int
            and value.get("schema_version") == 1
            and value.get("status") == "not_run"
            and isinstance(value.get("error_code"), str)
            and 1 <= len(cast(str, value.get("error_code"))) <= 128
        ):
            raise ActionError("external_not_run")
        raise ActionError("evidence_invalid")
    if (
        set(value) != PASS_ENVELOPE_FIELDS
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("status") != "pass"
    ):
        raise ActionError("evidence_invalid")
    payload = cast(dict[str, Any], request["payload"])
    provider_event_hash = _safe_hash(value.get("provider_event_hash"))
    evidence = _validate_evidence(
        value.get("evidence"),
        case=cast(str, payload["case"]),
        run_nonce=cast(str, request["run_nonce"]),
        observer_profile_sha256=cast(str, payload["observer_profile_sha256"]),
    )
    callback_expected = cast(dict[str, Any], evidence["callback_expected"])
    if callback_expected["message_id_sha256"] != provider_event_hash:
        raise ActionError("evidence_invalid")
    return evidence, provider_event_hash


def _acceptance_hash(domain: str, *parts: str) -> str:
    material = "\0".join(("trpc-im-acceptance-evidence-v1", domain, *parts))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _acceptance_run_id(config: ControlConfig, request: Mapping[str, Any]) -> str:
    payload = cast(Mapping[str, Any], request["payload"])
    material = "\0".join(
        (
            "trpc-im-acceptance-run-v1",
            "feishu",
            config.tenant_id,
            config.binding_id,
            cast(str, request["run_id"]),
            cast(str, payload["case"]),
        )
    )
    return "im-" + hashlib.sha256(material.encode("utf-8")).hexdigest()


def _evidence_hook(config: ControlConfig, hook: HookConfig) -> HookConfig:
    url = (
        f"{config.evidence_base_url}/v1/tenants/{quote(config.tenant_id, safe='')}"
        f"/bindings/{quote(config.binding_id, safe='')}"
        "/im-acceptance/event-evidence"
    )
    return HookConfig(
        action="feishu_event_evidence",
        url=url,
        target=urlsplit(url),
        timeout_seconds=hook.timeout_seconds,
    )


def _registration_hook(config: ControlConfig, hook: HookConfig) -> HookConfig:
    url = (
        f"{config.evidence_base_url}/v1/tenants/{quote(config.tenant_id, safe='')}"
        f"/bindings/{quote(config.binding_id, safe='')}"
        "/im-acceptance/runs"
    )
    return HookConfig(
        action="feishu_run_registration",
        url=url,
        target=urlsplit(url),
        timeout_seconds=hook.timeout_seconds,
    )


def _acceptance_run_binding_hash(config: ControlConfig, request: Mapping[str, Any]) -> str:
    run_id_hash = _acceptance_hash(
        "run", config.tenant_id, config.binding_id, _acceptance_run_id(config, request)
    )
    run_nonce_hash = _acceptance_hash(
        "run-nonce",
        config.tenant_id,
        config.binding_id,
        cast(str, request["run_nonce"]),
    )
    return _acceptance_hash(
        "run-binding",
        config.tenant_id,
        config.binding_id,
        "feishu",
        run_id_hash,
        run_nonce_hash,
    )


def _timestamp_shape(value: object) -> bool:
    return isinstance(value, str) and 20 <= len(value) <= 64


def _finite_number(value: object, *, minimum: float, maximum: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and minimum <= float(value) <= maximum
    )


def _ack_object(value: object, fields: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ActionError("ack_evidence_invalid")
    return value


def _validated_ack_outbound(value: object) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    item = _ack_object(value, ACK_OUTBOUND_FIELDS)
    provider_message_hash = item.get("provider_message_id_sha256")
    attempts_value = item.get("attempts")
    if (
        not isinstance(item.get("outbound_id_sha256"), str)
        or HASH_RE.fullmatch(cast(str, item.get("outbound_id_sha256"))) is None
        or not isinstance(item.get("delivery_status"), str)
        or SAFE_STATUS_RE.fullmatch(cast(str, item.get("delivery_status"))) is None
        or (
            provider_message_hash is not None
            and (
                not isinstance(provider_message_hash, str)
                or HASH_RE.fullmatch(provider_message_hash) is None
            )
        )
        or type(item.get("attempt_count")) is not int
        or not isinstance(attempts_value, list)
        or item.get("attempt_count") != len(attempts_value)
        or type(item.get("pending_count")) is not int
        or cast(int, item.get("pending_count")) < 0
        or type(item.get("dlq_count")) is not int
        or cast(int, item.get("dlq_count")) < 0
        or not _timestamp_shape(item.get("created_at"))
        or not _timestamp_shape(item.get("updated_at"))
    ):
        raise ActionError("ack_evidence_invalid")

    attempts: list[dict[str, Any]] = []
    for raw_attempt in attempts_value:
        attempt = _ack_object(raw_attempt, ACK_ATTEMPT_FIELDS)
        provider_code = attempt.get("provider_code")
        retry_after = attempt.get("retry_after_seconds")
        if (
            type(attempt.get("attempt_number")) is not int
            or cast(int, attempt.get("attempt_number")) < 1
            or not isinstance(attempt.get("status"), str)
            or SAFE_STATUS_RE.fullmatch(cast(str, attempt.get("status"))) is None
            or (provider_code is not None and provider_code not in SAFE_PROVIDER_CODES)
            or (
                retry_after is not None
                and not _finite_number(retry_after, minimum=0.0, maximum=3600.0)
            )
            or not _timestamp_shape(attempt.get("started_at"))
            or not _timestamp_shape(attempt.get("completed_at"))
        ):
            raise ActionError("ack_evidence_invalid")
        attempts.append(attempt)
    if [item["attempt_number"] for item in attempts] != list(range(1, len(attempts) + 1)):
        raise ActionError("ack_evidence_invalid")
    return item, attempts


def _validate_ack_artifact(value: object) -> list[dict[str, Any]]:
    artifact = _ack_object(value, ACK_ARTIFACT_FIELDS)
    items_value = artifact.get("items")
    if (
        artifact.get("availability") not in {"available", "not_found"}
        or type(artifact.get("count")) is not int
        or not isinstance(items_value, list)
        or artifact.get("count") != len(items_value)
        or (artifact.get("availability") == "available") != bool(items_value)
    ):
        raise ActionError("ack_evidence_invalid")
    items: list[dict[str, Any]] = []
    for raw_item in items_value:
        item = _ack_object(raw_item, ACK_ARTIFACT_ITEM_FIELDS)
        if (
            not isinstance(item.get("sha256"), str)
            or HASH_RE.fullmatch(cast(str, item.get("sha256"))) is None
            or type(item.get("bytes")) is not int
            or cast(int, item.get("bytes")) <= 0
            or not isinstance(item.get("status"), str)
            or SAFE_STATUS_RE.fullmatch(cast(str, item.get("status"))) is None
            or not _timestamp_shape(item.get("created_at"))
        ):
            raise ActionError("ack_evidence_invalid")
        items.append(item)
    return items


def _require_delivered(
    items: list[dict[str, Any]],
    attempts_by_outbound: list[list[dict[str, Any]]],
) -> None:
    if len(items) != 1:
        raise ActionError("ack_evidence_mismatch")
    attempts = attempts_by_outbound[0]
    if (
        items[0].get("delivery_status") != "delivered"
        or items[0].get("pending_count") != 0
        or items[0].get("dlq_count") != 0
        or not attempts
        or attempts[-1].get("status") != "delivered"
        or attempts[-1].get("provider_code") not in {"0", "200"}
    ):
        raise ActionError("ack_evidence_mismatch")


def _validate_ack_case(
    *,
    case: str,
    observation: Mapping[str, Any],
    delivery_count: int,
    items: list[dict[str, Any]],
    attempts_by_outbound: list[list[dict[str, Any]]],
    artifact_items: list[dict[str, Any]],
) -> None:
    if case == "idempotency":
        duplicate_count = observation.get("duplicate_count")
        provider_delivery_count = observation.get("provider_delivery_count")
        if (
            type(duplicate_count) is not int
            or type(provider_delivery_count) is not int
            or duplicate_count < 1
            or delivery_count != duplicate_count + 1
            or delivery_count != provider_delivery_count
        ):
            raise ActionError("ack_evidence_mismatch")
        _require_delivered(items, attempts_by_outbound)
        return
    if case == "media":
        expected_hash = observation.get("sha256")
        expected_bytes = observation.get("bytes")
        if (
            not isinstance(expected_hash, str)
            or HASH_RE.fullmatch(expected_hash) is None
            or type(expected_bytes) is not int
            or expected_bytes <= 0
            or len(
                [
                    item
                    for item in artifact_items
                    if item.get("status") == "available"
                    and item.get("sha256") == expected_hash
                    and item.get("bytes") == expected_bytes
                ]
            )
            != 1
        ):
            raise ActionError("ack_evidence_mismatch")
        return
    if case == "rate_limit_retry_after":
        _require_delivered(items, attempts_by_outbound)
        attempts = attempts_by_outbound[0]
        expected_code = str(observation.get("provider_error_code"))
        retry_after = observation.get("retry_after_seconds")
        retry_attempts = observation.get("retry_attempts")
        matching = [
            attempt
            for attempt in attempts
            if attempt.get("provider_code") == expected_code
            and _finite_number(attempt.get("retry_after_seconds"), minimum=0.0, maximum=3600.0)
            and _finite_number(retry_after, minimum=0.0, maximum=3600.0)
            and abs(
                float(cast(float | int, attempt.get("retry_after_seconds")))
                - float(cast(float | int, retry_after))
            )
            <= 0.001
        ]
        if (
            expected_code not in SAFE_PROVIDER_CODES - {"0", "200"}
            or type(retry_attempts) is not int
            or retry_attempts != len(attempts)
            or len(matching) != 1
        ):
            raise ActionError("ack_evidence_mismatch")
        return
    if case == "ambiguous":
        if (
            len(items) != 1
            or items[0].get("delivery_status") != "ambiguous"
            or items[0].get("pending_count") != 0
            or items[0].get("dlq_count") != 0
            or len(attempts_by_outbound[0]) != 1
            or attempts_by_outbound[0][0].get("status") != "ambiguous"
        ):
            raise ActionError("ack_evidence_mismatch")
        return
    if case in {
        "round_trip",
        "reconnect",
        "credential_rotation",
        "prolonged_outage",
    }:
        _require_delivered(items, attempts_by_outbound)


def _validate_ack_response(
    raw: bytes,
    *,
    config: ControlConfig,
    request: Mapping[str, Any],
    provider_event_hash: str,
    evidence: Mapping[str, Any],
) -> None:
    if not raw or len(raw) > MAX_RESPONSE_BYTES:
        raise ActionError("ack_evidence_invalid")
    try:
        decoded = _strict_json(raw)
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise ActionError("ack_evidence_invalid") from error
    value = _ack_object(decoded, ACK_RESPONSE_FIELDS)
    if (
        type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("tenant_id") != config.tenant_id
        or value.get("binding_id") != config.binding_id
        or value.get("channel") != "feishu"
        or value.get("requested_run_id_sha256")
        != _acceptance_hash(
            "run",
            config.tenant_id,
            config.binding_id,
            _acceptance_run_id(config, request),
        )
        or value.get("run_binding_sha256") != _acceptance_run_binding_hash(config, request)
        or value.get("provider_event_hash") != provider_event_hash
    ):
        raise ActionError("ack_evidence_mismatch")

    correlation_value = value.get("correlation")
    if isinstance(correlation_value, dict) and correlation_value == {"availability": "not_found"}:
        raise ActionError("ack_evidence_unavailable")
    correlation = _ack_object(correlation_value, ACK_CORRELATION_FIELDS)
    delivery_count = correlation.get("delivery_count")
    if (
        correlation.get("availability") != "available"
        or not isinstance(correlation.get("inbound_id_sha256"), str)
        or HASH_RE.fullmatch(cast(str, correlation.get("inbound_id_sha256"))) is None
        or correlation.get("status") != "committed"
        or type(delivery_count) is not int
        or delivery_count < 1
        or not _timestamp_shape(correlation.get("accepted_at"))
    ):
        raise ActionError("ack_evidence_unavailable")

    outbounds = _ack_object(value.get("outbounds"), ACK_OUTBOUNDS_FIELDS)
    outbound_values = outbounds.get("items")
    if (
        type(outbounds.get("count")) is not int
        or not isinstance(outbound_values, list)
        or outbounds.get("count") != len(outbound_values)
        or outbounds.get("truncated") is not False
        or len(outbound_values) > 10
    ):
        raise ActionError("ack_evidence_invalid")
    parsed_outbounds = [_validated_ack_outbound(item) for item in outbound_values]
    items = [item for item, _attempts in parsed_outbounds]
    attempts_by_outbound = [attempts for _item, attempts in parsed_outbounds]
    artifact_items = _validate_ack_artifact(value.get("artifact"))
    payload = cast(Mapping[str, Any], request["payload"])
    observation = cast(Mapping[str, Any], evidence["observation"])
    _validate_ack_case(
        case=cast(str, payload["case"]),
        observation=observation,
        delivery_count=delivery_count,
        items=items,
        attempts_by_outbound=attempts_by_outbound,
        artifact_items=artifact_items,
    )


def _validate_run_registration(
    raw: bytes, *, config: ControlConfig, request: Mapping[str, Any]
) -> None:
    if not raw or len(raw) > MAX_RESPONSE_BYTES:
        raise ActionError("run_registration_invalid")
    try:
        value = _ack_object(_strict_json(raw), RUN_REGISTRATION_FIELDS)
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise ActionError("run_registration_invalid") from error
    if (
        value.get("schema_version") != 1
        or value.get("tenant_id") != config.tenant_id
        or value.get("binding_id") != config.binding_id
        or value.get("channel") != "feishu"
        or value.get("run_id_sha256")
        != _acceptance_hash(
            "run", config.tenant_id, config.binding_id, _acceptance_run_id(config, request)
        )
        or value.get("run_binding_sha256") != _acceptance_run_binding_hash(config, request)
        or not _timestamp_shape(value.get("created_at"))
        or not _timestamp_shape(value.get("expires_at"))
    ):
        raise ActionError("run_registration_mismatch")


def _contains_string(value: object, expected: str) -> bool:
    if isinstance(value, str):
        return value == expected
    if isinstance(value, list):
        return any(_contains_string(item, expected) for item in value)
    if isinstance(value, dict):
        return any(
            _contains_string(key, expected) or _contains_string(item, expected)
            for key, item in value.items()
        )
    return False


def run_action(
    request_value: object,
    *,
    action_name: str,
    config_path: Path,
    invoke: HookInvoker = _invoke_https_hook,
) -> dict[str, Any]:
    try:
        request = _parse_request(request_value, action_name=action_name)
        config = _load_config(config_path)
        hook = config.hooks.get(action_name)
        if hook is None:
            raise ActionError("hook_not_configured")
        evidence_token = _load_token(config.evidence_token_file)
        registration_request = {
            "channel": "feishu",
            "run_id": _acceptance_run_id(config, request),
            "run_nonce": request["run_nonce"],
            "expires_in_seconds": 300,
        }
        try:
            registration_raw = invoke(
                _registration_hook(config, hook),
                evidence_token,
                _canonical_json(registration_request),
            )
        except ActionError as error:
            raise ActionError("run_registration_unavailable") from error
        _validate_run_registration(registration_raw, config=config, request=request)
        control_token = _load_token(config.control_token_file)
        raw = invoke(hook, control_token, _canonical_json(request))
        evidence, provider_event_hash = _parse_hook_response(raw, request=request)
        if _contains_string(evidence, control_token):
            raise ActionError("evidence_invalid")
        ack_request = {
            "channel": "feishu",
            "run_id": _acceptance_run_id(config, request),
            "run_nonce": request["run_nonce"],
            "provider_event_hash": provider_event_hash,
        }
        try:
            ack_raw = invoke(
                _evidence_hook(config, hook),
                evidence_token,
                _canonical_json(ack_request),
            )
        except ActionError as error:
            raise ActionError("ack_evidence_unavailable") from error
        _validate_ack_response(
            ack_raw,
            config=config,
            request=request,
            provider_event_hash=provider_event_hash,
            evidence=evidence,
        )
        if _contains_string(evidence, evidence_token):
            raise ActionError("evidence_invalid")
        return evidence
    except ActionError as error:
        return _not_run(error.error_code)
    except Exception:
        return _not_run("internal_error")


def check_configuration(config_path: Path) -> bool:
    try:
        config = _load_config(config_path)
        _load_token(config.control_token_file)
        _load_token(config.evidence_token_file)
    except ActionError:
        return False
    return set(config.hooks) == set(ACTION_CASES)


def _input_stream() -> BinaryIO:
    return cast(BinaryIO, getattr(sys.stdin, "buffer", sys.stdin))


def _output_stream() -> BinaryIO:
    return cast(BinaryIO, getattr(sys.stdout, "buffer", sys.stdout))


def _read_request() -> object:
    raw = _input_stream().read(MAX_INPUT_BYTES + 1)
    if not raw or len(raw) > MAX_INPUT_BYTES:
        raise ActionError("input_invalid")
    try:
        return _strict_json(raw)
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise ActionError("input_invalid") from error


def _write_result(value: object) -> None:
    _output_stream().write(_canonical_json(value) + b"\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    if args.check:
        if not check_configuration(args.config):
            _write_result({"status": "not_ready"})
            return 1
        _write_result({"status": "ready"})
        return 0
    action_name = os.environ.get(ACTION_ENV, "")
    try:
        request_value = _read_request()
    except ActionError as error:
        _write_result(_not_run(error.error_code))
        return 0
    _write_result(
        run_action(
            request_value,
            action_name=action_name,
            config_path=args.config,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
