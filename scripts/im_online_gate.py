#!/usr/bin/env python3
"""Fail-closed online Feishu/WeCom acceptance evidence producer.

The production release gate reserves this module as the only producer of
``runs/multitenant/im-online.json``.  A real result is deliberately opt-in and
requires an operator-supplied probe endpoint and both channel credentials.
This script never turns protocol configuration or a local mock into a
production pass.  The probe endpoint must return a content-free JSON receipt
for each required case; no credential or message body is written to the
report.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

# Keep direct invocation working for the release-documented
# ``python scripts/im_online_gate.py`` form.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evidence_lineage import (
    build_evidence,
    canonical_sha256,
    current_release_binding,
    new_run_id,
    source_fingerprint,
    validate_current_candidate_evidence,
)
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
PRODUCER = "scripts.im_online_gate"
REQUIRED_CHANNELS = ("feishu", "wecom")
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
# A production probe must demonstrate a real provider throttle, not merely
# return a generic ``pass`` value.  429 is accepted for both channels when it
# is the HTTP status observed by the probe; the remaining values are the
# provider-native quota/error codes used by the corresponding APIs.
RATE_LIMIT_CODES = {
    "feishu": frozenset({429, 99991400, 99991401, 99991402, 99991672}),
    "wecom": frozenset({429, 45009, 45011}),
}
CHANNEL_CREDENTIALS = {
    "feishu": (
        "FEISHU_APP_ID",
        "FEISHU_APP_SECRET",
        "FEISHU_VERIFICATION_TOKEN",
        "FEISHU_ENCRYPT_KEY",
    ),
    "wecom": ("WECOM_BOT_ID", "WECOM_BOT_SECRET"),
}
CHANNEL_ACCOUNT_VARIABLE = {
    "feishu": "FEISHU_APP_ID",
    "wecom": "WECOM_BOT_ID",
}
PROVIDER_EVIDENCE_SOURCE = {
    "feishu": "feishu_api_and_webhook",
    "wecom": "wecom_ws_and_send_ack",
}
PROVIDER_EVIDENCE_PATHS = {
    "feishu": ("provider_callback", "provider_send_ack"),
    "wecom": ("provider_ws_event", "provider_send_ack"),
}
FEISHU_RECONNECT_FIELDS = (
    "failed_endpoint_id",
    "replacement_endpoint_id",
    "endpoint_set_observed",
    "received_after_failover_event_id",
    "outbound_request_id",
    "acknowledged_request_id",
    "ready_endpoint_count",
    "unready_endpoint_count",
    "terminating_endpoint_count",
)
IDEMPOTENCY_BASE_FIELDS = ("duplicate_event_id", "unique_inbound_id", "duplicate_count")
FEISHU_IDEMPOTENCY_FIELDS = ("original_event_id", "provider_delivery_count")
WECOM_IDEMPOTENCY_FIELDS = ("duplicate_source", "original_event_id", "replayed_event_id")
WECOM_DUPLICATE_SOURCE = "service_replay_of_provider_event"
WECOM_RECONNECT_FIELDS = (
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
WECOM_OUTAGE_MODES = frozenset({"service_failover", "provider_delivery_gap"})
WECOM_SERVICE_FAILOVER_FIELDS = (
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
# Contract v1 has no provider replay/cursor API.  A total WSS outage therefore
# remains a fail/not-supported result; no self-reported replay fields can
# promote it to a pass.
WECOM_PROVIDER_DELIVERY_GAP_FIELDS: tuple[str, ...] = ()
HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$")
SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
PROBE_IDENTITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
PLACEHOLDER_MARKERS = ("replace-with", "change-me", "placeholder", "synthetic")
_TIMESTAMP_SKEW = timedelta(seconds=5)
MAX_RETRY_AFTER_SECONDS = 3600.0
MAX_OUTAGE_SECONDS = 7 * 24 * 60 * 60.0
MIN_PROLONGED_OUTAGE_SECONDS = 60.0
MAX_RETRY_ATTEMPTS = 100
MAX_PROBE_RESPONSE_BYTES = 64 * 1024
PROBE_URL_ALLOWLIST_ENV = "TRPC_IM_ONLINE_PROBE_URL_ALLOWLIST"
PROBE_URL_ALLOWLIST_ALIASES = (PROBE_URL_ALLOWLIST_ENV, "TRPC_IM_ONLINE_PROBE_ALLOWLIST")
PROBE_IDENTITY_ENV = "TRPC_IM_ONLINE_PROBE_IDENTITY"
PROBE_IDENTITY_HASH_ENV = "TRPC_IM_ONLINE_PROBE_IDENTITY_SHA256"
CONTROL_PROFILE_HASH_ENV = {
    "feishu": "TRPC_IM_ONLINE_FEISHU_CONTROL_PROFILE_SHA256",
    "wecom": "TRPC_IM_ONLINE_WECOM_CONTROL_PROFILE_SHA256",
}
PROBE_TRUST_PATH = ROOT / "deploy" / "im-probe-trust.json"
MAX_PROBE_TRUST_BYTES = 8 * 1024
RELEASE_ID_ENV = "TRPC_RELEASE_ID"
RELEASE_NONCE_ENV = "TRPC_RELEASE_NONCE"
RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
RELEASE_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{32,256}$")
ARTIFACT_CONTRACT_VERSION = 1


class _NoRedirect(HTTPRedirectHandler):
    """Do not let an operator-controlled probe URL redirect to another host."""

    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("probe redirects are not allowed")


def _finite_number(value: object, *, minimum: float, maximum: float) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    numeric = float(value)
    return math.isfinite(numeric) and minimum <= numeric <= maximum


def _canonical_probe_url(value: object) -> str | None:
    """Normalize an HTTPS base URL without accepting URL confusion inputs."""

    if not isinstance(value, str) or not value or len(value) > 2048:
        return None
    if any(character.isspace() or ord(character) < 0x20 for character in value):
        return None
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme.lower() != "https"
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "\\" in value
        or "%" in parsed.netloc
    ):
        return None
    hostname = hostname.lower()
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    # Treat the default HTTPS port as its canonical omission.  Non-default
    # ports remain part of the explicit allowlist identity.
    netloc = hostname if port in (None, 443) else f"{hostname}:{port}"
    path = parsed.path.rstrip("/")
    return f"https://{netloc}{path}"


def _probe_request_url(value: str) -> str:
    canonical = _canonical_probe_url(value)
    if canonical is None:
        raise ValueError(
            "TRPC_IM_ONLINE_PROBE_URL must be an HTTPS URL without userinfo, query, or fragment"
        )
    return canonical + "/probe"


def _probe_allowlist() -> tuple[str, ...]:
    raw = ""
    for name in PROBE_URL_ALLOWLIST_ALIASES:
        candidate = os.getenv(name, "").strip()
        if candidate:
            raw = candidate
            break
    values = tuple(
        canonical
        for item in re.split(r"[,\r\n]+", raw)
        if (canonical := _canonical_probe_url(item.strip())) is not None
    )
    return tuple(dict.fromkeys(values))


def _probe_allowlist_has_invalid_entry() -> bool:
    raw = ""
    for name in PROBE_URL_ALLOWLIST_ALIASES:
        candidate = os.getenv(name, "").strip()
        if candidate:
            raw = candidate
            break
    entries = [item.strip() for item in re.split(r"[,\r\n]+", raw) if item.strip()]
    return bool(raw) and (
        not entries or any(_canonical_probe_url(item) is None for item in entries)
    )


def _probe_identity() -> tuple[str | None, str | None, str | None]:
    """Return (configured identity, report-safe hash, configuration source)."""

    identity_hash = os.getenv(PROBE_IDENTITY_HASH_ENV, "").strip().lower()
    if identity_hash:
        if not HEX64_RE.fullmatch(identity_hash) or identity_hash in {"0" * 64, "f" * 64}:
            return None, None, None
        return None, identity_hash, PROBE_IDENTITY_HASH_ENV
    identity = os.getenv(PROBE_IDENTITY_ENV, "").strip()
    if (
        not identity
        or not PROBE_IDENTITY_RE.fullmatch(identity)
        or any(marker in identity.lower() for marker in PLACEHOLDER_MARKERS)
    ):
        return None, None, None
    return identity, _fingerprint(identity, label=PROBE_IDENTITY_ENV), PROBE_IDENTITY_ENV


def _assert_safe_output_path(output: Path) -> None:
    """Reject symlinked output or parent components before creating anything."""

    candidates = (output, *output.parents)
    for path in candidates:
        if path.is_symlink():
            raise RuntimeError("refusing to write an IM online report through a symlink")


def _strict_json_loads(raw: str) -> Any:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number {value} is not allowed")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key {key!r} is not allowed")
            result[key] = value
        return result

    return json.loads(
        raw,
        parse_constant=reject_constant,
        object_pairs_hook=reject_duplicate_keys,
    )


def _load_probe_trust() -> tuple[dict[str, Any] | None, str | None]:
    """Load the source-bound Ed25519 identity for the independent IM probe."""

    path = PROBE_TRUST_PATH
    deploy_root = (ROOT / "deploy").resolve()
    if path.is_symlink():
        return None, "probe trust file must not be a symlink"
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(deploy_root)
    except (OSError, ValueError):
        return None, "deploy/im-probe-trust.json is missing or outside deploy"
    current = resolved.parent
    while current != deploy_root:
        if current.is_symlink():
            return None, "probe trust path must not contain symlinks"
        if current.parent == current:
            return None, "probe trust path is not beneath deploy"
        current = current.parent
    try:
        before = resolved.stat()
        raw = resolved.read_bytes()
        after = resolved.stat()
    except OSError:
        return None, "probe trust file could not be read"
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        return None, "probe trust file changed while it was being read"
    if not raw or len(raw) > MAX_PROBE_TRUST_BYTES:
        return None, "probe trust file size is invalid"
    try:
        value = _strict_json_loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError):
        return None, "probe trust file is not strict JSON"
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "probe_url",
        "key_id",
        "ed25519_public_key",
    }:
        return None, "probe trust file has an invalid schema"
    probe_url = _canonical_probe_url(value.get("probe_url"))
    key_id = value.get("key_id")
    encoded_key = value.get("ed25519_public_key")
    if (
        type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or probe_url is None
    ):
        return None, "probe trust file has an invalid schema or URL"
    if not isinstance(key_id, str) or SAFE_CODE_RE.fullmatch(key_id) is None:
        return None, "probe trust key_id is invalid"
    if not isinstance(encoded_key, str):
        return None, "probe trust public key is invalid"
    try:
        public_key = base64.b64decode(encoded_key, validate=True)
    except ValueError:
        return None, "probe trust public key is not valid base64"
    if len(public_key) != 32 or public_key in {b"\0" * 32, b"\xff" * 32}:
        return None, "probe trust public key is not a valid Ed25519 key"
    trust_projection = {
        "schema_version": 1,
        "probe_url": probe_url,
        "key_id": key_id,
        "ed25519_public_key": encoded_key,
    }
    return {
        "probe_url": probe_url,
        "key_id": key_id,
        "public_key": public_key,
        "key_sha256": hashlib.sha256(public_key).hexdigest(),
        "file_sha256": hashlib.sha256(raw).hexdigest(),
        "config_sha256": hashlib.sha256(
            json.dumps(
                trust_projection,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest(),
    }, None


def _verify_probe_signature(response: dict[str, Any], trust: dict[str, Any]) -> None:
    """Verify a detached signature over the complete probe response body."""

    attestation = response.get("signature_attestation")
    if not isinstance(attestation, dict) or set(attestation) != {
        "algorithm",
        "key_id",
        "signature",
    }:
        raise RuntimeError("probe response signature attestation is missing")
    if attestation.get("algorithm") != "ed25519" or attestation.get("key_id") != trust["key_id"]:
        raise RuntimeError("probe response signature identity does not match trust config")
    encoded_signature = attestation.get("signature")
    if not isinstance(encoded_signature, str):
        raise RuntimeError("probe response signature is invalid")
    try:
        signature = base64.b64decode(encoded_signature, validate=True)
    except ValueError as error:
        raise RuntimeError("probe response signature is invalid") from error
    if len(signature) != 64:
        raise RuntimeError("probe response signature is invalid")
    signed_payload = dict(response)
    signed_payload.pop("signature_attestation", None)
    message = json.dumps(
        signed_payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    try:
        Ed25519PublicKey.from_public_bytes(trust["public_key"]).verify(signature, message)
    except (InvalidSignature, ValueError) as error:
        raise RuntimeError("probe response signature verification failed") from error


def _normalized_probe_response_sha256(
    channel: str,
    response: dict[str, Any],
    provider_evidence: dict[str, Any],
) -> str:
    """Hash only the signed response fields retained in release evidence.

    The provider may return message bodies or other opaque payloads.  They are
    intentionally absent from this projection; the detached signature is
    verified separately, while the report receives only this content-free
    digest and the already-sanitized provider evidence.
    """

    runtime = response.get("runtime")
    runtime_projection: dict[str, Any] = {}
    if isinstance(runtime, dict):
        for field in (
            "status",
            "run_nonce",
            "image_digest",
            "identity_fingerprint",
            "control_profile_sha256",
            "release_id",
            "release_nonce_sha256",
            "source_fingerprint",
        ):
            value = runtime.get(field)
            if isinstance(value, (str, int, float, bool)) or value is None:
                runtime_projection[field] = value
    cases = response.get("cases")
    case_projection = {
        case: {
            "status": value.get("status") if isinstance(value, dict) else None,
        }
        for case in REQUIRED_CASES
        for value in [cases.get(case) if isinstance(cases, dict) else None]
    }
    return canonical_sha256(
        {
            "schema_version": 1,
            "channel": channel,
            "runtime": runtime_projection,
            "cases": case_projection,
            "provider_evidence": provider_evidence,
        }
    )


def _probe_response_digest_binding(
    *,
    channel: str,
    run_id: str,
    run_nonce: str,
    response_sha256: str,
    control_profile_sha256: str,
    trust: dict[str, Any],
) -> str:
    return canonical_sha256(
        {
            "channel": channel,
            "run_id": run_id,
            "run_nonce": run_nonce,
            "response_sha256": response_sha256,
            "control_profile_sha256": control_profile_sha256,
            "trust_key_id": trust["key_id"],
            "trust_key_sha256": trust["key_sha256"],
            "trust_config_sha256": trust["config_sha256"],
            "trust_file_sha256": trust["file_sha256"],
        }
    )


def _validate_probe_runtime(
    runtime: object,
    *,
    run_nonce: str,
    image_digest: str,
    configured_identity: str | None,
    identity_hash: str,
    control_profile_sha256: str,
    release_id: str,
    release_nonce_sha256: str,
    source_fingerprint_value: str,
) -> tuple[bool, str | None]:
    """Validate a content-free probe attestation without retaining identity data."""

    if not isinstance(runtime, dict):
        return False, "probe runtime attestation is missing"
    if runtime.get("status") != "pass":
        return False, "probe runtime attestation status is not pass"
    if runtime.get("run_nonce") != run_nonce:
        return False, "probe runtime attestation nonce does not match this run"
    observed_digest = runtime.get("image_digest")
    if observed_digest != image_digest:
        return False, "probe runtime image attestation did not match candidate"
    if runtime.get("control_profile_sha256") != control_profile_sha256:
        return False, "probe runtime control profile did not match candidate"
    if runtime.get("release_id") != release_id:
        return False, "probe runtime release ID did not match candidate"
    if runtime.get("release_nonce_sha256") != release_nonce_sha256:
        return False, "probe runtime release nonce did not match candidate"
    if runtime.get("source_fingerprint") != source_fingerprint_value:
        return False, "probe runtime source fingerprint did not match deployed candidate"
    observed_hash = runtime.get("identity_fingerprint")
    if observed_hash != identity_hash:
        observed_identity = runtime.get("identity")
        if configured_identity is None or not isinstance(observed_identity, str):
            return False, "probe runtime identity attestation did not match the fixed identity"
        if not hmac.compare_digest(observed_identity, configured_identity):
            return False, "probe runtime identity attestation did not match the fixed identity"
    return True, None


def _validate_artifact_attestation(value: object) -> tuple[dict[str, Any] | None, str | None]:
    required = {
        "schema_version",
        "runner_sha256",
        "runner_contract_version",
        "driver_sha256",
        "driver_contract_version",
    }
    if not isinstance(value, dict) or set(value) != required:
        return None, "provider artifact attestation is missing or invalid"
    if (
        value.get("schema_version") != ARTIFACT_CONTRACT_VERSION
        or value.get("runner_contract_version") != ARTIFACT_CONTRACT_VERSION
        or value.get("driver_contract_version") != ARTIFACT_CONTRACT_VERSION
    ):
        return None, "provider artifact contract version is invalid"
    for field in ("runner_sha256", "driver_sha256"):
        digest = value.get(field)
        if (
            not isinstance(digest, str)
            or HEX64_RE.fullmatch(digest) is None
            or digest.lower() in {"0" * 64, "f" * 64}
        ):
            return None, f"provider artifact {field} is invalid"
    return {
        "schema_version": ARTIFACT_CONTRACT_VERSION,
        "runner_sha256": str(value["runner_sha256"]).lower(),
        "runner_contract_version": ARTIFACT_CONTRACT_VERSION,
        "driver_sha256": str(value["driver_sha256"]).lower(),
        "driver_contract_version": ARTIFACT_CONTRACT_VERSION,
    }, None


def _enabled() -> bool:
    return os.getenv("TRPC_IM_ONLINE_TESTS_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _safe_post(url: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
    if not _finite_number(timeout, minimum=0.001, maximum=300.0):
        raise ValueError("probe timeout must be finite and between 0.001 and 300 seconds")
    request_url = _probe_request_url(url)
    body = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8")
    request = Request(  # noqa: S310 - scheme is validated before _safe_post is called
        request_url,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    opener = build_opener(_NoRedirect)
    try:
        response_context = opener.open(request, timeout=timeout)
    except HTTPError as error:
        if 300 <= error.code < 400:
            raise RuntimeError("probe redirects are not allowed") from error
        raise
    with response_context as response:
        if response.geturl() != request_url:
            raise RuntimeError("probe response URL does not match the configured endpoint")
        if response.status != 200:
            raise RuntimeError(f"probe returned HTTP {response.status}")
        value = _strict_json_loads(response.read(MAX_PROBE_RESPONSE_BYTES + 1).decode("utf-8"))
        if (
            len(json.dumps(value, separators=(",", ":"), allow_nan=False).encode("utf-8"))
            > MAX_PROBE_RESPONSE_BYTES
        ):
            raise RuntimeError("probe response exceeds the bounded JSON size")
    if not isinstance(value, dict):
        raise RuntimeError("probe response is not a JSON object")
    return value


def _fingerprint(value: str, *, label: str) -> str:
    """Return a one-way, report-safe identity for an operator secret/value."""

    return hashlib.sha256((label + "\0" + value).encode("utf-8")).hexdigest()


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or len(value) > 80:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        return None
    return parsed.astimezone(UTC)


def _timestamp_is_valid(value: object) -> bool:
    return _parse_timestamp(value) is not None


def _normalized_timestamp(value: object) -> str | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _safe_identifier(value: object) -> str | None:
    if not isinstance(value, str) or not SAFE_ID_RE.fullmatch(value):
        return None
    return value


def _provider_code_number(value: object) -> int | None:
    """Parse a provider/platform code without accepting booleans or floats."""

    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _required_observation_fields(
    case: str,
    *,
    channel: str | None = None,
    observation: object | None = None,
) -> tuple[str, ...]:
    common = ("provider_event_id", "observed_at")
    specific: dict[str, tuple[str, ...]] = {
        "round_trip": ("callback_event_id", "outbound_request_id", "provider_code"),
        "idempotency": IDEMPOTENCY_BASE_FIELDS,
        "media": ("media_id_hash", "sha256", "bytes"),
        "reconnect": (),
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
        ),
        "prolonged_outage": ("outage_event_id", "recovery_event_id", "outage_seconds"),
        "ambiguous": (
            "ambiguous_event_id",
            "manual_review_id",
            "drop_response_observed",
            "auto_replay_count",
        ),
    }
    fields = common + specific[case]
    if case == "idempotency":
        return fields + (
            FEISHU_IDEMPOTENCY_FIELDS if channel == "feishu" else WECOM_IDEMPOTENCY_FIELDS
        )
    if case == "reconnect":
        fields += FEISHU_RECONNECT_FIELDS if channel == "feishu" else WECOM_RECONNECT_FIELDS
    if case == "credential_rotation" and channel == "wecom":
        fields += ("outbound_request_id", "acknowledged_request_id", "provider_code")
    if case != "prolonged_outage" or channel != "wecom":
        return fields
    fields += ("outage_mode",)
    mode = observation.get("outage_mode") if isinstance(observation, dict) else None
    if mode == "service_failover":
        fields += WECOM_SERVICE_FAILOVER_FIELDS
    elif mode == "provider_delivery_gap":
        fields += WECOM_PROVIDER_DELIVERY_GAP_FIELDS
    return fields


def _validate_provider_evidence(
    channel: str,
    response: dict[str, Any],
    *,
    run_nonce: str,
    credential_fingerprints: dict[str, str],
    run_started_at: datetime | None = None,
    now: datetime | None = None,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Validate the platform-originated evidence contract for one channel.

    A probe response is not accepted merely because it says pass. It must
    bind two provider paths (callback/WS observation and send/API
    acknowledgement) to this run nonce, the configured account fingerprint,
    and every required resilience case. Unknown fields are ignored and never
    copied into the report, preventing provider payloads or credentials from
    leaking into release evidence.
    """

    errors: list[str] = []
    evidence = response.get("provider_evidence")
    if not isinstance(evidence, dict):
        return None, ["provider_evidence is missing"]
    if evidence.get("source") != PROVIDER_EVIDENCE_SOURCE[channel]:
        errors.append("provider_evidence.source is not the channel contract")
    if evidence.get("independent_paths") != list(PROVIDER_EVIDENCE_PATHS[channel]):
        errors.append("provider_evidence.independent_paths are not the channel contract")
    if evidence.get("run_nonce") != run_nonce:
        errors.append("provider_evidence.run_nonce does not match this run")
    account_variable = CHANNEL_ACCOUNT_VARIABLE[channel]
    expected_account_hash = credential_fingerprints.get(account_variable)
    if (
        not isinstance(expected_account_hash, str)
        or HEX64_RE.fullmatch(expected_account_hash) is None
    ):
        errors.append("configured account credential fingerprint is missing or invalid")
        expected_account_hash = ""
    if evidence.get("account_fingerprint") != expected_account_hash:
        errors.append("provider_evidence.account_fingerprint does not match the configured account")

    artifact_attestation, artifact_error = _validate_artifact_attestation(
        evidence.get("artifact_attestation")
    )
    if artifact_error is not None:
        errors.append(artifact_error)

    credential_attestation = response.get("credential_attestation")
    if not isinstance(credential_attestation, dict):
        errors.append("credential_attestation is missing")
    else:
        if credential_attestation.get("status") != "pass":
            errors.append("credential_attestation.status is not pass")
        if credential_attestation.get("run_nonce") != run_nonce:
            errors.append("credential_attestation.run_nonce does not match this run")
        observed_fingerprints = credential_attestation.get("fingerprints")
        if observed_fingerprints != credential_fingerprints:
            errors.append("credential_attestation.fingerprints do not match configured credentials")

    observations = evidence.get("observations")
    if not isinstance(observations, dict):
        errors.append("provider_evidence.observations is missing")
        observations = {}
    sanitized_observations: dict[str, Any] = {}
    provider_event_ids: set[str] = set()
    observed_now = (now or datetime.now(UTC)).astimezone(UTC)
    observed_after = observed_now + _TIMESTAMP_SKEW
    observed_before = (
        run_started_at.astimezone(UTC) - _TIMESTAMP_SKEW if run_started_at is not None else None
    )
    event_fields = (
        "callback_event_id",
        "outbound_request_id",
        "duplicate_event_id",
        "disconnect_event_id",
        "reconnect_event_id",
        "received_after_reconnect_event_id",
        "failed_endpoint_id",
        "replacement_endpoint_id",
        "received_after_failover_event_id",
        "lock_takeover_event_id",
        "retry_request_id",
        "old_credential_event_id",
        "new_credential_event_id",
        "post_rotation_event_id",
        "outage_event_id",
        "recovery_event_id",
        "failed_instance_id",
        "takeover_instance_id",
        "event_during_outage_id",
        "reply_for_event_id",
        "acknowledged_request_id",
        "original_event_id",
        "replayed_event_id",
        "ambiguous_event_id",
        "manual_review_id",
    )
    for case in REQUIRED_CASES:
        observation = observations.get(case)
        if not isinstance(observation, dict) or observation.get("status") != "pass":
            errors.append(f"provider_evidence.observations.{case} is not pass")
            continue
        if observation.get("run_nonce") != run_nonce:
            errors.append(f"provider_evidence.observations.{case}.run_nonce mismatch")
        required_fields = _required_observation_fields(
            case, channel=channel, observation=observation
        )
        for field in required_fields:
            if field not in observation:
                errors.append(f"provider_evidence.observations.{case}.{field} is missing")
        if case == "idempotency" and set(observation) != {
            "status",
            "run_nonce",
            *required_fields,
        }:
            errors.append("provider_evidence.observations.idempotency has an invalid schema")
        provider_id = _safe_identifier(observation.get("provider_event_id"))
        if provider_id is None:
            errors.append(f"provider_evidence.observations.{case}.provider_event_id is invalid")
        elif provider_id in provider_event_ids:
            errors.append(f"provider_evidence.observations.{case}.provider_event_id must be unique")
        else:
            provider_event_ids.add(provider_id)
        observed_at = _parse_timestamp(observation.get("observed_at"))
        if observed_at is None:
            errors.append(f"provider_evidence.observations.{case}.observed_at is invalid")
        elif observed_before is not None and observed_at < observed_before:
            errors.append(f"provider_evidence.observations.{case}.observed_at predates this run")
        elif observed_at > observed_after:
            errors.append(f"provider_evidence.observations.{case}.observed_at is from the future")
        if (
            "unique_inbound_id" in observation
            and _safe_identifier(observation.get("unique_inbound_id")) is None
        ):
            errors.append(f"provider_evidence.observations.{case}.unique_inbound_id is invalid")
        if case == "idempotency":
            if (
                not isinstance(observation.get("duplicate_count"), int)
                or isinstance(observation.get("duplicate_count"), bool)
                or observation["duplicate_count"] < 1
            ):
                errors.append(
                    f"provider_evidence.observations.{case}.duplicate_count must be positive"
                )
            if channel == "feishu":
                delivery_count = observation.get("provider_delivery_count")
                if (
                    not isinstance(delivery_count, int)
                    or isinstance(delivery_count, bool)
                    or delivery_count < 2
                ):
                    errors.append(
                        "provider_evidence.observations.idempotency."
                        "provider_delivery_count must be at least 2"
                    )
                duplicate_event = _safe_identifier(observation.get("duplicate_event_id"))
                original_event = _safe_identifier(observation.get("original_event_id"))
                if (
                    duplicate_event is not None
                    and original_event is not None
                    and duplicate_event != original_event
                ):
                    errors.append(
                        "provider_evidence.observations.idempotency.duplicate_event_id "
                        "must match original_event_id"
                    )
            else:
                if observation.get("duplicate_source") != WECOM_DUPLICATE_SOURCE:
                    errors.append(
                        "provider_evidence.observations.idempotency.duplicate_source is invalid"
                    )
                original_event = _safe_identifier(observation.get("original_event_id"))
                replayed_event = _safe_identifier(observation.get("replayed_event_id"))
                if (
                    original_event is not None
                    and replayed_event is not None
                    and original_event == replayed_event
                ):
                    errors.append(
                        "provider_evidence.observations.idempotency processing actions must differ"
                    )
        if case == "media":
            if not isinstance(observation.get("bytes"), int) or observation["bytes"] <= 0:
                errors.append(f"provider_evidence.observations.{case}.bytes must be positive")
        if case == "rate_limit_retry_after":
            provider_code = _provider_code_number(observation.get("provider_error_code"))
            if provider_code not in RATE_LIMIT_CODES[channel]:
                errors.append(
                    f"provider_evidence.observations.{case}.provider_error_code is not a "
                    f"{channel} rate-limit code"
                )
            retry_after = observation.get("retry_after_seconds")
            if not _finite_number(
                retry_after,
                minimum=0.001,
                maximum=MAX_RETRY_AFTER_SECONDS,
            ):
                errors.append(
                    f"provider_evidence.observations.{case}.retry_after_seconds is invalid"
                )
            retry_attempts = observation.get("retry_attempts")
            if (
                not isinstance(retry_attempts, int)
                or isinstance(retry_attempts, bool)
                or not 2 <= retry_attempts <= MAX_RETRY_ATTEMPTS
            ):
                errors.append(
                    f"provider_evidence.observations.{case}.retry_attempts must be between 2 "
                    f"and {MAX_RETRY_ATTEMPTS}"
                )
            retry_elapsed = observation.get("retry_elapsed_seconds")
            if not _finite_number(
                retry_elapsed,
                minimum=0.001,
                maximum=MAX_RETRY_AFTER_SECONDS,
            ):
                errors.append(
                    f"provider_evidence.observations.{case}.retry_elapsed_seconds is invalid"
                )
            elif (
                isinstance(retry_elapsed, (int, float))
                and not isinstance(retry_elapsed, bool)
                and isinstance(retry_after, (int, float))
                and not isinstance(retry_after, bool)
                and _finite_number(
                    retry_after,
                    minimum=0.001,
                    maximum=MAX_RETRY_AFTER_SECONDS,
                )
                and float(retry_elapsed) < float(retry_after) * 0.9
            ):
                errors.append(
                    f"provider_evidence.observations.{case}.retry_elapsed_seconds did not "
                    "honor Retry-After"
                )
        if case == "reconnect" and channel == "wecom":
            for field in ("old_lock_owner_released", "new_lock_owner_acquired"):
                if observation.get(field) is not True:
                    errors.append(f"provider_evidence.observations.{case}.{field} must be true")
            lock_epoch = observation.get("lock_epoch")
            if not isinstance(lock_epoch, int) or isinstance(lock_epoch, bool) or lock_epoch < 1:
                errors.append(f"provider_evidence.observations.{case}.lock_epoch is invalid")
        if channel == "wecom" and case in {"reconnect", "credential_rotation"}:
            outbound_request = _safe_identifier(observation.get("outbound_request_id"))
            acknowledged_request = _safe_identifier(observation.get("acknowledged_request_id"))
            if (
                outbound_request is not None
                and acknowledged_request is not None
                and acknowledged_request != outbound_request
            ):
                errors.append(
                    f"provider_evidence.observations.{case}.acknowledged_request_id must "
                    "match outbound_request_id"
                )
            if _provider_code_number(observation.get("provider_code")) not in {0, 200}:
                errors.append(
                    f"provider_evidence.observations.{case}.provider_code is not successful"
                )
        if case == "reconnect" and channel == "feishu":
            failed_endpoint = _safe_identifier(observation.get("failed_endpoint_id"))
            replacement_endpoint = _safe_identifier(observation.get("replacement_endpoint_id"))
            if failed_endpoint is None:
                errors.append(
                    "provider_evidence.observations.reconnect.failed_endpoint_id is invalid"
                )
            if replacement_endpoint is None:
                errors.append(
                    "provider_evidence.observations.reconnect.replacement_endpoint_id is invalid"
                )
            if (
                failed_endpoint is not None
                and replacement_endpoint is not None
                and failed_endpoint == replacement_endpoint
            ):
                errors.append(
                    "provider_evidence.observations.reconnect.failed_endpoint_id and "
                    "replacement_endpoint_id must differ"
                )
            if observation.get("endpoint_set_observed") is not True:
                errors.append(
                    "provider_evidence.observations.reconnect.endpoint_set_observed must be true"
                )
            outbound_request = _safe_identifier(observation.get("outbound_request_id"))
            acknowledged_request = _safe_identifier(observation.get("acknowledged_request_id"))
            if (
                outbound_request is not None
                and acknowledged_request is not None
                and outbound_request != acknowledged_request
            ):
                errors.append(
                    "provider_evidence.observations.reconnect.acknowledged_request_id must "
                    "match outbound_request_id"
                )
            ready_count = observation.get("ready_endpoint_count")
            if not isinstance(ready_count, int) or isinstance(ready_count, bool) or ready_count < 1:
                errors.append(
                    "provider_evidence.observations.reconnect.ready_endpoint_count must be positive"
                )
            for field in ("unready_endpoint_count", "terminating_endpoint_count"):
                value = observation.get(field)
                if not isinstance(value, int) or isinstance(value, bool) or value != 0:
                    errors.append(f"provider_evidence.observations.reconnect.{field} must be 0")
        if case == "credential_rotation" and observation.get("old_credential_rejected") is not True:
            errors.append(
                f"provider_evidence.observations.{case}.old_credential_rejected must be true"
            )
        if case == "prolonged_outage":
            outage_seconds = observation.get("outage_seconds")
            if not _finite_number(
                outage_seconds,
                minimum=MIN_PROLONGED_OUTAGE_SECONDS,
                maximum=MAX_OUTAGE_SECONDS,
            ):
                errors.append(
                    f"provider_evidence.observations.{case}.outage_seconds must be between "
                    f"{MIN_PROLONGED_OUTAGE_SECONDS} and {MAX_OUTAGE_SECONDS}"
                )
            if channel == "wecom":
                outage_mode = observation.get("outage_mode")
                if outage_mode not in WECOM_OUTAGE_MODES:
                    errors.append(f"provider_evidence.observations.{case}.outage_mode is invalid")
                elif outage_mode == "service_failover":
                    failure_instance = _safe_identifier(observation.get("failed_instance_id"))
                    takeover_instance = _safe_identifier(observation.get("takeover_instance_id"))
                    if failure_instance is None:
                        errors.append(
                            f"provider_evidence.observations.{case}.failed_instance_id is invalid"
                        )
                    if takeover_instance is None:
                        errors.append(
                            f"provider_evidence.observations.{case}.takeover_instance_id is invalid"
                        )
                    if (
                        failure_instance is not None
                        and takeover_instance is not None
                        and failure_instance == takeover_instance
                    ):
                        errors.append(
                            f"provider_evidence.observations.{case}.failed_instance_id and "
                            "takeover_instance_id must differ"
                        )
                    for field in ("old_lock_owner_released", "new_lock_owner_acquired"):
                        if observation.get(field) is not True:
                            errors.append(
                                f"provider_evidence.observations.{case}.{field} must be true"
                            )
                    connection_epoch = observation.get("connection_epoch")
                    if (
                        not isinstance(connection_epoch, int)
                        or isinstance(connection_epoch, bool)
                        or connection_epoch < 1
                    ):
                        errors.append(
                            f"provider_evidence.observations.{case}.connection_epoch is invalid"
                        )
                    outage_event = _safe_identifier(observation.get("event_during_outage_id"))
                    reply_event = _safe_identifier(observation.get("reply_for_event_id"))
                    outbound_request = _safe_identifier(observation.get("outbound_request_id"))
                    acknowledged_request = _safe_identifier(
                        observation.get("acknowledged_request_id")
                    )
                    for field, value in (
                        ("event_during_outage_id", outage_event),
                        ("reply_for_event_id", reply_event),
                        ("outbound_request_id", outbound_request),
                        ("acknowledged_request_id", acknowledged_request),
                    ):
                        if value is None:
                            errors.append(
                                f"provider_evidence.observations.{case}.{field} is invalid"
                            )
                    if (
                        outage_event is not None
                        and reply_event is not None
                        and reply_event != outage_event
                    ):
                        errors.append(
                            f"provider_evidence.observations.{case}.reply_for_event_id must "
                            "match event_during_outage_id"
                        )
                    if (
                        outbound_request is not None
                        and acknowledged_request is not None
                        and acknowledged_request != outbound_request
                    ):
                        errors.append(
                            f"provider_evidence.observations.{case}.acknowledged_request_id "
                            "must match outbound_request_id"
                        )
                    for field, expected in (
                        ("reply_count", 1),
                        ("ack_count", 1),
                        ("pending_count", 0),
                        ("dlq_count", 0),
                    ):
                        value = observation.get(field)
                        if (
                            not isinstance(value, int)
                            or isinstance(value, bool)
                            or value != expected
                        ):
                            errors.append(
                                f"provider_evidence.observations.{case}.{field} must be {expected}"
                            )
                else:
                    errors.append(
                        f"provider_evidence.observations.{case}.provider_delivery_gap is "
                        "not supported by contract v1; provider replay is unavailable"
                    )
        if case == "ambiguous":
            if observation.get("drop_response_observed") is not True:
                errors.append(
                    f"provider_evidence.observations.{case}.drop_response_observed must be true"
                )
            if observation.get("auto_replay_count") != 0:
                errors.append(
                    f"provider_evidence.observations.{case}.auto_replay_count must be zero"
                )
        for field in event_fields:
            if field in observation and _safe_identifier(observation.get(field)) is None:
                errors.append(f"provider_evidence.observations.{case}.{field} is invalid")
        for field in ("media_id_hash", "sha256"):
            if field in observation and not HEX64_RE.fullmatch(str(observation.get(field))):
                errors.append(f"provider_evidence.observations.{case}.{field} is invalid")
        for field in ("provider_code", "provider_error_code"):
            value = observation.get(field)
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (str, int))
                or not SAFE_CODE_RE.fullmatch(str(value))
            ):
                errors.append(f"provider_evidence.observations.{case}.{field} is invalid")
        sanitized: dict[str, Any] = {"status": "pass", "run_nonce": run_nonce}
        normalized_observed_at = _normalized_timestamp(observation.get("observed_at"))
        if normalized_observed_at is not None:
            sanitized["observed_at"] = normalized_observed_at
        if provider_id is not None:
            sanitized["provider_event_id_hash"] = _fingerprint(
                provider_id, label=f"{channel}:{case}"
            )
        correlation_labels: dict[str, str] = {}
        if case == "idempotency" and channel == "feishu":
            correlation_labels = {
                "duplicate_event_id": "provider_delivery_event",
                "original_event_id": "provider_delivery_event",
            }
        if case == "idempotency" and channel == "wecom":
            correlation_labels = {
                "original_event_id": "service_processing_action",
                "replayed_event_id": "service_processing_action",
            }
        if channel == "feishu" and case == "reconnect":
            correlation_labels = {
                "failed_endpoint_id": "endpoint",
                "replacement_endpoint_id": "endpoint",
                "outbound_request_id": "outbound_request",
                "acknowledged_request_id": "outbound_request",
            }
        if channel == "wecom" and case in {"reconnect", "credential_rotation"}:
            correlation_labels = {
                "outbound_request_id": "outbound_request",
                "acknowledged_request_id": "outbound_request",
            }
        for field in (*event_fields, "unique_inbound_id"):
            value = _safe_identifier(observation.get(field))
            if value is not None:
                semantic_label = correlation_labels.get(field, field)
                sanitized[field + "_hash"] = _fingerprint(
                    value, label=f"{channel}:{case}:{semantic_label}"
                )
        for field in ("provider_code", "provider_error_code"):
            value = observation.get(field)
            if isinstance(value, (str, int)) and not isinstance(value, bool):
                sanitized[field] = str(value)[:128]
        for field in ("retry_after_seconds", "outage_seconds"):
            value = observation.get(field)
            maximum = (
                MAX_RETRY_AFTER_SECONDS if field == "retry_after_seconds" else MAX_OUTAGE_SECONDS
            )
            if (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and _finite_number(value, minimum=0.0, maximum=maximum)
            ):
                sanitized[field] = float(value)
        if case == "rate_limit_retry_after":
            value = observation.get("retry_attempts")
            if (
                isinstance(value, int)
                and not isinstance(value, bool)
                and 2 <= value <= MAX_RETRY_ATTEMPTS
            ):
                sanitized["retry_attempts"] = value
            value = observation.get("retry_elapsed_seconds")
            if (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and _finite_number(value, minimum=0.0, maximum=MAX_RETRY_AFTER_SECONDS)
            ):
                sanitized["retry_elapsed_seconds"] = float(value)
        if case == "reconnect" and channel == "wecom":
            value = observation.get("lock_epoch")
            if isinstance(value, int) and not isinstance(value, bool) and value >= 1:
                sanitized["lock_epoch"] = value
            for field in ("old_lock_owner_released", "new_lock_owner_acquired"):
                if isinstance(observation.get(field), bool):
                    sanitized[field] = observation[field]
        if case == "reconnect" and channel == "feishu":
            if isinstance(observation.get("endpoint_set_observed"), bool):
                sanitized["endpoint_set_observed"] = observation["endpoint_set_observed"]
            for field in (
                "ready_endpoint_count",
                "unready_endpoint_count",
                "terminating_endpoint_count",
            ):
                value = observation.get(field)
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    sanitized[field] = value
        if channel == "wecom" and case == "prolonged_outage":
            mode = observation.get("outage_mode")
            if mode in WECOM_OUTAGE_MODES:
                sanitized["outage_mode"] = mode
            if mode == "service_failover":
                correlation_labels = {
                    "event_during_outage_id": "inbound_event",
                    "reply_for_event_id": "inbound_event",
                    "outbound_request_id": "outbound_request",
                    "acknowledged_request_id": "outbound_request",
                }
                for field in (
                    "failed_instance_id",
                    "takeover_instance_id",
                    *correlation_labels,
                ):
                    value = _safe_identifier(observation.get(field))
                    if value is not None:
                        semantic_label = correlation_labels.get(field, field)
                        sanitized[field + "_hash"] = _fingerprint(
                            value, label=f"{channel}:{case}:{semantic_label}"
                        )
                for field in ("old_lock_owner_released", "new_lock_owner_acquired"):
                    if isinstance(observation.get(field), bool):
                        sanitized[field] = observation[field]
                connection_epoch = observation.get("connection_epoch")
                if (
                    isinstance(connection_epoch, int)
                    and not isinstance(connection_epoch, bool)
                    and connection_epoch >= 1
                ):
                    sanitized["connection_epoch"] = connection_epoch
                for field in ("reply_count", "ack_count", "pending_count", "dlq_count"):
                    value = observation.get(field)
                    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                        sanitized[field] = value
        for field in (
            "duplicate_count",
            "provider_delivery_count",
            "bytes",
            "auto_replay_count",
        ):
            value = observation.get(field)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                sanitized[field] = value
        if (
            channel == "wecom"
            and case == "idempotency"
            and observation.get("duplicate_source") == WECOM_DUPLICATE_SOURCE
        ):
            sanitized["duplicate_source"] = WECOM_DUPLICATE_SOURCE
        if isinstance(observation.get("old_credential_rejected"), bool):
            sanitized["old_credential_rejected"] = observation["old_credential_rejected"]
        if isinstance(observation.get("drop_response_observed"), bool):
            sanitized["drop_response_observed"] = observation["drop_response_observed"]
        sanitized_observations[case] = sanitized

    if errors:
        return None, sorted(set(errors))
    sanitized_evidence: dict[str, Any] = {
        "source": PROVIDER_EVIDENCE_SOURCE[channel],
        "independent_paths": list(PROVIDER_EVIDENCE_PATHS[channel]),
        "run_nonce": run_nonce,
        "account_fingerprint": expected_account_hash,
        "observations": sanitized_observations,
        "credential_attestation": {
            "status": "pass",
            "run_nonce": run_nonce,
            "credential_count": len(credential_fingerprints),
        },
        "artifact_attestation": artifact_attestation,
    }
    if run_started_at is not None:
        normalized_run_started_at = _normalized_timestamp(run_started_at.isoformat())
        if normalized_run_started_at is not None:
            sanitized_evidence["run_started_at"] = normalized_run_started_at
    return sanitized_evidence, []


def _report(
    output: Path,
    *,
    gate: str,
    production_gate: str,
    reasons: list[str],
    candidate: dict[str, Any],
    run_id: str,
    expected_source_fingerprint: dict[str, Any] | None = None,
    expected_release_binding: dict[str, str] | None = None,
) -> dict[str, Any]:
    runtime_digest = candidate.get("runtime_image_digest")
    runtime: dict[str, Any] | None = None
    if isinstance(runtime_digest, str) and re.fullmatch(r"[0-9a-fA-F]{64}", runtime_digest):
        # The image digest is supplied by the deployed probe and is included
        # only as a hash-safe runtime lineage value, never as a credential.
        runtime = {
            "algorithm": "sha256",
            "status": "available",
            "value": runtime_digest.lower(),
        }
    evidence = build_evidence(
        root=ROOT,
        producer=PRODUCER,
        run_id=run_id,
        runtime=runtime,
    )
    if expected_source_fingerprint is not None or expected_release_binding is not None:
        lineage_reasons = validate_current_candidate_evidence(
            evidence,
            current_source=expected_source_fingerprint or source_fingerprint(ROOT),
            expected_release_binding=expected_release_binding,
            require_release_binding=expected_release_binding is not None,
        )
        if lineage_reasons:
            if gate == "pass":
                gate = "not_run"
            production_gate = "not_run"
            reasons.extend(lineage_reasons)
    result: dict[str, Any] = {
        "schema_version": 1,
        "baseline": {
            "channels": list(REQUIRED_CHANNELS),
            "cases": list(REQUIRED_CASES),
            "production_requires_real_credentials_and_round_trip": True,
            "production_requires_probe_https_and_image_digest_attestation": True,
            "rate_limit_requires_provider_code_and_retry_timing": True,
            "reconnect_requires_lock_takeover": True,
            "prolonged_outage_minimum_seconds": MIN_PROLONGED_OUTAGE_SECONDS,
            "ambiguous_requires_provider_drop_response": True,
        },
        "candidate": candidate,
        "case_deltas": {
            "failed_cases": [
                name
                for channel in candidate.get("channels", {}).values()
                if isinstance(channel, dict)
                for name, value in channel.get("cases", {}).items()
                if isinstance(value, dict) and value.get("status") != "pass"
            ]
        },
        "run_id": run_id,
        "evidence": evidence,
        "gate": gate,
        "production_gate": production_gate,
        "rejection_reasons": reasons,
        "production_rejection_reasons": reasons,
    }
    _assert_safe_output_path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    _assert_safe_output_path(output)
    atomic_write_json(output, result)
    return result


def _not_run(output: Path, reasons: list[str], *, run_id: str) -> dict[str, Any]:
    return _report(
        output,
        gate="not_run",
        production_gate="not_run",
        reasons=reasons,
        candidate={
            "mode": "online_channel_acceptance",
            "runtime_configured": False,
            "channels": {
                channel: {
                    "status": "not_run",
                    "cases": {
                        case: {"status": "not_run", "reason": "online acceptance not enabled"}
                        for case in REQUIRED_CASES
                    },
                }
                for channel in REQUIRED_CHANNELS
            },
        },
        run_id=run_id,
    )


def _run(output: Path, *, timeout: float, require_production: bool) -> int:
    run_id = new_run_id(PRODUCER)
    if not _enabled():
        result = _not_run(
            output,
            [
                "TRPC_IM_ONLINE_TESTS_ENABLED=true was not supplied; "
                "real Feishu/WeCom acceptance is opt-in"
            ],
            run_id=run_id,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
        return 1 if require_production else 0

    expected_release_binding: dict[str, str] | None = None
    try:
        expected_release_binding = current_release_binding(required=True)
    except ValueError as error:
        result = _not_run(output, [str(error)], run_id=run_id)
        print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
        return 1 if require_production else 0

    probe_url = os.getenv("TRPC_IM_ONLINE_PROBE_URL", "").strip()
    image_digest = os.getenv("TRPC_IM_ONLINE_IMAGE_DIGEST", "").strip()
    release_id = os.getenv(RELEASE_ID_ENV, "").strip()
    release_nonce = os.getenv(RELEASE_NONCE_ENV, "").strip()
    allowlist = _probe_allowlist()
    configured_identity, identity_hash, identity_source = _probe_identity()
    probe_trust, probe_trust_error = _load_probe_trust()
    missing = []
    credential_fingerprints: dict[str, dict[str, str]] = {}
    control_profile_hashes: dict[str, str] = {}
    for channel, variables in CHANNEL_CREDENTIALS.items():
        channel_fingerprints: dict[str, str] = {}
        for variable in variables:
            credential_value = os.getenv(variable, "").strip()
            if not credential_value or any(
                marker in credential_value.lower() for marker in PLACEHOLDER_MARKERS
            ):
                missing.append(variable)
            else:
                channel_fingerprints[variable] = _fingerprint(credential_value, label=variable)
        credential_fingerprints[channel] = channel_fingerprints
        control_env = CONTROL_PROFILE_HASH_ENV[channel]
        control_hash = os.getenv(control_env, "").strip().lower()
        if HEX64_RE.fullmatch(control_hash) is None or control_hash == "0" * 64:
            missing.append(f"{control_env} (nonzero 64 hex)")
        else:
            control_profile_hashes[channel] = control_hash
    if not probe_url:
        missing.append("TRPC_IM_ONLINE_PROBE_URL")
    if not re.fullmatch(r"sha256:[0-9a-fA-F]{64}", image_digest) or image_digest.lower() in {
        "sha256:" + "0" * 64,
        "sha256:" + "f" * 64,
    }:
        missing.append("TRPC_IM_ONLINE_IMAGE_DIGEST (sha256:<64 hex>)")
    if RELEASE_ID_RE.fullmatch(release_id) is None:
        missing.append(f"{RELEASE_ID_ENV} (current release binding)")
    if RELEASE_NONCE_RE.fullmatch(release_nonce) is None:
        missing.append(f"{RELEASE_NONCE_ENV} (current release binding)")
    canonical_probe_url = _canonical_probe_url(probe_url)
    if canonical_probe_url is None:
        missing.append(
            "TRPC_IM_ONLINE_PROBE_URL must be an HTTPS URL without userinfo, query, or fragment"
        )
    if _probe_allowlist_has_invalid_entry():
        missing.append(f"{PROBE_URL_ALLOWLIST_ENV} contains an invalid HTTPS endpoint")
    elif not allowlist:
        missing.append(f"{PROBE_URL_ALLOWLIST_ENV} must contain an explicit HTTPS endpoint")
    elif canonical_probe_url not in allowlist:
        missing.append("TRPC_IM_ONLINE_PROBE_URL is not in the explicit probe URL allowlist")
    if probe_trust_error is not None or probe_trust is None:
        missing.append(probe_trust_error or "source-bound probe trust is unavailable")
    elif canonical_probe_url != probe_trust["probe_url"]:
        missing.append("TRPC_IM_ONLINE_PROBE_URL does not match source-bound probe trust")
    if identity_hash is None:
        missing.append(
            f"{PROBE_IDENTITY_ENV} or {PROBE_IDENTITY_HASH_ENV} must specify a fixed probe identity"
        )
    if not _finite_number(timeout, minimum=0.001, maximum=300.0):
        missing.append("probe timeout must be finite and between 0.001 and 300 seconds")
    if missing:
        result = _not_run(
            output,
            ["online acceptance prerequisites are missing: " + ", ".join(sorted(set(missing)))],
            run_id=run_id,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
        return 1 if require_production else 0

    nonce = secrets.token_urlsafe(18)
    run_started_at = datetime.now(UTC)
    expected_source_fingerprint = source_fingerprint(ROOT)
    source_fingerprint_value = expected_source_fingerprint.get("value")
    if (
        expected_source_fingerprint.get("status") != "available"
        or not isinstance(source_fingerprint_value, str)
        or HEX64_RE.fullmatch(source_fingerprint_value) is None
    ):
        result = _not_run(
            output,
            ["current candidate source fingerprint is unavailable"],
            run_id=run_id,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
        return 1 if require_production else 0
    assert expected_release_binding is not None
    release_nonce_sha256 = expected_release_binding["nonce_sha256"]
    image_digest = image_digest.lower()
    assert identity_hash is not None  # guarded by the prerequisite branch above
    assert canonical_probe_url is not None
    assert probe_trust is not None
    candidate: dict[str, Any] = {
        "mode": "real_feishu_wecom_online",
        "runtime_configured": True,
        "runtime_image_digest": image_digest[7:].lower(),
        "channels": {},
        "probe": {
            "status": "not_run",
            "endpoint_configured": True,
            "endpoint_allowlisted": True,
            "identity_attestation": {
                "status": "not_run",
                "run_nonce": nonce,
                "identity_sha256": identity_hash,
                "identity_source": identity_source,
                "signature_verified": False,
            },
        },
    }
    reasons: list[str] = []
    attested_channels: list[str] = []
    signed_channels: list[str] = []
    for channel in REQUIRED_CHANNELS:
        channel_cases: dict[str, Any] = {}
        try:
            response = _safe_post(
                probe_url,
                {
                    "run_id": run_id,
                    "channel": channel,
                    "nonce": nonce,
                    "cases": list(REQUIRED_CASES),
                    "expected_image_digest": image_digest,
                    "release_id": release_id,
                    "release_nonce_sha256": release_nonce_sha256,
                    "source_fingerprint": source_fingerprint_value,
                    "credential_fingerprints": credential_fingerprints[channel],
                    "probe_identity_sha256": identity_hash,
                    "control_profile_sha256": control_profile_hashes[channel],
                    "account_fingerprint": _fingerprint(
                        os.getenv(CHANNEL_ACCOUNT_VARIABLE[channel], "").strip(),
                        label=CHANNEL_ACCOUNT_VARIABLE[channel],
                    ),
                },
                timeout,
            )
            _verify_probe_signature(response, probe_trust)
            signed_channels.append(channel)
            runtime_ok, runtime_error = _validate_probe_runtime(
                response.get("runtime"),
                run_nonce=nonce,
                image_digest=image_digest,
                configured_identity=configured_identity,
                identity_hash=identity_hash,
                control_profile_sha256=control_profile_hashes[channel],
                release_id=release_id,
                release_nonce_sha256=release_nonce_sha256,
                source_fingerprint_value=source_fingerprint_value,
            )
            if not runtime_ok:
                raise RuntimeError(runtime_error or "probe runtime attestation failed")
            attested_channels.append(channel)
            response_cases = response.get("cases")
            if not isinstance(response_cases, dict):
                raise RuntimeError("probe response cases are missing")
            for case in REQUIRED_CASES:
                value = response_cases.get(case)
                status = value.get("status") if isinstance(value, dict) else None
                channel_cases[case] = {"status": status if status == "pass" else "not_run"}
                if status != "pass":
                    reasons.append(f"{channel}.{case} did not return pass evidence")
            provider_evidence, evidence_errors = _validate_provider_evidence(
                channel,
                response,
                run_nonce=nonce,
                credential_fingerprints=credential_fingerprints[channel],
                run_started_at=run_started_at,
            )
            if evidence_errors:
                reasons.extend(f"{channel}: {reason}" for reason in evidence_errors)
            elif provider_evidence is not None and all(
                item.get("status") == "pass" for item in channel_cases.values()
            ):
                runtime_attestation = {
                    "status": "pass",
                    "run_nonce": nonce,
                    "image_digest": image_digest,
                    "release_id": release_id,
                    "release_nonce_sha256": release_nonce_sha256,
                    "source_fingerprint": source_fingerprint_value,
                }
                candidate["channels"][channel] = {
                    "status": "pass",
                    "control_profile_sha256": control_profile_hashes[channel],
                    "cases": channel_cases,
                    "provider_evidence": provider_evidence,
                    "runtime_attestation": runtime_attestation,
                    "artifact_attestation": provider_evidence["artifact_attestation"],
                    "signature_response": {
                        "algorithm": "sha256",
                        "response_sha256": _normalized_probe_response_sha256(
                            channel, response, provider_evidence
                        ),
                    },
                }
                candidate["channels"][channel]["signature_response"]["binding_sha256"] = (
                    _probe_response_digest_binding(
                        channel=channel,
                        run_id=run_id,
                        run_nonce=nonce,
                        response_sha256=candidate["channels"][channel]["signature_response"][
                            "response_sha256"
                        ],
                        control_profile_sha256=control_profile_hashes[channel],
                        trust=probe_trust,
                    )
                )
        except Exception as error:
            channel_cases = {case: {"status": "not_run"} for case in REQUIRED_CASES}
            reasons.append(f"{channel} online probe failed: {type(error).__name__}")
        if channel not in candidate["channels"]:
            candidate["channels"][channel] = {
                "status": "not_run",
                "control_profile_sha256": control_profile_hashes[channel],
                "cases": channel_cases,
            }

    if (
        len(attested_channels) == len(REQUIRED_CHANNELS)
        and len(signed_channels) == len(REQUIRED_CHANNELS)
        and not reasons
    ):
        candidate["probe"] = {
            "status": "pass",
            "endpoint_configured": True,
            "endpoint_allowlisted": True,
            "identity_attestation": {
                "status": "pass",
                "run_nonce": nonce,
                "identity_sha256": identity_hash,
                "identity_source": identity_source,
                "channels": list(REQUIRED_CHANNELS),
                "signature_verified": True,
                "signed_channels": list(REQUIRED_CHANNELS),
                "signature_algorithm": "ed25519",
                "trust_key_id": probe_trust["key_id"],
                "trust_probe_url": probe_trust["probe_url"],
                "trust_key_sha256": probe_trust["key_sha256"],
                "trust_config_sha256": probe_trust["config_sha256"],
                "trust_file_sha256": probe_trust["file_sha256"],
            },
        }

    passed = not reasons and all(
        channel.get("status") == "pass"
        for channel in candidate["channels"].values()
        if isinstance(channel, dict)
    )
    gate = "pass" if passed else "not_run"
    result = _report(
        output,
        gate=gate,
        production_gate=gate,
        reasons=reasons,
        candidate=candidate,
        run_id=run_id,
        expected_source_fingerprint=expected_source_fingerprint,
        expected_release_binding=expected_release_binding,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
    return 0 if passed or not require_production else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("runs/multitenant/im-online.json"))
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--require-production", action="store_true")
    args = parser.parse_args()
    if not _finite_number(args.timeout_seconds, minimum=0.001, maximum=300.0):
        parser.error("--timeout-seconds must be finite and between 0.001 and 300 seconds")
    return _run(
        args.output,
        timeout=args.timeout_seconds,
        require_production=args.require_production,
    )


if __name__ == "__main__":
    raise SystemExit(main())
