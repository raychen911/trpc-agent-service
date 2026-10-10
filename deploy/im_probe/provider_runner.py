#!/usr/bin/python3
"""Fail-closed orchestration boundary for an independent IM evidence driver.

This file is intentionally self-contained so it can be installed outside the
application checkout.  It does not call a provider and it never manufactures
an observation.  A provider-specific, independently reviewed executable is
selected from the request channel and is given only that channel's account ID
and secret-file paths.  The executable must return the complete, strict
observation contract; this process only adds the channel-bound evidence
envelope consumed by ``deploy.im_probe.server``.

The driver protocol is deliberately small:

* stdin: ``schema_version``, ``channel``, ``run_id``, ``run_nonce``,
  ``expected_image_digest``, ``control_profile_sha256`` and the ordered
  required ``cases`` list;
* environment: current-channel account ID, secret-file paths, control-profile
  path and the local control-socket path only;
* stdout: exactly ``{"schema_version": 1, "observations": {...}}``.

The runner itself has no application imports, no ``PYTHONPATH`` handling and
no provider SDK.  ``--check`` validates configured paths and returns only a
content-free status object; it never invokes a driver or emits evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO, cast

MAX_INPUT_BYTES = 64 * 1024
MAX_DRIVER_OUTPUT_BYTES = 256 * 1024
MAX_CONTROL_PROFILE_BYTES = 64 * 1024
DEFAULT_DRIVER_TIMEOUT_SECONDS = 180.0
MAX_DRIVER_TIMEOUT_SECONDS = 15 * 60.0
TIMESTAMP_SKEW_SECONDS = 5.0
MIN_PROLONGED_OUTAGE_SECONDS = 60.0
MAX_PROLONGED_OUTAGE_SECONDS = 7 * 24 * 60 * 60.0
MAX_RETRY_AFTER_SECONDS = 3600.0
MAX_RETRY_ATTEMPTS = 100

CHANNELS = ("feishu", "wecom")
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
DRIVER_RESULT_FIELDS = frozenset({"schema_version", "observations"})

DRIVER_ENV_BY_CHANNEL = {
    "feishu": "TRPC_IM_PROBE_FEISHU_DRIVER",
    "wecom": "TRPC_IM_PROBE_WECOM_DRIVER",
}
ACCOUNT_ENV_BY_CHANNEL = {
    "feishu": "TRPC_IM_PROBE_FEISHU_APP_ID",
    "wecom": "TRPC_IM_PROBE_WECOM_BOT_ID",
}
CONTROL_PROFILE_ENV_BY_CHANNEL = {
    "feishu": "TRPC_IM_PROBE_FEISHU_CONTROL_PROFILE_FILE",
    "wecom": "TRPC_IM_PROBE_WECOM_CONTROL_PROFILE_FILE",
}
CONTROL_SOCKET_ENV = "TRPC_IM_PROBE_CONTROL_SOCKET"
RUNNER_SHA256_ENV = "TRPC_IM_PROBE_RUNNER_SHA256"
BROKER_UID_ENV = "TRPC_IM_PROBE_BROKER_UID"
BROKER_GID_ENV = "TRPC_IM_PROBE_BROKER_GID"
ARTIFACT_CONTRACT_VERSION = 1
CONTROL_SOCKET_MODE = 0o660
CONTROL_SOCKET_PARENT_MODE = 0o750
ACCOUNT_LABEL_BY_CHANNEL = {
    "feishu": "FEISHU_APP_ID",
    "wecom": "WECOM_BOT_ID",
}
SOURCE_BY_CHANNEL = {
    "feishu": "feishu_api_and_webhook",
    "wecom": "wecom_ws_and_send_ack",
}
PATHS_BY_CHANNEL = {
    "feishu": ("provider_callback", "provider_send_ack"),
    "wecom": ("provider_ws_event", "provider_send_ack"),
}
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
REQUIRED_SECRET_ENV_BY_CHANNEL = {
    "feishu": (
        "TRPC_IM_PROBE_FEISHU_APP_SECRET_FILE",
        "TRPC_IM_PROBE_FEISHU_VERIFICATION_TOKEN_FILE",
        "TRPC_IM_PROBE_FEISHU_ENCRYPT_KEY_FILE",
    ),
    "wecom": ("TRPC_IM_PROBE_WECOM_BOT_SECRET_FILE",),
}
OPTIONAL_SECRET_ENV_BY_CHANNEL = {
    "feishu": (
        "TRPC_IM_PROBE_FEISHU_OLD_APP_SECRET_FILE",
        "TRPC_IM_PROBE_FEISHU_NEW_APP_SECRET_FILE",
    ),
    "wecom": (
        "TRPC_IM_PROBE_WECOM_OLD_BOT_SECRET_FILE",
        "TRPC_IM_PROBE_WECOM_NEW_BOT_SECRET_FILE",
    ),
}
RATE_LIMIT_CODES = {
    "feishu": frozenset({429, 99991400, 99991401, 99991402, 99991672}),
    "wecom": frozenset({429, 45009, 45011}),
}

SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")
IMAGE_RE = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{16,256}$")
RELEASE_PLACEHOLDERS = ("replace-with", "change-me", "placeholder", "synthetic")


class RunnerError(RuntimeError):
    """A fail-closed configuration, protocol or driver failure."""


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value} is forbidden")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r} is forbidden")
        result[key] = value
    return result


def _strict_json(raw: str | bytes) -> Any:
    return json.loads(
        raw,
        parse_constant=_reject_constant,
        object_pairs_hook=_reject_duplicate_keys,
    )


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _safe_id(value: object, *, label: str, pattern: re.Pattern[str] = SAFE_ID_RE) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise RunnerError(f"{label} is invalid")
    if any(marker in value.lower() for marker in RELEASE_PLACEHOLDERS):
        raise RunnerError(f"{label} contains a placeholder")
    return value


def _safe_digest(value: object) -> str:
    if (
        not isinstance(value, str)
        or IMAGE_RE.fullmatch(value) is None
        or value.lower() in {"sha256:" + "0" * 64, "sha256:" + "f" * 64}
    ):
        raise RunnerError("expected_image_digest is invalid")
    return value.lower()


def _safe_hash(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or HEX64_RE.fullmatch(value) is None
        or value.lower() in {"0" * 64, "f" * 64}
    ):
        raise RunnerError(f"{label} is invalid")
    return value.lower()


def _parse_request(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != REQUEST_FIELDS:
        raise RunnerError("request schema is invalid")
    if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        raise RunnerError("request schema_version is invalid")
    channel = value.get("channel")
    if channel not in CHANNELS:
        raise RunnerError("request channel is invalid")
    run_id = _safe_id(value.get("run_id"), label="run_id")
    run_nonce = _safe_id(value.get("run_nonce"), label="run_nonce", pattern=NONCE_RE)
    digest = _safe_digest(value.get("expected_image_digest"))
    release_id = _safe_id(value.get("release_id"), label="release_id")
    release_nonce_sha256 = _safe_hash(
        value.get("release_nonce_sha256"), label="release_nonce_sha256"
    )
    deployed_source_fingerprint = _safe_hash(
        value.get("source_fingerprint"), label="source_fingerprint"
    )
    control_profile_sha256 = _safe_hash(
        value.get("control_profile_sha256"),
        label="control_profile_sha256",
    )
    cases = value.get("cases")
    if (
        not isinstance(cases, list)
        or any(type(case) is not str for case in cases)
        or cases != list(REQUIRED_CASES)
    ):
        raise RunnerError("request cases are not the required ordered contract")
    return {
        "schema_version": 1,
        "channel": cast(str, channel),
        "run_id": run_id,
        "run_nonce": run_nonce,
        "expected_image_digest": digest,
        "release_id": release_id,
        "release_nonce_sha256": release_nonce_sha256,
        "source_fingerprint": deployed_source_fingerprint,
        "control_profile_sha256": control_profile_sha256,
        "cases": list(REQUIRED_CASES),
    }


def _no_symlink_path(raw: str, *, label: str, directory: bool = False) -> Path:
    path = Path(raw)
    if (
        not path.is_absolute()
        or path.is_symlink()
        or any(parent.is_symlink() for parent in path.parents)
    ):
        raise RunnerError(f"{label} must be an absolute non-symlink path")
    try:
        resolved = path.resolve(strict=True)
        metadata = resolved.stat()
    except (OSError, RuntimeError) as error:
        raise RunnerError(f"{label} is unavailable") from error
    mode = metadata.st_mode
    if directory:
        if not stat.S_ISDIR(mode):
            raise RunnerError(f"{label} must be a directory")
    elif not stat.S_ISREG(mode):
        raise RunnerError(f"{label} must be a regular file")
    return resolved


def _safe_file_path(raw: str, *, label: str) -> Path:
    path = _no_symlink_path(raw, label=label)
    try:
        mode = path.stat().st_mode
    except OSError as error:
        raise RunnerError(f"{label} is unavailable") from error
    # POSIX mode bits are authoritative on the Linux probe host.  Windows
    # uses ACLs rather than these compatibility bits; the deployment check on
    # that host must enforce the equivalent ACL policy separately.
    if os.name != "nt" and mode & 0o022:
        raise RunnerError(f"{label} must not be group/other writable")
    return path


def _trusted_artifact_sha256(path: Path, *, label: str) -> str:
    if os.name != "nt":
        current = path.parent
        while True:
            try:
                metadata = current.lstat()
            except OSError as error:
                raise RunnerError(f"{label} parent is unavailable") from error
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                raise RunnerError(f"{label} parent is not a trusted directory")
            if metadata.st_mode & 0o022 or (metadata.st_uid != 0 and metadata.st_mode & 0o200):
                raise RunnerError(f"{label} parent is writable by an untrusted user")
            parent = current.parent
            if parent == current:
                break
            current = parent
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise RunnerError(f"{label} is unavailable") from error
    digest = hashlib.sha256()
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise RunnerError(f"{label} must be a regular file")
        if os.name != "nt":
            if metadata.st_uid != 0:
                raise RunnerError(f"{label} must be root-owned")
            if metadata.st_mode & 0o022:
                raise RunnerError(f"{label} must not be group- or other-writable")
            if not metadata.st_mode & 0o111:
                raise RunnerError(f"{label} must be executable")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            while chunk := stream.read(64 * 1024):
                digest.update(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _numeric_environment(variable: str) -> int:
    try:
        value = int(os.environ.get(variable, ""))
    except ValueError as error:
        raise RunnerError(f"{variable} is invalid") from error
    if value < 0:
        raise RunnerError(f"{variable} is invalid")
    return value


def _validated_control_profile_path(
    raw: str,
    *,
    label: str,
    expected_sha256: str | None = None,
) -> Path:
    path = _safe_file_path(raw, label=label)
    try:
        mode = path.stat().st_mode
        with path.open("rb") as profile:
            contents = profile.read(MAX_CONTROL_PROFILE_BYTES + 1)
    except OSError as error:
        raise RunnerError(f"{label} is unavailable") from error
    if os.name != "nt" and mode & 0o027:
        raise RunnerError(f"{label} must not be readable by other users")
    if not contents:
        raise RunnerError(f"{label} is empty")
    if len(contents) > MAX_CONTROL_PROFILE_BYTES:
        raise RunnerError(f"{label} is too large")
    observed_sha256 = hashlib.sha256(contents).hexdigest()
    if expected_sha256 is not None and observed_sha256 != expected_sha256:
        raise RunnerError("control profile hash does not match the request")
    return path


def _control_profile_path(channel: str, expected_sha256: str | None = None) -> Path:
    variable = CONTROL_PROFILE_ENV_BY_CHANNEL[channel]
    raw = os.environ.get(variable, "").strip()
    if not raw:
        raise RunnerError(f"{variable} is not configured")
    return _validated_control_profile_path(
        raw,
        label=variable,
        expected_sha256=expected_sha256,
    )


def _control_socket_path() -> Path:
    raw = os.environ.get(CONTROL_SOCKET_ENV, "").strip()
    if not raw:
        raise RunnerError(f"{CONTROL_SOCKET_ENV} is not configured")
    path = Path(raw)
    if (
        not path.is_absolute()
        or path.is_symlink()
        or any(parent.is_symlink() for parent in path.parents)
    ):
        raise RunnerError(f"{CONTROL_SOCKET_ENV} must be an absolute non-symlink path")
    try:
        resolved = path.resolve(strict=True)
        metadata = resolved.stat()
        mode = metadata.st_mode
    except (OSError, RuntimeError) as error:
        raise RunnerError(f"{CONTROL_SOCKET_ENV} is unavailable") from error
    if os.name != "nt" and not stat.S_ISSOCK(mode):
        raise RunnerError(f"{CONTROL_SOCKET_ENV} must be a socket")
    if os.name == "nt" and stat.S_ISDIR(mode):
        raise RunnerError(f"{CONTROL_SOCKET_ENV} must not be a directory")
    if os.name != "nt":
        expected_uid = _numeric_environment(BROKER_UID_ENV)
        expected_gid = _numeric_environment(BROKER_GID_ENV)
        current_euid = getattr(os, "geteuid", None)
        if current_euid is None or expected_uid == current_euid():
            raise RunnerError("control broker must use a dedicated uid")
        try:
            parent = resolved.parent.lstat()
        except OSError as error:
            raise RunnerError(f"{CONTROL_SOCKET_ENV} parent is unavailable") from error
        if (
            stat.S_ISLNK(parent.st_mode)
            or not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != expected_uid
            or parent.st_gid != expected_gid
            or stat.S_IMODE(parent.st_mode) != CONTROL_SOCKET_PARENT_MODE
            or metadata.st_uid != expected_uid
            or metadata.st_gid != expected_gid
            or stat.S_IMODE(mode) != CONTROL_SOCKET_MODE
        ):
            raise RunnerError(f"{CONTROL_SOCKET_ENV} owner or mode is invalid")
    return resolved


def _application_root() -> Path | None:
    configured = os.environ.get("TRPC_IM_PROBE_APPLICATION_ROOT", "").strip()
    if configured:
        return _no_symlink_path(
            configured,
            label="TRPC_IM_PROBE_APPLICATION_ROOT",
            directory=True,
        )

    # When running from source, reject drivers inside the checkout.  An
    # installed copy under /usr/local/libexec has no .git ancestor and thus
    # does not accidentally treat / as the application checkout.
    source = Path(__file__).resolve()
    for parent in source.parents:
        if (parent / ".git").exists():
            return parent
    return None


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _driver_path(channel: str) -> Path:
    variable = DRIVER_ENV_BY_CHANNEL[channel]
    raw = os.environ.get(variable, "").strip()
    if not raw:
        raise RunnerError(f"{variable} is not configured")
    driver = _no_symlink_path(raw, label=variable)
    root = _application_root()
    if root is not None and _is_within(driver, root):
        raise RunnerError("provider driver must be outside the application checkout")
    hash_variable = f"{variable}_SHA256"
    expected_hash = _safe_hash(os.environ.get(hash_variable), label=hash_variable)
    if _trusted_artifact_sha256(driver, label=variable) != expected_hash:
        raise RunnerError(f"{variable} hash does not match")
    try:
        executable = os.access(driver, os.X_OK)
    except OSError as error:
        raise RunnerError(f"{variable} is unavailable") from error
    if not executable:
        raise RunnerError(f"{variable} must be executable")
    return driver


def _account_id(channel: str) -> tuple[str, str]:
    variable = ACCOUNT_ENV_BY_CHANNEL[channel]
    value = os.environ.get(variable, "").strip()
    if not value or SAFE_ID_RE.fullmatch(value) is None:
        raise RunnerError(f"{variable} is invalid")
    if any(marker in value.lower() for marker in RELEASE_PLACEHOLDERS):
        raise RunnerError(f"{variable} contains a placeholder")
    return variable, value


def _secret_paths(channel: str) -> dict[str, Path]:
    result: dict[str, Path] = {}
    names = (*REQUIRED_SECRET_ENV_BY_CHANNEL[channel], *OPTIONAL_SECRET_ENV_BY_CHANNEL[channel])
    required = set(REQUIRED_SECRET_ENV_BY_CHANNEL[channel])
    for variable in names:
        raw = os.environ.get(variable, "").strip()
        if not raw:
            if variable in required:
                raise RunnerError(f"{variable} is not configured")
            continue
        result[variable] = _safe_file_path(raw, label=variable)
    return result


def _validate_channel_configuration(
    channel: str,
    expected_control_profile_sha256: str | None = None,
) -> tuple[Path, str, dict[str, Path], Path, Path]:
    driver = _driver_path(channel)
    _account_variable, account_id = _account_id(channel)
    paths = _secret_paths(channel)
    control_profile = _control_profile_path(channel, expected_control_profile_sha256)
    control_socket = _control_socket_path()
    return driver, account_id, paths, control_profile, control_socket


def _driver_environment(
    channel: str,
    account_variable: str,
    account_id: str,
    paths: Mapping[str, Path],
    control_profile: Path,
    control_socket: Path,
) -> dict[str, str]:
    # This is intentionally a fresh allowlist, not a copy of os.environ.
    # Metadata travels over stdin; no channel, run ID, digest, PYTHONPATH,
    # runner path, or other-channel value reaches the driver environment.
    expected_account_variable = ACCOUNT_ENV_BY_CHANNEL[channel]
    if account_variable != expected_account_variable:
        raise RunnerError("driver account environment is not channel-bound")
    prefix = f"TRPC_IM_PROBE_{channel.upper()}_"
    allowed_paths = set(
        REQUIRED_SECRET_ENV_BY_CHANNEL[channel] + OPTIONAL_SECRET_ENV_BY_CHANNEL[channel]
    )
    if any(
        variable not in allowed_paths
        or not variable.startswith(prefix)
        or not variable.endswith("_FILE")
        for variable in paths
    ):
        raise RunnerError("driver secret paths are not channel-bound")
    environment = {account_variable: account_id}
    for variable, path in paths.items():
        environment[variable] = str(path)
    environment[CONTROL_PROFILE_ENV_BY_CHANNEL[channel]] = str(control_profile)
    environment[CONTROL_SOCKET_ENV] = str(control_socket)
    environment[BROKER_UID_ENV] = str(_numeric_environment(BROKER_UID_ENV))
    environment[BROKER_GID_ENV] = str(_numeric_environment(BROKER_GID_ENV))
    return environment


def _driver_timeout() -> float:
    raw = os.environ.get(
        "TRPC_IM_PROBE_DRIVER_TIMEOUT_SECONDS",
        str(DEFAULT_DRIVER_TIMEOUT_SECONDS),
    ).strip()
    try:
        timeout = float(raw)
    except ValueError as error:
        raise RunnerError("TRPC_IM_PROBE_DRIVER_TIMEOUT_SECONDS is invalid") from error
    if not math.isfinite(timeout) or not 0.001 <= timeout <= MAX_DRIVER_TIMEOUT_SECONDS:
        raise RunnerError("TRPC_IM_PROBE_DRIVER_TIMEOUT_SECONDS is outside the safe range")
    return timeout


def _driver_input(request: Mapping[str, Any]) -> bytes:
    return _canonical_json(
        {
            "schema_version": 1,
            "channel": request["channel"],
            "run_id": request["run_id"],
            "run_nonce": request["run_nonce"],
            "expected_image_digest": request["expected_image_digest"],
            "release_id": request["release_id"],
            "release_nonce_sha256": request["release_nonce_sha256"],
            "source_fingerprint": request["source_fingerprint"],
            "control_profile_sha256": request["control_profile_sha256"],
            "cases": list(REQUIRED_CASES),
        }
    )


def _invoke_driver(
    driver: Path,
    request: Mapping[str, Any],
    environment: Mapping[str, str],
) -> dict[str, Any]:
    channel = cast(str, request["channel"])
    variable = DRIVER_ENV_BY_CHANNEL[channel]
    hash_variable = f"{variable}_SHA256"
    expected_hash = _safe_hash(os.environ.get(hash_variable), label=hash_variable)
    if _trusted_artifact_sha256(driver, label=variable) != expected_hash:
        raise RunnerError("provider evidence driver hash changed before execution")
    try:
        with tempfile.TemporaryFile(mode="w+b") as driver_output:
            process = subprocess.Popen(  # noqa: S603 - absolute, validated executable; no shell
                [str(driver)],
                stdin=subprocess.PIPE,
                stdout=driver_output,
                stderr=subprocess.DEVNULL,
                cwd=str(driver.parent),
                env=dict(environment),
            )
            try:
                process.communicate(input=_driver_input(request), timeout=_driver_timeout())
            except subprocess.TimeoutExpired as error:
                process.kill()
                process.communicate()
                raise RunnerError("provider evidence driver failed or timed out") from error
            driver_output.seek(0)
            raw = driver_output.read(MAX_DRIVER_OUTPUT_BYTES + 1)
    except OSError as error:
        raise RunnerError("provider evidence driver failed or timed out") from error
    if process.returncode != 0:
        raise RunnerError("provider evidence driver exited non-zero")
    if len(raw) > MAX_DRIVER_OUTPUT_BYTES:
        raise RunnerError("provider evidence driver output is too large")
    try:
        parsed = _strict_json(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise RunnerError("provider evidence driver output is not strict JSON") from error
    if not isinstance(parsed, dict):
        raise RunnerError("provider evidence driver output is not an object")
    return parsed


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


def _safe_identifier(value: object) -> bool:
    return (
        isinstance(value, str)
        and SAFE_ID_RE.fullmatch(value) is not None
        and not any(marker in value.lower() for marker in RELEASE_PLACEHOLDERS)
    )


def _finite_number(value: object, *, minimum: float, maximum: float) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    numeric = float(value)
    return math.isfinite(numeric) and minimum <= numeric <= maximum


def _provider_code_number(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
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
    specific = {
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


def _validate_observation(case: str, observation: object, *, channel: str, run_nonce: str) -> None:
    if not isinstance(observation, dict):
        raise RunnerError(f"observation {case} is not an object")
    required = _required_observation_fields(case, channel=channel, observation=observation)
    if set(observation) != {"status", "run_nonce", *required}:
        raise RunnerError(f"observation {case} has an invalid schema")
    if observation.get("status") != "pass" or observation.get("run_nonce") != run_nonce:
        raise RunnerError(f"observation {case} is not bound to this run")
    if not _safe_identifier(observation.get("provider_event_id")):
        raise RunnerError(f"observation {case}.provider_event_id is invalid")
    observed_at = _parse_timestamp(observation.get("observed_at"))
    if observed_at is None:
        raise RunnerError(f"observation {case}.observed_at is invalid")
    # The probe gate permits a small clock skew.  Reject clearly future
    # evidence here while leaving the final timestamp check to the server.
    if (observed_at - datetime.now(UTC)).total_seconds() > TIMESTAMP_SKEW_SECONDS:
        raise RunnerError(f"observation {case}.observed_at is from the future")

    id_fields = {
        field
        for field in required
        if field.endswith("_event_id")
        or field.endswith("_request_id")
        or field.endswith("_review_id")
        or field.endswith("_endpoint_id")
    }
    id_fields.update(
        {
            field
            for field in (
                "failed_instance_id",
                "takeover_instance_id",
                "event_during_outage_id",
                "reply_for_event_id",
                "outbound_request_id",
                "acknowledged_request_id",
                "original_event_id",
                "replayed_event_id",
            )
            if field in required
        }
    )
    if "unique_inbound_id" in observation:
        id_fields.add("unique_inbound_id")
    for field in id_fields:
        if not _safe_identifier(observation.get(field)):
            raise RunnerError(f"observation {case}.{field} is invalid")

    for field in ("media_id_hash", "sha256"):
        if field in observation and (
            not isinstance(observation[field], str)
            or HEX64_RE.fullmatch(observation[field]) is None
        ):
            raise RunnerError(f"observation {case}.{field} is invalid")
    provider_code = observation.get("provider_code")
    if "provider_code" in required and (
        provider_code is None
        or isinstance(provider_code, bool)
        or not isinstance(provider_code, (str, int))
        or SAFE_CODE_RE.fullmatch(str(provider_code)) is None
    ):
        raise RunnerError(f"observation {case}.provider_code is invalid")
    error_code = observation.get("provider_error_code")
    if error_code is not None and (
        isinstance(error_code, bool)
        or not isinstance(error_code, (str, int))
        or SAFE_CODE_RE.fullmatch(str(error_code)) is None
    ):
        raise RunnerError(f"observation {case}.provider_error_code is invalid")

    if case == "idempotency":
        if (
            isinstance(observation["duplicate_count"], bool)
            or not isinstance(observation["duplicate_count"], int)
            or observation["duplicate_count"] < 1
        ):
            raise RunnerError("observation idempotency.duplicate_count is invalid")
        if channel == "feishu":
            delivery_count = observation["provider_delivery_count"]
            if (
                isinstance(delivery_count, bool)
                or not isinstance(delivery_count, int)
                or delivery_count < 2
            ):
                raise RunnerError("observation idempotency.provider_delivery_count is invalid")
            if observation["duplicate_event_id"] != observation["original_event_id"]:
                raise RunnerError(
                    "observation idempotency.duplicate_event_id must match original_event_id"
                )
        else:
            if observation["duplicate_source"] != WECOM_DUPLICATE_SOURCE:
                raise RunnerError("observation idempotency.duplicate_source is invalid")
            if observation["original_event_id"] == observation["replayed_event_id"]:
                raise RunnerError("observation idempotency processing actions must differ")
    if case == "media" and (
        isinstance(observation["bytes"], bool)
        or not isinstance(observation["bytes"], int)
        or observation["bytes"] <= 0
    ):
        raise RunnerError("observation media.bytes is invalid")
    if case == "reconnect":
        if channel == "feishu":
            if observation["failed_endpoint_id"] == observation["replacement_endpoint_id"]:
                raise RunnerError("observation reconnect endpoints must differ")
            if observation["endpoint_set_observed"] is not True:
                raise RunnerError("observation reconnect endpoint set was not observed")
            ready_count = observation["ready_endpoint_count"]
            if isinstance(ready_count, bool) or not isinstance(ready_count, int) or ready_count < 1:
                raise RunnerError("observation reconnect.ready_endpoint_count must be positive")
            for field in ("unready_endpoint_count", "terminating_endpoint_count"):
                value = observation[field]
                if isinstance(value, bool) or not isinstance(value, int) or value != 0:
                    raise RunnerError(f"observation reconnect.{field} must be 0")
            if observation["acknowledged_request_id"] != observation["outbound_request_id"]:
                raise RunnerError(
                    "observation reconnect acknowledgement must match the outbound request"
                )
        else:
            if (
                type(observation["old_lock_owner_released"]) is not bool
                or observation["new_lock_owner_acquired"] is not True
            ):
                raise RunnerError("observation reconnect lock ownership is invalid")
            if (
                isinstance(observation["lock_epoch"], bool)
                or not isinstance(observation["lock_epoch"], int)
                or observation["lock_epoch"] < 2
            ):
                raise RunnerError("observation reconnect.lock_epoch is invalid")
    if case == "rate_limit_retry_after":
        code = _provider_code_number(error_code)
        if code not in RATE_LIMIT_CODES[channel]:
            raise RunnerError("observation rate_limit_retry_after provider code is invalid")
        retry_after = observation["retry_after_seconds"]
        retry_elapsed = observation["retry_elapsed_seconds"]
        if not _finite_number(retry_after, minimum=0.001, maximum=MAX_RETRY_AFTER_SECONDS):
            raise RunnerError("observation rate_limit_retry_after retry_after is invalid")
        if (
            isinstance(observation["retry_attempts"], bool)
            or not isinstance(observation["retry_attempts"], int)
            or not 2 <= observation["retry_attempts"] <= MAX_RETRY_ATTEMPTS
        ):
            raise RunnerError("observation rate_limit_retry_after retry_attempts is invalid")
        if not _finite_number(retry_elapsed, minimum=0.001, maximum=MAX_RETRY_AFTER_SECONDS):
            raise RunnerError("observation rate_limit_retry_after elapsed time is invalid")
        if float(retry_elapsed) < float(retry_after) * 0.9:
            raise RunnerError("observation rate_limit_retry_after did not honor Retry-After")
    if case == "credential_rotation" and observation["old_credential_rejected"] is not True:
        raise RunnerError("observation credential_rotation is invalid")
    if channel == "wecom" and case in {"reconnect", "credential_rotation"}:
        code = _provider_code_number(observation["provider_code"])
        if observation["acknowledged_request_id"] != observation[
            "outbound_request_id"
        ] or code not in {0, 200}:
            raise RunnerError("observation acknowledgement is invalid")
    if case == "prolonged_outage" and not _finite_number(
        observation["outage_seconds"],
        minimum=MIN_PROLONGED_OUTAGE_SECONDS,
        maximum=MAX_PROLONGED_OUTAGE_SECONDS,
    ):
        raise RunnerError("observation prolonged_outage is invalid")
    if channel == "wecom" and case == "prolonged_outage":
        outage_mode = observation.get("outage_mode")
        if outage_mode not in WECOM_OUTAGE_MODES:
            raise RunnerError("observation prolonged_outage.outage_mode is invalid")
        if outage_mode == "service_failover":
            failure_instance = observation["failed_instance_id"]
            takeover_instance = observation["takeover_instance_id"]
            if failure_instance == takeover_instance:
                raise RunnerError(
                    "observation prolonged_outage failure and takeover instances must differ"
                )
            if (
                type(observation["old_lock_owner_released"]) is not bool
                or observation["new_lock_owner_acquired"] is not True
            ):
                raise RunnerError("observation prolonged_outage lock ownership is invalid")
            connection_epoch = observation["connection_epoch"]
            if (
                isinstance(connection_epoch, bool)
                or not isinstance(connection_epoch, int)
                or connection_epoch < 2
            ):
                raise RunnerError("observation prolonged_outage.connection_epoch is invalid")
            outage_event = observation["event_during_outage_id"]
            if observation["reply_for_event_id"] != outage_event:
                raise RunnerError("observation prolonged_outage reply must match the outage event")
            if observation["acknowledged_request_id"] != observation["outbound_request_id"]:
                raise RunnerError(
                    "observation prolonged_outage acknowledgement must match the outbound request"
                )
            for field, expected in (
                ("reply_count", 1),
                ("ack_count", 1),
                ("pending_count", 0),
                ("dlq_count", 0),
            ):
                value = observation[field]
                if isinstance(value, bool) or not isinstance(value, int) or value != expected:
                    raise RunnerError(f"observation prolonged_outage.{field} must be {expected}")
        else:
            raise RunnerError(
                "observation prolonged_outage provider_delivery_gap is not supported by "
                "contract v1; provider replay is unavailable"
            )
    if case == "ambiguous" and (
        observation["drop_response_observed"] is not True
        or type(observation["auto_replay_count"]) is not int
        or observation["auto_replay_count"] != 0
    ):
        raise RunnerError("observation ambiguous is invalid")


def _validate_driver_result(
    result: Mapping[str, Any],
    *,
    channel: str,
    run_nonce: str,
) -> dict[str, dict[str, Any]]:
    if (
        set(result) != DRIVER_RESULT_FIELDS
        or type(result.get("schema_version")) is not int
        or result.get("schema_version") != 1
    ):
        raise RunnerError("provider evidence driver result schema is invalid")
    observations = result.get("observations")
    if not isinstance(observations, dict) or set(observations) != set(REQUIRED_CASES):
        raise RunnerError("provider evidence driver did not return all required cases")
    validated: dict[str, dict[str, Any]] = {}
    seen_provider_ids: set[str] = set()
    for case in REQUIRED_CASES:
        observation = observations[case]
        _validate_observation(case, observation, channel=channel, run_nonce=run_nonce)
        typed_observation = cast(dict[str, Any], observation)
        provider_event_id = cast(str, typed_observation["provider_event_id"])
        if provider_event_id in seen_provider_ids:
            raise RunnerError("provider event IDs must be unique")
        seen_provider_ids.add(provider_event_id)
        # Copy only the already-whitelisted contract fields.  A driver's
        # payload can therefore never add message bodies, credentials or
        # arbitrary provider response data to runner stdout.
        allowed = {
            "status",
            "run_nonce",
            *_required_observation_fields(case, channel=channel, observation=typed_observation),
        }
        validated[case] = {key: typed_observation[key] for key in allowed}
    return validated


def _account_fingerprint(channel: str, account_id: str) -> str:
    label = ACCOUNT_LABEL_BY_CHANNEL[channel]
    return hashlib.sha256((label + "\0" + account_id).encode("utf-8")).hexdigest()


def _run(request: Mapping[str, Any]) -> dict[str, Any]:
    channel = cast(str, request["channel"])
    expected_runner_hash = _safe_hash(os.environ.get(RUNNER_SHA256_ENV), label=RUNNER_SHA256_ENV)
    runner_path = Path(__file__).resolve(strict=True)
    if _trusted_artifact_sha256(runner_path, label="provider runner") != expected_runner_hash:
        raise RunnerError("provider runner hash does not match")
    driver, account_id, paths, control_profile, control_socket = _validate_channel_configuration(
        channel,
        cast(str, request["control_profile_sha256"]),
    )
    account_variable = ACCOUNT_ENV_BY_CHANNEL[channel]
    result = _invoke_driver(
        driver,
        request,
        _driver_environment(
            channel,
            account_variable,
            account_id,
            paths,
            control_profile,
            control_socket,
        ),
    )
    observations = _validate_driver_result(
        result,
        channel=channel,
        run_nonce=cast(str, request["run_nonce"]),
    )
    evidence = {
        "source": SOURCE_BY_CHANNEL[channel],
        "independent_paths": list(PATHS_BY_CHANNEL[channel]),
        "run_nonce": request["run_nonce"],
        "account_fingerprint": _account_fingerprint(channel, account_id),
        "observations": observations,
    }
    driver_hash = _safe_hash(
        os.environ.get(f"{DRIVER_ENV_BY_CHANNEL[channel]}_SHA256"),
        label=f"{DRIVER_ENV_BY_CHANNEL[channel]}_SHA256",
    )
    return {
        "provider_evidence": evidence,
        "artifact_attestation": {
            "schema_version": ARTIFACT_CONTRACT_VERSION,
            "runner_sha256": expected_runner_hash,
            "runner_contract_version": ARTIFACT_CONTRACT_VERSION,
            "driver_sha256": driver_hash,
            "driver_contract_version": ARTIFACT_CONTRACT_VERSION,
        },
    }


def _input_stream() -> BinaryIO:
    stream = getattr(sys.stdin, "buffer", sys.stdin)
    return cast(BinaryIO, stream)


def _read_request() -> dict[str, Any]:
    raw = _input_stream().read(MAX_INPUT_BYTES + 1)
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    if not isinstance(raw, bytes) or len(raw) > MAX_INPUT_BYTES:
        raise RunnerError("request is too large")
    try:
        value = _strict_json(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise RunnerError("request is not strict JSON") from error
    return _parse_request(value)


def _configured_channels(requested: str | None) -> tuple[str, ...]:
    if requested is not None:
        return (requested,)
    configured = tuple(
        channel
        for channel in CHANNELS
        if os.environ.get(DRIVER_ENV_BY_CHANNEL[channel], "").strip()
    )
    if not configured:
        raise RunnerError("no provider evidence driver is configured")
    return configured


def _print_status(status: str) -> None:
    sys.stdout.write(json.dumps({"status": status}, separators=(",", ":")) + "\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate configured driver/account/secret paths without invoking a driver",
    )
    parser.add_argument("--channel", choices=CHANNELS, help="scope --check to one channel")
    args = parser.parse_args(argv)
    if args.channel is not None and not args.check:
        parser.error("--channel is valid only with --check")
    try:
        if args.check:
            for channel in _configured_channels(args.channel):
                _validate_channel_configuration(channel)
            _driver_timeout()
            _print_status("ready")
            return 0
        if args.channel is not None:
            parser.error("--channel is valid only with --check")
        request = _read_request()
        output = _run(request)
        sys.stdout.buffer.write(_canonical_json(output) + b"\n")
        return 0
    except RunnerError as error:
        if args.check:
            _print_status("not_ready")
        print(f"provider runner failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
