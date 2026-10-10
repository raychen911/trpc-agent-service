#!/usr/bin/env python3
"""Perform a content-free preflight for the independent IM probe host.

This checker validates configuration and file boundaries only.  It never makes
network requests, invokes the provider runner, reads provider credentials into
the report, or writes an IM acceptance result.  ``local`` mode is useful while
developing the deployment bundle: host files may be absent, but malformed or
unsafe values still fail validation and readiness remains ``not_run``.  Only a
complete ``host`` check can report ``readiness=pass``.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import math
import os
import re
import stat
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from scripts.evidence_lineage import source_fingerprint

ROOT = Path(__file__).resolve().parents[1]
PRODUCER = "scripts.im_probe_preflight"
DEFAULT_ENV_FILE = ROOT / "deploy" / "im_probe" / "im-probe.env"
DEFAULT_CANDIDATE_LOCK = ROOT / "runs" / "multitenant" / "candidate-lock.json"
DEFAULT_TRUST_FILE = ROOT / "deploy" / "im-probe-trust.json"

IMAGE_RE = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KEY_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
RELEASE_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{32,256}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
REFERENCE_RE = re.compile(r"^[^\s:@]+(?::[0-9]+)?(?:/[^\s:@]+)+@sha256:[0-9a-fA-F]{64}$")
PLACEHOLDER_MARKERS = (
    "replace-with",
    "change-me",
    "placeholder",
    "synthetic",
    "<candidate",
)
URL_MAX_LENGTH = 2048
MAX_ENV_BYTES = 64 * 1024
MAX_DOCUMENT_BYTES = 1024 * 1024
MAX_SECRET_BYTES = 4096
MAX_SEED_BYTES = 4096
MAX_CONTROL_PROFILE_BYTES = 64 * 1024
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1"})

REQUIRED_PROBE_KEYS = (
    "TRPC_IM_PROBE_BIND_HOST",
    "TRPC_IM_PROBE_PORT",
    "TRPC_IM_PROBE_SIGNING_KEY_FILE",
    "TRPC_IM_PROBE_KEY_ID",
    "TRPC_IM_PROBE_RELEASE_CONTEXT_FILE",
    "TRPC_IM_PROBE_IDENTITY_SHA256",
    "TRPC_IM_PROBE_FEISHU_APP_ID",
    "TRPC_IM_PROBE_WECOM_BOT_ID",
    "TRPC_IM_PROBE_FEISHU_APP_SECRET_FILE",
    "TRPC_IM_PROBE_FEISHU_VERIFICATION_TOKEN_FILE",
    "TRPC_IM_PROBE_FEISHU_ENCRYPT_KEY_FILE",
    "TRPC_IM_PROBE_WECOM_BOT_SECRET_FILE",
    "TRPC_IM_PROBE_FEISHU_CONTROL_PROFILE_FILE",
    "TRPC_IM_PROBE_WECOM_CONTROL_PROFILE_FILE",
    "TRPC_IM_PROBE_CONTROL_SOCKET",
    "TRPC_IM_PROBE_RUNNER",
    "TRPC_IM_PROBE_RUNNER_SHA256",
    "TRPC_IM_PROBE_FEISHU_DRIVER",
    "TRPC_IM_PROBE_FEISHU_DRIVER_SHA256",
    "TRPC_IM_PROBE_WECOM_DRIVER",
    "TRPC_IM_PROBE_WECOM_DRIVER_SHA256",
    "TRPC_IM_PROBE_BROKER_UID",
    "TRPC_IM_PROBE_BROKER_GID",
    "TRPC_IM_PROBE_RUNNER_TIMEOUT_SECONDS",
    "TRPC_IM_PROBE_DRIVER_TIMEOUT_SECONDS",
)
REQUIRED_GATE_KEYS = (
    "TRPC_IM_ONLINE_TESTS_ENABLED",
    "TRPC_IM_ONLINE_PROBE_URL",
    "TRPC_IM_ONLINE_PROBE_URL_ALLOWLIST",
    "TRPC_IM_ONLINE_IMAGE_DIGEST",
    "TRPC_IM_ONLINE_FEISHU_CONTROL_PROFILE_SHA256",
    "TRPC_IM_ONLINE_WECOM_CONTROL_PROFILE_SHA256",
    "TRPC_RELEASE_ID",
    "TRPC_RELEASE_NONCE",
)
IDENTITY_KEYS = (
    "TRPC_IM_ONLINE_PROBE_IDENTITY_SHA256",
    "TRPC_IM_ONLINE_PROBE_IDENTITY",
)
ALLOWLIST_KEYS = (
    "TRPC_IM_ONLINE_PROBE_URL_ALLOWLIST",
    "TRPC_IM_ONLINE_PROBE_ALLOWLIST",
)
SECRET_PATH_KEYS = (
    "TRPC_IM_PROBE_FEISHU_APP_SECRET_FILE",
    "TRPC_IM_PROBE_FEISHU_VERIFICATION_TOKEN_FILE",
    "TRPC_IM_PROBE_FEISHU_ENCRYPT_KEY_FILE",
    "TRPC_IM_PROBE_WECOM_BOT_SECRET_FILE",
)
OPTIONAL_SECRET_PATH_KEYS = (
    "TRPC_IM_PROBE_FEISHU_OLD_APP_SECRET_FILE",
    "TRPC_IM_PROBE_FEISHU_NEW_APP_SECRET_FILE",
    "TRPC_IM_PROBE_WECOM_OLD_BOT_SECRET_FILE",
    "TRPC_IM_PROBE_WECOM_NEW_BOT_SECRET_FILE",
)
DRIVER_KEYS = (
    "TRPC_IM_PROBE_FEISHU_DRIVER",
    "TRPC_IM_PROBE_WECOM_DRIVER",
)
CONTROL_PROFILE_PATH_KEYS = {
    "feishu": "TRPC_IM_PROBE_FEISHU_CONTROL_PROFILE_FILE",
    "wecom": "TRPC_IM_PROBE_WECOM_CONTROL_PROFILE_FILE",
}
CONTROL_PROFILE_HASH_KEYS = {
    "feishu": "TRPC_IM_ONLINE_FEISHU_CONTROL_PROFILE_SHA256",
    "wecom": "TRPC_IM_ONLINE_WECOM_CONTROL_PROFILE_SHA256",
}
CONTROL_SOCKET_KEY = "TRPC_IM_PROBE_CONTROL_SOCKET"
ARTIFACT_HASH_KEYS = {
    "TRPC_IM_PROBE_RUNNER": "TRPC_IM_PROBE_RUNNER_SHA256",
    "TRPC_IM_PROBE_FEISHU_DRIVER": "TRPC_IM_PROBE_FEISHU_DRIVER_SHA256",
    "TRPC_IM_PROBE_WECOM_DRIVER": "TRPC_IM_PROBE_WECOM_DRIVER_SHA256",
}


def _sha256(value: str | bytes) -> str:
    """Return a SHA-256 digest without exposing the input."""

    raw = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(raw).hexdigest()


def _fingerprint(value: str, *, label: str) -> str:
    return _sha256(label + "\0" + value)


def _strict_json(raw: bytes) -> Any:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON constant {value} is forbidden")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    return json.loads(
        raw.decode("utf-8"),
        parse_constant=reject_constant,
        object_pairs_hook=reject_duplicate_keys,
    )


def _check(name: str, status: str, reason: str | None = None, **details: Any) -> dict[str, Any]:
    result: dict[str, Any] = {"name": name, "status": status}
    if reason is not None:
        result["reason"] = reason
    result.update(details)
    return result


def _path_has_symlink(path: Path) -> bool:
    try:
        return any(candidate.is_symlink() for candidate in (path, *path.parents))
    except OSError:
        return True


def _contains_placeholder(value: str) -> bool:
    lowered = value.lower()
    return any(marker in lowered for marker in PLACEHOLDER_MARKERS) or "<" in value or ">" in value


def _resolved_path(value: str) -> Path | None:
    try:
        return Path(value).resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        return None


def _inside_checkout(path: Path, checkout: Path) -> bool:
    resolved = _resolved_path(str(path))
    checkout_resolved = _resolved_path(str(checkout))
    if resolved is None or checkout_resolved is None:
        return True
    try:
        resolved.relative_to(checkout_resolved)
    except ValueError:
        return False
    return True


def _permission_reason(path: Path, *, private: bool, executable: bool) -> str | None:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except OSError:
        return "file permissions are unavailable"

    # Windows ACLs do not map to POSIX mode bits.  The real probe host is a
    # Unix service; on Windows, existence/readability is still checked and
    # deployment ACLs are left to the host's service installer.
    if os.name != "nt":
        if private and mode & 0o037:
            return "private file permissions are too broad"
        if not private and mode & 0o022:
            return "executable file is writable by group or other users"
        if executable and not mode & 0o111:
            return "executable file has no execute permission"
    if executable:
        try:
            if not os.access(path, os.X_OK):
                return "executable file is not executable"
        except OSError:
            return "executable permission is unavailable"
    return None


def _check_path(
    name: str,
    raw_value: str | None,
    *,
    mode: str,
    checkout: Path,
    private: bool = False,
    executable: bool = False,
    outside_checkout: bool = False,
) -> tuple[dict[str, Any], Path | None]:
    if raw_value is None or not raw_value.strip():
        return _check(name, "fail", "path value is missing"), None
    value = raw_value.strip()
    if _contains_placeholder(value):
        return _check(name, "fail", "path contains a placeholder"), None
    path = Path(value)
    if not path.is_absolute():
        return _check(name, "fail", "path must be absolute"), None
    if _path_has_symlink(path):
        return _check(name, "fail", "path or parent contains a symlink"), None
    resolved = _resolved_path(value)
    if resolved is None:
        return _check(name, "fail", "path cannot be resolved"), None
    if outside_checkout and _inside_checkout(resolved, checkout):
        return _check(name, "fail", "path must be outside application checkout"), None

    try:
        exists = resolved.exists()
    except OSError:
        exists = False
    if not exists:
        if mode == "local":
            return _check(name, "not_run", "host path is unavailable in local mode"), None
        return _check(name, "fail", "required host path is unavailable"), None
    try:
        is_file = resolved.is_file()
    except OSError:
        is_file = False
    if not is_file:
        return _check(name, "fail", "path must be a regular file"), None
    permission_reason = _permission_reason(resolved, private=private, executable=executable)
    if permission_reason is not None:
        return _check(name, "fail", permission_reason), None
    return _check(name, "pass", permissions_checked=True), resolved


def _check_control_socket(
    raw_value: str | None,
    *,
    mode: str,
    checkout: Path,
    expected_uid: int | None,
    expected_gid: int | None,
) -> tuple[dict[str, Any], Path | None]:
    name = CONTROL_SOCKET_KEY
    if raw_value is None or not raw_value.strip():
        return _check(name, "fail", "path value is missing"), None
    value = raw_value.strip()
    if _contains_placeholder(value):
        return _check(name, "fail", "path contains a placeholder"), None
    path = Path(value)
    if not path.is_absolute():
        return _check(name, "fail", "path must be absolute"), None
    if _path_has_symlink(path):
        return _check(name, "fail", "path or parent contains a symlink"), None
    resolved = _resolved_path(value)
    if resolved is None:
        return _check(name, "fail", "path cannot be resolved"), None
    if _inside_checkout(resolved, checkout):
        return _check(name, "fail", "path must be outside application checkout"), None
    try:
        exists = resolved.exists()
    except OSError:
        exists = False
    if not exists:
        if mode == "local":
            return _check(name, "not_run", "host socket is unavailable in local mode"), None
        return _check(name, "fail", "required host socket is unavailable"), None
    try:
        metadata = resolved.stat()
        socket_mode = metadata.st_mode
    except OSError:
        return _check(name, "fail", "host socket metadata is unavailable"), None
    if os.name != "nt" and not stat.S_ISSOCK(socket_mode):
        return _check(name, "fail", "path must be a Unix socket"), None
    if os.name == "nt" and stat.S_ISDIR(socket_mode):
        return _check(name, "fail", "path must not be a directory"), None
    if os.name != "nt" and (
        expected_uid is None
        or expected_gid is None
        or metadata.st_uid != expected_uid
        or metadata.st_gid != expected_gid
        or stat.S_IMODE(socket_mode) != 0o660
        or resolved.parent.stat().st_uid != expected_uid
        or resolved.parent.stat().st_gid != expected_gid
        or stat.S_IMODE(resolved.parent.stat().st_mode) != 0o750
    ):
        return _check(name, "fail", "socket owner, group, parent, or mode is invalid"), None
    return _check(name, "pass", socket_type_checked=os.name != "nt"), resolved


def _artifact_binding_check(
    *,
    name: str,
    path: Path | None,
    path_status: str,
    expected_sha256: str,
) -> dict[str, Any]:
    expected = expected_sha256.strip().lower()
    if HEX64_RE.fullmatch(expected) is None or expected in {"0" * 64, "f" * 64}:
        return _check(f"{name}_binding", "fail", "artifact hash is invalid")
    if path is None:
        status = "not_run" if path_status == "not_run" else "fail"
        return _check(f"{name}_binding", status, "artifact is unavailable")
    if os.name != "nt":
        try:
            metadata = path.stat()
            current = path.parent
            while True:
                parent = current.lstat()
                if (
                    stat.S_ISLNK(parent.st_mode)
                    or not stat.S_ISDIR(parent.st_mode)
                    or parent.st_mode & 0o022
                    or (parent.st_uid != 0 and parent.st_mode & 0o200)
                ):
                    return _check(
                        f"{name}_binding", "fail", "artifact parent chain is not root-controlled"
                    )
                if current.parent == current:
                    break
                current = current.parent
        except OSError:
            return _check(f"{name}_binding", "fail", "artifact metadata is unavailable")
        if metadata.st_uid != 0 or metadata.st_mode & 0o022:
            return _check(f"{name}_binding", "fail", "artifact is not root-owned immutable")
    try:
        observed = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return _check(f"{name}_binding", "fail", "artifact cannot be read")
    if observed != expected:
        return _check(f"{name}_binding", "fail", "artifact hash does not match")
    return _check(f"{name}_binding", "pass", sha256=observed)


def _control_profile_binding_check(
    *,
    channel: str,
    profile: Path | None,
    path_status: str,
    expected_sha256: str,
) -> dict[str, Any]:
    name = f"{channel}_control_profile_binding"
    expected = expected_sha256.strip().lower()
    if HEX64_RE.fullmatch(expected) is None or expected in {"0" * 64, "f" * 64}:
        return _check(name, "fail", "control profile hash is invalid")
    if profile is None:
        status = "not_run" if path_status == "not_run" else "fail"
        return _check(name, status, "control profile is unavailable")
    try:
        with profile.open("rb") as handle:
            raw = handle.read(MAX_CONTROL_PROFILE_BYTES + 1)
    except OSError:
        return _check(name, "fail", "control profile is unavailable")
    if not raw or len(raw) > MAX_CONTROL_PROFILE_BYTES:
        return _check(name, "fail", "control profile size is invalid")
    observed = _sha256(raw)
    if observed != expected:
        return _check(name, "fail", "control profile hash does not match online gate")
    return _check(name, "pass", control_profile_sha256=observed)


def _read_env_file(path: Path, *, mode: str) -> tuple[dict[str, str], dict[str, Any], bytes | None]:
    if _path_has_symlink(path):
        return {}, _check("env_file", "fail", "env file or parent contains a symlink"), None
    try:
        if not path.is_file():
            if mode == "local":
                return (
                    {},
                    _check("env_file", "not_run", "host env file is unavailable in local mode"),
                    None,
                )
            return {}, _check("env_file", "fail", "host env file is unavailable"), None
        raw = path.read_bytes()
    except (OSError, RuntimeError):
        return {}, _check("env_file", "fail", "host env file is unreadable"), None
    if not raw or len(raw) > MAX_ENV_BYTES:
        return {}, _check("env_file", "fail", "host env file size is invalid"), raw

    values: dict[str, str] = {}
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {}, _check("env_file", "fail", "host env file is not UTF-8"), raw
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[7:].lstrip()
        if "=" not in stripped:
            return {}, _check("env_file", "fail", "host env file contains a malformed entry"), raw
        key, value = stripped.split("=", 1)
        key = key.strip()
        if ENV_KEY_RE.fullmatch(key) is None:
            return {}, _check("env_file", "fail", "host env file contains an invalid key"), raw
        if key in values:
            return {}, _check("env_file", "fail", "host env file contains a duplicate key"), raw
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if "\x00" in value:
            return {}, _check("env_file", "fail", "host env file contains a NUL value"), raw
        values[key] = value
    return values, _check("env_file", "pass", keys=list(sorted(values))), raw


def _required_key_check(values: Mapping[str, str]) -> dict[str, Any]:
    required = set(REQUIRED_PROBE_KEYS) | set(REQUIRED_GATE_KEYS)
    required.difference_update(ALLOWLIST_KEYS)
    present = {key for key in required if values.get(key, "").strip()}
    missing = sorted(required - present)
    if not any(values.get(key, "").strip() for key in IDENTITY_KEYS):
        missing.extend(IDENTITY_KEYS)
    # The primary allowlist name is documented, while the legacy alias remains
    # accepted by the online gate.  It is still a required configuration item.
    if not any(values.get(key, "").strip() for key in ALLOWLIST_KEYS):
        missing.extend(ALLOWLIST_KEYS)
    return _check(
        "required_env_keys",
        "pass" if not missing else "fail",
        None if not missing else "required env keys are missing",
        missing_keys=sorted(set(missing)),
        present_keys=sorted(present),
    )


def _canonical_url(value: str) -> str | None:
    if not value or len(value) > URL_MAX_LENGTH:
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
    netloc = hostname if port in (None, 443) else f"{hostname}:{port}"
    return f"https://{netloc}{parsed.path.rstrip('/')}"


def _identity_value(values: Mapping[str, str]) -> tuple[str | None, str | None]:
    direct = values.get("TRPC_IM_ONLINE_PROBE_IDENTITY_SHA256", "").strip().lower()
    raw = values.get("TRPC_IM_ONLINE_PROBE_IDENTITY", "").strip()
    direct_value = (
        direct if HEX64_RE.fullmatch(direct) and direct not in {"0" * 64, "f" * 64} else None
    )
    raw_value = (
        _fingerprint(raw, label="TRPC_IM_ONLINE_PROBE_IDENTITY")
        if raw and not _contains_placeholder(raw)
        else None
    )
    if direct_value is not None and raw and raw_value != direct_value:
        return None, "online identity hash and identity value disagree"
    if direct_value is not None:
        return direct_value, None
    if raw_value is not None:
        return raw_value, None
    return None, "online probe identity is invalid"


def _endpoint_check(values: Mapping[str, str], trust_url: str | None) -> dict[str, Any]:
    configured = _canonical_url(values.get("TRPC_IM_ONLINE_PROBE_URL", "").strip())
    raw_allowlists = [
        values.get(key, "").strip() for key in ALLOWLIST_KEYS if values.get(key, "").strip()
    ]
    if len(raw_allowlists) > 1 and raw_allowlists[0] != raw_allowlists[1]:
        return _check("online_endpoint", "fail", "online probe allowlist aliases disagree")
    raw_allowlist = raw_allowlists[0] if raw_allowlists else ""
    entries = [item.strip() for item in re.split(r"[,\r\n]+", raw_allowlist) if item.strip()]
    canonical_entries = [_canonical_url(item) for item in entries]
    if configured is None:
        return _check("online_endpoint", "fail", "online probe URL is not a valid HTTPS URL")
    if not entries or any(item is None for item in canonical_entries):
        return _check("online_endpoint", "fail", "online probe allowlist contains an invalid URL")
    if configured not in canonical_entries:
        return _check("online_endpoint", "fail", "online probe URL is not allowlisted")
    if trust_url is not None and configured != trust_url:
        return _check("online_endpoint", "fail", "online probe URL does not match trust")
    return _check(
        "online_endpoint",
        "pass",
        probe_url_sha256=_sha256(configured),
        allowlist_count=len(set(canonical_entries)),
    )


def _probe_value_checks(values: Mapping[str, str]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    host = values.get("TRPC_IM_PROBE_BIND_HOST", "").strip()
    port_text = values.get("TRPC_IM_PROBE_PORT", "").strip()
    timeout_text = values.get("TRPC_IM_PROBE_RUNNER_TIMEOUT_SECONDS", "").strip()
    driver_timeout_text = values.get("TRPC_IM_PROBE_DRIVER_TIMEOUT_SECONDS", "").strip()
    try:
        port = int(port_text)
    except ValueError:
        port = -1
    try:
        timeout = float(timeout_text)
    except ValueError:
        timeout = math.nan
    try:
        driver_timeout = float(driver_timeout_text)
    except ValueError:
        driver_timeout = math.nan
    listener_ok = (
        host in LOOPBACK_HOSTS
        and 1 <= port <= 65535
        and math.isfinite(timeout)
        and 0.001 <= timeout <= 900
        and math.isfinite(driver_timeout)
        and 0.001 <= driver_timeout <= 900
    )
    checks.append(
        _check(
            "probe_listener",
            "pass" if listener_ok else "fail",
            None if listener_ok else "probe bind or timeout configuration is invalid",
        )
    )

    key_id = values.get("TRPC_IM_PROBE_KEY_ID", "").strip()
    probe_identity = values.get("TRPC_IM_PROBE_IDENTITY_SHA256", "").strip().lower()
    feishu_id = values.get("TRPC_IM_PROBE_FEISHU_APP_ID", "").strip()
    wecom_id = values.get("TRPC_IM_PROBE_WECOM_BOT_ID", "").strip()
    account_ok = (
        KEY_ID_RE.fullmatch(key_id) is not None
        and not _contains_placeholder(key_id)
        and HEX64_RE.fullmatch(probe_identity) is not None
        and probe_identity not in {"0" * 64, "f" * 64}
        and re.fullmatch(r"cli_[A-Za-z0-9]+", feishu_id) is not None
        and not _contains_placeholder(feishu_id)
        and SAFE_ID_RE.fullmatch(wecom_id) is not None
        and not _contains_placeholder(wecom_id)
    )
    checks.append(
        _check(
            "probe_identity_and_accounts",
            "pass" if account_ok else "fail",
            None if account_ok else "probe identity, key, or account configuration is invalid",
            values_recorded=False,
        )
    )

    enabled = values.get("TRPC_IM_ONLINE_TESTS_ENABLED", "").strip().lower() == "true"
    online_identity, identity_reason = _identity_value(values)
    identity_ok = enabled and online_identity == probe_identity and identity_reason is None
    checks.append(
        _check(
            "online_identity_binding",
            "pass" if identity_ok else "fail",
            None
            if identity_ok
            else (
                identity_reason
                or "online tests must be explicitly enabled and identity must match probe"
            ),
            identity_sha256=online_identity or None,
        )
    )
    return checks


def _read_document(path: Path) -> tuple[Mapping[str, Any] | None, dict[str, Any], bytes | None]:
    if _path_has_symlink(path):
        return None, _check("document", "fail", "document or parent contains a symlink"), None
    try:
        if not path.is_file():
            return None, _check("document", "not_run", "document is unavailable"), None
        raw = path.read_bytes()
    except (OSError, RuntimeError):
        return None, _check("document", "fail", "document is unreadable"), None
    if not raw or len(raw) > MAX_DOCUMENT_BYTES:
        return None, _check("document", "fail", "document size is invalid"), raw
    try:
        value = _strict_json(raw)
    except (UnicodeError, ValueError, json.JSONDecodeError):
        return None, _check("document", "fail", "document is not strict JSON"), raw
    if not isinstance(value, Mapping):
        return None, _check("document", "fail", "document root must be an object"), raw
    return value, _check("document", "pass"), raw


def _candidate_lock_check(
    path: Path,
) -> tuple[
    dict[str, Any],
    str | None,
    str | None,
    str | None,
    str | None,
    bytes | None,
]:
    value, document_check, raw = _read_document(path)
    document_check["name"] = "candidate_lock_file"
    if value is None:
        return document_check, None, None, None, None, raw
    if value.get("schema_version") != 1 or value.get("kind") != "release_candidate_lock":
        return (
            _check("candidate_lock", "fail", "candidate lock schema is invalid"),
            None,
            None,
            None,
            None,
            raw,
        )
    images = value.get("images")
    if not isinstance(images, Mapping) or set(images) != {"initial", "upgrade"}:
        return (
            _check("candidate_lock", "fail", "candidate lock image set is invalid"),
            None,
            None,
            None,
            None,
            raw,
        )
    initial = images.get("initial") if isinstance(images, Mapping) else None
    upgrade = images.get("upgrade") if isinstance(images, Mapping) else None
    initial_digest = initial.get("digest") if isinstance(initial, Mapping) else None
    upgrade_digest = upgrade.get("digest") if isinstance(upgrade, Mapping) else None
    initial_reference = initial.get("reference") if isinstance(initial, Mapping) else None
    upgrade_reference = upgrade.get("reference") if isinstance(upgrade, Mapping) else None
    image_digest = value.get("image_digest")
    valid_initial = (
        isinstance(initial_digest, str) and IMAGE_RE.fullmatch(initial_digest) is not None
    )
    valid_upgrade = (
        isinstance(upgrade_digest, str) and IMAGE_RE.fullmatch(upgrade_digest) is not None
    )
    valid_references = (
        isinstance(initial_reference, str)
        and isinstance(upgrade_reference, str)
        and REFERENCE_RE.fullmatch(initial_reference) is not None
        and REFERENCE_RE.fullmatch(upgrade_reference) is not None
        and initial_reference.endswith("@" + str(initial_digest))
        and upgrade_reference.endswith("@" + str(upgrade_digest))
    )
    valid_top = isinstance(image_digest, str) and image_digest == initial_digest
    nonzero = valid_initial and str(initial_digest).lower() not in {
        "sha256:" + "0" * 64,
        "sha256:" + "f" * 64,
    }
    different = (
        valid_initial
        and valid_upgrade
        and str(initial_digest).lower() != str(upgrade_digest).lower()
    )
    release = value.get("release_binding")
    release_id = release.get("release_id") if isinstance(release, Mapping) else None
    nonce_sha256 = release.get("nonce_sha256") if isinstance(release, Mapping) else None
    release_ok = (
        isinstance(release_id, str)
        and RELEASE_ID_RE.fullmatch(release_id) is not None
        and isinstance(nonce_sha256, str)
        and HEX64_RE.fullmatch(nonce_sha256) is not None
    )
    source = value.get("source_fingerprint")
    source_value = source.get("value") if isinstance(source, Mapping) else None
    source_ok = (
        isinstance(source, Mapping)
        and set(source) == {"status", "value"}
        and source.get("status") == "available"
        and isinstance(source_value, str)
        and HEX64_RE.fullmatch(source_value) is not None
    )
    valid = (
        valid_initial
        and valid_upgrade
        and valid_references
        and valid_top
        and nonzero
        and different
        and release_ok
        and source_ok
    )
    check = _check(
        "candidate_lock",
        "pass" if valid else "fail",
        None if valid else "candidate lock image, source, or release binding is invalid",
        initial_digest_sha256=_sha256(str(initial_digest).lower()) if valid_initial else None,
        release_id_sha256=_sha256(str(release_id)) if isinstance(release_id, str) else None,
    )
    return (
        check,
        str(initial_digest).lower() if valid_initial else None,
        str(release_id) if release_ok and isinstance(release_id, str) else None,
        str(nonce_sha256).lower() if release_ok else None,
        str(source_value).lower() if source_ok else None,
        raw,
    )


def _release_context_check(
    raw_value: str | None,
    *,
    mode: str,
    checkout: Path,
) -> tuple[dict[str, Any], Mapping[str, str] | None, bytes | None]:
    name = "release_context"
    path_check, path = _check_path(
        "TRPC_IM_PROBE_RELEASE_CONTEXT_FILE",
        raw_value,
        mode=mode,
        checkout=checkout,
        private=True,
    )
    if path is None:
        return path_check, None, None
    try:
        metadata = path.stat()
    except OSError:
        return _check(name, "fail", "release context metadata is unavailable"), None, None
    if os.name != "nt":
        expected_gid = getattr(os, "getegid", lambda: metadata.st_gid)()
        if metadata.st_uid != 0 or metadata.st_gid != expected_gid:
            return (
                _check(name, "fail", "release context owner or group is invalid"),
                None,
                None,
            )
    try:
        raw = path.read_bytes()
    except OSError:
        return _check(name, "fail", "release context is unreadable"), None, None
    if not raw or len(raw) > 64 * 1024:
        return _check(name, "fail", "release context size is invalid"), None, raw
    try:
        value = _strict_json(raw)
    except (UnicodeError, ValueError, json.JSONDecodeError):
        return _check(name, "fail", "release context is not strict JSON"), None, raw
    fields = {
        "schema_version",
        "release_id",
        "nonce_sha256",
        "source_fingerprint",
        "image_digest",
    }
    if not isinstance(value, Mapping) or set(value) != fields or value.get("schema_version") != 1:
        return _check(name, "fail", "release context schema is invalid"), None, raw
    release_id = value.get("release_id")
    nonce_sha256 = value.get("nonce_sha256")
    source = value.get("source_fingerprint")
    image = value.get("image_digest")
    valid = (
        isinstance(release_id, str)
        and RELEASE_ID_RE.fullmatch(release_id) is not None
        and isinstance(nonce_sha256, str)
        and HEX64_RE.fullmatch(nonce_sha256) is not None
        and isinstance(source, str)
        and HEX64_RE.fullmatch(source) is not None
        and isinstance(image, str)
        and IMAGE_RE.fullmatch(image) is not None
    )
    if not valid:
        return _check(name, "fail", "release context identity is invalid"), None, raw
    assert isinstance(release_id, str)
    assert isinstance(nonce_sha256, str)
    assert isinstance(source, str)
    assert isinstance(image, str)
    measured = source_fingerprint(checkout)
    if measured.get("status") != "available" or measured.get("value") != source.lower():
        return _check(name, "fail", "deployed source does not match release context"), None, raw
    identity = {
        "release_id": release_id,
        "nonce_sha256": nonce_sha256.lower(),
        "source_fingerprint": source.lower(),
        "image_digest": image.lower(),
    }
    return _check(name, "pass", identity_values_recorded=False), identity, raw


def _trust_check(
    path: Path,
) -> tuple[dict[str, Any], str | None, str | None, bytes | None, bytes | None]:
    value, document_check, raw = _read_document(path)
    document_check["name"] = "trust_file"
    if value is None:
        return document_check, None, None, None, raw
    if set(value) != {"schema_version", "probe_url", "key_id", "ed25519_public_key"}:
        return _check("trust_file", "fail", "trust JSON schema is invalid"), None, None, None, raw
    schema = value.get("schema_version")
    url = value.get("probe_url")
    key_id = value.get("key_id")
    encoded = value.get("ed25519_public_key")
    canonical_url = _canonical_url(url) if isinstance(url, str) else None
    key_id_ok = (
        isinstance(key_id, str)
        and KEY_ID_RE.fullmatch(key_id) is not None
        and not _contains_placeholder(key_id)
    )
    public_key: bytes | None = None
    try:
        if isinstance(encoded, str):
            public_key = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError, TypeError):
        public_key = None
    public_ok = (
        public_key is not None
        and len(public_key) == 32
        and public_key not in {b"\0" * 32, b"\xff" * 32}
    )
    valid = (
        schema == 1
        and isinstance(schema, int)
        and not isinstance(schema, bool)
        and canonical_url is not None
        and key_id_ok
        and public_ok
    )
    check = _check(
        "trust_file",
        "pass" if valid else "fail",
        None if valid else "trust schema, HTTPS URL, key ID, or public key is invalid",
        probe_url_sha256=_sha256(canonical_url) if canonical_url is not None else None,
        key_id_sha256=_sha256(str(key_id)) if isinstance(key_id, str) else None,
        public_key_sha256=_sha256(public_key) if public_ok and public_key is not None else None,
    )
    return (
        check,
        canonical_url if valid else None,
        str(key_id) if valid else None,
        public_key if valid else None,
        raw,
    )


def _seed_check(
    path: Path | None,
    *,
    status_from_path: str,
    trusted_public_key: bytes | None,
) -> dict[str, Any]:
    if path is None:
        return _check("signing_seed", status_from_path, "signing seed is unavailable")
    if trusted_public_key is None and status_from_path == "not_run":
        return _check("signing_seed", "not_run", "trust public key is unavailable")
    try:
        raw = path.read_bytes()
    except OSError:
        return _check("signing_seed", "fail", "signing seed is unreadable")
    if not raw or len(raw) > MAX_SEED_BYTES:
        return _check("signing_seed", "fail", "signing seed size is invalid")
    try:
        seed = base64.b64decode(b"".join(raw.split()), validate=True)
    except (binascii.Error, ValueError):
        return _check("signing_seed", "fail", "signing seed is not valid base64")
    if len(seed) != 32 or seed in {b"\0" * 32, b"\xff" * 32}:
        return _check("signing_seed", "fail", "signing seed must encode a non-zero 32-byte seed")
    try:
        derived = Ed25519PrivateKey.from_private_bytes(seed).public_key().public_bytes_raw()
    except (ValueError, TypeError):
        return _check("signing_seed", "fail", "signing seed is not a valid Ed25519 seed")
    matches = trusted_public_key is not None and derived == trusted_public_key
    return _check(
        "signing_seed",
        "pass" if matches else "fail",
        None if matches else "Ed25519 private seed does not match trust public key",
        derived_public_key_sha256=_sha256(derived),
        trusted_public_key_sha256=_sha256(trusted_public_key)
        if trusted_public_key is not None
        else None,
    )


def _secret_content_check(path: Path | None, *, name: str, status_from_path: str) -> dict[str, Any]:
    if path is None:
        return _check(name, status_from_path, "secret file is unavailable")
    try:
        raw = path.read_bytes()
    except OSError:
        return _check(name, "fail", "secret file is unreadable")
    if not raw or len(raw) > MAX_SECRET_BYTES:
        return _check(name, "fail", "secret file size is invalid")
    return _check(name, "pass", content_recorded=False)


def _release_and_image_binding_check(
    values: Mapping[str, str],
    *,
    candidate_digest: str | None,
    candidate_release_id: str | None,
    candidate_nonce_sha256: str | None,
    candidate_source_fingerprint: str | None,
    release_context: Mapping[str, str] | None,
) -> list[dict[str, Any]]:
    online_digest = values.get("TRPC_IM_ONLINE_IMAGE_DIGEST", "").strip().lower()
    context_digest = release_context.get("image_digest") if release_context else None
    image_ok = (
        candidate_digest is not None
        and context_digest is not None
        and IMAGE_RE.fullmatch(online_digest) is not None
        and context_digest == candidate_digest
        and online_digest == candidate_digest
    )
    image_check = _check(
        "image_binding",
        "pass" if image_ok else "fail",
        None
        if image_ok
        else "release context and online image digests do not match candidate initial digest",
        candidate_initial_digest_sha256=_sha256(candidate_digest) if candidate_digest else None,
        context_digest_sha256=_sha256(context_digest) if context_digest else None,
        online_digest_sha256=_sha256(online_digest) if IMAGE_RE.fullmatch(online_digest) else None,
    )
    release_id = values.get("TRPC_RELEASE_ID", "").strip()
    release_nonce = values.get("TRPC_RELEASE_NONCE", "").strip()
    nonce_hash = _sha256(release_nonce) if RELEASE_NONCE_RE.fullmatch(release_nonce) else None
    release_ok = (
        candidate_release_id is not None
        and candidate_nonce_sha256 is not None
        and release_id == candidate_release_id
        and nonce_hash == candidate_nonce_sha256
        and release_context is not None
        and release_context.get("release_id") == candidate_release_id
        and release_context.get("nonce_sha256") == candidate_nonce_sha256
    )
    release_check = _check(
        "release_binding",
        "pass" if release_ok else "fail",
        None if release_ok else "release ID or nonce does not match candidate lock",
        release_id_sha256=_sha256(release_id) if RELEASE_ID_RE.fullmatch(release_id) else None,
        release_nonce_sha256=nonce_hash,
    )
    source_ok = (
        candidate_source_fingerprint is not None
        and release_context is not None
        and release_context.get("source_fingerprint") == candidate_source_fingerprint
    )
    source_check = _check(
        "source_binding",
        "pass" if source_ok else "fail",
        None if source_ok else "release context source does not match candidate lock",
        source_fingerprint_sha256=(
            _sha256(candidate_source_fingerprint) if candidate_source_fingerprint else None
        ),
    )
    return [image_check, release_check, source_check]


def build_preflight(
    env_file: Path,
    *,
    mode: str = "local",
    candidate_lock: Path = DEFAULT_CANDIDATE_LOCK,
    trust_file: Path = DEFAULT_TRUST_FILE,
    checkout: Path = ROOT,
) -> dict[str, Any]:
    """Build a secret/path-free preflight result."""

    if mode not in {"local", "host"}:
        raise ValueError("mode must be local or host")
    checks: list[dict[str, Any]] = []
    values, env_check, env_raw = _read_env_file(env_file, mode=mode)
    checks.append(env_check)
    checks.append(_required_key_check(values))

    trust_info, trust_url, _key_id, trusted_public_key, trust_raw = _trust_check(trust_file)
    checks.append(trust_info)
    (
        candidate_info,
        candidate_digest,
        candidate_release_id,
        candidate_nonce_hash,
        candidate_source,
        candidate_raw,
    ) = _candidate_lock_check(candidate_lock)
    checks.append(candidate_info)

    checkout_status = "pass" if checkout.is_dir() and not _path_has_symlink(checkout) else "fail"
    checks.append(
        _check(
            "application_checkout",
            checkout_status,
            None
            if checkout_status == "pass"
            else "application checkout is unavailable or contains a symlink",
        )
    )

    release_context_check, release_context, release_context_raw = _release_context_check(
        values.get("TRPC_IM_PROBE_RELEASE_CONTEXT_FILE"),
        mode=mode,
        checkout=checkout,
    )
    checks.append(release_context_check)

    checks.extend(_probe_value_checks(values))
    checks.append(_endpoint_check(values, trust_url))
    checks.extend(
        _release_and_image_binding_check(
            values,
            candidate_digest=candidate_digest,
            candidate_release_id=candidate_release_id,
            candidate_nonce_sha256=candidate_nonce_hash,
            candidate_source_fingerprint=candidate_source,
            release_context=release_context,
        )
    )

    path_specs: list[tuple[str, str | None, bool, bool, bool]] = [
        (
            "TRPC_IM_PROBE_SIGNING_KEY_FILE",
            values.get("TRPC_IM_PROBE_SIGNING_KEY_FILE"),
            True,
            False,
            False,
        ),
        *[(name, values.get(name), True, False, False) for name in SECRET_PATH_KEYS],
        *[
            (name, values.get(name), True, False, False)
            for name in OPTIONAL_SECRET_PATH_KEYS
            if values.get(name, "").strip()
        ],
        ("TRPC_IM_PROBE_RUNNER", values.get("TRPC_IM_PROBE_RUNNER"), False, True, True),
        *[(name, values.get(name), False, True, True) for name in DRIVER_KEYS],
        *[
            (name, values.get(name), True, False, True)
            for name in CONTROL_PROFILE_PATH_KEYS.values()
        ],
    ]

    resolved_paths: dict[str, Path | None] = {}
    path_statuses: dict[str, str] = {}
    for name, raw_value, private, executable, outside_checkout in path_specs:
        path_check, resolved = _check_path(
            name,
            raw_value,
            mode=mode,
            checkout=checkout,
            private=private,
            executable=executable,
            outside_checkout=outside_checkout,
        )
        checks.append(path_check)
        resolved_paths[name] = resolved
        path_statuses[name] = str(path_check["status"])

    for artifact_name, hash_name in ARTIFACT_HASH_KEYS.items():
        checks.append(
            _artifact_binding_check(
                name=artifact_name,
                path=resolved_paths.get(artifact_name),
                path_status=path_statuses.get(artifact_name, "fail"),
                expected_sha256=values.get(hash_name, ""),
            )
        )

    try:
        broker_uid = int(values.get("TRPC_IM_PROBE_BROKER_UID", ""))
        broker_gid = int(values.get("TRPC_IM_PROBE_BROKER_GID", ""))
    except ValueError:
        broker_uid = None
        broker_gid = None
    if broker_uid is not None and broker_uid < 0:
        broker_uid = None
    if broker_gid is not None and broker_gid < 0:
        broker_gid = None
    current_euid = getattr(os, "geteuid", None)
    dedicated_uid = broker_uid is not None and (
        os.name == "nt" or (current_euid is not None and broker_uid != current_euid())
    )
    checks.append(
        _check(
            "control_broker_identity",
            "pass" if dedicated_uid and broker_gid is not None else "fail",
            None
            if dedicated_uid and broker_gid is not None
            else "control broker must use a valid dedicated uid and shared gid",
        )
    )

    socket_check, socket_path = _check_control_socket(
        values.get(CONTROL_SOCKET_KEY),
        mode=mode,
        checkout=checkout,
        expected_uid=broker_uid,
        expected_gid=broker_gid,
    )
    checks.append(socket_check)
    resolved_paths[CONTROL_SOCKET_KEY] = socket_path
    path_statuses[CONTROL_SOCKET_KEY] = str(socket_check["status"])

    for channel, profile_key in CONTROL_PROFILE_PATH_KEYS.items():
        checks.append(
            _control_profile_binding_check(
                channel=channel,
                profile=resolved_paths.get(profile_key),
                path_status=path_statuses.get(profile_key, "fail"),
                expected_sha256=values.get(CONTROL_PROFILE_HASH_KEYS[channel], ""),
            )
        )

    seed_status = path_statuses.get("TRPC_IM_PROBE_SIGNING_KEY_FILE", "fail")
    trust_status = str(trust_info.get("status", "fail"))
    checks.append(
        _seed_check(
            resolved_paths.get("TRPC_IM_PROBE_SIGNING_KEY_FILE"),
            status_from_path=(
                "not_run" if seed_status == "not_run" or trust_status == "not_run" else "fail"
            ),
            trusted_public_key=trusted_public_key,
        )
    )
    for name in SECRET_PATH_KEYS:
        path_status = path_statuses.get(name, "fail")
        checks.append(
            _secret_content_check(
                resolved_paths.get(name),
                name=name,
                status_from_path="not_run" if path_status == "not_run" else "fail",
            )
        )
    for name in OPTIONAL_SECRET_PATH_KEYS:
        if name in resolved_paths:
            path_status = path_statuses.get(name, "fail")
            checks.append(
                _secret_content_check(
                    resolved_paths.get(name),
                    name=name,
                    status_from_path="not_run" if path_status == "not_run" else "fail",
                )
            )

    failures = [check for check in checks if check.get("status") == "fail"]
    pending = [check for check in checks if check.get("status") == "not_run"]
    validation_gate = "pass" if not failures else "fail"
    if mode == "host":
        readiness = "pass" if not failures and not pending else "fail"
    else:
        readiness = "not_run"
    report: dict[str, Any] = {
        "schema_version": 1,
        "producer": PRODUCER,
        "generated_at": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "mode": mode,
        "readiness": readiness,
        "validation_gate": validation_gate,
        "gate": "not_run",
        "production_gate": "not_run",
        "network_calls": False,
        "production_evidence_written": False,
        "secrets_recorded": False,
        "paths_recorded": False,
        "checks": checks,
        "rejection_reasons": [
            f"{check['name']}: {check['reason']}"
            for check in checks
            if check.get("status") != "pass" and isinstance(check.get("reason"), str)
        ],
        "input_hashes": {
            "env_file_sha256": _sha256(env_raw) if env_raw is not None else None,
            "candidate_lock_sha256": _sha256(candidate_raw) if candidate_raw is not None else None,
            "trust_file_sha256": _sha256(trust_raw) if trust_raw is not None else None,
            "release_context_sha256": (
                _sha256(release_context_raw) if release_context_raw is not None else None
            ),
        },
    }
    return report


def _write_report(path: Path, report: Mapping[str, Any]) -> None:
    if _path_has_symlink(path):
        raise ValueError("output path or parent contains a symlink")
    path.write_text(
        json.dumps(dict(report), ensure_ascii=True, sort_keys=True, indent=2, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("local", "host"), default="local")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--candidate-lock", type=Path, default=DEFAULT_CANDIDATE_LOCK)
    parser.add_argument("--trust-file", type=Path, default=DEFAULT_TRUST_FILE)
    parser.add_argument("--checkout", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = build_preflight(
            args.env_file,
            mode=args.mode,
            candidate_lock=args.candidate_lock,
            trust_file=args.trust_file,
            checkout=args.checkout,
        )
        if args.output is not None:
            _write_report(args.output, report)
    except (OSError, ValueError):
        report = {
            "schema_version": 1,
            "producer": PRODUCER,
            "mode": args.mode,
            "readiness": "not_run" if args.mode == "local" else "fail",
            "validation_gate": "fail",
            "gate": "not_run",
            "production_gate": "not_run",
            "network_calls": False,
            "production_evidence_written": False,
            "secrets_recorded": False,
            "paths_recorded": False,
            "checks": [_check("execution", "fail", "preflight execution failed")],
            "rejection_reasons": ["execution: preflight execution failed"],
        }
        print(json.dumps(report, ensure_ascii=True, sort_keys=True, allow_nan=False))
        return 1
    print(json.dumps(report, ensure_ascii=True, sort_keys=True, allow_nan=False))
    if args.mode == "local":
        return 0 if report["validation_gate"] == "pass" else 1
    return 0 if report["readiness"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
