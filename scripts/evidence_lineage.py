#!/usr/bin/env python3
"""Small, dependency-free helpers for candidate evidence lineage.

The release gate and the real performance gate both need to answer the same
question: does a report describe the checkout and runtime that are being
evaluated now?  Keeping the framing here avoids subtly different fingerprints
being accepted by the two scripts.

Only standard-library types are used intentionally.  The helpers are also
conservative: symlinks are never followed, source enumeration is bounded, and
runtime identities are represented by hashes rather than persisted verbatim.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

SOURCE_FINGERPRINT_ROOTS: tuple[str, ...] = (
    "Dockerfile",
    ".dockerignore",
    "alembic.ini",
    "README.md",
    "pyproject.toml",
    "uv.lock",
    "docker-compose.yml",
    "deploy",
    "migrations",
    "scripts",
    "tests/integration",
    "trpc_service",
)
FINGERPRINT_MAX_FILES = 10_000
FINGERPRINT_MAX_BYTES = 128 * 1024 * 1024
FINGERPRINT_IGNORED_DIRS = frozenset(
    {
        "__pycache__",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".cache",
        "cache",
        "runs",
        "secrets",
    }
)
FINGERPRINT_IGNORED_SUFFIXES = frozenset({".pyc", ".pyo"})
# These files are operator-local runtime inputs rather than candidate source
# inputs.  Keep this list exact: broad deploy-directory exclusions would allow
# release-relevant manifests to drift without changing the candidate binding.
FINGERPRINT_IGNORED_FILES = frozenset(
    {
        "deploy/runtime-gate.yaml",
    }
)
EVIDENCE_SCHEMA_VERSION = 1
EVIDENCE_KIND = "current_candidate"
DEFAULT_EVIDENCE_TTL_SECONDS = 24 * 60 * 60
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_RUN_NONCE_RE = re.compile(r"^[A-Za-z0-9._:-]{16,128}$")
_RELEASE_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{32,256}$")


def current_release_binding(*, required: bool = False) -> dict[str, str] | None:
    """Return the release binding configured for the current execution.

    The raw release nonce is deliberately never returned.  Only its SHA-256
    digest is exposed to report builders and validators.  A completely absent
    binding is allowed for non-production/offline reports when ``required`` is
    false; a partial or malformed binding is always rejected so a live gate
    cannot silently fall back to an unbound candidate.
    """

    release_id = os.getenv("TRPC_RELEASE_ID", "").strip()
    release_nonce = os.getenv("TRPC_RELEASE_NONCE", "").strip()
    if not release_id and not release_nonce:
        if required:
            raise ValueError(
                "TRPC_RELEASE_ID and TRPC_RELEASE_NONCE are required for current-candidate evidence"
            )
        return None
    if (
        _RUN_ID_RE.fullmatch(release_id) is None
        or _RELEASE_NONCE_RE.fullmatch(release_nonce) is None
    ):
        if required:
            raise ValueError(
                "TRPC_RELEASE_ID and TRPC_RELEASE_NONCE must be valid for "
                "current-candidate evidence"
            )
        return None
    return {
        "release_id": release_id,
        "nonce_sha256": hashlib.sha256(release_nonce.encode("utf-8")).hexdigest(),
    }


def validate_release_binding(
    evidence: Any,
    *,
    expected: Mapping[str, str] | None = None,
    required: bool = True,
) -> list[str]:
    """Validate the release ID/nonce digest carried by a report envelope.

    ``expected`` is normally the binding recorded in a release manifest.  If
    omitted, only the shape is checked; callers that need to compare against
    the current operator environment can pass ``current_release_binding``.
    """

    binding = evidence.get("release_binding") if isinstance(evidence, Mapping) else None
    if binding is None:
        return ["production evidence release_binding is missing"] if required else []
    if not isinstance(binding, Mapping):
        return ["production evidence release_binding is invalid"]
    release_id = binding.get("release_id")
    nonce_sha256 = binding.get("nonce_sha256")
    if _RUN_ID_RE.fullmatch(release_id or "") is None:
        return ["production evidence release_binding.release_id is invalid"]
    if not _valid_sha256(nonce_sha256):
        return ["production evidence release_binding.nonce_sha256 is invalid"]
    if expected is not None:
        if binding.get("release_id") != expected.get("release_id"):
            return ["production evidence release_id does not match expected release"]
        if binding.get("nonce_sha256") != expected.get("nonce_sha256"):
            return ["production evidence nonce_sha256 does not match expected release"]
    return []


def _is_ignored_path(relative_path: Path) -> bool:
    normalized_path = relative_path.as_posix().lower()
    if normalized_path in FINGERPRINT_IGNORED_FILES:
        return True
    return any(part.lower() in FINGERPRINT_IGNORED_DIRS for part in relative_path.parts)


def _unavailable(
    reason: str,
    roots: Sequence[str],
    *,
    max_files: int,
    max_bytes: int,
    error_type: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "algorithm": "sha256",
        "status": "unavailable",
        "reason": reason,
        "file_count_limit": max_files,
        "byte_limit": max_bytes,
        "included_roots": list(roots),
    }
    if error_type is not None:
        result["error_type"] = error_type
    return result


def source_fingerprint(
    root: Path,
    relative_roots: Sequence[str] = SOURCE_FINGERPRINT_ROOTS,
    *,
    max_files: int = FINGERPRINT_MAX_FILES,
    max_bytes: int = FINGERPRINT_MAX_BYTES,
) -> dict[str, Any]:
    """Return a bounded content-addressed fingerprint for candidate inputs.

    Paths are framed with their relative path and content length before their
    bytes are hashed.  A second stat after reading detects a concurrent edit so
    a report cannot silently contain a mixed snapshot.  Symlink roots and
    entries are skipped, as are generated Python bytecode/cache files.
    """

    digest = hashlib.sha256()
    files: list[tuple[Path, int, int]] = []
    total_bytes = 0

    for relative_root in relative_roots:
        candidate_root = root / relative_root
        if _is_ignored_path(Path(relative_root)):
            continue
        if candidate_root.is_symlink():
            continue
        if candidate_root.is_file():
            candidates: Sequence[Path] = (candidate_root,)
        elif candidate_root.is_dir():
            try:
                candidates = tuple(candidate_root.rglob("*"))
            except OSError as error:
                return _unavailable(
                    "source_walk_failed",
                    relative_roots,
                    max_files=max_files,
                    max_bytes=max_bytes,
                    error_type=type(error).__name__,
                )
        else:
            continue

        for candidate in candidates:
            relative_candidate = candidate.relative_to(root)
            if _is_ignored_path(relative_candidate):
                continue
            if candidate.is_symlink():
                continue
            if (
                not candidate.is_file()
                or relative_candidate.suffix.lower() in FINGERPRINT_IGNORED_SUFFIXES
            ):
                continue
            try:
                stat = candidate.stat()
            except OSError as error:
                return _unavailable(
                    "source_stat_failed",
                    relative_roots,
                    max_files=max_files,
                    max_bytes=max_bytes,
                    error_type=type(error).__name__,
                )
            if len(files) >= max_files:
                return _unavailable(
                    "source_file_count_limit_exceeded",
                    relative_roots,
                    max_files=max_files,
                    max_bytes=max_bytes,
                )
            if total_bytes + stat.st_size > max_bytes:
                return _unavailable(
                    "source_byte_limit_exceeded",
                    relative_roots,
                    max_files=max_files,
                    max_bytes=max_bytes,
                )
            files.append((candidate, stat.st_size, stat.st_mtime_ns))
            total_bytes += stat.st_size

    files.sort(key=lambda item: item[0].relative_to(root).as_posix())
    for path, expected_size, expected_mtime_ns in files:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        try:
            content = path.read_bytes()
            current_stat = path.stat()
        except OSError as error:
            return _unavailable(
                "source_read_failed",
                relative_roots,
                max_files=max_files,
                max_bytes=max_bytes,
                error_type=type(error).__name__,
            )
        if (
            len(content) != expected_size
            or current_stat.st_size != expected_size
            or current_stat.st_mtime_ns != expected_mtime_ns
        ):
            return _unavailable(
                "source_changed_during_fingerprint",
                relative_roots,
                max_files=max_files,
                max_bytes=max_bytes,
            )
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)

    return {
        "algorithm": "sha256",
        "status": "available",
        "value": digest.hexdigest(),
        "file_count": len(files),
        "total_bytes": total_bytes,
        "file_count_limit": max_files,
        "byte_limit": max_bytes,
        "included_roots": list(relative_roots),
    }


def canonical_sha256(value: Any) -> str:
    """Hash JSON data with stable framing and no permissive NaN encoding."""

    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _worker_identity_projection(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _worker_identity_projection(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_worker_identity_projection(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return type(value).__name__


def runtime_fingerprint(
    *,
    mode: str | None,
    worker_identities: Sequence[Any] | None,
    stream: str | None,
    group: str | None,
    parameters: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Hash runtime identity inputs while retaining only safe metadata.

    The returned object never contains worker IDs, stream names, group names,
    DSNs, tenant IDs, or tokens.  It contains hashes of those inputs plus a
    small amount of non-sensitive context useful for diagnosis.
    """

    if not mode or not worker_identities or not stream or not group or parameters is None:
        return {
            "algorithm": "sha256",
            "status": "unavailable",
            "reason": "runtime_not_available",
        }

    projected_workers = _worker_identity_projection(list(worker_identities))
    worker_hash = canonical_sha256(projected_workers)
    stream_group_hash = canonical_sha256({"group": group, "stream": stream})
    parameter_hash = canonical_sha256(dict(parameters))
    raw_mode = str(mode)
    safe_mode = raw_mode
    if (
        len(raw_mode) > 80
        or not re.fullmatch(r"[A-Za-z0-9_.:-]+", raw_mode)
        or any(
            marker in raw_mode.lower()
            for marker in ("dsn", "token", "secret", "password", "tenant", "session")
        )
    ):
        safe_mode = "custom"
    material = {
        "mode": raw_mode,
        "worker_identity_summary_sha256": worker_hash,
        "stream_group_sha256": stream_group_hash,
        "parameters_sha256": parameter_hash,
    }
    return {
        "algorithm": "sha256",
        "status": "available",
        "value": canonical_sha256(material),
        "mode": safe_mode,
        "worker_count": len(worker_identities),
        "worker_identity_summary_sha256": worker_hash,
        "stream_group_sha256": stream_group_hash,
        "parameters_sha256": parameter_hash,
    }


def _utc_timestamp(value: datetime | None = None) -> str:
    timestamp = value or datetime.now(UTC)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    timestamp = timestamp.astimezone(UTC)
    return timestamp.isoformat(timespec="seconds").replace("+00:00", "Z")


def new_run_id(producer: str) -> str:
    """Create an opaque, report-local run ID without embedding environment data."""

    prefix = re.sub(r"[^A-Za-z0-9_.:-]+", "-", producer).strip("-._:") or "evidence"
    return f"{prefix}-{uuid4().hex}"


def _safe_runtime_fingerprint(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep only the allowlisted, non-identity runtime evidence fields."""

    if value is None:
        return {
            "algorithm": "sha256",
            "status": "unavailable",
            "reason": "runtime_not_available",
        }
    result: dict[str, Any] = {}
    for key in (
        "algorithm",
        "status",
        "value",
        "mode",
        "worker_count",
        "worker_identity_summary_sha256",
        "stream_group_sha256",
        "parameters_sha256",
        "reason",
    ):
        if key in value:
            item = value[key]
            if key == "mode":
                if isinstance(item, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,80}", item):
                    if not any(
                        marker in item.lower()
                        for marker in ("dsn", "token", "secret", "password", "tenant", "session")
                    ):
                        result[key] = item
                continue
            if key == "worker_count":
                if isinstance(item, int) and not isinstance(item, bool) and 0 <= item <= 10_000:
                    result[key] = item
                continue
            if key.endswith("sha256") or key == "value":
                if _valid_sha256(item):
                    result[key] = item
                continue
            if key in {"algorithm", "status", "reason"} and isinstance(item, str):
                result[key] = item[:128]
    return result


def build_evidence(
    *,
    root: Path,
    producer: str,
    run_id: str | None = None,
    generated_at: datetime | None = None,
    runtime: Mapping[str, Any] | None = None,
    source_roots: Sequence[str] = SOURCE_FINGERPRINT_ROOTS,
    max_files: int = FINGERPRINT_MAX_FILES,
    max_bytes: int = FINGERPRINT_MAX_BYTES,
) -> dict[str, Any]:
    """Build the versioned current-candidate evidence envelope."""

    source = source_fingerprint(
        root,
        source_roots,
        max_files=max_files,
        max_bytes=max_bytes,
    )
    runtime_value = _safe_runtime_fingerprint(runtime)
    evidence_run_id = run_id if isinstance(run_id, str) and _RUN_ID_RE.fullmatch(run_id) else None
    result = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "kind": EVIDENCE_KIND,
        "producer": producer,
        "generated_at": _utc_timestamp(generated_at),
        "run_id": evidence_run_id or new_run_id(producer),
        "run_nonce": uuid4().hex,
        "source_fingerprint": source,
        "runtime_fingerprint": runtime_value,
    }
    release_binding = current_release_binding()
    if release_binding is not None:
        result["release_binding"] = release_binding
    return result


def _valid_sha256(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def validate_current_candidate_evidence(
    evidence: Any,
    *,
    current_source: Mapping[str, Any],
    expected_release_binding: Mapping[str, str] | None = None,
    require_release_binding: bool = True,
    now: datetime | None = None,
    ttl_seconds: int = DEFAULT_EVIDENCE_TTL_SECONDS,
) -> list[str]:
    """Return release-gate reasons for invalid or stale production evidence."""

    if not isinstance(evidence, Mapping):
        return ["production evidence is missing current-candidate lineage"]
    schema_version = evidence.get("schema_version")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != EVIDENCE_SCHEMA_VERSION
    ):
        return ["production evidence schema_version is missing or unsupported"]
    if evidence.get("kind") != EVIDENCE_KIND:
        return ["production evidence is not marked current_candidate"]
    producer = evidence.get("producer")
    if not isinstance(producer, str) or not producer.strip():
        return ["production evidence producer is missing"]

    run_id = evidence.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip() or _RUN_ID_RE.fullmatch(run_id) is None:
        return ["production evidence run_id is missing or invalid"]

    run_nonce = evidence.get("run_nonce")
    if not isinstance(run_nonce, str) or _RUN_NONCE_RE.fullmatch(run_nonce) is None:
        return ["production evidence run_nonce is missing or invalid"]

    generated_at = evidence.get("generated_at")
    if not isinstance(generated_at, str) or not generated_at.strip():
        return ["production evidence generated_at is missing or invalid"]
    try:
        parsed = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError:
        return ["production evidence generated_at is missing or invalid"]
    if parsed.tzinfo is None:
        return ["production evidence generated_at is missing or invalid"]
    parsed = parsed.astimezone(UTC)
    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    if parsed > current_time:
        return ["production evidence generated_at is in the future"]
    if ttl_seconds < 0 or current_time - parsed >= timedelta(seconds=ttl_seconds):
        return ["production evidence has expired"]

    recorded_source = evidence.get("source_fingerprint")
    if (
        not isinstance(recorded_source, Mapping)
        or recorded_source.get("algorithm") != "sha256"
        or recorded_source.get("status") != "available"
        or not _valid_sha256(recorded_source.get("value"))
    ):
        return ["production evidence source fingerprint is missing or invalid"]
    if (
        current_source.get("algorithm") != "sha256"
        or current_source.get("status") != "available"
        or not _valid_sha256(current_source.get("value"))
    ):
        return ["current candidate source fingerprint is unavailable"]
    if recorded_source.get("value") != current_source.get("value"):
        return ["production evidence source fingerprint belongs to a different candidate"]

    recorded_runtime = evidence.get("runtime_fingerprint")
    if (
        not isinstance(recorded_runtime, Mapping)
        or recorded_runtime.get("algorithm") != "sha256"
        or recorded_runtime.get("status") != "available"
        or not _valid_sha256(recorded_runtime.get("value"))
    ):
        return ["production evidence runtime fingerprint is unavailable"]
    binding_reasons = validate_release_binding(
        evidence,
        expected=expected_release_binding,
        required=require_release_binding,
    )
    if binding_reasons:
        return binding_reasons
    return []
