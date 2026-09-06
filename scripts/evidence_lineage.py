"""Common primitives for release-bound evidence and source identity."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOTS = (
    "trpc_service",
    "scripts",
    "deployment",
    "Dockerfile",
    ".dockerignore",
    "alembic.ini",
    "README.md",
    "pyproject.toml",
    "uv.lock",
    "requirements.txt",
    "requirements-dev.txt",
)
EXCLUDED_PARTS = {".git", ".venv", "__pycache__", "build", "dist", "data", "runs"}


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_value(value: Any) -> str:
    return sha256_bytes(canonical_json(value))


def _relative(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _excluded(path: Path, root: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return True
    return any(part in EXCLUDED_PARTS for part in relative.parts)


def source_files(root: Path = ROOT) -> tuple[Path, ...]:
    paths: list[Path] = []
    for name in SOURCE_ROOTS:
        path = root / name
        if path.is_file() and not _excluded(path, root):
            paths.append(path)
            continue
        if not path.is_dir():
            continue
        for child in path.rglob("*"):
            if child.is_file() and not _excluded(child, root):
                paths.append(child)
    return tuple(sorted(set(paths), key=lambda item: _relative(item, root)))


def source_fingerprint(root: Path = ROOT) -> dict[str, Any]:
    records = [
        {
            "path": _relative(path, root),
            "size": path.stat().st_size,
            "sha256": sha256_bytes(path.read_bytes()),
        }
        for path in source_files(root)
    ]
    return {
        "algorithm": "sha256",
        "value": sha256_value(records),
        "file_count": len(records),
        "files": records,
    }


def fingerprint_projection(fingerprint: dict[str, Any]) -> dict[str, Any]:
    return {
        "algorithm": str(fingerprint.get("algorithm", "")),
        "value": str(fingerprint.get("value", "")),
        "file_count": int(fingerprint.get("file_count", 0)),
    }


def git_value(root: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=root,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"
    value = result.stdout.strip()
    return value if result.returncode == 0 and value else "unknown"


def repository_identity(root: Path = ROOT) -> dict[str, Any]:
    status = git_value(root, "status", "--porcelain")
    return {
        "branch": git_value(root, "branch", "--show-current"),
        "commit": git_value(root, "rev-parse", "HEAD"),
        "dirty": bool(status),
    }


def release_binding(
    context: dict[str, Any],
    candidate_lock: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fingerprint = context.get("source_fingerprint", {})
    binding: dict[str, Any] = {
        "release_id": context.get("release_id", ""),
        "context_sha256": context.get("context_sha256", ""),
        "source_fingerprint": fingerprint.get("value", ""),
    }
    if candidate_lock is not None:
        binding["candidate_lock_sha256"] = candidate_lock.get("lock_sha256", "")
        binding["image_digests"] = {
            name: value.get("digest", "")
            for name, value in candidate_lock.get("images", {}).items()
            if isinstance(value, dict)
        }
    return binding


def make_evidence(
    evidence_type: str,
    producer: str,
    payload: Any,
    binding: dict[str, Any],
    previous_evidence_sha256: str = "",
) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        "schema_version": 1,
        "evidence_type": evidence_type,
        "producer": producer,
        "generated_at": datetime.now(UTC).isoformat(),
        "release_binding": binding,
        "payload": payload,
        "payload_sha256": sha256_value(payload),
    }
    if previous_evidence_sha256:
        envelope["previous_evidence_sha256"] = previous_evidence_sha256
    envelope["evidence_sha256"] = sha256_value(envelope)
    if previous_evidence_sha256:
        envelope["chain_sha256"] = sha256_value(
            {
                "previous_evidence_sha256": previous_evidence_sha256,
                "evidence_sha256": envelope["evidence_sha256"],
            }
        )
    return envelope


def verify_evidence(envelope: dict[str, Any]) -> tuple[bool, str]:
    if envelope.get("schema_version") != 1:
        return False, "unsupported evidence schema"
    if not isinstance(envelope.get("release_binding"), dict):
        return False, "release binding is missing"
    payload = envelope.get("payload")
    if envelope.get("payload_sha256") != sha256_value(payload):
        return False, "payload hash does not match"
    expected_hash = envelope.get("evidence_sha256")
    unsigned = {key: value for key, value in envelope.items() if key not in {"evidence_sha256", "chain_sha256"}}
    if expected_hash != sha256_value(unsigned):
        return False, "evidence hash does not match"
    previous = envelope.get("previous_evidence_sha256", "")
    if previous:
        expected_chain = sha256_value(
            {
                "previous_evidence_sha256": previous,
                "evidence_sha256": expected_hash,
            }
        )
        if envelope.get("chain_sha256") != expected_chain:
            return False, "evidence chain hash does not match"
    return True, "ok"


def binding_matches(actual: dict[str, Any], expected: dict[str, Any]) -> tuple[bool, str]:
    for key, value in expected.items():
        if actual.get(key) != value:
            return False, f"release binding field {key!r} does not match"
    return True, "ok"


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
