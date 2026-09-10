"""Create and verify a release-bound artifact evidence bundle."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# Support both module and direct script execution.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.candidate_lock import verify_lock
from scripts.evidence_lineage import (
    ROOT,
    binding_matches,
    make_evidence,
    read_json,
    release_binding,
    sha256_bytes,
    verify_evidence,
    write_json,
)
from scripts.release_context import verify_context


def _safe_path(path: Path, root: Path) -> str:
    if path.is_symlink():
        raise ValueError(f"evidence artifact must not be a symlink: {path}")
    resolved_root = root.resolve()
    resolved = path.resolve(strict=True)
    try:
        relative = resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"evidence artifact is outside the evidence root: {path}") from exc
    if any(part in {".git", ".venv", "__pycache__"} for part in relative.parts):
        raise ValueError(f"evidence artifact is in a forbidden directory: {path}")
    if not resolved.is_file():
        raise ValueError(f"evidence artifact is not a regular file: {path}")
    return relative.as_posix()


def artifact_record(path: Path, root: Path = ROOT) -> dict[str, Any]:
    relative = _safe_path(path, root)
    resolved = root.resolve() / relative
    raw = resolved.read_bytes()
    return {
        "path": relative,
        "size": len(raw),
        "sha256": sha256_bytes(raw),
    }


def _artifact_paths(payload: dict[str, Any]) -> list[dict[str, Any]]:
    value = payload.get("artifacts")
    if not isinstance(value, list) or not value:
        raise ValueError("evidence payload must contain a non-empty artifacts list")
    if any(not isinstance(item, dict) for item in value):
        raise ValueError("evidence artifact records must be objects")
    return value


def create_bundle(
    context_path: Path,
    lock_path: Path,
    artifact_paths: list[Path],
    output: Path,
    root: Path = ROOT,
) -> dict[str, Any]:
    context = verify_context(context_path, root=root)
    lock = verify_lock(context_path, lock_path, root=root)
    artifacts = [artifact_record(path if path.is_absolute() else root / path, root) for path in artifact_paths]
    envelope = make_evidence(
        "release-artifacts",
        "scripts.release_evidence",
        {"artifacts": artifacts},
        release_binding(context, lock),
    )
    write_json(output, envelope)
    return envelope


def verify_bundle(
    context_path: Path,
    lock_path: Path,
    bundle_path: Path,
    root: Path = ROOT,
) -> dict[str, Any]:
    context = verify_context(context_path, root=root)
    lock = verify_lock(context_path, lock_path, root=root)
    envelope = read_json(bundle_path)
    valid, reason = verify_evidence(envelope)
    if not valid:
        raise ValueError(reason)
    expected_binding = release_binding(context, lock)
    matches, reason = binding_matches(envelope["release_binding"], expected_binding)
    if not matches:
        raise ValueError(reason)
    for record in _artifact_paths(envelope["payload"]):
        path_value = record.get("path")
        if not isinstance(path_value, str) or Path(path_value).is_absolute():
            raise ValueError("evidence artifact path must be relative")
        current = artifact_record(root / path_value, root)
        if current != record:
            raise ValueError(f"evidence artifact changed: {path_value}")
    return envelope


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create")
    create.add_argument("--context", type=Path, required=True)
    create.add_argument("--lock", type=Path, required=True)
    create.add_argument("--artifact", type=Path, action="append", required=True)
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--root", type=Path, default=ROOT)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--context", type=Path, required=True)
    verify.add_argument("--lock", type=Path, required=True)
    verify.add_argument("--bundle", type=Path, required=True)
    verify.add_argument("--root", type=Path, default=ROOT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "create":
            bundle = create_bundle(args.context, args.lock, args.artifact, args.output, args.root)
            print(
                json.dumps(
                    {
                        "status": "pass",
                        "evidence_sha256": bundle["evidence_sha256"],
                        "path": str(args.output),
                    }
                )
            )
        else:
            bundle = verify_bundle(args.context, args.lock, args.bundle, args.root)
            print(json.dumps({"status": "pass", "evidence_sha256": bundle["evidence_sha256"]}))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "fail", "error": str(exc)}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
