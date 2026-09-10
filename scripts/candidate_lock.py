"""Bind immutable initial and upgrade image references to a release context."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Support both ``python -m scripts.candidate_lock`` and direct execution.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evidence_lineage import (
    canonical_json,
    read_json,
    sha256_bytes,
    write_json,
)
from scripts.release_context import verify_context

IMAGE_RE = re.compile(r"^(?P<repository>[A-Za-z0-9][A-Za-z0-9./:_-]*)@(?P<digest>sha256:[0-9a-f]{64})$")


def parse_image_reference(value: str) -> dict[str, str]:
    match = IMAGE_RE.fullmatch(value.strip())
    if match is None:
        raise ValueError("image reference must be an immutable repository@sha256:<64-hex> value")
    return {
        "reference": value.strip(),
        "repository": match.group("repository"),
        "digest": match.group("digest"),
    }


def _unsigned_lock(lock: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in lock.items() if key != "lock_sha256"}


def _image_record(value: object, name: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"candidate lock {name} image record is invalid")
    reference = value.get("reference")
    if not isinstance(reference, str):
        raise ValueError(f"candidate lock {name} image reference is invalid")
    return parse_image_reference(reference)


def create_lock(
    context_path: Path,
    initial_image: str,
    upgrade_image: str,
    output: Path,
    allow_same_image: bool = False,
    root: Path = Path(__file__).resolve().parents[1],
    trust_key_file: Path | None = None,
    require_signature: bool = False,
) -> dict[str, Any]:
    context = verify_context(
        context_path,
        root=root,
        trust_key_file=trust_key_file,
        require_signature=require_signature,
    )
    initial = parse_image_reference(initial_image)
    upgrade = parse_image_reference(upgrade_image)
    if not allow_same_image and initial["digest"] == upgrade["digest"]:
        raise ValueError("initial and upgrade image digests must differ")
    lock: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "release_binding": {
            "release_id": context["release_id"],
            "context_sha256": context["context_sha256"],
            "source_fingerprint": context["source_fingerprint"]["value"],
        },
        "images": {
            "initial": initial,
            "upgrade": upgrade,
        },
    }
    lock["lock_sha256"] = sha256_bytes(canonical_json(_unsigned_lock(lock)))
    write_json(output, lock)
    return lock


def verify_lock(
    context_path: Path,
    lock_path: Path,
    allow_same_image: bool = False,
    root: Path = Path(__file__).resolve().parents[1],
    trust_key_file: Path | None = None,
    require_signature: bool = False,
) -> dict[str, Any]:
    context = verify_context(
        context_path,
        root=root,
        trust_key_file=trust_key_file,
        require_signature=require_signature,
    )
    lock = read_json(lock_path)
    if lock.get("schema_version") != 1:
        raise ValueError("unsupported candidate lock schema")
    if lock.get("lock_sha256") != sha256_bytes(canonical_json(_unsigned_lock(lock))):
        raise ValueError("candidate lock hash does not match")
    binding = lock.get("release_binding")
    expected_binding = {
        "release_id": context["release_id"],
        "context_sha256": context["context_sha256"],
        "source_fingerprint": context["source_fingerprint"]["value"],
    }
    if binding != expected_binding:
        raise ValueError("candidate lock release binding does not match the release context")
    images = lock.get("images")
    if not isinstance(images, dict) or set(images) != {"initial", "upgrade"}:
        raise ValueError("candidate lock must contain initial and upgrade images")
    initial = _image_record(images["initial"], "initial")
    upgrade = _image_record(images["upgrade"], "upgrade")
    for name, expected in (("initial", initial), ("upgrade", upgrade)):
        if images[name] != expected:
            raise ValueError(f"candidate lock {name} image projection does not match its immutable reference")
    if not allow_same_image and initial["digest"] == upgrade["digest"]:
        raise ValueError("initial and upgrade image digests must differ")
    return lock


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create")
    create.add_argument("--context", type=Path, required=True)
    create.add_argument("--initial-image", required=True)
    create.add_argument("--upgrade-image", required=True)
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--allow-same-image", action="store_true")
    create.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    verify = subparsers.add_parser("verify")
    verify.add_argument("--context", type=Path, required=True)
    verify.add_argument("--lock", type=Path, required=True)
    verify.add_argument("--allow-same-image", action="store_true")
    verify.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    verify.add_argument("--trust-key", type=Path)
    verify.add_argument("--require-signature", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "create":
            lock = create_lock(
                args.context,
                args.initial_image,
                args.upgrade_image,
                args.output,
                allow_same_image=args.allow_same_image,
                root=args.root,
            )
            print(json.dumps({"status": "pass", "lock_sha256": lock["lock_sha256"], "path": str(args.output)}))
        else:
            lock = verify_lock(
                args.context,
                args.lock,
                allow_same_image=args.allow_same_image,
                root=args.root,
                trust_key_file=args.trust_key,
                require_signature=args.require_signature,
            )
            print(json.dumps({"status": "pass", "lock_sha256": lock["lock_sha256"]}))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "fail", "error": str(exc)}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
