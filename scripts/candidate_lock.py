#!/usr/bin/env python3
"""Freeze one source fingerprint and immutable registry image set for a release run."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_REPO_IMPORT_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_IMPORT_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_IMPORT_ROOT))

from scripts.evidence_lineage import (  # noqa: E402
    canonical_sha256,
    current_release_binding,
    source_fingerprint,
)
from scripts.report_io import atomic_write_json  # noqa: E402

ROOT = _REPO_IMPORT_ROOT
IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
REFERENCE_RE = re.compile(r"^[^\s:@]+(?::[0-9]+)?(?:/[^\s:@]+)+@sha256:[0-9a-f]{64}$")


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _validate_binding(
    binding: Mapping[str, Any], *, root: Path, require_current_release: bool = False
) -> list[str]:
    reasons: list[str] = []
    current_source = source_fingerprint(root)
    expected_release = current_release_binding(required=require_current_release)
    source = _mapping(binding.get("source_fingerprint"))
    if binding.get("kind") != "registry_candidate_binding":
        reasons.append("registry candidate binding kind is invalid")
    release = _mapping(binding.get("release_binding"))
    if (
        not isinstance(release.get("release_id"), str)
        or not release.get("release_id")
        or not isinstance(release.get("nonce_sha256"), str)
        or not re.fullmatch(r"[0-9a-f]{64}", str(release.get("nonce_sha256")))
    ):
        reasons.append("registry candidate binding release binding is invalid")
    elif expected_release is not None and binding.get("release_binding") != expected_release:
        reasons.append("registry candidate binding belongs to a different release")
    if (
        source.get("status") != "available"
        or current_source.get("status") != "available"
        or source.get("value") != current_source.get("value")
    ):
        reasons.append("registry candidate binding belongs to a different source checkout")
    image_digest = binding.get("image_digest")
    if not isinstance(image_digest, str) or IMAGE_RE.fullmatch(image_digest) is None:
        reasons.append("registry candidate binding image_digest is invalid")
    images = _mapping(binding.get("images"))
    if set(images) != {"initial", "upgrade"}:
        reasons.append("registry candidate binding image set is incomplete")
    for role in ("initial", "upgrade"):
        image = _mapping(images.get(role))
        digest = image.get("digest")
        reference = image.get("reference")
        if (
            not isinstance(digest, str)
            or IMAGE_RE.fullmatch(digest) is None
            or not isinstance(reference, str)
            or REFERENCE_RE.fullmatch(reference) is None
            or not reference.endswith("@" + digest)
        ):
            reasons.append(f"registry candidate binding {role} image is not immutable")
    if _mapping(images.get("initial")).get("digest") != image_digest:
        reasons.append("registry candidate binding primary digest is inconsistent")
    return reasons


def create_candidate_lock(
    binding: Mapping[str, Any], *, root: Path = ROOT, output: Path | None = None
) -> dict[str, Any]:
    """Create a content-addressed lock after rechecking the current checkout."""

    reasons = _validate_binding(binding, root=root, require_current_release=True)
    if reasons:
        raise ValueError("; ".join(reasons))
    lock = {
        "schema_version": 1,
        "kind": "release_candidate_lock",
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "release_binding": binding["release_binding"],
        "source_fingerprint": binding["source_fingerprint"],
        "binding_sha256": canonical_sha256(binding),
        "repository": binding["repository"],
        "image_digest": binding["image_digest"],
        "images": binding["images"],
    }
    if output is not None:
        atomic_write_json(output, lock)
    return lock


def verify_candidate_lock(
    lock: Mapping[str, Any], binding: Mapping[str, Any], *, root: Path = ROOT
) -> list[str]:
    reasons = _validate_binding(binding, root=root)
    if lock.get("schema_version") != 1 or lock.get("kind") != "release_candidate_lock":
        reasons.append("candidate lock schema or kind is invalid")
    if lock.get("binding_sha256") != canonical_sha256(binding):
        reasons.append("candidate lock binding content hash changed")
    for field in (
        "release_binding",
        "source_fingerprint",
        "repository",
        "image_digest",
        "images",
    ):
        if lock.get(field) != binding.get(field):
            reasons.append(f"candidate lock {field} changed")
    return list(dict.fromkeys(reasons))


def _read(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"candidate input is missing or a symlink: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"candidate input root is not an object: {path}")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("create", "verify"))
    parser.add_argument(
        "--binding",
        type=Path,
        default=Path("runs/multitenant/registry-image-binding.json"),
    )
    parser.add_argument("--lock", type=Path, default=Path("runs/multitenant/candidate-lock.json"))
    args = parser.parse_args(argv)
    try:
        binding = _read(args.binding)
        if args.command == "create":
            result = create_candidate_lock(binding, output=args.lock)
            print(json.dumps(result, indent=2))
            return 0
        reasons = verify_candidate_lock(_read(args.lock), binding)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as error:
        parser.error(str(error))
    result = {"gate": "fail" if reasons else "pass", "rejection_reasons": reasons}
    print(json.dumps(result, indent=2))
    return 1 if reasons else 0


if __name__ == "__main__":
    raise SystemExit(main())
