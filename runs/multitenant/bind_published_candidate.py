#!/usr/bin/env python3
# ruff: noqa: E402
"""Bind already-published immutable images to the current release checkout."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.candidate_lock import create_candidate_lock
from scripts.candidate_session import install_candidate_pair
from scripts.evidence_lineage import current_release_binding, source_fingerprint
from scripts.registry_image import registry_reference, validate_repository, validate_tag

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def bind_published_candidate(
    *,
    expected_source: str,
    repository: str,
    initial_tag: str,
    initial_digest: str,
    upgrade_tag: str,
    upgrade_digest: str,
    output: Path,
    lock_output: Path,
    root: Path = ROOT,
) -> dict[str, Any]:
    """Write one candidate binding after rechecking source and release identity."""

    source_value = expected_source.strip().lower()
    if _SHA256_RE.fullmatch(source_value) is None:
        raise ValueError("expected source fingerprint is invalid")
    repository_value = validate_repository(repository)
    initial_tag_value = validate_tag(initial_tag)
    upgrade_tag_value = validate_tag(upgrade_tag)
    if initial_tag_value == upgrade_tag_value:
        raise ValueError("initial and upgrade image tags must differ")
    initial_reference = registry_reference(repository_value, initial_digest)
    upgrade_reference = registry_reference(repository_value, upgrade_digest)
    initial_digest_value = initial_reference.rsplit("@", 1)[1]
    upgrade_digest_value = upgrade_reference.rsplit("@", 1)[1]
    if initial_digest_value == upgrade_digest_value:
        raise ValueError("initial and upgrade image digests must differ")

    source = source_fingerprint(root)
    if source.get("status") != "available" or source.get("value") != source_value:
        raise ValueError("checkout source fingerprint does not match the published images")
    binding: dict[str, Any] = {
        "schema_version": 1,
        "kind": "registry_candidate_binding",
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "run_id": f"registry-image-{uuid4().hex}",
        "release_binding": current_release_binding(required=True),
        "source_fingerprint": source,
        "repository": repository_value,
        "image_digest": initial_digest_value,
        "images": {
            "initial": {
                "tag": initial_tag_value,
                "reference": initial_reference,
                "digest": initial_digest_value,
            },
            "upgrade": {
                "tag": upgrade_tag_value,
                "reference": upgrade_reference,
                "digest": upgrade_digest_value,
            },
        },
    }
    lock = create_candidate_lock(binding, root=root)
    install_candidate_pair(binding, lock, output=output, lock_output=lock_output)
    return binding


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-source", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--initial-tag", required=True)
    parser.add_argument("--initial-digest", required=True)
    parser.add_argument("--upgrade-tag", required=True)
    parser.add_argument("--upgrade-digest", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--lock-output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        binding = bind_published_candidate(
            expected_source=args.expected_source,
            repository=args.repository,
            initial_tag=args.initial_tag,
            initial_digest=args.initial_digest,
            upgrade_tag=args.upgrade_tag,
            upgrade_digest=args.upgrade_digest,
            output=args.output,
            lock_output=args.lock_output,
        )
    except (OSError, UnicodeError, ValueError) as error:
        parser.error(str(error))
    print(
        json.dumps(
            {
                "source_fingerprint": binding["source_fingerprint"]["value"],
                "initial": binding["images"]["initial"]["reference"],
                "upgrade": binding["images"]["upgrade"]["reference"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
