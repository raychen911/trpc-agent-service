"""Split one rendered ACK manifest into ordered migration/runtime batches."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from scripts.kubernetes_runtime_gate import (
    _schema_head_check_manifest,
    _split_migration_manifests,
)


def split_stage1_manifests(rendered: str, *, namespace: str) -> tuple[str, str, str]:
    """Return migration, schema-head-check, and runtime manifests in apply order."""

    migration, runtime = _split_migration_manifests(rendered)
    head_check = yaml.safe_dump(
        _schema_head_check_manifest(migration, namespace=namespace),
        sort_keys=False,
    )
    return migration, head_check, runtime


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--migration-output", type=Path, required=True)
    parser.add_argument("--head-check-output", type=Path, required=True)
    parser.add_argument("--runtime-output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    rendered = sys.stdin.read()
    migration, head_check, runtime = split_stage1_manifests(
        rendered,
        namespace=str(args.namespace),
    )
    args.migration_output.write_text(migration, encoding="utf-8")
    args.head_check_output.write_text(head_check, encoding="utf-8")
    args.runtime_output.write_text(runtime, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
