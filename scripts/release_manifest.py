"""Aggregate release-bound reports into one verifiable release manifest."""

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
    sha256_value,
    verify_evidence,
    write_json,
)
from scripts.release_context import verify_context
from scripts.release_evidence import artifact_record
from scripts.supply_chain_gate import verify_report


def _status(report: dict[str, Any]) -> str:
    value = report.get("status")
    if value in {"pass", "fail", "not_run"}:
        return str(value)
    payload = report.get("payload")
    if isinstance(payload, dict) and payload.get("status") in {"pass", "fail", "not_run"}:
        return str(payload["status"])
    value = report.get("gate")
    return str(value) if value in {"pass", "fail", "not_run"} else "unknown"


def _verify_report_shape(report: dict[str, Any], expected_binding: dict[str, Any]) -> tuple[bool, str]:
    if report.get("gate") == "supply-chain":
        valid, reason = verify_report(report)
        if not valid:
            return False, reason
    elif "evidence_sha256" in report:
        valid, reason = verify_evidence(report)
        if not valid:
            return False, reason
    elif not isinstance(report.get("release_binding"), dict):
        return False, "report release binding is missing"
    return binding_matches(report["release_binding"], expected_binding)


def _report_record(path: Path, root: Path, expected_binding: dict[str, Any]) -> dict[str, Any]:
    record = artifact_record(path, root)
    report = read_json(path)
    valid, reason = _verify_report_shape(report, expected_binding)
    if not valid:
        raise ValueError(f"{record['path']}: {reason}")
    return {
        **record,
        "gate": str(report.get("gate", report.get("evidence_type", "unknown"))),
        "status": _status(report),
    }


def create_manifest(
    context_path: Path,
    lock_path: Path,
    report_paths: list[Path],
    output: Path,
    root: Path = ROOT,
    require_pass: bool = False,
    trust_key_file: Path | None = None,
    require_signature: bool = False,
) -> dict[str, Any]:
    context = verify_context(
        context_path,
        root=root,
        trust_key_file=trust_key_file,
        require_signature=require_signature,
    )
    lock = verify_lock(
        context_path,
        lock_path,
        root=root,
        trust_key_file=trust_key_file,
        require_signature=require_signature,
    )
    binding = release_binding(context, lock)
    if not report_paths:
        raise ValueError("at least one release report is required")
    reports = [_report_record(path if path.is_absolute() else root / path, root, binding) for path in report_paths]
    previous_chain = ""
    for report in reports:
        report["previous_chain_sha256"] = previous_chain
        previous_chain = sha256_value(
            {
                "previous_chain_sha256": previous_chain,
                "report_sha256": report["sha256"],
            }
        )
        report["chain_sha256"] = previous_chain
    if require_pass and any(item["status"] != "pass" for item in reports):
        raise ValueError("release manifest contains a report that is not pass")
    envelope = make_evidence(
        "release-manifest",
        "scripts.release_manifest",
        {"reports": reports, "require_pass": require_pass},
        binding,
    )
    write_json(output, envelope)
    return envelope


def verify_manifest(
    context_path: Path,
    lock_path: Path,
    manifest_path: Path,
    root: Path = ROOT,
    require_pass: bool = False,
    trust_key_file: Path | None = None,
    require_signature: bool = False,
) -> dict[str, Any]:
    context = verify_context(
        context_path,
        root=root,
        trust_key_file=trust_key_file,
        require_signature=require_signature,
    )
    lock = verify_lock(
        context_path,
        lock_path,
        root=root,
        trust_key_file=trust_key_file,
        require_signature=require_signature,
    )
    manifest = read_json(manifest_path)
    valid, reason = verify_evidence(manifest)
    if not valid:
        raise ValueError(reason)
    expected_binding = release_binding(context, lock)
    valid, reason = binding_matches(manifest.get("release_binding", {}), expected_binding)
    if not valid:
        raise ValueError(reason)
    payload = manifest.get("payload")
    if not isinstance(payload, dict) or not isinstance(payload.get("reports"), list) or not payload["reports"]:
        raise ValueError("release manifest reports are missing")
    previous_chain = ""
    for item in payload["reports"]:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise ValueError("release manifest report record is invalid")
        current = _report_record(root / item["path"], root, expected_binding)
        stored_identity = {
            key: value
            for key, value in item.items()
            if key not in {"previous_chain_sha256", "chain_sha256"}
        }
        if current != stored_identity:
            raise ValueError(f"release report changed: {item['path']}")
        expected_chain = sha256_value(
            {
                "previous_chain_sha256": previous_chain,
                "report_sha256": item["sha256"],
            }
        )
        if item.get("previous_chain_sha256") != previous_chain or item.get("chain_sha256") != expected_chain:
            raise ValueError(f"release report chain changed: {item['path']}")
        previous_chain = expected_chain
        if require_pass and item.get("status") != "pass":
            raise ValueError(f"release report is not pass: {item['path']}")
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create")
    create.add_argument("--context", type=Path, required=True)
    create.add_argument("--lock", type=Path, required=True)
    create.add_argument("--report", type=Path, action="append", required=True)
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--root", type=Path, default=ROOT)
    create.add_argument("--require-pass", action="store_true")
    create.add_argument("--trust-key", type=Path)
    create.add_argument("--require-signature", action="store_true")
    verify = subparsers.add_parser("verify")
    verify.add_argument("--context", type=Path, required=True)
    verify.add_argument("--lock", type=Path, required=True)
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--root", type=Path, default=ROOT)
    verify.add_argument("--require-pass", action="store_true")
    verify.add_argument("--trust-key", type=Path)
    verify.add_argument("--require-signature", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "create":
            manifest = create_manifest(
                args.context,
                args.lock,
                args.report,
                args.output,
                args.root,
                args.require_pass,
                args.trust_key,
                args.require_signature,
            )
            print(
                json.dumps(
                    {
                        "status": "pass",
                        "evidence_sha256": manifest["evidence_sha256"],
                        "path": str(args.output),
                    }
                )
            )
        else:
            manifest = verify_manifest(
                args.context,
                args.lock,
                args.manifest,
                args.root,
                args.require_pass,
                args.trust_key,
                args.require_signature,
            )
            print(json.dumps({"status": "pass", "evidence_sha256": manifest["evidence_sha256"]}))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "fail", "error": str(exc)}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
