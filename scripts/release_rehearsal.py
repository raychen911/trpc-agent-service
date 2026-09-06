"""Build and verify a non-production release evidence rehearsal."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

# Support both ``python -m scripts.release_rehearsal`` and direct execution.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.candidate_lock import create_lock
from scripts.evidence_lineage import ROOT, make_evidence, read_json, release_binding, write_json
from scripts.release_context import create_context, create_trust_key
from scripts.release_manifest import create_manifest, verify_manifest
from scripts.supply_chain_gate import evaluate

DEFAULT_INITIAL_IMAGE = "registry.example/trpc-agent-service@sha256:" + "a" * 64
DEFAULT_UPGRADE_IMAGE = "registry.example/trpc-agent-service@sha256:" + "b" * 64


def _inside_root(path: Path, root: Path) -> Path:
    resolved = path if path.is_absolute() else root / path
    resolved = resolved.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"rehearsal output must be inside the repository: {path}") from exc
    return resolved


def _quality_evidence(
    quality_path: Path | None,
    output: Path,
    binding: dict[str, Any],
) -> Path:
    if quality_path is None:
        payload: dict[str, Any] = {
            "status": "not_run",
            "reason": "quality gate report was not provided",
        }
    else:
        quality = read_json(quality_path)
        payload = {
            "status": "pass" if quality.get("ok") is True else "fail",
            "quality_gate": quality,
        }
    evidence = make_evidence(
        "quality-gate",
        "scripts.quality_gate",
        payload,
        binding,
    )
    path = output / "quality-evidence.json"
    write_json(path, evidence)
    return path


def create_rehearsal(
    output: Path,
    *,
    root: Path = ROOT,
    release_id: str = "ci-release-rehearsal",
    quality_path: Path | None = None,
    initial_image: str = DEFAULT_INITIAL_IMAGE,
    upgrade_image: str = DEFAULT_UPGRADE_IMAGE,
    allow_dirty: bool = False,
) -> dict[str, Any]:
    root = root.resolve()
    output = _inside_root(output, root)
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="trpc-release-rehearsal-") as key_directory:
        key_root = Path(key_directory)
        private_key = Ed25519PrivateKey.generate()
        private_key_path = key_root / "release-signing-key.pem"
        public_key_path = key_root / "release-public-key.pem"
        private_key_path.write_bytes(
            private_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        public_key_path.write_bytes(
            private_key.public_key().public_bytes(
                serialization.Encoding.PEM,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        )
        context = create_context(
            root=root,
            release_id=release_id,
            allow_dirty=allow_dirty,
            signing_key_file=private_key_path,
            key_id="rehearsal",
        )
        trust_path = output / "release-trust-key.json"
        create_trust_key(public_key_path, "rehearsal", trust_path)
    context_path = output / "release-context.json"
    write_json(context_path, context)
    lock_path = output / "candidate-lock.json"
    lock = create_lock(
        context_path,
        initial_image,
        upgrade_image,
        lock_path,
        root=root,
        trust_key_file=trust_path,
        require_signature=True,
    )
    quality_path = quality_path.resolve() if quality_path is not None else None
    quality_evidence_path = _quality_evidence(
        quality_path,
        output,
        release_binding(context, lock),
    )
    supply_path = output / "supply-chain.json"
    supply = evaluate(
        context_path=context_path,
        lock_path=lock_path,
        require_production=False,
        trust_key_file=trust_path,
        require_signature=True,
    )
    write_json(supply_path, supply)
    manifest_path = output / "release-manifest.json"
    manifest = create_manifest(
        context_path,
        lock_path,
        [quality_evidence_path, supply_path],
        manifest_path,
        root=root,
        require_pass=False,
        trust_key_file=trust_path,
        require_signature=True,
    )
    verify_manifest(
        context_path,
        lock_path,
        manifest_path,
        root=root,
        require_pass=False,
        trust_key_file=trust_path,
        require_signature=True,
    )
    if not isinstance(manifest.get("payload"), dict):
        raise ValueError("rehearsal manifest payload is invalid")
    return {
        "status": "pass",
        "release_id": context["release_id"],
        "context": str(context_path),
        "candidate_lock": str(lock_path),
        "trust_key": str(trust_path),
        "quality_evidence": str(quality_evidence_path),
        "supply_chain": str(supply_path),
        "manifest": str(manifest_path),
        "supply_chain_status": supply["status"],
        "manifest_evidence_sha256": manifest["evidence_sha256"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--release-id", default="ci-release-rehearsal")
    parser.add_argument("--quality-report", type=Path)
    parser.add_argument("--initial-image", default=DEFAULT_INITIAL_IMAGE)
    parser.add_argument("--upgrade-image", default=DEFAULT_UPGRADE_IMAGE)
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = create_rehearsal(
            args.output,
            root=args.root,
            release_id=args.release_id,
            quality_path=args.quality_report,
            initial_image=args.initial_image,
            upgrade_image=args.upgrade_image,
            allow_dirty=args.allow_dirty,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "fail", "error": str(exc)}))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
