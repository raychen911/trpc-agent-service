"""Create and verify a source-bound release context."""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import platform
import re
import secrets
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

# Support both ``python -m scripts.release_context`` and the documented
# ``python scripts/release_context.py`` invocation.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evidence_lineage import (
    ROOT,
    canonical_json,
    fingerprint_projection,
    git_value,
    read_json,
    repository_identity,
    sha256_bytes,
    source_fingerprint,
    write_json,
)

RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")


def _validate_release_id(value: str) -> str:
    if not RELEASE_ID_RE.fullmatch(value):
        raise ValueError("release id must contain only letters, digits, '.', '_' or '-'")
    return value


def _unsigned_context(context: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in context.items()
        if key not in {"context_sha256", "signature_attestation"}
    }


def _load_private_key(path: Path) -> Ed25519PrivateKey:
    value = serialization.load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(value, Ed25519PrivateKey):
        raise ValueError("release signing key must be an Ed25519 private key")
    return value


def _load_public_key(path: Path) -> Ed25519PublicKey:
    raw = path.read_bytes()
    try:
        value = serialization.load_pem_public_key(raw)
    except (TypeError, ValueError):
        try:
            value = Ed25519PublicKey.from_public_bytes(base64.b64decode(raw, validate=True))
        except (binascii.Error, ValueError, TypeError) as exc:
            raise ValueError("release trust key must be PEM or base64-encoded Ed25519 public key") from exc
    if not isinstance(value, Ed25519PublicKey):
        raise ValueError("release trust key must be an Ed25519 public key")
    return value


def _validate_key_id(value: str) -> str:
    if not value or not re.fullmatch(r"[A-Za-z0-9_.:/@+-]{1,128}", value):
        raise ValueError("release signing key id is invalid")
    return value


def _signature_attestation(
    context_sha256: str,
    signing_key_file: Path,
    key_id: str,
) -> dict[str, str]:
    _validate_key_id(key_id)
    private_key = _load_private_key(signing_key_file)
    signature = private_key.sign(context_sha256.encode("ascii"))
    return {
        "algorithm": "ed25519",
        "key_id": key_id,
        "signature": base64.b64encode(signature).decode("ascii"),
    }


def _verify_signature(
    context: dict[str, Any],
    trust_key_file: Path | None,
    require_signature: bool,
) -> None:
    attestation = context.get("signature_attestation")
    if attestation is None:
        if require_signature:
            raise ValueError("signed release context is required")
        return
    if not isinstance(attestation, dict):
        raise ValueError("release signature attestation is invalid")
    if attestation.get("algorithm") != "ed25519":
        raise ValueError("release signature algorithm is unsupported")
    key_id = attestation.get("key_id")
    encoded = attestation.get("signature")
    if not isinstance(key_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:/@+-]{1,128}", key_id):
        raise ValueError("release signature key id is invalid")
    if not isinstance(encoded, str):
        raise ValueError("release signature is missing")
    try:
        signature = base64.b64decode(encoded, validate=True)
    except (binascii.Error, TypeError, ValueError) as exc:
        raise ValueError("release signature is not valid base64") from exc
    if len(signature) != 64:
        raise ValueError("release signature has an invalid length")
    if trust_key_file is None:
        if require_signature:
            raise ValueError("release trust key is required for signed production verification")
        return
    trust = read_json(trust_key_file)
    if trust.get("schema_version") != 1:
        raise ValueError("unsupported release trust key schema")
    if trust.get("algorithm") != "ed25519" or trust.get("key_id") != key_id:
        raise ValueError("release signature identity does not match the trust key")
    public_key_value = trust.get("public_key")
    if not isinstance(public_key_value, str):
        raise ValueError("release trust key public_key is missing")
    try:
        public_key_bytes = base64.b64decode(public_key_value, validate=True)
        expected_public_key_sha256 = trust.get("public_key_sha256")
        if expected_public_key_sha256 is not None and expected_public_key_sha256 != sha256_bytes(public_key_bytes):
            raise ValueError("release trust key public_key hash does not match")
        public_key = Ed25519PublicKey.from_public_bytes(public_key_bytes)
        public_key.verify(signature, str(context["context_sha256"]).encode("ascii"))
    except (binascii.Error, InvalidSignature, ValueError, TypeError) as exc:
        raise ValueError("release context signature verification failed") from exc


def create_trust_key(
    public_key_file: Path,
    key_id: str,
    output: Path,
) -> dict[str, Any]:
    """Create the checked-in/out-of-band trust-key document used by CI."""

    selected_key_id = _validate_key_id(key_id)
    public_key = _load_public_key(public_key_file)
    public_key_bytes = public_key.public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    document = {
        "schema_version": 1,
        "algorithm": "ed25519",
        "key_id": selected_key_id,
        "public_key": base64.b64encode(public_key_bytes).decode("ascii"),
        "public_key_sha256": sha256_bytes(public_key_bytes),
    }
    write_json(output, document)
    return document


def create_context(
    root: Path = ROOT,
    release_id: str | None = None,
    allow_dirty: bool = False,
    signing_key_file: Path | None = None,
    key_id: str = "",
) -> dict[str, Any]:
    identity = repository_identity(root)
    if identity["dirty"] and not allow_dirty:
        raise ValueError("release context requires a clean working tree; use --allow-dirty only for local rehearsal")
    fingerprint = source_fingerprint(root)
    selected_release_id = _validate_release_id(release_id or f"local-{datetime.now(UTC):%Y%m%d%H%M%S}")
    context: dict[str, Any] = {
        "schema_version": 1,
        "release_id": selected_release_id,
        "nonce": secrets.token_urlsafe(24),
        "created_at": datetime.now(UTC).isoformat(),
        "repository": identity,
        "source_fingerprint": fingerprint_projection(fingerprint),
        "toolchain": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "git": git_value(root, "--version"),
        },
    }
    context["context_sha256"] = sha256_bytes(canonical_json(_unsigned_context(context)))
    if signing_key_file is not None:
        context["signature_attestation"] = _signature_attestation(
            context["context_sha256"], signing_key_file, key_id
        )
    return context


def verify_context(
    path: Path,
    root: Path = ROOT,
    expected_release_id: str | None = None,
    expected_source_fingerprint: str | None = None,
    require_clean: bool = False,
    check_current_source: bool = True,
    trust_key_file: Path | None = None,
    require_signature: bool = False,
) -> dict[str, Any]:
    context = read_json(path)
    if context.get("schema_version") != 1:
        raise ValueError("unsupported release context schema")
    release_id = context.get("release_id")
    if not isinstance(release_id, str) or not RELEASE_ID_RE.fullmatch(release_id):
        raise ValueError("release context release_id is invalid")
    if expected_release_id and release_id != expected_release_id:
        raise ValueError("release context release_id does not match the requested release")
    fingerprint = context.get("source_fingerprint")
    if not isinstance(fingerprint, dict) or fingerprint.get("algorithm") != "sha256":
        raise ValueError("release context source fingerprint is invalid")
    value = fingerprint.get("value")
    if not isinstance(value, str) or not HEX64_RE.fullmatch(value):
        raise ValueError("release context source fingerprint value is invalid")
    if not isinstance(context.get("nonce"), str) or len(context["nonce"]) < 16:
        raise ValueError("release context nonce is invalid")
    if context.get("context_sha256") != sha256_bytes(canonical_json(_unsigned_context(context))):
        raise ValueError("release context hash does not match")
    _verify_signature(context, trust_key_file, require_signature)
    identity = repository_identity(root)
    if require_clean and identity["dirty"]:
        raise ValueError("current working tree is dirty")
    if check_current_source:
        current = source_fingerprint(root)["value"]
        if current != value:
            raise ValueError("release context source fingerprint does not match the current checkout")
    if expected_source_fingerprint and value != expected_source_fingerprint:
        raise ValueError("release context source fingerprint does not match the requested value")
    return context


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create")
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--release-id", default="")
    create.add_argument("--allow-dirty", action="store_true")
    create.add_argument("--root", type=Path, default=ROOT)
    create.add_argument("--signing-key", type=Path)
    create.add_argument("--key-id", default="")
    verify = subparsers.add_parser("verify")
    verify.add_argument("--context", type=Path, required=True)
    verify.add_argument("--release-id", default="")
    verify.add_argument("--source-fingerprint", default="")
    verify.add_argument("--require-clean", action="store_true")
    verify.add_argument("--skip-current-source-check", action="store_true")
    verify.add_argument("--root", type=Path, default=ROOT)
    verify.add_argument("--trust-key", type=Path)
    verify.add_argument("--require-signature", action="store_true")
    export = subparsers.add_parser("export-trust-key")
    export.add_argument("--public-key", type=Path, required=True)
    export.add_argument("--key-id", required=True)
    export.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "create":
            context = create_context(
                release_id=args.release_id or None,
                allow_dirty=args.allow_dirty,
                root=args.root,
                signing_key_file=args.signing_key,
                key_id=args.key_id,
            )
            write_json(args.output, context)
            print(json.dumps({"status": "pass", "release_id": context["release_id"], "path": str(args.output)}))
        elif args.command == "verify":
            context = verify_context(
                args.context,
                root=args.root,
                expected_release_id=args.release_id or None,
                expected_source_fingerprint=args.source_fingerprint or None,
                require_clean=args.require_clean,
                check_current_source=not args.skip_current_source_check,
                trust_key_file=args.trust_key,
                require_signature=args.require_signature,
            )
            print(json.dumps({"status": "pass", "release_id": context["release_id"]}))
        else:
            document = create_trust_key(args.public_key, args.key_id, args.output)
            print(
                json.dumps(
                    {
                        "status": "pass",
                        "key_id": document["key_id"],
                        "public_key_sha256": document["public_key_sha256"],
                        "path": str(args.output),
                    }
                )
            )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "fail", "error": str(exc)}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
