#!/usr/bin/env python3
"""Create or resume one private release nonce without exposing it in evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import stat
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from scripts.evidence_lineage import canonical_sha256
from scripts.report_io import atomic_write_json

SCHEMA_VERSION = 1
KIND = "private_release_context"
PUBLIC_KIND = "release_context_binding"
MAX_CONTEXT_BYTES = 16 * 1024
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{32,256}$")


def _identity(
    *, release_id: str, source_fingerprint: str, initial_digest: str, upgrade_digest: str
) -> dict[str, Any]:
    values: dict[str, Any] = {
        "release_id": release_id.strip(),
        "source_fingerprint": source_fingerprint.strip().lower(),
        "images": {
            "initial": initial_digest.strip().lower(),
            "upgrade": upgrade_digest.strip().lower(),
        },
    }
    if _RUN_ID_RE.fullmatch(values["release_id"]) is None:
        raise ValueError("release id is invalid")
    if _SHA256_RE.fullmatch(values["source_fingerprint"]) is None:
        raise ValueError("source fingerprint is invalid")
    images = values["images"]
    if any(_IMAGE_RE.fullmatch(value) is None for value in images.values()):
        raise ValueError("release image digest is invalid")
    if images["initial"] == images["upgrade"]:
        raise ValueError("initial and upgrade image digests must differ")
    return values


def _private_path(path: Path) -> Path:
    candidate = path.expanduser().absolute()
    if not candidate.name or candidate.name in {".", ".."}:
        raise ValueError("private release context path is invalid")
    parent = candidate.parent
    if parent.exists() and (parent.is_symlink() or not parent.is_dir()):
        raise ValueError("private release context parent is unsafe")
    if candidate.exists() and (candidate.is_symlink() or not candidate.is_file()):
        raise ValueError("private release context is not a regular file")
    return candidate


def _read_private(path: Path) -> dict[str, Any]:
    safe_path = _private_path(path)
    try:
        descriptor = os.open(
            safe_path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as error:
        raise ValueError("private release context cannot be opened safely") from error
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode) or details.st_size > MAX_CONTEXT_BYTES:
            raise ValueError("private release context size or type is invalid")
        with os.fdopen(descriptor, "r", encoding="utf-8", closefd=False) as handle:
            value = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("private release context is invalid JSON") from error
    finally:
        os.close(descriptor)
    if not isinstance(value, dict):
        raise ValueError("private release context root is invalid")
    return value


def _validate_context(context: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    if context.get("schema_version") != SCHEMA_VERSION or context.get("kind") != KIND:
        raise ValueError("private release context schema or kind is invalid")
    actual_identity = {
        "release_id": context.get("release_id"),
        "source_fingerprint": context.get("source_fingerprint"),
        "images": context.get("images"),
    }
    if actual_identity != expected:
        raise ValueError("private release context belongs to a different candidate")
    nonce = context.get("nonce")
    nonce_sha256 = context.get("nonce_sha256")
    if not isinstance(nonce, str) or _NONCE_RE.fullmatch(nonce) is None:
        raise ValueError("private release context nonce is invalid")
    expected_hash = hashlib.sha256(nonce.encode("utf-8")).hexdigest()
    if nonce_sha256 != expected_hash:
        raise ValueError("private release context nonce hash is invalid")
    public = public_binding(context)
    if context.get("context_id") != public["context_id"]:
        raise ValueError("private release context identity hash is invalid")


def public_binding(context: Mapping[str, Any]) -> dict[str, Any]:
    """Return the auditable portion of a context, excluding the raw nonce."""

    material = {
        "schema_version": SCHEMA_VERSION,
        "kind": PUBLIC_KIND,
        "release_id": context.get("release_id"),
        "nonce_sha256": context.get("nonce_sha256"),
        "source_fingerprint": context.get("source_fingerprint"),
        "images": context.get("images"),
    }
    return {**material, "context_id": canonical_sha256(material)}


def _write_private(path: Path, context: Mapping[str, Any]) -> None:
    safe_path = _private_path(path)
    safe_path.parent.mkdir(parents=True, exist_ok=True)
    if safe_path.parent.is_symlink():
        raise ValueError("private release context parent is unsafe")
    temporary = safe_path.with_name(f".{safe_path.name}.{uuid4().hex}.tmp")
    descriptor: int | None = None
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        payload = json.dumps(context, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        with os.fdopen(descriptor, "w", encoding="utf-8", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.close(descriptor)
        descriptor = None
        os.replace(temporary, safe_path)
        with suppress(OSError):
            os.chmod(safe_path, 0o600)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        with suppress(OSError):
            temporary.unlink()


@contextmanager
def _creation_lock(path: Path) -> Iterator[None]:
    lock_path = path.with_name(f".{path.name}.lock")
    if lock_path.is_symlink():
        raise ValueError("private release context lock is unsafe")
    try:
        descriptor = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise ValueError("private release context creation is already in progress") from error
    except OSError as error:
        raise ValueError("private release context lock cannot be created") from error
    try:
        yield
    finally:
        os.close(descriptor)
        with suppress(OSError):
            lock_path.unlink()


def ensure_release_context(
    path: Path,
    *,
    release_id: str,
    source_fingerprint: str,
    initial_digest: str,
    upgrade_digest: str,
) -> dict[str, Any]:
    """Create the private context once, or return the matching existing context."""

    expected = _identity(
        release_id=release_id,
        source_fingerprint=source_fingerprint,
        initial_digest=initial_digest,
        upgrade_digest=upgrade_digest,
    )
    safe_path = _private_path(path)
    if safe_path.exists():
        loaded = _read_private(safe_path)
        _validate_context(loaded, expected)
        return loaded
    safe_path.parent.mkdir(parents=True, exist_ok=True)
    with _creation_lock(safe_path):
        if safe_path.exists():
            loaded = _read_private(safe_path)
            _validate_context(loaded, expected)
            return loaded
        nonce = secrets.token_urlsafe(32)
        nonce_sha256 = hashlib.sha256(nonce.encode("utf-8")).hexdigest()
        context: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "kind": KIND,
            "created_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            **expected,
            "nonce": nonce,
            "nonce_sha256": nonce_sha256,
        }
        context["context_id"] = public_binding(context)["context_id"]
        _write_private(safe_path, context)
        return context


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("ensure", "verify"))
    parser.add_argument("--private-context", type=Path, required=True)
    parser.add_argument("--public-output", type=Path)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--source-fingerprint", required=True)
    parser.add_argument("--initial-digest", required=True)
    parser.add_argument("--upgrade-digest", required=True)
    args = parser.parse_args(argv)
    options = {
        "release_id": args.release_id,
        "source_fingerprint": args.source_fingerprint,
        "initial_digest": args.initial_digest,
        "upgrade_digest": args.upgrade_digest,
    }
    try:
        context = (
            ensure_release_context(args.private_context, **options)
            if args.command == "ensure"
            else _read_private(args.private_context)
        )
        _validate_context(context, _identity(**options))
        public = public_binding(context)
        if args.public_output is not None:
            atomic_write_json(args.public_output, public)
    except (OSError, UnicodeError, ValueError) as error:
        parser.error(str(error))
    print(json.dumps(public, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
