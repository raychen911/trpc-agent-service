#!/usr/bin/env python3
"""Publish one immutable candidate and bind it to one private release context."""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import shutil
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from scripts.candidate_lock import create_candidate_lock, verify_candidate_lock
from scripts.evidence_lineage import current_release_binding, source_fingerprint
from scripts.registry_image import (
    RegistryImageError,
    publish_candidate,
    registry_reference,
    validate_repository,
    validate_tag,
)
from scripts.release_context import ensure_release_context, public_binding
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
_RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def _mapping(value: object, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} is invalid")
    return value


@contextmanager
def _release_environment(release_id: str, nonce: str) -> Iterator[None]:
    previous_id = os.environ.get("TRPC_RELEASE_ID")
    previous_nonce = os.environ.get("TRPC_RELEASE_NONCE")
    os.environ["TRPC_RELEASE_ID"] = release_id
    os.environ["TRPC_RELEASE_NONCE"] = nonce
    try:
        yield
    finally:
        if previous_id is None:
            os.environ.pop("TRPC_RELEASE_ID", None)
        else:
            os.environ["TRPC_RELEASE_ID"] = previous_id
        if previous_nonce is None:
            os.environ.pop("TRPC_RELEASE_NONCE", None)
        else:
            os.environ["TRPC_RELEASE_NONCE"] = previous_nonce


@contextmanager
def _publication_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    _safe_existing_target(path)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise ValueError("candidate publication is already in progress") from error
    try:
        yield
    finally:
        os.close(descriptor)
        with suppress(FileNotFoundError):
            path.unlink()


def _replace_file(source: Path, target: Path) -> None:
    os.replace(source, target)


def _safe_existing_target(path: Path) -> None:
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise ValueError(f"candidate artifact target is unsafe: {path}")
    for parent in path.absolute().parents:
        if parent.exists() and parent.is_symlink():
            raise ValueError(f"candidate artifact parent is unsafe: {parent}")


def install_candidate_pair(
    binding: Mapping[str, Any],
    lock: Mapping[str, Any],
    *,
    output: Path,
    lock_output: Path,
) -> None:
    """Stage and install a binding/lock pair, restoring the old pair on failure."""

    output = output.absolute()
    lock_output = lock_output.absolute()
    if output == lock_output or output.parent != lock_output.parent:
        raise ValueError("candidate binding and lock must be distinct files in one directory")
    output.parent.mkdir(parents=True, exist_ok=True)
    _safe_existing_target(output)
    _safe_existing_target(lock_output)

    token = uuid4().hex
    staged_output = output.parent / f".{output.name}.{token}.stage"
    staged_lock = output.parent / f".{lock_output.name}.{token}.stage"
    backup_output = output.parent / f".{output.name}.{token}.backup"
    backup_lock = output.parent / f".{lock_output.name}.{token}.backup"
    output_existed = output.exists()
    lock_existed = lock_output.exists()
    output_installed = False
    lock_installed = False
    try:
        atomic_write_json(staged_output, binding)
        atomic_write_json(staged_lock, lock)
        if output_existed:
            shutil.copyfile(output, backup_output)
        if lock_existed:
            shutil.copyfile(lock_output, backup_lock)
        _replace_file(staged_output, output)
        output_installed = True
        _replace_file(staged_lock, lock_output)
        lock_installed = True
    except Exception:
        if lock_installed:
            if lock_existed:
                _replace_file(backup_lock, lock_output)
            else:
                lock_output.unlink(missing_ok=True)
        if output_installed:
            if output_existed:
                _replace_file(backup_output, output)
            else:
                output.unlink(missing_ok=True)
        raise
    finally:
        for path in (staged_output, staged_lock, backup_output, backup_lock):
            path.unlink(missing_ok=True)


def _binding_from_receipt(
    receipt: Mapping[str, Any],
    *,
    expected_source: str,
    root: Path,
) -> dict[str, Any]:
    current_source = source_fingerprint(root)
    receipt_source = _mapping(receipt.get("source_fingerprint"), name="receipt source")
    if (
        current_source.get("status") != "available"
        or current_source.get("value") != expected_source
        or receipt_source.get("status") != "available"
        or receipt_source.get("value") != expected_source
    ):
        raise ValueError("checkout source fingerprint changed after image publication")
    if receipt.get("kind") != "registry_candidate_binding":
        raise ValueError("published image receipt kind is invalid")
    if receipt.get("schema_version") != 1:
        raise ValueError("published image receipt schema is invalid")
    receipt_release = _mapping(receipt.get("release_binding"), name="receipt release binding")
    formal_release = current_release_binding(required=True)
    assert formal_release is not None
    if receipt_release.get("release_id") != formal_release["release_id"]:
        raise ValueError("published image receipt belongs to a different release")

    repository = validate_repository(str(receipt.get("repository", "")))
    images = _mapping(receipt.get("images"), name="published image set")
    if set(images) != {"initial", "upgrade"}:
        raise ValueError("published image receipt is incomplete")
    normalized_images: dict[str, dict[str, str]] = {}
    for role in ("initial", "upgrade"):
        image = _mapping(images.get(role), name=f"published {role} image")
        tag = validate_tag(str(image.get("tag", "")))
        reference = registry_reference(repository, str(image.get("digest", "")))
        if image.get("reference") != reference:
            raise ValueError(f"published {role} image reference is inconsistent")
        normalized_images[role] = {
            "tag": tag,
            "reference": reference,
            "digest": reference.rsplit("@", 1)[1],
        }
    if normalized_images["initial"]["digest"] == normalized_images["upgrade"]["digest"]:
        raise ValueError("initial and upgrade image digests must differ")
    if receipt.get("image_digest") != normalized_images["initial"]["digest"]:
        raise ValueError("published image receipt primary digest is inconsistent")
    return {
        "schema_version": 1,
        "kind": "registry_candidate_binding",
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "run_id": f"registry-image-{uuid4().hex}",
        "release_binding": formal_release,
        "source_fingerprint": current_source,
        "repository": repository,
        "image_digest": normalized_images["initial"]["digest"],
        "images": normalized_images,
    }


def finalize_published_candidate(
    receipt: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    expected_source: str,
    output: Path,
    lock_output: Path,
    root: Path = ROOT,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Rebind one published receipt to a verified context without rebuilding images."""

    binding = _binding_from_receipt(receipt, expected_source=expected_source, root=root)
    release = _mapping(binding["release_binding"], name="formal release binding")
    context_images = _mapping(context.get("images"), name="release context images")
    expected_context = {
        "release_id": release.get("release_id"),
        "nonce_sha256": release.get("nonce_sha256"),
        "source_fingerprint": expected_source,
        "images": {
            "initial": binding["images"]["initial"]["digest"],
            "upgrade": binding["images"]["upgrade"]["digest"],
        },
    }
    actual_context = {
        "release_id": context.get("release_id"),
        "nonce_sha256": context.get("nonce_sha256"),
        "source_fingerprint": context.get("source_fingerprint"),
        "images": dict(context_images),
    }
    if actual_context != expected_context:
        raise ValueError("release context does not match the published candidate")
    if context.get("context_id") != public_binding(context)["context_id"]:
        raise ValueError("release context identity hash is invalid")

    lock = create_candidate_lock(binding, root=root)
    reasons = verify_candidate_lock(lock, binding, root=root)
    if reasons:
        raise ValueError("; ".join(reasons))
    install_candidate_pair(binding, lock, output=output, lock_output=lock_output)
    return binding, lock


def publish_candidate_session(
    *,
    repository: str,
    output: Path,
    lock_output: Path,
    private_directory: Path,
    public_directory: Path,
    root: Path = ROOT,
    release_id: str | None = None,
) -> dict[str, Any]:
    """Build/push once, create the formal context, then install the final pair."""

    source = source_fingerprint(root)
    source_value = source.get("value")
    if source.get("status") != "available" or not isinstance(source_value, str):
        raise ValueError("current source fingerprint is unavailable")
    release_value = release_id or (
        f"release-{datetime.now(UTC):%Y%m%d}-{source_value[:8]}-{uuid4().hex[:8]}"
    )
    if _RELEASE_ID_RE.fullmatch(release_value) is None:
        raise ValueError("release id is invalid or unsafe for artifact paths")
    session_suffix = release_value.rsplit("-", 1)[-1]
    initial_tag = f"candidate-{source_value[:12]}-{session_suffix}"
    upgrade_tag = f"upgrade-{source_value[:12]}-{session_suffix}"
    private_directory = private_directory.absolute()
    public_directory = public_directory.absolute()
    receipt_path = private_directory / f"publish-receipt-{release_value}-amd64.json"
    context_path = private_directory / f"release-context-{release_value}-amd64.json"
    public_path = public_directory / f"release-context-binding-{release_value}-amd64.json"

    with _publication_lock(private_directory / ".candidate-publication.lock"):
        temporary_nonce = secrets.token_urlsafe(32)
        with _release_environment(release_value, temporary_nonce):
            receipt = publish_candidate(
                repository=repository,
                context=root,
                tag=initial_tag,
                upgrade_tag=upgrade_tag,
                output=receipt_path,
                lock_output=None,
            )
        receipt_source = _mapping(receipt.get("source_fingerprint"), name="receipt source")
        if receipt_source.get("value") != source_value:
            raise ValueError("published image receipt source does not match the session")
        receipt_images = _mapping(receipt.get("images"), name="published image set")
        initial = _mapping(receipt_images.get("initial"), name="published initial image")
        upgrade = _mapping(receipt_images.get("upgrade"), name="published upgrade image")
        context = ensure_release_context(
            context_path,
            release_id=release_value,
            source_fingerprint=source_value,
            initial_digest=str(initial.get("digest", "")),
            upgrade_digest=str(upgrade.get("digest", "")),
        )
        formal_nonce = context.get("nonce")
        if not isinstance(formal_nonce, str):
            raise ValueError("private release context nonce is invalid")
        atomic_write_json(public_path, public_binding(context))
        with _release_environment(release_value, formal_nonce):
            binding, lock = finalize_published_candidate(
                receipt,
                context,
                expected_source=source_value,
                output=output,
                lock_output=lock_output,
                root=root,
            )

    return {
        "release_id": release_value,
        "source_fingerprint": source_value,
        "initial_reference": binding["images"]["initial"]["reference"],
        "upgrade_reference": binding["images"]["upgrade"]["reference"],
        "binding_sha256": lock["binding_sha256"],
        "private_context": str(context_path),
        "public_context": str(public_path),
        "receipt": str(receipt_path),
        "binding": str(output.absolute()),
        "lock": str(lock_output.absolute()),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("publish", nargs="?", choices=("publish",))
    parser.add_argument("--repository", required=True)
    parser.add_argument("--context", type=Path, default=ROOT)
    parser.add_argument("--release-id")
    parser.add_argument(
        "--output", type=Path, default=Path("runs/multitenant/registry-image-binding.json")
    )
    parser.add_argument(
        "--lock-output", type=Path, default=Path("runs/multitenant/candidate-lock.json")
    )
    parser.add_argument(
        "--private-directory",
        type=Path,
        default=Path("runs/multitenant/.ack-runtime-private"),
    )
    parser.add_argument("--public-directory", type=Path, default=Path("runs/multitenant"))
    args = parser.parse_args(argv)
    try:
        result = publish_candidate_session(
            repository=args.repository,
            output=args.output,
            lock_output=args.lock_output,
            private_directory=args.private_directory,
            public_directory=args.public_directory,
            root=args.context,
            release_id=args.release_id,
        )
    except (OSError, UnicodeError, ValueError, RegistryImageError) as error:
        parser.error(str(error))
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
