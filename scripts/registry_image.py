#!/usr/bin/env python3
"""Build and publish a source-attested candidate with registry digest evidence.

The command intentionally keeps registry credentials in Docker's credential
helper and never prints Docker output.  A successful publish emits only the
immutable ``repository@sha256:...`` references and a release-lineage report.
It does not deploy Kubernetes resources or promote a release by itself.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from scripts.evidence_lineage import (
    SOURCE_FINGERPRINT_ROOTS,
    current_release_binding,
    source_fingerprint,
)
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
SOURCE_LABEL = "io.trpc.agent-service.source-fingerprint"
IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
REPOSITORY_COMPONENT_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*[a-z0-9]$")
TAG_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$")
PUSH_DIGEST_RE = re.compile(r"\bdigest:\s*(sha256:[0-9a-f]{64})\b", re.IGNORECASE)


class RegistryImageError(RuntimeError):
    """Raised when an image cannot be built, published, or attested safely."""


def _valid_digest(value: object) -> bool:
    return isinstance(value, str) and IMAGE_DIGEST_RE.fullmatch(value.lower()) is not None


def validate_repository(value: str) -> str:
    """Validate a repository name without accepting tags, credentials, or URLs."""

    repository = value.strip()
    if (
        not repository
        or repository != repository.lower()
        or "@" in repository
        or "://" in repository
        or any(character.isspace() for character in repository)
    ):
        raise ValueError("repository must be a lowercase image repository without tag or digest")

    components = repository.split("/")
    if len(components) < 2 or any(not component for component in components):
        raise ValueError("repository contains an empty path component")
    first = components[0]
    had_port = ":" in first
    if ":" in first:
        host, port = first.rsplit(":", 1)
        if (
            not host
            or not port.isdecimal()
            or not 1 <= int(port) <= 65535
            or REPOSITORY_COMPONENT_RE.fullmatch(host) is None
        ):
            raise ValueError("registry port must be a decimal value between 1 and 65535")
        components[0] = host
    else:
        host = first
    if "." not in host and not had_port and host != "localhost":
        raise ValueError("repository must include an explicit registry host")
    if any(":" in component for component in components[1:]):
        raise ValueError("repository must not contain a tag")
    if any(
        component in {".", ".."} or REPOSITORY_COMPONENT_RE.fullmatch(component) is None
        for component in components
    ):
        raise ValueError("repository contains an invalid path component")
    return repository


def validate_tag(value: str) -> str:
    tag = value.strip()
    if TAG_RE.fullmatch(tag) is None:
        raise ValueError("image tag is invalid")
    return tag


def registry_reference(repository: str, digest: str) -> str:
    """Return an immutable registry reference after validating both parts."""

    normalized_repository = validate_repository(repository)
    normalized_digest = digest.lower()
    if not _valid_digest(normalized_digest):
        raise ValueError("registry digest must match sha256:<64 lowercase hex>")
    return f"{normalized_repository}@{normalized_digest}"


def _docker_executable() -> str:
    executable = shutil.which("docker")
    if executable is None:
        raise RegistryImageError("docker CLI is not installed")
    return executable


def _docker(
    arguments: Sequence[str], *, context: Path | None = None
) -> subprocess.CompletedProcess[str]:
    """Run Docker without exposing its output or environment-backed secrets."""

    executable = _docker_executable()
    try:
        result = subprocess.run(  # noqa: S603 - executable and fixed argument list
            [executable, *arguments],
            cwd=str(context) if context is not None else None,
            check=False,
            capture_output=True,
            text=True,
            timeout=1800,
        )
    except subprocess.TimeoutExpired as error:
        raise RegistryImageError("docker command timed out") from error
    except OSError as error:
        raise RegistryImageError("docker command could not be started") from error
    if result.returncode != 0:
        operation = arguments[0] if arguments else "command"
        raise RegistryImageError(f"docker {operation} failed with exit code {result.returncode}")
    return result


def _source_label(image: str, *, source: str) -> None:
    result = _docker(("image", "inspect", image, "--format", "{{json .Config.Labels}}"))
    try:
        labels = json.loads(result.stdout.strip() or "null")
    except json.JSONDecodeError as error:
        raise RegistryImageError("candidate image labels are not valid JSON") from error
    if not isinstance(labels, Mapping) or labels.get(SOURCE_LABEL) != source:
        raise RegistryImageError("candidate image source fingerprint label does not match checkout")


def _inspect_registry_digest(image: str) -> str | None:
    result = _docker(("image", "inspect", image, "--format", "{{json .RepoDigests}}"))
    try:
        references = json.loads(result.stdout.strip() or "null")
    except json.JSONDecodeError as error:
        raise RegistryImageError("Docker image digest metadata is not valid JSON") from error
    if not isinstance(references, list):
        return None
    for reference in references:
        if not isinstance(reference, str) or "@" not in reference:
            continue
        digest = reference.rsplit("@", 1)[-1].lower()
        if _valid_digest(digest):
            return digest
    return None


def _push_digest(image: str) -> str:
    result = _docker(("push", image))
    output = f"{result.stdout}\n{result.stderr}"
    matches: list[str] = PUSH_DIGEST_RE.findall(output)
    if matches:
        return matches[-1].lower()
    digest = _inspect_registry_digest(image)
    if digest is None:
        raise RegistryImageError("registry push completed without an immutable manifest digest")
    return digest


def _build_image(
    *,
    repository: str,
    tag: str,
    source: str,
    context: Path,
    role: str | None = None,
) -> str:
    image = f"{repository}:{tag}"
    arguments = [
        "build",
        "--pull=false",
        "--provenance=false",
        "--platform",
        "linux/amd64",
        "--build-arg",
        f"TRPC_SOURCE_FINGERPRINT={source}",
        "--tag",
        image,
    ]
    if role is not None:
        arguments.extend(("--label", f"io.trpc.agent-service.release-role={role}"))
    arguments.append(str(context))
    _docker(arguments)
    _source_label(image, source=source)
    return image


def publish_candidate(
    *,
    repository: str,
    context: Path = ROOT,
    tag: str | None = None,
    upgrade_tag: str | None = None,
    output: Path | None = None,
    lock_output: Path | None = None,
) -> dict[str, Any]:
    """Build/push initial and rollout images and return their safe binding report."""

    normalized_repository = validate_repository(repository)
    binding = current_release_binding(required=True)
    source = source_fingerprint(context, SOURCE_FINGERPRINT_ROOTS)
    source_value = source.get("value")
    if source.get("status") != "available" or not isinstance(source_value, str):
        raise RegistryImageError("current source fingerprint is unavailable")
    if not context.is_dir():
        raise RegistryImageError("build context is not a directory")
    initial_tag = validate_tag(tag or f"candidate-{source_value[:12]}")
    rollout_tag = validate_tag(upgrade_tag or f"upgrade-{source_value[:12]}")
    if initial_tag == rollout_tag:
        raise RegistryImageError("initial and upgrade image tags must differ")

    initial_image = _build_image(
        repository=normalized_repository,
        tag=initial_tag,
        source=source_value,
        context=context,
    )
    initial_digest = _push_digest(initial_image)
    upgrade_image = _build_image(
        repository=normalized_repository,
        tag=rollout_tag,
        source=source_value,
        context=context,
        role="upgrade",
    )
    upgrade_digest = _push_digest(upgrade_image)
    if initial_digest == upgrade_digest:
        raise RegistryImageError("initial and upgrade registry digests must differ")
    source_after = source_fingerprint(context, SOURCE_FINGERPRINT_ROOTS)
    if source_after.get("status") != "available" or source_after.get("value") != source_value:
        raise RegistryImageError(
            "checkout changed during image build/push; discard the candidate binding and restart"
        )

    report: dict[str, Any] = {
        "schema_version": 1,
        "kind": "registry_candidate_binding",
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "run_id": f"registry-image-{uuid4().hex}",
        "release_binding": binding,
        "source_fingerprint": source,
        "repository": normalized_repository,
        "image_digest": initial_digest,
        "images": {
            "initial": {
                "tag": initial_tag,
                "reference": registry_reference(normalized_repository, initial_digest),
                "digest": initial_digest,
            },
            "upgrade": {
                "tag": rollout_tag,
                "reference": registry_reference(normalized_repository, upgrade_digest),
                "digest": upgrade_digest,
            },
        },
    }
    if output is not None:
        atomic_write_json(output, report)
    if lock_output is not None:
        from scripts.candidate_lock import create_candidate_lock

        create_candidate_lock(report, root=context, output=lock_output)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("publish", nargs="?", help="publish a candidate image")
    parser.add_argument("--repository", required=True, help="lowercase registry repository")
    parser.add_argument("--tag", help="initial image tag")
    parser.add_argument("--upgrade-tag", help="rollout image tag")
    parser.add_argument("--context", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runs/multitenant/registry-image-binding.json"),
    )
    parser.add_argument(
        "--lock-output",
        type=Path,
        default=Path("runs/multitenant/candidate-lock.json"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.publish not in {None, "publish"}:
        parser.error("only the publish command is supported")
    try:
        report = publish_candidate(
            repository=args.repository,
            context=args.context,
            tag=args.tag,
            upgrade_tag=args.upgrade_tag,
            output=args.output,
            lock_output=args.lock_output,
        )
    except (RegistryImageError, ValueError) as error:
        parser.error(str(error))
    print(
        json.dumps(
            {
                "repository": report["repository"],
                "source_fingerprint": report["source_fingerprint"]["value"],
                "image_digest": report["image_digest"],
                "initial_reference": report["images"]["initial"]["reference"],
                "upgrade_reference": report["images"]["upgrade"]["reference"],
                "output": str(args.output),
                "candidate_lock": str(args.lock_output),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
