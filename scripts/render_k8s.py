"""Render all Kubernetes documents with one immutable application image digest."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

import yaml

IMAGE_PATTERN = re.compile(r"^[^\s@]+@sha256:[0-9a-fA-F]{64}$")


def render_documents(
    documents: list[dict[str, Any]],
    image: str,
    *,
    release_id: str | None = None,
) -> int:
    if not IMAGE_PATTERN.fullmatch(image):
        raise ValueError("image must be an immutable registry@sha256 digest")
    if release_id and not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?", release_id):
        raise ValueError("release_id must be a short lowercase DNS label")
    replacements = 0
    for document in documents:
        kind = document.get("kind")
        if kind not in {"Deployment", "StatefulSet", "DaemonSet", "Job"}:
            continue
        if kind == "Job" and release_id:
            document["metadata"]["name"] = (
                f"{document['metadata']['name']}-{release_id}"  # type: ignore[index]
            )[
                # type: ignore[index]
                :63
            ].rstrip("-")
        spec = document.get("spec", {})
        template = spec.get("template", {})
        pod_spec = template.get("spec", {})
        for container in (*pod_spec.get("initContainers", []), *pod_spec.get("containers", [])):
            if str(container.get("image", "")).startswith("tenant-agent-platform:"):
                container["image"] = image
                replacements += 1
    return replacements


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--release-id")
    args = parser.parse_args()
    documents = [
        document
        for document in yaml.safe_load_all(args.input.read_text(encoding="utf-8"))
        if document is not None
    ]
    try:
        replacements = render_documents(
            documents,
            args.image,
            release_id=args.release_id,
        )
    except ValueError as exc:
        parser.error(str(exc))
    if replacements == 0:
        parser.error("input did not contain an application workload image")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        yaml.safe_dump_all(documents, sort_keys=False),
        encoding="utf-8",
    )
    print(f"rendered {replacements} workload image(s) to {args.output}")


if __name__ == "__main__":
    main()
