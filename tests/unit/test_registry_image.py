from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from scripts import registry_image


def _digest(letter: str) -> str:
    return f"sha256:{letter * 64}"


def test_repository_and_digest_validation() -> None:
    assert registry_image.validate_repository("ghcr.io/acme/trpc-agent-service") == (
        "ghcr.io/acme/trpc-agent-service"
    )
    assert registry_image.validate_repository("registry.example:5000/team/repo") == (
        "registry.example:5000/team/repo"
    )
    assert registry_image.registry_reference("ghcr.io/acme/trpc-agent-service", _digest("a")) == (
        "ghcr.io/acme/trpc-agent-service@" + _digest("a")
    )
    for value in (
        "https://ghcr.io/acme/service",
        "ghcr.io/acme/service:latest",
        "registry.example:abc/team/repo",
        "registry.example:0/team/repo",
        "user:password@registry.example/team/repo",
        "ghcr.io",
        "acme/service",
        "ACME/service",
    ):
        with pytest.raises(ValueError):
            registry_image.validate_repository(value)
    with pytest.raises(ValueError):
        registry_image.registry_reference("ghcr.io/acme/service", "sha256:bad")


def test_publish_candidate_builds_two_pinned_images_and_writes_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = "a" * 64
    initial = _digest("b")
    upgrade = _digest("c")
    commands: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        del kwargs
        commands.append(command)
        if command[1] == "image" and command[2] == "inspect":
            if "Config.Labels" in command[-1]:
                return subprocess.CompletedProcess(
                    command,
                    0,
                    json.dumps(
                        {registry_image.SOURCE_LABEL: source},
                    ),
                    "",
                )
            digest = initial if ":candidate-" in command[3] else upgrade
            return subprocess.CompletedProcess(command, 0, json.dumps([f"repo@{digest}"]), "")
        if command[1] == "push":
            digest = initial if ":candidate-" in command[2] else upgrade
            return subprocess.CompletedProcess(command, 0, f"digest: {digest} size: 1\n", "")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(registry_image.shutil, "which", lambda _: "docker")
    monkeypatch.setattr(registry_image.subprocess, "run", fake_run)
    monkeypatch.setattr(
        registry_image,
        "source_fingerprint",
        lambda *_args, **_kwargs: {
            "algorithm": "sha256",
            "status": "available",
            "value": source,
        },
    )
    monkeypatch.setenv("TRPC_RELEASE_ID", "release-registry-test")
    monkeypatch.setenv("TRPC_RELEASE_NONCE", "n" * 32)
    output = tmp_path / "binding.json"

    report = registry_image.publish_candidate(
        repository="registry.example/acme/trpc-agent-service",
        context=tmp_path,
        output=output,
    )

    assert report["kind"] == "registry_candidate_binding"
    assert report["source_fingerprint"]["value"] == source
    assert report["image_digest"] == initial
    assert report["images"]["initial"]["reference"].endswith("@" + initial)
    assert report["images"]["upgrade"]["reference"].endswith("@" + upgrade)
    assert report["release_binding"]["release_id"] == "release-registry-test"
    assert "n" * 32 not in json.dumps(report)
    assert json.loads(output.read_text(encoding="utf-8"))["image_digest"] == initial
    builds = [command for command in commands if command[1] == "build"]
    assert len(builds) == 2
    assert all("--provenance=false" in command for command in builds)
    assert all("--platform" in command and "linux/amd64" in command for command in builds)
    assert any("io.trpc.agent-service.release-role=upgrade" in command for command in builds[1])


def test_push_falls_back_to_repo_digest_inspection(monkeypatch: pytest.MonkeyPatch) -> None:
    digest = _digest("d")
    calls: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        del kwargs
        calls.append(command)
        if command[1] == "push":
            return subprocess.CompletedProcess(command, 0, "pushed\n", "")
        return subprocess.CompletedProcess(command, 0, json.dumps([f"repo@{digest}"]), "")

    monkeypatch.setattr(registry_image.shutil, "which", lambda _: "docker")
    monkeypatch.setattr(registry_image.subprocess, "run", fake_run)

    assert registry_image._push_digest("repo:tag") == digest
    assert calls[-1][1:3] == ["image", "inspect"]


def test_publish_rejects_same_initial_and_upgrade_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    digest = _digest("e")
    monkeypatch.setattr(registry_image.shutil, "which", lambda _: "docker")
    monkeypatch.setattr(
        registry_image.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command,
            0,
            json.dumps({registry_image.SOURCE_LABEL: "f" * 64})
            if command[1] == "image" and "Config.Labels" in command[-1]
            else f"digest: {digest}\n",
            "",
        ),
    )
    monkeypatch.setattr(
        registry_image,
        "source_fingerprint",
        lambda *_args, **_kwargs: {
            "algorithm": "sha256",
            "status": "available",
            "value": "f" * 64,
        },
    )
    monkeypatch.setenv("TRPC_RELEASE_ID", "release-registry-test")
    monkeypatch.setenv("TRPC_RELEASE_NONCE", "r" * 32)

    with pytest.raises(registry_image.RegistryImageError, match="must differ"):
        registry_image.publish_candidate(
            repository="registry.example/acme/trpc-agent-service",
            context=tmp_path,
        )


def test_publish_rejects_checkout_changed_during_push(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = "a" * 64
    changed_source = "f" * 64
    digest_values = iter((_digest("b"), _digest("c")))
    source_values = iter((source, changed_source))

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        del kwargs
        if command[1] == "image" and command[2] == "inspect":
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps({registry_image.SOURCE_LABEL: source})
                if "Config.Labels" in command[-1]
                else json.dumps([f"repo@{_digest('d')}"]),
                "",
            )
        if command[1] == "push":
            return subprocess.CompletedProcess(command, 0, f"digest: {next(digest_values)}\n", "")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(registry_image.shutil, "which", lambda _: "docker")
    monkeypatch.setattr(registry_image.subprocess, "run", fake_run)
    monkeypatch.setattr(
        registry_image,
        "source_fingerprint",
        lambda *_args, **_kwargs: {
            "algorithm": "sha256",
            "status": "available",
            "value": next(source_values),
        },
    )
    monkeypatch.setenv("TRPC_RELEASE_ID", "release-registry-test")
    monkeypatch.setenv("TRPC_RELEASE_NONCE", "r" * 32)
    output = tmp_path / "binding.json"

    with pytest.raises(registry_image.RegistryImageError, match="checkout changed"):
        registry_image.publish_candidate(
            repository="registry.example:5000/acme/trpc-agent-service",
            context=tmp_path,
            output=output,
        )
    assert not output.exists()
