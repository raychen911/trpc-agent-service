from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.release_context import (
    ensure_release_context,
    main,
    public_binding,
)

SOURCE = "a" * 64
INITIAL = "sha256:" + "b" * 64
UPGRADE = "sha256:" + "c" * 64


def _options() -> dict[str, str]:
    return {
        "release_id": "release-acceptance-1",
        "source_fingerprint": SOURCE,
        "initial_digest": INITIAL,
        "upgrade_digest": UPGRADE,
    }


def test_ensure_release_context_is_idempotent_and_public_binding_hides_nonce(
    tmp_path: Path,
) -> None:
    private = tmp_path / ".ack-runtime-private" / "release-context.json"

    first = ensure_release_context(private, **_options())
    second = ensure_release_context(private, **_options())
    public = public_binding(first)

    assert second == first
    assert "nonce" in first
    assert "nonce" not in public
    assert first["nonce"] not in json.dumps(public)
    assert public["nonce_sha256"] == first["nonce_sha256"]


def test_existing_context_rejects_a_different_candidate(tmp_path: Path) -> None:
    private = tmp_path / ".ack-runtime-private" / "release-context.json"
    ensure_release_context(private, **_options())

    with pytest.raises(ValueError, match="different candidate"):
        ensure_release_context(
            private,
            **{**_options(), "upgrade_digest": "sha256:" + "d" * 64},
        )


def test_context_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "release-context.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlinks are unavailable")

    with pytest.raises(ValueError, match="regular file"):
        ensure_release_context(link, **_options())


def test_cli_writes_only_public_binding_to_stdout_and_public_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    private = tmp_path / ".ack-runtime-private" / "release-context.json"
    public = tmp_path / "release-context-binding.json"
    args = [
        "ensure",
        "--private-context",
        str(private),
        "--public-output",
        str(public),
        "--release-id",
        _options()["release_id"],
        "--source-fingerprint",
        SOURCE,
        "--initial-digest",
        INITIAL,
        "--upgrade-digest",
        UPGRADE,
    ]

    assert main(args) == 0
    context = json.loads(private.read_text(encoding="utf-8"))
    captured = capsys.readouterr().out
    public_text = public.read_text(encoding="utf-8")
    assert context["nonce"] not in captured
    assert context["nonce"] not in public_text
    assert json.loads(captured)["context_id"] == json.loads(public_text)["context_id"]


def test_context_rejects_equal_initial_and_upgrade_digests(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must differ"):
        ensure_release_context(
            tmp_path / ".ack-runtime-private" / "release-context.json",
            **{**_options(), "upgrade_digest": INITIAL},
        )


def test_context_fails_closed_when_another_creator_owns_the_lock(tmp_path: Path) -> None:
    private = tmp_path / ".ack-runtime-private" / "release-context.json"
    private.parent.mkdir(parents=True)
    private.with_name(f".{private.name}.lock").write_text("owned", encoding="utf-8")

    with pytest.raises(ValueError, match="already in progress"):
        ensure_release_context(private, **_options())
