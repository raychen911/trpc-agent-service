from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from scripts import candidate_session
from scripts.evidence_lineage import source_fingerprint
from scripts.report_io import atomic_write_json


def _fake_receipt(
    root: Path, *, repository: str, tag: str | None, upgrade_tag: str | None
) -> dict[str, Any]:
    source = source_fingerprint(root)
    release_id = candidate_session.os.environ["TRPC_RELEASE_ID"]
    nonce = candidate_session.os.environ["TRPC_RELEASE_NONCE"]
    initial_digest = "sha256:" + "a" * 64
    upgrade_digest = "sha256:" + "b" * 64
    return {
        "schema_version": 1,
        "kind": "registry_candidate_binding",
        "release_binding": {
            "release_id": release_id,
            "nonce_sha256": hashlib.sha256(nonce.encode()).hexdigest(),
        },
        "source_fingerprint": source,
        "repository": repository,
        "image_digest": initial_digest,
        "images": {
            "initial": {
                "tag": tag,
                "reference": f"{repository}@{initial_digest}",
                "digest": initial_digest,
            },
            "upgrade": {
                "tag": upgrade_tag,
                "reference": f"{repository}@{upgrade_digest}",
                "digest": upgrade_digest,
            },
        },
    }


def test_session_builds_once_and_formal_artifacts_use_context_nonce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_publish(**options: Any) -> dict[str, Any]:
        calls.append(options)
        receipt = _fake_receipt(
            tmp_path,
            repository=options["repository"],
            tag=options["tag"],
            upgrade_tag=options["upgrade_tag"],
        )
        atomic_write_json(options["output"], receipt)
        return receipt

    monkeypatch.setattr(candidate_session, "publish_candidate", fake_publish)
    output = tmp_path / "runs" / "binding.json"
    lock_output = tmp_path / "runs" / "lock.json"
    result = candidate_session.publish_candidate_session(
        repository="docker.io/example/service",
        output=output,
        lock_output=lock_output,
        private_directory=tmp_path / "private",
        public_directory=tmp_path / "public",
        root=tmp_path,
        release_id="release-test-session-one",
    )

    assert len(calls) == 1
    assert calls[0]["lock_output"] is None
    context = json.loads(Path(result["private_context"]).read_text(encoding="utf-8"))
    receipt = json.loads(Path(result["receipt"]).read_text(encoding="utf-8"))
    binding = json.loads(output.read_text(encoding="utf-8"))
    lock = json.loads(lock_output.read_text(encoding="utf-8"))
    public = json.loads(Path(result["public_context"]).read_text(encoding="utf-8"))

    assert receipt["release_binding"]["nonce_sha256"] != context["nonce_sha256"]
    assert binding["release_binding"]["nonce_sha256"] == context["nonce_sha256"]
    assert lock["release_binding"] == binding["release_binding"]
    assert context["nonce"] not in output.read_text(encoding="utf-8")
    assert context["nonce"] not in lock_output.read_text(encoding="utf-8")
    assert context["nonce"] not in json.dumps(public)
    assert context["images"]["initial"] == binding["images"]["initial"]["digest"]
    assert context["images"]["upgrade"] == binding["images"]["upgrade"]["digest"]
    assert "release-test-session-one" in result["private_context"]


def test_session_rejects_source_drift_without_replacing_formal_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "README.md").write_text("before\n", encoding="utf-8")
    output = tmp_path / "runs" / "binding.json"
    lock_output = tmp_path / "runs" / "lock.json"
    output.parent.mkdir(parents=True)
    output.write_text('{"old":"binding"}\n', encoding="utf-8")
    lock_output.write_text('{"old":"lock"}\n', encoding="utf-8")

    def fake_publish(**options: Any) -> dict[str, Any]:
        receipt = _fake_receipt(
            tmp_path,
            repository=options["repository"],
            tag=options["tag"],
            upgrade_tag=options["upgrade_tag"],
        )
        atomic_write_json(options["output"], receipt)
        (tmp_path / "README.md").write_text("after\n", encoding="utf-8")
        return receipt

    monkeypatch.setattr(candidate_session, "publish_candidate", fake_publish)
    with pytest.raises(ValueError, match="source"):
        candidate_session.publish_candidate_session(
            repository="docker.io/example/service",
            output=output,
            lock_output=lock_output,
            private_directory=tmp_path / "private",
            public_directory=tmp_path / "public",
            root=tmp_path,
            release_id="release-test-source-drift",
        )

    assert output.read_text(encoding="utf-8") == '{"old":"binding"}\n'
    assert lock_output.read_text(encoding="utf-8") == '{"old":"lock"}\n'


def test_session_rejects_unsafe_release_id_before_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    called = False

    def fake_publish(**options: Any) -> dict[str, Any]:
        nonlocal called
        called = True
        return options

    monkeypatch.setattr(candidate_session, "publish_candidate", fake_publish)
    with pytest.raises(ValueError, match="release id"):
        candidate_session.publish_candidate_session(
            repository="docker.io/example/service",
            output=tmp_path / "binding.json",
            lock_output=tmp_path / "lock.json",
            private_directory=tmp_path / "private",
            public_directory=tmp_path / "public",
            root=tmp_path,
            release_id="../escape",
        )

    assert not called
