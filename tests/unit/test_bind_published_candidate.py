from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

from scripts.evidence_lineage import source_fingerprint


def _module() -> ModuleType:
    path = Path("runs/multitenant/bind_published_candidate.py").resolve()
    spec = importlib.util.spec_from_file_location("bind_published_candidate", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_binding_cli_logic_writes_matching_lock_without_raw_nonce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _module()
    source = source_fingerprint(tmp_path)["value"]
    monkeypatch.setenv("TRPC_RELEASE_ID", "release-test-one")
    monkeypatch.setenv("TRPC_RELEASE_NONCE", "n" * 32)
    output = tmp_path / "binding.json"
    lock = tmp_path / "lock.json"

    result = module.bind_published_candidate(
        expected_source=source,
        repository="docker.io/example/service",
        initial_tag="candidate-a",
        initial_digest="sha256:" + "a" * 64,
        upgrade_tag="candidate-b",
        upgrade_digest="sha256:" + "b" * 64,
        output=output,
        lock_output=lock,
        root=tmp_path,
    )

    rendered = output.read_text(encoding="utf-8") + lock.read_text(encoding="utf-8")
    assert result["source_fingerprint"]["value"] == source
    assert "n" * 32 not in rendered
    assert json.loads(lock.read_text(encoding="utf-8"))["images"] == result["images"]


def test_binding_rejects_equal_digests(tmp_path: Path) -> None:
    module = _module()
    digest = "sha256:" + "a" * 64
    with pytest.raises(ValueError, match="digests must differ"):
        module.bind_published_candidate(
            expected_source=source_fingerprint(tmp_path)["value"],
            repository="docker.io/example/service",
            initial_tag="candidate-a",
            initial_digest=digest,
            upgrade_tag="candidate-b",
            upgrade_digest=digest,
            output=tmp_path / "binding.json",
            lock_output=tmp_path / "lock.json",
            root=tmp_path,
        )


def test_binding_restores_old_pair_when_lock_install_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _module()
    source = source_fingerprint(tmp_path)["value"]
    monkeypatch.setenv("TRPC_RELEASE_ID", "release-test-rollback")
    monkeypatch.setenv("TRPC_RELEASE_NONCE", "r" * 32)
    output = tmp_path / "binding.json"
    lock = tmp_path / "lock.json"
    output.write_text('{"old":"binding"}\n', encoding="utf-8")
    lock.write_text('{"old":"lock"}\n', encoding="utf-8")
    globals_ = module.install_candidate_pair.__globals__
    original_replace = globals_["_replace_file"]
    calls = 0

    def fail_second(source_path: Path, target_path: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated lock replacement failure")
        original_replace(source_path, target_path)

    monkeypatch.setitem(globals_, "_replace_file", fail_second)
    with pytest.raises(OSError, match="simulated"):
        module.bind_published_candidate(
            expected_source=source,
            repository="docker.io/example/service",
            initial_tag="candidate-a",
            initial_digest="sha256:" + "a" * 64,
            upgrade_tag="candidate-b",
            upgrade_digest="sha256:" + "b" * 64,
            output=output,
            lock_output=lock,
            root=tmp_path,
        )

    assert output.read_text(encoding="utf-8") == '{"old":"binding"}\n'
    assert lock.read_text(encoding="utf-8") == '{"old":"lock"}\n'
