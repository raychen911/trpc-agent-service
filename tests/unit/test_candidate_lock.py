import json
import subprocess
import sys
from pathlib import Path

from scripts import candidate_lock

ROOT = Path(__file__).resolve().parents[2]


def test_candidate_lock_documented_direct_entrypoint_imports_repository() -> None:
    completed = subprocess.run(  # noqa: S603
        [sys.executable, str(ROOT / "scripts" / "candidate_lock.py"), "--help"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr


def _digest(letter: str) -> str:
    return f"sha256:{letter * 64}"


def _binding() -> dict[str, object]:
    repository = "registry.example/acme/trpc-agent-service"
    initial = _digest("a")
    upgrade = _digest("b")
    return {
        "schema_version": 1,
        "kind": "registry_candidate_binding",
        "release_binding": {"release_id": "release-one", "nonce_sha256": "c" * 64},
        "source_fingerprint": {"status": "available", "value": "d" * 64},
        "repository": repository,
        "image_digest": initial,
        "images": {
            "initial": {
                "digest": initial,
                "reference": f"{repository}@{initial}",
                "tag": "candidate",
            },
            "upgrade": {
                "digest": upgrade,
                "reference": f"{repository}@{upgrade}",
                "tag": "upgrade",
            },
        },
    }


def _current(monkeypatch) -> None:
    monkeypatch.setattr(
        candidate_lock,
        "source_fingerprint",
        lambda _root: {"status": "available", "value": "d" * 64},
    )
    monkeypatch.setattr(
        candidate_lock,
        "current_release_binding",
        lambda **_kwargs: {"release_id": "release-one", "nonce_sha256": "c" * 64},
    )


def test_candidate_lock_freezes_binding_source_and_digest(tmp_path: Path, monkeypatch) -> None:
    _current(monkeypatch)
    binding = _binding()
    output = tmp_path / "candidate-lock.json"

    lock = candidate_lock.create_candidate_lock(binding, root=tmp_path, output=output)

    assert lock["kind"] == "release_candidate_lock"
    assert lock["image_digest"] == _digest("a")
    assert lock["binding_sha256"] == candidate_lock.canonical_sha256(binding)
    assert candidate_lock.verify_candidate_lock(lock, binding, root=tmp_path) == []
    assert json.loads(output.read_text(encoding="utf-8"))["images"] == binding["images"]


def test_candidate_lock_detects_binding_or_mutable_reference_drift(
    tmp_path: Path, monkeypatch
) -> None:
    _current(monkeypatch)
    binding = _binding()
    lock = candidate_lock.create_candidate_lock(binding, root=tmp_path)
    changed = json.loads(json.dumps(binding))
    changed["images"]["initial"]["digest"] = _digest("e")
    reasons = candidate_lock.verify_candidate_lock(lock, changed, root=tmp_path)
    assert "candidate lock binding content hash changed" in reasons

    mutable = _binding()
    mutable["images"]["initial"]["reference"] = "registry.example/acme/trpc-agent-service:latest"
    reasons = candidate_lock._validate_binding(mutable, root=tmp_path)
    assert "registry candidate binding initial image is not immutable" in reasons
