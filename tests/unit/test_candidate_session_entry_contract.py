from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENTRY = ROOT / "runs/multitenant/run-current-candidate-session.ps1"
FORMAL_ENTRY = ROOT / "runs/multitenant/run-current-final-acceptance.ps1"


def test_candidate_entry_uses_one_tracked_session_orchestrator() -> None:
    entry = ENTRY.read_text(encoding="utf-8")

    assert "scripts.candidate_session publish" in entry
    assert "scripts.registry_image" not in entry
    assert "scripts.release_context" not in entry
    assert "candidate_lock.py verify" in entry
    assert "TRPC_RELEASE_NONCE" in entry
    assert "TRPC_CANDIDATE_REPOSITORY" in entry
    assert "zixuan760" not in entry


def test_hpa_kubeconfig_is_rebuilt_from_current_ack_cluster() -> None:
    entry = FORMAL_ENTRY.read_text(encoding="utf-8")
    refresh = entry.split("function Refresh-HpaDriverCredential", 1)[1].split(
        "$candidateLock =", 1
    )[0]

    assert "config view --raw --minify -o json" in refresh
    assert "certificate-authority-data" in refresh
    assert "config set-cluster current-ack" in refresh
    assert "--embed-certs=true" in refresh
    assert "config set-context $hpaDriverContext" in refresh
    assert "auth whoami" in refresh


def test_formal_entry_requires_operator_supplied_ack_identity() -> None:
    entry = FORMAL_ENTRY.read_text(encoding="utf-8")

    assert "TRPC_ACK_KUBECONFIG" in entry
    assert "TRPC_ACK_CONTEXT" in entry
    assert "C:/Users/" not in entry
    assert "kubernetes-admin-" not in entry
