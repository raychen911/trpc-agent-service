from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STAGE1 = ROOT / "runs/multitenant/run-final-ack-performance.ps1"
FORMAL_ENTRY = ROOT / "runs/multitenant/run-current-final-acceptance.ps1"


def test_stage1_reuses_verified_context_and_binds_canonical_index_digests() -> None:
    stage1 = STAGE1.read_text(encoding="utf-8")
    formal_entry = FORMAL_ENTRY.read_text(encoding="utf-8")

    for script in (stage1, formal_entry):
        assert 'scripts/candidate_lock.py", "verify' in script or (
            "scripts/candidate_lock.py verify" in script
        )
        assert "ConvertFrom-Json" in script
        assert ".source_fingerprint.value" in script
        assert ".images.initial.digest" in script
        assert ".images.upgrade.digest" in script
        assert ".images.initial.reference" in script
        assert ".release_binding.release_id" in script
        assert ".binding_sha256" in script
        assert "release-context-$releaseId-amd64.json" in script
        assert "candidate lock changed while its release context was being verified" in script

    assert "scripts/release_context.py verify" in stage1
    assert "scripts/release_context.py ensure" not in stage1
    assert "bind_published_candidate.py" not in stage1
    assert '$expectedSource = "' not in stage1
    assert '$candidateDigest = "sha256:' not in formal_entry
    assert stage1.count("scripts/candidate_lock.py verify") == 1
    assert formal_entry.count('"scripts/candidate_lock.py", "verify"') == 1
    assert stage1.index("$env:TRPC_RELEASE_NONCE") < stage1.index(
        "scripts/candidate_lock.py verify"
    )
    assert formal_entry.index("$env:TRPC_RELEASE_NONCE") < formal_entry.index(
        '"scripts/candidate_lock.py", "verify"'
    )
    assert stage1.index("scripts/candidate_lock.py verify") < stage1.index(
        "--public-output $publicReleaseContext"
    )

    verify_block = stage1.split("scripts/release_context.py verify", 1)[1].split(
        "$releaseContext =", 1
    )[0]
    assert "--private-context $privateReleaseContext" in verify_block
    assert "--initial-digest $imageDigest" in verify_block
    assert "--upgrade-digest $upgradeDigest" in verify_block

    render_check = stage1.split("$renderedText", 1)[1].split("Invoke-Kubectl", 1)[0]
    assert "$lockedInitialReference" in render_check
    assert "$dockerHubRepository@$imageDigest" not in render_check


def test_stage1_failure_summary_tolerates_missing_optional_metrics() -> None:
    stage1 = STAGE1.read_text(encoding="utf-8")

    assert "function Get-OptionalPropertyValue" in stage1
    assert "$performanceReport.candidate.burst." not in stage1
    assert "$performanceReport.candidate.sustained." not in stage1
    assert "if ($gateExit -ne 0)" in stage1
    assert 'throw "formal ACK Performance gate failed; no automatic rerun was attempted"' in stage1


def test_formal_entry_forwards_operator_supplied_ack_identity_to_stage1() -> None:
    formal_entry = FORMAL_ENTRY.read_text(encoding="utf-8")
    invocation = formal_entry.split('"[1/8] Performance + external-metric HPA"', 1)[1].split(
        "if ($LASTEXITCODE -ne 0)", 1
    )[0]

    assert "-KubeconfigPath $kubeconfig" in invocation
    assert "-KubeContext $context" in invocation


def test_stage1_applies_schema_before_starting_runtime_pods() -> None:
    stage1 = STAGE1.read_text(encoding="utf-8")

    assert "scripts.split_kubernetes_runtime_manifest" in stage1
    assert 'Invoke-Kubectl -Arguments @("apply", "-k", $renderedPerformance)' not in stage1
    migration_apply = stage1.index('Invoke-Kubectl -Arguments @("apply", "-f", $migrationManifest)')
    migration_wait = stage1.index('"job/trpc-schema-migration", "--timeout=600s"')
    head_apply = stage1.index('Invoke-Kubectl -Arguments @("apply", "-f", $headCheckManifest)')
    head_wait = stage1.index('"job/trpc-schema-head-check", "--timeout=600s"')
    runtime_apply = stage1.index('Invoke-Kubectl -Arguments @("apply", "-f", $runtimeManifest)')

    assert migration_apply < migration_wait < head_apply < head_wait < runtime_apply
