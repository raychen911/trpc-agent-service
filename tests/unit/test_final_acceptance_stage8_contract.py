from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STAGE8_SCRIPT = ROOT / "runs/multitenant/run-current-final-acceptance.ps1"


def test_stage8_reports_optional_production_gate_without_masking_release_failures() -> None:
    script = STAGE8_SCRIPT.read_text(encoding="utf-8-sig")
    stage8 = script.split(
        "if ($StartStage -le 8 -and $EndStage -ge 8) {",
        1,
    )[1]

    assert '$productionGate = "n/a"' in stage8
    assert '$report.PSObject.Properties["production_gate"]' in stage8
    assert "$report.production_gate" not in stage8

    manifest_guard = (
        'throw "release manifest generation failed after all requested gates completed"'
    )
    release_guard = (
        'throw "aggregate release gate remains blocked; inspect release-gate-current-final.json"'
    )
    assert manifest_guard in stage8
    assert release_guard in stage8
    assert stage8.index(manifest_guard) < stage8.index(release_guard)


def test_stage8_preflights_before_manifest_and_allows_only_external_not_run() -> None:
    script = STAGE8_SCRIPT.read_text(encoding="utf-8-sig")
    stage8 = script.split(
        "if ($StartStage -le 8 -and $EndStage -ge 8) {",
        1,
    )[1]

    preflight = (
        "& $python scripts/release_gate.py `\n"
        "        --directory runs/multitenant `\n"
        "        --output runs/multitenant/release-gate-current-final.json `\n"
        "        --allow-functional-dr"
    )
    manifest = "& $python scripts/release_manifest.py `"
    strict_gate = (
        "& $python scripts/release_gate.py `\n"
        "        --directory runs/multitenant `\n"
        "        --output runs/multitenant/release-gate-current-final.json `\n"
        "        --allow-functional-dr `\n"
        "        --require-production"
    )
    assert preflight in stage8
    assert "--require-production" not in stage8.split(manifest, 1)[0]
    assert stage8.index(preflight) < stage8.index(manifest) < stage8.index(strict_gate)
    assert '$allowedNotRun = @("disaster_recovery", "release_bundle")' in stage8
    assert '"online_im"' not in stage8.split("$allowedNotRun", 1)[1].split(")", 1)[0]
    assert "production_manifest=not_generated" not in stage8
    assert "production_manifest=generated" in stage8
    assert "authorized_not_run=disaster_recovery" in stage8
    assert "Move-Item -LiteralPath $manifestPath -Destination $manifestArchivePath" in stage8
    assert "Remove-Item" not in stage8


def test_stage8_binds_functional_dr_when_destructive_dr_is_explicitly_waived() -> None:
    script = STAGE8_SCRIPT.read_text(encoding="utf-8-sig")
    stage8 = script.split(
        "if ($StartStage -le 8 -and $EndStage -ge 8) {",
        1,
    )[1]

    assert '"disaster-recovery.json"' in stage8
    assert '"disaster-recovery-functional.json"' in stage8
    manifest_block = stage8.split("foreach ($name", 1)[0]
    assert "--output $manifestPath" in manifest_block
    assert "--allow-functional-dr" in manifest_block
    assert "$preflight.candidate.disaster_recovery" in manifest_block
    assert "$preflight.candidate.functional_disaster_recovery" in manifest_block


def test_stage8_summary_includes_online_im_report() -> None:
    script = STAGE8_SCRIPT.read_text(encoding="utf-8-sig")
    stage8 = script.split(
        "if ($StartStage -le 8 -and $EndStage -ge 8) {",
        1,
    )[1]

    summary = stage8.split("foreach ($name", 1)[1]
    assert '"im-online.json"' in summary
