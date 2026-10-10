from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STAGE3_SCRIPT = ROOT / "runs/multitenant/run-current-final-acceptance.ps1"


def test_stage3_applies_production_worker_hpa_before_runtime_gate() -> None:
    script = STAGE3_SCRIPT.read_text(encoding="utf-8-sig")
    stage3 = script.split(
        "if ($StartStage -le 3 -and $EndStage -ge 3) {",
        1,
    )[1].split(
        "if ($StartStage -le 4 -and $EndStage -ge 4) {",
        1,
    )[0]

    manifest = (
        '$productionHpaManifest = Join-Path $projectRoot "deploy/kustomize/base/autoscaling.yaml"'
    )
    apply = (
        "& $kubectl --kubeconfig $kubeconfig --context $context --namespace $serviceNamespace `\n"
        "        apply -f $productionHpaManifest"
    )
    bounds = (
        "$workerHpaBounds = & $kubectl --kubeconfig $kubeconfig --context $context `\n"
        "        --namespace $serviceNamespace get hpa/trpc-worker `\n"
        '        -o "jsonpath={.spec.minReplicas}:{.spec.maxReplicas}"'
    )
    runtime_gate = '"-m", "scripts.kubernetes_runtime_gate"'

    assert manifest in stage3
    assert apply in stage3
    assert bounds in stage3
    assert '$workerHpaBounds -ne "2:20"' in stage3
    assert stage3.index(apply) < stage3.index(bounds) < stage3.index(runtime_gate)


def test_stage3_removes_completed_schema_jobs_before_runtime_gate() -> None:
    script = STAGE3_SCRIPT.read_text(encoding="utf-8-sig")
    stage3 = script.split(
        "if ($StartStage -le 3 -and $EndStage -ge 3) {",
        1,
    )[1].split(
        "if ($StartStage -le 4 -and $EndStage -ge 4) {",
        1,
    )[0]

    job_list = '$completedSchemaJobs = @("trpc-schema-migration", "trpc-schema-head-check")'
    wait = "wait --for=condition=complete $schemaJobResource --timeout=60s"
    delete = "delete $schemaJobResource --wait=true"
    runtime_gate = '"-m", "scripts.kubernetes_runtime_gate"'

    assert job_list in stage3
    assert wait in stage3
    assert delete in stage3
    assert stage3.index(job_list) < stage3.index(wait) < stage3.index(delete)
    assert stage3.index(delete) < stage3.index(runtime_gate)
