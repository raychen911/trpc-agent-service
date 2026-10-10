from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME_STAGE = ROOT / "runs/multitenant/run-final-runtime-gates.ps1"


def test_backend_contract_runs_before_application_consumers_start() -> None:
    script = RUNTIME_STAGE.read_text(encoding="utf-8")

    infrastructure_start = script.index(
        "docker compose @compose up -d --no-build postgres redis minio minio-init migrate toxiproxy"
    )
    backend_gate = script.index('"scripts/contract_gate.py" backend')
    application_start = script.index(
        "docker compose @compose up -d --no-build --scale worker=4 "
        "--scale outbox-dispatcher=1 gateway worker outbox-dispatcher"
    )

    assert infrastructure_start < backend_gate < application_start
    assert (
        'Wait-SuccessfulOneShot -ComposeArgs $compose -Project $project -Service "migrate"'
        in script
    )
    assert (
        'Wait-SuccessfulOneShot -ComposeArgs $compose -Project $project -Service "minio-init"'
        in script
    )
