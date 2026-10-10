from __future__ import annotations

from pathlib import Path

_MIGRATION_RUNNER = (
    Path(__file__).resolve().parents[2] / "runs" / "multitenant" / "run-e5b583a0-migration.ps1"
)


def test_stage6_migration_wait_attests_container_image_before_exit_success() -> None:
    script = _MIGRATION_RUNNER.read_text(encoding="utf-8")
    wait_block = script.split("$migrateDeadline", 1)[1].split("$env:TRPC_RUN_REAL_MIGRATION", 1)[0]

    image_guard = "if ([string]$migrateDocument.Image -ne [string]$initialId)"
    assert "docker inspect $migrateId" in wait_block
    assert image_guard in wait_block
    assert "schema migration image binding mismatch; project retained" in wait_block
    assert '$migrateParts[0] -eq "exited"' in wait_block
    assert '$migrateExit -ne "0"' in wait_block
    assert wait_block.index(image_guard) < wait_block.index('$migrateParts[0] -eq "exited"')
