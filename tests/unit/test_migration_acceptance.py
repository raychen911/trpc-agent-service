from __future__ import annotations

import json
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from scripts.migration_acceptance_gate import execute_offline_acceptance, main


@pytest.mark.asyncio
async def test_offline_migration_acceptance_covers_resume_diff_cleanup_and_rollback() -> None:
    report = await execute_offline_acceptance()

    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    deltas = report["case_deltas"]
    assert deltas["tenant_count"] == 2
    assert deltas["cross_tenant_key_collisions"] == 0
    assert deltas["checkpoint_resume"] == "pass"
    assert deltas["expected_verify_drift"] == "pass"
    assert deltas["cleanup"] == "pass"
    assert deltas["rollback"] == "pass"

    tenant_a = deltas["per_tenant"]["migration-tenant-a"]
    checkpoint = tenant_a["interrupted_checkpoint"]
    assert checkpoint["cursor"] == "2"
    assert checkpoint["source_count"] == checkpoint["target_count"] == 2
    assert checkpoint["completed"] is False
    assert len(checkpoint["checksum"]) == 64
    assert tenant_a["shadow_read"]["differences"] == []
    assert tenant_a["verify"]["source_count"] == tenant_a["verify"]["target_count"] == 6
    assert tenant_a["cleanup"]["differences"] == []

    tenant_b = deltas["per_tenant"]["migration-tenant-b"]
    assert tenant_b["verify_expected_failure"]["gate"] == "fail"
    assert tenant_b["verify_expected_failure"]["differences"] == ["session/shared-session-0"]
    assert tenant_b["rollback"]["phase"] == "rollback"


def test_offline_migration_acceptance_writes_machine_report(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    output = tmp_path / "migration-acceptance.json"
    monkeypatch.setattr(
        "sys.argv",
        ["migration_acceptance_gate", "--output", str(output)],
    )
    assert main() == 0
    rendered = json.loads(output.read_text(encoding="utf-8"))
    assert rendered["gate"] == "pass"
    assert rendered["production_gate"] == "not_run"
