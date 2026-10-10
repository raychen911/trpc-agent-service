#!/usr/bin/env python3
"""Run deterministic substitutes for production-only acceptance environments."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

_REPO_IMPORT_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_IMPORT_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_IMPORT_ROOT))

from scripts.performance_gate import MAX_CONCURRENCY  # noqa: E402
from scripts.report_io import atomic_write_json  # noqa: E402

ROOT = _REPO_IMPORT_ROOT

SCENARIOS = {
    "multinode_load": {
        "emulates": (
            "four tenants and eight independent worker nodes under duplicate/out-of-order load"
        ),
        "selectors": (
            "tests/simulation/test_multitenant_multinode.py::"
            "test_eight_nodes_isolate_four_tenants_under_duplicate_and_out_of_order_load",
        ),
        "production_gap": (
            "workers share one Python process and an in-memory authoritative repository"
        ),
    },
    "fault_injection": {
        "emulates": "node death, lease takeover, fencing, dependency failures and encrypted replay",
        "selectors": (
            "tests/unit/test_worker_consistency.py",
            "tests/unit/test_queue_dispatchers_and_projection.py",
            "tests/unit/test_agent_extended.py",
        ),
        "production_gap": (
            "faults are deterministic fakes rather than process kill and network corruption"
        ),
    },
    "migration": {
        "emulates": (
            "two-tenant backfill interruption, checkpoint resume, shadow read, cutover and rollback"
        ),
        "selectors": (
            "tests/unit/test_migration.py",
            "tests/simulation/test_multitenant_multinode.py::"
            "test_two_tenant_migration_resumes_independently_after_target_interruption",
        ),
        "production_gap": (
            "source and target are in-memory adapters rather than independently operated stores"
        ),
    },
    "kubernetes_runtime": {
        "emulates": "rolling update, HPA scale-up, PDB eviction and graceful termination",
        "selectors": ("tests/simulation/test_kubernetes_runtime_model.py",),
        "production_gap": (
            "controller behavior is modeled from manifests without a Kubernetes control plane"
        ),
    },
    "im_protocol": {
        "emulates": (
            "WeCom and Feishu signature, AES, callback, reconnect, media and delivery behavior"
        ),
        "selectors": (
            "tests/unit/test_feishu.py",
            "tests/unit/test_feishu_gateway.py",
            "tests/unit/test_wecom.py",
        ),
        "production_gap": (
            "recorded frames and fake clients replace provider accounts and rate limits"
        ),
    },
}


def _pytest(selectors: tuple[str, ...]) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(  # noqa: S603 - fixed interpreter and audited selectors
        [sys.executable, "-m", "pytest", "-q", *selectors],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    combined = f"{completed.stdout}\n{completed.stderr}"
    summaries = re.findall(r"\d+ passed(?:, \d+ skipped)?", combined)
    if completed.returncode:
        print(combined[-4000:], file=sys.stderr)
    return {
        "status": "pass" if completed.returncode == 0 else "fail",
        "exit_code": completed.returncode,
        "duration_seconds": time.perf_counter() - started,
        "summary": summaries[-1] if summaries else "pytest did not report a pass summary",
    }


def _performance(output: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    completed = subprocess.run(  # noqa: S603 - fixed local script and interpreter
        [
            sys.executable,
            str(ROOT / "scripts" / "performance_gate.py"),
            "--callbacks",
            "500",
            "--turns",
            "200",
            "--concurrency",
            str(MAX_CONCURRENCY),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    try:
        report = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        report = {"gate": "fail", "rejection_reasons": [type(error).__name__]}
    result = {
        "status": "pass" if completed.returncode == 0 and report.get("gate") == "pass" else "fail",
        "exit_code": completed.returncode,
        "metrics": report.get("candidate", {}),
    }
    if result["status"] == "fail":
        print(f"{completed.stdout}\n{completed.stderr}"[-4000:], file=sys.stderr)
    return result, report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runs/multitenant/production-mock.json"),
    )
    parser.add_argument(
        "--performance-output",
        type=Path,
        default=Path("runs/multitenant/performance-mock.json"),
    )
    args = parser.parse_args()

    performance, _ = _performance(args.performance_output)
    scenario_results: dict[str, dict[str, Any]] = {}
    for name, definition in SCENARIOS.items():
        result = _pytest(tuple(definition["selectors"]))
        result.update(
            {
                "emulates": definition["emulates"],
                "production_gap": definition["production_gap"],
                "selectors": list(definition["selectors"]),
            }
        )
        if name == "multinode_load":
            result["performance"] = performance
            if performance["status"] != "pass":
                result["status"] = "fail"
        scenario_results[name] = result

    failures = [name for name, result in scenario_results.items() if result["status"] != "pass"]
    simulation_gate = "fail" if failures else "pass"
    production_reasons = [
        "real multi-process load with PostgreSQL/Redis/MinIO/pgvector was not executed",
        "Toxiproxy network faults and operating-system process kills were not executed",
        "migration between independent live source and target backends was not executed",
        "Kubernetes rollout/HPA/eviction was not executed by a real control plane",
        "real WeCom and Feishu provider credentials and quotas were not exercised",
    ]
    result = {
        "baseline": {
            "required_simulations": list(SCENARIOS),
            "all_simulations_must_pass": True,
            "mock_results_must_not_upgrade_production": True,
        },
        "candidate": {
            "mode": "deterministic_mock_without_external_credentials",
            "scenarios": scenario_results,
        },
        "case_deltas": {
            "total_simulations": len(scenario_results),
            "passed_simulations": len(scenario_results) - len(failures),
            "failed_simulations": failures,
        },
        "gate": simulation_gate,
        "simulation_gate": simulation_gate,
        "production_gate": "not_run",
        "rejection_reasons": [f"simulation failed: {name}" for name in failures],
        "production_rejection_reasons": production_reasons,
    }
    rendered = atomic_write_json(args.output, result).rstrip("\n")
    print(rendered)
    return 0 if simulation_gate == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
