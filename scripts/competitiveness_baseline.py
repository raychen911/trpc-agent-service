"""Build a reproducible capability baseline for production delivery work.

The baseline is intentionally conservative.  Source files and local tests can
show that a capability exists, but they cannot prove a real provider, cluster,
or disaster-recovery run.  Those external checks remain ``not_run`` until an
operator supplies the corresponding environment and evidence.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REFERENCE_REPOSITORY = "https://github.com/neutronstar238/trpc-agent-service"
REFERENCE_COMMIT = "dc9781393272e04342b34b4b8fbba24ec775c4ba"
REFERENCE_COMMIT_OBSERVED_AT = "2026-09-02T11:43:46Z"
REFERENCE_INVENTORY = {
    "trpc_service_python_files": 82,
    "scripts_python_files": 48,
    "test_python_files": 171,
    "deploy_files": 79,
    "migration_files": 24,
    "run_files": 15,
}


CAPABILITIES: tuple[dict[str, Any], ...] = (
    {
        "id": "tenant_isolation",
        "area": "multi-tenant isolation",
        "local_paths": [
            "trpc_service/tenant",
            "trpc_service/policy",
            "trpc_service/storage/postgres_rls.py",
            "tests/test_postgres_rls.py",
            "tests/test_hardening.py",
        ],
        "reference_paths": [
            "trpc_service/tenant",
            "trpc_service/storage/postgres.py",
            "tests/unit/test_tenant_routing.py",
            "tests/unit/test_postgres_v2_branch_coverage.py",
        ],
        "local_implementation": "present",
        "local_external_evidence": "not_run",
        "position": "parity_candidate",
        "objective": (
            "Prove zero cross-tenant reads, writes, tool calls, audit leaks, and RLS "
            "bypasses under adversarial tests."
        ),
        "next_action": (
            "Add a repeatable cross-tenant attack matrix against application filters, "
            "database roles, and RLS."
        ),
    },
    {
        "id": "session_reliability",
        "area": "session ordering and recovery",
        "local_paths": [
            "trpc_service/storage/session_mailbox.py",
            "trpc_service/storage/postgres_session_mailbox.py",
            "trpc_service/storage/retry.py",
            "tests/test_session_mailbox_v2.py",
            "tests/test_durable_recovery.py",
        ],
        "reference_paths": [
            "trpc_service/storage/mailbox.py",
            "trpc_service/queue/session_ready.py",
            "tests/unit/test_session_ready_branch_coverage.py",
            "tests/unit/test_session_recovery.py",
        ],
        "local_implementation": "present",
        "local_external_evidence": "not_run",
        "position": "parity_candidate",
        "objective": (
            "Preserve per-session ordering, fence stale workers, recover poison messages, "
            "and replay only with an auditable operator action."
        ),
        "next_action": (
            "Run multi-process database and Redis recovery scenarios and retain evidence "
            "bound to one candidate."
        ),
    },
    {
        "id": "channel_breadth",
        "area": "IM channel coverage",
        "local_paths": [
            "trpc_service/channels/feishu.py",
            "trpc_service/channels/wecom_ai_bot.py",
            "trpc_service/channels/telegram.py",
            "trpc_service/channels/web.py",
            "tests/test_channel_protocol.py",
        ],
        "reference_paths": [
            "trpc_service/channels/feishu.py",
            "trpc_service/channels/wecom.py",
            "tests/unit/test_feishu.py",
            "tests/unit/test_wecom.py",
        ],
        "local_implementation": "present",
        "local_external_evidence": "not_run",
        "position": "advantage_candidate",
        "objective": (
            "Keep one channel contract across Web, Telegram, Feishu, and WeCom AI Bot "
            "while preserving channel-specific semantics."
        ),
        "next_action": (
            "Give every production channel an independent online driver and "
            "provider-originated receipt path."
        ),
    },
    {
        "id": "release_evidence",
        "area": "release evidence and supply chain",
        "local_paths": [
            "scripts/release_gate.py",
            "scripts/security_gate.py",
            "scripts/production_acceptance.py",
            "Dockerfile",
            "pyproject.toml",
            "uv.lock",
        ],
        "reference_paths": [
            "scripts/release_context.py",
            "scripts/candidate_lock.py",
            "scripts/evidence_lineage.py",
            "scripts/supply_chain_gate.py",
            "scripts/registry_image.py",
        ],
        "local_implementation": "partial",
        "local_external_evidence": "not_run",
        "position": "gap",
        "objective": (
            "Bind source fingerprint, release context, candidate lock, image digest, "
            "SBOM, vulnerability results, and every runtime report to one immutable release."
        ),
        "next_action": (
            "Implement release context, candidate locking, evidence lineage, SBOM/SARIF "
            "validation, and mixed-candidate rejection."
        ),
    },
    {
        "id": "real_im_acceptance",
        "area": "real IM acceptance",
        "local_paths": [
            "scripts/online_im_gate.py",
            "trpc_service/channels/feishu.py",
            "trpc_service/channels/wecom_ai_bot.py",
            "docs/IM_INTEGRATION.md",
        ],
        "reference_paths": [
            "scripts/im_online_gate.py",
            "deploy/im_probe/server.py",
            "deploy/im_probe/feishu_provider_driver.py",
            "deploy/im_probe/wecom_provider_driver.py",
            "scripts/im_probe_preflight.py",
        ],
        "local_implementation": "partial",
        "local_external_evidence": "not_run",
        "position": "gap",
        "objective": (
            "Verify provider-originated callback or WebSocket event, outbound ACK, "
            "duplicate delivery, media, reconnect, rate limit, credential rotation, "
            "outage, and ambiguous-result recovery."
        ),
        "next_action": (
            "Build an independently hosted signed Probe and drivers for Feishu and "
            "WeCom before using real credentials."
        ),
    },
    {
        "id": "real_runtime_acceptance",
        "area": "real runtime and Kubernetes acceptance",
        "local_paths": [
            "deployment/docker-compose.yml",
            "deployment/kubernetes/platform.yaml",
            "scripts/kubernetes_runtime_gate.py",
            "scripts/concurrency_gate.py",
            "scripts/database_migration_gate.py",
        ],
        "reference_paths": [
            "scripts/real_runtime_gate.py",
            "scripts/kubernetes_runtime_gate.py",
            "scripts/kind_runtime_gate.py",
            "deploy/kustomize/overlays/production",
        ],
        "local_implementation": "partial",
        "local_external_evidence": "not_run",
        "position": "gap",
        "objective": (
            "Prove real PostgreSQL, Redis, object storage, multi-process workers, "
            "migrations, rolling upgrade, HPA, PDB, node drain, and network recovery."
        ),
        "next_action": (
            "Add a topology-aware runtime driver with preflight, immutable image "
            "identity checks, and retained cluster evidence."
        ),
    },
    {
        "id": "performance_acceptance",
        "area": "performance and capacity",
        "local_paths": [
            "scripts/concurrency_gate.py",
            "scripts/load_test_web_ui.py",
            "tests/test_production_depth.py",
            "deployment/prometheus-alerts.yml",
        ],
        "reference_paths": [
            "scripts/performance_gate.py",
            "scripts/real_performance_gate.py",
            "scripts/ack_performance_acceptance.py",
            "scripts/kubernetes_performance_job.py",
        ],
        "local_implementation": "partial",
        "local_external_evidence": "not_run",
        "position": "gap",
        "objective": (
            "Report throughput, P50/P95/P99 latency, errors, queue backlog, CPU/memory "
            "saturation, tenant concurrency, and scale behavior under a fixed workload."
        ),
        "next_action": (
            "Define a versioned workload and run the same scenario through local, "
            "Compose, and Kubernetes drivers."
        ),
    },
    {
        "id": "disaster_recovery",
        "area": "disaster recovery",
        "local_paths": [
            "scripts/disaster_recovery_gate.py",
            "scripts/fault_injection_gate.py",
            "trpc_service/storage/compensation.py",
            "tests/test_durable_recovery.py",
        ],
        "reference_paths": [
            "scripts/disaster_recovery_gate.py",
            "scripts/functional_disaster_recovery_gate.py",
            "scripts/kubernetes_functional_disaster_recovery.py",
            "scripts/kubernetes_disaster_recovery.py",
        ],
        "local_implementation": "partial",
        "local_external_evidence": "not_run",
        "position": "gap",
        "objective": (
            "Restore isolated database, mailbox, outbox, and object data; verify "
            "checksums and tenant counts; measure RPO/RTO; prove rollback safety."
        ),
        "next_action": (
            "Extend backup/restore from connectivity checks to data-integrity and "
            "isolated functional recovery jobs."
        ),
    },
    {
        "id": "test_system",
        "area": "test and simulation system",
        "local_paths": [
            "tests",
            "scripts/quality_gate.py",
            "scripts/release_gate.py",
            "tests/test_hardening.py",
            "tests/test_production_depth.py",
        ],
        "reference_paths": [
            "tests/unit",
            "tests/integration",
            "tests/simulation",
            "tests/sdk_compat",
            "scripts/check_coverage.py",
        ],
        "local_implementation": "partial",
        "local_external_evidence": "available",
        "position": "gap",
        "objective": (
            "Cover critical state-machine branches and cross-module contracts with "
            "unit, integration, simulation, concurrency, fault, security, IM, and "
            "Kubernetes tests."
        ),
        "next_action": (
            "Add the missing test layers as each runtime gate is implemented; enforce "
            "branch coverage and no silent external skips in release mode."
        ),
    },
    {
        "id": "operations_observability",
        "area": "operations and observability",
        "local_paths": [
            "trpc_service/telemetry",
            "trpc_service/web/app.py",
            "scripts/observability_gate.py",
            "deployment/prometheus-alerts.yml",
            "docs/OPERATIONS.md",
        ],
        "reference_paths": [
            "trpc_service/metrics",
            "trpc_service/log",
            "scripts/privacy_leak_gate.py",
            "scripts/report_io.py",
        ],
        "local_implementation": "present",
        "local_external_evidence": "not_run",
        "position": "parity_candidate",
        "objective": (
            "Keep health, metrics, traces, audit queries, redaction, and operator "
            "reports safe, actionable, and bound to the release."
        ),
        "next_action": (
            "Bind observability and privacy reports into the release evidence chain "
            "and add deployed-log leak scans."
        ),
    },
)


def _git(*args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"
    value = result.stdout.strip()
    return value if result.returncode == 0 and value else "unknown"


def _path_exists(relative: str) -> bool:
    return (ROOT / relative).exists()


def _inventory() -> dict[str, int]:
    def count(directory: str) -> int:
        path = ROOT / directory
        return sum(1 for item in path.rglob("*.py") if item.is_file()) if path.is_dir() else 0

    return {
        "trpc_service_python_files": count("trpc_service"),
        "scripts_python_files": count("scripts"),
        "test_python_files": count("tests"),
        "deploy_files": sum(1 for item in (ROOT / "deployment").rglob("*") if item.is_file())
        if (ROOT / "deployment").is_dir()
        else 0,
        "migration_files": sum(1 for item in (ROOT / "trpc_service" / "database").rglob("*") if item.is_file())
        if (ROOT / "trpc_service" / "database").is_dir()
        else 0,
    }


def build_baseline() -> dict[str, Any]:
    capabilities: list[dict[str, Any]] = []
    for item in CAPABILITIES:
        local_paths = list(item["local_paths"])
        missing = [path for path in local_paths if not _path_exists(path)]
        capability = dict(item)
        capability["local_paths_present"] = not missing
        capability["missing_local_paths"] = missing
        if missing:
            capability["local_implementation"] = "incomplete"
        capability["reference_commit"] = REFERENCE_COMMIT
        capabilities.append(capability)

    local_inventory = _inventory()
    return {
        "schema_version": 1,
        "baseline_type": "competitiveness",
        "generated_at": datetime.now(UTC).isoformat(),
        "repository": {
            "root": ".",
            "branch": _git("branch", "--show-current"),
            "head": _git("rev-parse", "HEAD"),
            "working_tree": "dirty" if _git("status", "--porcelain") != "" else "clean",
        },
        "reference": {
            "repository": REFERENCE_REPOSITORY,
            "commit": REFERENCE_COMMIT,
            "commit_observed_at": REFERENCE_COMMIT_OBSERVED_AT,
            "commit_url": f"{REFERENCE_REPOSITORY}/commit/{REFERENCE_COMMIT}",
            "inventory_source": f"{REFERENCE_REPOSITORY}/tree/{REFERENCE_COMMIT}",
            "inventory": REFERENCE_INVENTORY,
            "inventory_note": (
                "Public GitHub tree inventory; file count is not a quality score and "
                "does not prove runtime success."
            ),
        },
        "local_inventory": local_inventory,
        "status_semantics": {
            "present": "The implementation path exists in this checkout.",
            "partial": "Some implementation exists, but the target production contract is incomplete.",
            "available": "A local test or gate path exists; it is not a claim of external runtime success.",
            "not_run": "The required external environment or real provider evidence was not executed.",
            "parity_candidate": "Core code appears competitive, pending equal-scope runtime evidence.",
            "advantage_candidate": "A potential scope advantage exists, pending behavior and performance evidence.",
            "gap": "The reference has a materially deeper implementation or evidence path today.",
        },
        "capabilities": capabilities,
        "decision": {
            "current_assessment": "production_candidate_not_yet_parity",
            "reason": (
                "Core runtime capability is present, but release evidence, real IM, "
                "real runtime, performance, disaster recovery, and test depth are not "
                "yet proven at equal scope."
            ),
            "promotion_rule": (
                "Only promote to parity or advantage after the corresponding external "
                "evidence is pass and is bound to the same release candidate."
            ),
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    local = report["repository"]
    reference = report["reference"]
    lines = [
        "# Competitiveness Baseline",
        "",
        "> This document is a conservative code-and-evidence baseline. `not_run` is not a pass.",
        "",
        "## Snapshot",
        "",
        f"- Generated: `{report['generated_at']}`",
        f"- Local branch: `{local['branch']}`",
        f"- Local HEAD: `{local['head']}`",
        f"- Local worktree: `{local['working_tree']}`",
        f"- Reference: [`{reference['commit'][:12]}`]({reference['commit_url']})",
        "",
        "## Inventory",
        "",
        "| Scope | Local | Reference | Meaning |",
        "|---|---:|---:|---|",
    ]
    for key, label in (
        ("trpc_service_python_files", "Core Python files"),
        ("scripts_python_files", "Gate/script Python files"),
        ("test_python_files", "Test Python files"),
        ("deploy_files", "Deployment files"),
        ("migration_files", "Migration files"),
    ):
        local_value = report["local_inventory"].get(key, 0)
        reference_value = reference["inventory"].get(key, 0)
        lines.append(f"| {label} | {local_value} | {reference_value} | Inventory only; not a quality score |")
    lines.extend(
        [
            "",
            "## Capability Matrix",
            "",
            "| Area | Local code | Local external proof | Current position | Next action |",
            "|---|---|---|---|---|",
        ]
    )
    for item in report["capabilities"]:
        lines.append(
            f"| {item['area']} | `{item['local_implementation']}` | `{item['local_external_evidence']}` | "
            f"`{item['position']}` | {item['next_action']} |"
        )
    lines.extend(
        [
            "",
            "## Current Decision",
            "",
            f"**{report['decision']['current_assessment']}**.",
            "",
            report["decision"]["reason"],
            "",
            report["decision"]["promotion_rule"],
            "",
            "## Competitive Objectives",
            "",
        ]
    )
    for item in report["capabilities"]:
        lines.append(f"- **{item['area']}**: {item['objective']}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("runs/phase0/competitiveness-baseline.json"))
    parser.add_argument("--markdown", type=Path, default=Path("docs/COMPETITIVENESS_BASELINE.md"))
    args = parser.parse_args()

    report = build_baseline()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    markdown = args.markdown if args.markdown.is_absolute() else ROOT / args.markdown
    output.parent.mkdir(parents=True, exist_ok=True)
    markdown.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps({"status": "pass", "json": str(output), "markdown": str(markdown)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
