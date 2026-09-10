"""Release gate for the production reliability contract."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.candidate_lock import verify_lock
from scripts.evidence_lineage import binding_matches, read_json, release_binding
from scripts.release_context import verify_context
from scripts.release_evidence import artifact_record
from scripts.release_manifest import verify_manifest
from scripts.supply_chain_gate import verify_report

ROOT = Path(__file__).resolve().parents[1]


def run(command: list[str]) -> dict[str, object]:
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
    return {
        "command": " ".join(command),
        "returncode": result.returncode,
        "output": (result.stdout + result.stderr)[-2000:],
    }


def _evidence_gate(require_production: bool) -> dict[str, object]:
    context_path_value = os.getenv("TRPC_RELEASE_CONTEXT", "").strip()
    lock_path_value = os.getenv("TRPC_CANDIDATE_LOCK", "").strip()
    if not context_path_value or not lock_path_value:
        status = "fail" if require_production else "not_run"
        return {
            "command": "release evidence chain",
            "returncode": 1 if require_production else 0,
            "output": "set TRPC_RELEASE_CONTEXT and TRPC_CANDIDATE_LOCK for release evidence verification",
            "status": status,
        }
    context_path = ROOT / context_path_value if not Path(context_path_value).is_absolute() else Path(context_path_value)
    lock_path = ROOT / lock_path_value if not Path(lock_path_value).is_absolute() else Path(lock_path_value)
    try:
        trust_key_value = os.getenv("TRPC_RELEASE_TRUST_KEY", "").strip()
        trust_key = None
        if trust_key_value:
            trust_key = ROOT / trust_key_value if not Path(trust_key_value).is_absolute() else Path(trust_key_value)
        context = verify_context(
            context_path,
            require_clean=require_production,
            trust_key_file=trust_key,
            require_signature=require_production,
        )
        lock = verify_lock(
            context_path,
            lock_path,
            trust_key_file=trust_key,
            require_signature=require_production,
        )
        expected_binding = release_binding(context, lock)
        supply_chain_path_value = os.getenv("TRPC_SUPPLY_CHAIN_REPORT", "").strip()
        manifest_path_value = os.getenv("TRPC_RELEASE_MANIFEST", "").strip()
        if not supply_chain_path_value or not manifest_path_value:
            if require_production:
                raise ValueError("TRPC_SUPPLY_CHAIN_REPORT and TRPC_RELEASE_MANIFEST are required")
            return {
                "command": "release evidence chain",
                "returncode": 0,
                "output": (
                    "release context and candidate lock verified; supply-chain report and "
                    "manifest are not configured"
                ),
                "status": "not_run",
            }
        supply_chain_path = (
            ROOT / supply_chain_path_value
            if not Path(supply_chain_path_value).is_absolute()
            else Path(supply_chain_path_value)
        )
        manifest_path = (
            ROOT / manifest_path_value
            if not Path(manifest_path_value).is_absolute()
            else Path(manifest_path_value)
        )
        supply_chain = read_json(supply_chain_path)
        valid, reason = verify_report(supply_chain)
        if not valid:
            raise ValueError(f"supply-chain report: {reason}")
        valid, reason = binding_matches(supply_chain.get("release_binding", {}), expected_binding)
        if not valid:
            raise ValueError(f"supply-chain report: {reason}")
        manifest = verify_manifest(
            context_path,
            lock_path,
            manifest_path,
            require_pass=require_production,
            trust_key_file=trust_key,
            require_signature=require_production,
        )
        manifest_payload = manifest.get("payload", {})
        if require_production and manifest_payload.get("require_pass") is not True:
            raise ValueError("production release manifest must require passing reports")
        supply_record = artifact_record(supply_chain_path, ROOT)
        manifest_reports = manifest.get("payload", {}).get("reports", [])
        if not any(
            isinstance(item, dict)
            and item.get("path") == supply_record["path"]
            and item.get("size") == supply_record["size"]
            and item.get("sha256") == supply_record["sha256"]
            for item in manifest_reports
        ):
            raise ValueError("release manifest does not include the configured supply-chain report")
        statuses = {
            str(item.get("status"))
            for item in manifest.get("payload", {}).get("reports", [])
            if isinstance(item, dict)
        }
        if supply_chain.get("status") == "fail" or "fail" in statuses:
            raise ValueError("release evidence contains a failed report")
        if require_production and supply_chain.get("require_production") is not True:
            raise ValueError("production release requires a production supply-chain report")
        if require_production and supply_chain.get("status") != "pass":
            raise ValueError("supply-chain report is not passing")
        if not require_production and (
            supply_chain.get("status") == "not_run" or "not_run" in statuses
        ):
            return {
                "command": "release evidence chain",
                "returncode": 0,
                "output": f"release_id={context['release_id']} evidence is not_run",
                "status": "not_run",
            }
        return {
            "command": "release evidence chain",
            "returncode": 0,
            "output": f"release_id={context['release_id']} lock_sha256={lock['lock_sha256']}",
            "status": "pass",
        }
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "command": "release evidence chain",
            "returncode": 1,
            "output": str(exc),
            "status": "fail",
        }


def main() -> int:
    require_production = os.getenv("TRPC_RELEASE_REQUIRE_PRODUCTION", "0").lower() in {"1", "true", "yes", "on"}
    checks = [
        run([sys.executable, "-m", "compileall", "-q", "trpc_service", "tests"]),
        run([sys.executable, "-m", "unittest", "discover", "-s", "tests"]),
        run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "tests/test_session_mailbox_v2.py",
                "tests/test_durable_recovery.py",
            ]
        ),
        run([sys.executable, "-m", "flake8", "trpc_service", "tests", "scripts"]),
        run([sys.executable, "scripts/kubernetes_runtime_gate.py", "--static"]),
        run([sys.executable, "scripts/observability_gate.py"]),
        run([sys.executable, "scripts/security_gate.py"]),
    ]
    checks.append(_evidence_gate(require_production))
    manifest = (ROOT / "deployment" / "kubernetes" / "platform.yaml").read_text(encoding="utf-8")
    registry = (ROOT / "trpc_service" / "channels" / "registry.py").read_text(encoding="utf-8")
    rls = (ROOT / "trpc_service" / "storage" / "postgres_rls.py").read_text(encoding="utf-8")
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    compose = (ROOT / "deployment" / "docker-compose.yml").read_text(encoding="utf-8")
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    migration_runner = (ROOT / "trpc_service" / "database" / "runner.py").read_text(encoding="utf-8")
    migration_baseline = (
        ROOT / "trpc_service" / "database" / "versions" / "20260903_01_initial_schema.py"
    ).read_text(encoding="utf-8")
    tool_governance = (
        ROOT / "trpc_service" / "storage" / "tool_governance.py"
    ).read_text(encoding="utf-8")
    session_mailbox = (
        ROOT / "trpc_service" / "storage" / "session_mailbox.py"
    ).read_text(encoding="utf-8")
    retry_policy = (
        ROOT / "trpc_service" / "storage" / "retry.py"
    ).read_text(encoding="utf-8")
    cli = (ROOT / "trpc_service" / "_cli.py").read_text(encoding="utf-8")
    required_manifest_values = {
        "DURABLE_INBOX_OUTBOX: \"1\"": "durable Inbox/Outbox",
        "WORKER_QUEUE_TRANSPORT: streams": "Redis Streams transport",
        "POSTGRES_RLS_ENABLED: \"1\"": "PostgreSQL RLS",
        "readOnlyRootFilesystem: true": "read-only containers",
        "POSTGRES_RLS_WORKER_ROLE: trpc_worker": "dedicated PostgreSQL worker role",
    }
    manifest_checks = [
        {
            "name": name,
            "ok": needle in manifest,
            "detail": needle if needle in manifest else f"missing: {needle}",
        }
        for needle, name in required_manifest_values.items()
    ]
    checks.extend(
        {
            "command": item["name"],
            "returncode": 0 if item["ok"] else 1,
            "output": item["detail"],
        }
        for item in manifest_checks
    )
    checks.extend(
        {
            "command": name,
            "returncode": 0 if ok else 1,
            "output": detail,
        }
        for name, ok, detail in (
            (
                "supported IM registry",
                all(
                    value in registry
                    for value in ("WeComAIBotAdapter", "FeishuAdapter", "TelegramAdapter", "WebAdapter")
                ),
                "default registry contains WeCom AI Bot, Feishu, Telegram, and Web",
            ),
            (
                "legacy WeCom disabled by default",
                _default_channels() == {"web", "wecom_ai_bot", "feishu", "telegram"},
                "traditional WeCom callback adapter requires ENABLE_LEGACY_WECOM=1",
            ),
            (
                "retired IM channels excluded",
                "WeChatOfficialAccountAdapter" not in registry and "WeChatCustomerServiceAdapter" not in registry,
                "official account and customer service are not default adapters",
            ),
            (
                "tool execution ledger",
                "tool_executions" in rls and "tool_executions" in tool_governance,
                "durable tool execution table and RLS coverage present",
            ),
            (
                "image source fingerprint labels",
                "TRPC_SOURCE_FINGERPRINT" in dockerfile and "source-fingerprint" in dockerfile,
                "Docker image carries source fingerprint metadata",
            ),
            (
                "compose immutable image selector",
                "TRPC_IMAGE:-trpc-agent-service:local" in compose,
                "Compose application services accept a release image reference",
            ),
            (
                "versioned PostgreSQL migrations",
                "alembic>=1.16,<2" in pyproject
                and "pg_advisory_lock" in migration_runner
                and "20260903_01" in migration_baseline,
                "Alembic baseline and serialized migration runner are present",
            ),
            (
                "deployment migration gate",
                "trpc-agent-migrate db upgrade" in manifest
                and "trpc-agent-migrate db check" in manifest
                and "trpc-agent-migrate db upgrade" in compose
                and "trpc-agent-migrate db check" in compose
                and "POSTGRES_AUTO_CREATE_SCHEMA: ${POSTGRES_AUTO_CREATE_SCHEMA:-1}" not in compose,
                "Compose and Kubernetes upgrade and verify the schema before startup",
            ),
            (
                "session recovery operations",
                "session.dead_letter.v2" in session_mailbox
                and "retry_delay_seconds" in retry_policy
                and "mailbox-maintenance" in cli
                and "operator_replay" in cli,
                "session poison handling, bounded retry, maintenance, and audited replay are present",
            ),
        )
    )
    # Production must opt into durable ordering explicitly. Local tests can
    # continue to set TRPC_AGENT_RUNTIME_MODE=local.
    production_mode = os.getenv("TRPC_AGENT_RUNTIME_MODE", "trpc").strip().lower()
    if production_mode != "local" and os.getenv("DURABLE_INBOX_OUTBOX", "1").lower() not in {
        "1", "true", "yes", "on"
    }:
        checks.append(
            {
                "command": "production durable mode",
                "returncode": 1,
                "output": "DURABLE_INBOX_OUTBOX must be enabled outside local mode",
            }
        )
    print(json.dumps({"ok": all(item["returncode"] == 0 for item in checks), "checks": checks}, indent=2))
    return 0 if all(item["returncode"] == 0 for item in checks) else 1


def _default_channels() -> set[str]:
    code = (
        "from trpc_service.channels import default_channel_adapters; "
        "print(','.join(sorted(default_channel_adapters())))"
    )
    env = os.environ.copy()
    env.pop("ENABLE_LEGACY_WECOM", None)
    result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=env, text=True, capture_output=True)
    if result.returncode != 0:
        return set()
    return {item for item in result.stdout.strip().split(",") if item}


if __name__ == "__main__":
    raise SystemExit(main())
