"""Unified production-candidate acceptance report.

This command deliberately distinguishes local proof from external runtime
proof.  A missing disposable database, cluster, proxy, or IM credential is
reported as ``not_run`` and never as a successful check.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
PYTHON = sys.executable

from scripts.candidate_lock import verify_lock
from scripts.evidence_lineage import binding_matches, release_binding, verify_evidence
from scripts.release_context import verify_context
from trpc_service.security.secrets import redact_secret_text


def _safe_command(command: list[str]) -> list[str]:
    """Keep credentials out of acceptance reports and CI logs."""

    safe: list[str] = []
    for value in command:
        safe.append(re.sub(r"(://[^:/\s]+:)[^@/\s]+@", r"\1[redacted]@", value))
    return safe


def _run(command: list[str], timeout: int = 180) -> dict[str, object]:
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"status": "fail", "command": command, "error": str(exc)[:1000]}
    output = redact_secret_text(completed.stdout + completed.stderr)[-4000:]
    return {
        "status": "pass" if completed.returncode == 0 else "fail",
        "command": _safe_command(command),
        "returncode": completed.returncode,
        "output": output,
    }


def _not_run(reason: str) -> dict[str, object]:
    return {"status": "not_run", "reason": reason}


def _runtime_probe() -> dict[str, object]:
    postgres = os.getenv("PRODUCTION_ACCEPTANCE_POSTGRES_DSN", "").strip()
    redis = os.getenv("PRODUCTION_ACCEPTANCE_REDIS_URL", "").strip()
    if not postgres or not redis:
        return _not_run(
            "set PRODUCTION_ACCEPTANCE_POSTGRES_DSN and "
            "PRODUCTION_ACCEPTANCE_REDIS_URL to run disposable multi-process checks"
        )
    return _run(
        [
            PYTHON,
            str(ROOT / "scripts" / "concurrency_gate.py"),
            "--postgres-dsn",
            postgres,
            "--redis-url",
            redis,
        ],
        timeout=240,
    )


def _session_reliability_probe() -> dict[str, object]:
    postgres = os.getenv("PRODUCTION_ACCEPTANCE_POSTGRES_DSN", "").strip()
    redis = os.getenv("PRODUCTION_ACCEPTANCE_REDIS_URL", "").strip()
    if not postgres or not redis:
        return _not_run(
            "set PRODUCTION_ACCEPTANCE_POSTGRES_DSN and "
            "PRODUCTION_ACCEPTANCE_REDIS_URL for session recovery checks"
        )
    return _run(
        [
            PYTHON,
            str(ROOT / "scripts" / "session_reliability_gate.py"),
            "--postgres-dsn",
            postgres,
            "--redis-url",
            redis,
        ],
        timeout=240,
    )


def _rls_probe() -> dict[str, object]:
    dsn = os.getenv("POSTGRES_RLS_TEST_DSN", "").strip()
    allowed = os.getenv("POSTGRES_RLS_TEST_ALLOW_DESTRUCTIVE", "").lower()
    if not dsn or allowed not in {"1", "true", "yes", "on"}:
        return _not_run(
            "set POSTGRES_RLS_TEST_DSN and POSTGRES_RLS_TEST_ALLOW_DESTRUCTIVE=1 "
            "for a disposable PostgreSQL role/RLS test"
        )
    result = _run([PYTHON, "-m", "pytest", "-q", "tests/test_postgres_rls.py"], timeout=240)
    return result


def _migration_probe() -> dict[str, object]:
    source = os.getenv("PRODUCTION_ACCEPTANCE_MIGRATION_SOURCE_DSN", "").strip()
    target = os.getenv("PRODUCTION_ACCEPTANCE_MIGRATION_TARGET_DSN", "").strip()
    if not source or not target:
        return _not_run(
            "set PRODUCTION_ACCEPTANCE_MIGRATION_SOURCE_DSN and "
            "PRODUCTION_ACCEPTANCE_MIGRATION_TARGET_DSN for a disposable migration run"
        )
    tenant_id = f"acceptance-migration-{uuid4().hex}"
    data_dir = ROOT / "data" / "acceptance-migration" / uuid4().hex
    data_dir.mkdir(parents=True, exist_ok=True)
    seeded = _seed_migration_source(source, tenant_id, data_dir / "stores" / "source")
    command = [
        PYTHON,
        "-m",
        "trpc_service.migrate",
        "migration-run",
        "--tenant",
        tenant_id,
        "--source-backend",
        "postgres",
        "--target-backend",
        "postgres",
        "--source-sql-dsn",
        source,
        "--target-sql-dsn",
        target,
        "--state-file",
        str(data_dir / "state.json"),
        "--snapshot",
        str(data_dir / "snapshot.json"),
        "--data-dir",
        str(data_dir / "stores"),
    ]
    result = _run(command, timeout=300)
    try:
        state = json.loads((data_dir / "state.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        result["status"] = "fail"
        result["error"] = f"migration state unavailable: {exc}"
        return result
    verification = state.get("metadata", {}).get("verification", {})
    expected = verification.get("expected", {})
    actual = verification.get("actual", {})
    required_nonzero = {
        name for name, count in seeded.items() if count > 0
    }
    data_ok = (
        result["status"] == "pass"
        and state.get("phase") == "cleanup"
        and verification.get("ok") is True
        and expected == seeded
        and actual == seeded
        and required_nonzero.issubset({name for name, count in expected.items() if count > 0})
    )
    result.update(
        {
            "status": "pass" if data_ok else "fail",
            "tenant_id": tenant_id,
            "seeded_counts": seeded,
            "state_phase": state.get("phase"),
            "verification": verification,
        }
    )
    if not data_ok:
        result["error"] = "migration did not preserve the seeded non-empty tenant dataset"
    return result


def _migration_rollback_probe() -> dict[str, object]:
    source = os.getenv("PRODUCTION_ACCEPTANCE_MIGRATION_SOURCE_DSN", "").strip()
    target = os.getenv("PRODUCTION_ACCEPTANCE_MIGRATION_ROLLBACK_TARGET_DSN", "").strip()
    if not source or not target:
        return _not_run(
            "set PRODUCTION_ACCEPTANCE_MIGRATION_SOURCE_DSN and "
            "PRODUCTION_ACCEPTANCE_MIGRATION_ROLLBACK_TARGET_DSN for a disposable rollback run"
        )
    tenant_id = f"acceptance-rollback-{uuid4().hex}"
    data_dir = ROOT / "data" / "acceptance-migration" / uuid4().hex
    data_dir.mkdir(parents=True, exist_ok=True)
    seeded = _seed_migration_source(source, tenant_id, data_dir / "rollback-stores" / "source")
    state_file = data_dir / "rollback-state.json"
    snapshot_file = data_dir / "rollback-snapshot.json"
    command = [
        PYTHON,
        "-m",
        "trpc_service.migrate",
        "migration-run",
        "--tenant",
        tenant_id,
        "--source-backend",
        "postgres",
        "--target-backend",
        "postgres",
        "--source-sql-dsn",
        source,
        "--target-sql-dsn",
        target,
        "--state-file",
        str(state_file),
        "--snapshot",
        str(snapshot_file),
        "--data-dir",
        str(data_dir / "rollback-stores"),
        "--inject-failure-phase",
        os.getenv("PRODUCTION_ACCEPTANCE_MIGRATION_ROLLBACK_PHASE", "verify"),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=300,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"status": "fail", "command": _safe_command(command), "error": str(exc)[:1000]}
    try:
        state = json.loads(state_file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {
            "status": "fail",
            "command": _safe_command(command),
            "error": f"rollback state unavailable: {exc}",
        }
    history = {(item.get("from"), item.get("to")) for item in state.get("history", [])}
    ok = (
        completed.returncode != 0
        and state.get("phase") == "rolled_back"
        and ("failed", "rolled_back") in history
    )
    return {
        "status": "pass" if ok else "fail",
        "command": _safe_command(command),
        "returncode": completed.returncode,
        "tenant_id": tenant_id,
        "seeded_counts": seeded,
        "state_phase": state.get("phase"),
        "history": state.get("history", []),
        "output": (completed.stdout + completed.stderr)[-2000:],
    }


def _seed_migration_source(dsn: str, tenant_id: str, data_dir: Path) -> dict[str, int]:
    """Seed every durable migration section with isolated disposable data."""

    from trpc_service.migrate import _counts, _profile, export_tenant
    from trpc_service.storage.base import AuditRecord, MemoryItem, SessionEvent, Summary
    from trpc_service.storage.factory import create_storage
    from trpc_service.storage.tool_governance import arguments_hash
    from trpc_service.storage.vector_store import KnowledgeChunk

    previous_auto_create = os.environ.get("POSTGRES_AUTO_CREATE_SCHEMA")
    os.environ["POSTGRES_AUTO_CREATE_SCHEMA"] = "1"
    storage = create_storage(_profile("postgres", "", dsn), data_dir)
    try:
        session_id = "acceptance-session"
        request_id = "acceptance-request"
        args_hash = arguments_hash({"target": "acceptance"})
        storage.session.append_event(
            SessionEvent(
                tenant_id,
                session_id,
                f"{tenant_id}:acceptance-event-1",
                "user_message",
                {"text": "migration acceptance"},
                "acceptance-trace",
                f"{tenant_id}:acceptance-message-1",
            )
        )
        storage.summary.put(
            Summary(tenant_id, session_id, "migration acceptance summary", 1)
        )
        storage.memory.put(
            MemoryItem(tenant_id, "acceptance-memory", "session", "durable migration memory")
        )
        storage.audit.append(
            AuditRecord(
                audit_id=f"{tenant_id}:acceptance-audit",
                tenant_id=tenant_id,
                decision="allow",
                trace_id="acceptance-trace",
                channel="acceptance",
                session_id=session_id,
            )
        )
        storage.idempotency.start(tenant_id, "acceptance-idempotency", "acceptance-trace")
        storage.idempotency.complete(
            tenant_id,
            "acceptance-idempotency",
            "acceptance-response",
            {"ok": True},
        )
        storage.knowledge.upsert(
            KnowledgeChunk(
                tenant_id,
                "acceptance",
                "acceptance-knowledge",
                "durable migration knowledge",
            )
        )
        storage.artifacts.put(tenant_id, b"migration acceptance artifact", "text/plain")
        storage.mailbox.enqueue(
            tenant_id,
            session_id,
            "acceptance-mailbox-message",
            "acceptance-mailbox-dedupe",
            {"text": "mailbox acceptance"},
        )
        session_mailbox_v2 = getattr(storage, "session_mailbox_v2", None)
        if session_mailbox_v2 is None:
            raise RuntimeError("session mailbox v2 is required for acceptance")
        session_mailbox_v2.accept(
            tenant_id,
            session_id,
            "acceptance-session-mailbox-message",
            trace_id="acceptance-session-mailbox-trace",
        )
        inbox, accepted = storage.inbox_outbox.accept_inbox(
            tenant_id,
            "acceptance-inbox-dedupe",
            session_id,
            {"text": "inbox acceptance"},
            "acceptance-seeder",
        )
        if not accepted:
            raise RuntimeError("acceptance inbox seed was unexpectedly deduplicated")
        storage.inbox_outbox.complete_inbox_and_enqueue_outbox(
            tenant_id,
            inbox.dedupe_key,
            "acceptance-seeder",
            {"ok": True},
            "acceptance.sent",
            session_id,
            f"{tenant_id}:acceptance-outbox-event",
        )
        governance = storage.tool_governance
        governance.create_or_get(
            tenant_id,
            "acceptance-approval",
            session_id,
            request_id,
            "acceptance_tool",
            args_hash,
        )
        governance.approve(tenant_id, "acceptance-approval", "acceptance-user")
        governance.consume(tenant_id, "acceptance-approval", request_id, args_hash)
        governance.reserve_call(tenant_id, request_id, "acceptance-call", True, 4, 2)
        governance.begin_execution(
            tenant_id,
            "acceptance-execution",
            request_id,
            session_id,
            "acceptance_tool",
            "acceptance-call",
            args_hash,
            True,
        )
        governance.complete_execution(
            tenant_id,
            "acceptance-call",
            {"ok": True},
        )
        return _counts(export_tenant(storage, tenant_id))
    finally:
        storage.close()
        if previous_auto_create is None:
            os.environ.pop("POSTGRES_AUTO_CREATE_SCHEMA", None)
        else:
            os.environ["POSTGRES_AUTO_CREATE_SCHEMA"] = previous_auto_create


def _toxiproxy_probe() -> dict[str, object]:
    if not os.getenv("TOXIPROXY_API_URL", "").strip():
        return _not_run("TOXIPROXY_API_URL is not configured")
    result = _run(
        [
            PYTHON,
            str(ROOT / "scripts" / "fault_injection_gate.py"),
            "--output",
            "data/acceptance-toxiproxy.json",
        ],
        timeout=300,
    )
    if result.get("returncode") == 2:
        result["status"] = "not_run"
    return result


def _kubernetes_probe() -> dict[str, object]:
    if shutil.which("kubectl") is None:
        return _not_run("kubectl is not installed")
    if os.getenv("KUBERNETES_ACCEPTANCE_REQUIRED", "0").lower() not in {"1", "true", "yes", "on"}:
        return _not_run("set KUBERNETES_ACCEPTANCE_REQUIRED=1 for a live cluster test")
    return _run(
        [PYTHON, str(ROOT / "scripts" / "kubernetes_runtime_gate.py")],
        timeout=600,
    )


def _kubernetes_manifest_probe() -> dict[str, object]:
    return _run(
        [PYTHON, str(ROOT / "scripts" / "kubernetes_runtime_gate.py"), "--static"],
        timeout=120,
    )


def _validate_im_evidence(evidence_path: Path) -> dict[str, object]:
    context_path = os.getenv("TRPC_RELEASE_CONTEXT", "").strip()
    lock_path = os.getenv("TRPC_CANDIDATE_LOCK", "").strip()
    if not context_path or not lock_path:
        raise ValueError(
            "TRPC_RELEASE_CONTEXT and TRPC_CANDIDATE_LOCK are required for online IM evidence"
        )

    context = verify_context(Path(context_path), root=ROOT)
    lock = verify_lock(Path(context_path), Path(lock_path), root=ROOT)
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    if not isinstance(evidence, dict):
        raise ValueError("online IM evidence must be a JSON object")
    valid, reason = verify_evidence(evidence)
    if not valid:
        raise ValueError(reason)
    if evidence.get("evidence_type") != "online-im":
        raise ValueError("online IM evidence has an unexpected evidence type")
    if evidence.get("producer") != "scripts.online_im_gate":
        raise ValueError("online IM evidence has an unexpected producer")
    payload = evidence.get("payload")
    if not isinstance(payload, dict) or payload.get("status") != "pass":
        raise ValueError("online IM evidence payload status is not pass")
    matches, reason = binding_matches(
        evidence.get("release_binding", {}), release_binding(context, lock)
    )
    if not matches:
        raise ValueError(reason)
    return evidence


def _im_probe() -> dict[str, object]:
    channels = os.getenv("ONLINE_IM_CHANNELS", "").strip()
    if not channels:
        return _not_run("ONLINE_IM_CHANNELS is empty; configure real WeCom, Feishu, and Telegram payloads")
    result = _run([PYTHON, str(ROOT / "scripts" / "online_im_gate.py")], timeout=240)
    output_path = os.getenv("ONLINE_IM_EVIDENCE_OUTPUT", "data/online-im-evidence.json").strip()
    evidence_path = ROOT / output_path
    if evidence_path.is_file():
        try:
            evidence = _validate_im_evidence(evidence_path)
            result["evidence_path"] = str(evidence_path)
            result["evidence_sha256"] = evidence.get("evidence_sha256", "")
            result["evidence_status"] = evidence.get("payload", {}).get("status", "")
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            result["status"] = "fail"
            result["error"] = f"online IM evidence is invalid: {exc}"
    elif result.get("status") == "pass":
        result["status"] = "fail"
        result["error"] = f"online IM probe passed without evidence file: {evidence_path}"
    return result


def _security_probe() -> dict[str, object]:
    return _run([PYTHON, str(ROOT / "scripts" / "security_gate.py")], timeout=120)


def _disaster_recovery_probe() -> dict[str, object]:
    source = os.getenv("DISASTER_RECOVERY_SOURCE_DSN", "").strip()
    target = os.getenv("DISASTER_RECOVERY_TARGET_DSN", "").strip()
    if not source or not target:
        return _not_run(
            "set DISASTER_RECOVERY_SOURCE_DSN and DISASTER_RECOVERY_TARGET_DSN "
            "for a disposable backup/restore run"
        )
    return _run(
        [PYTHON, str(ROOT / "scripts" / "disaster_recovery_gate.py")],
        timeout=600,
    )


def _performance_probe() -> dict[str, object]:
    base_url = os.getenv("PRODUCTION_ACCEPTANCE_PERFORMANCE_BASE_URL", "").strip()
    if not base_url:
        return _not_run(
            "set PRODUCTION_ACCEPTANCE_PERFORMANCE_BASE_URL for a live HTTP performance run"
        )
    output_path = os.getenv(
        "PRODUCTION_ACCEPTANCE_PERFORMANCE_OUTPUT",
        "data/acceptance-performance.json",
    ).strip()
    raw_path = os.getenv(
        "PRODUCTION_ACCEPTANCE_PERFORMANCE_RAW_OUTPUT",
        "data/acceptance-performance-raw.json",
    ).strip()
    command = [
        PYTHON,
        str(ROOT / "scripts" / "performance_gate.py"),
        "--base-url",
        base_url,
        "--requests",
        os.getenv("PRODUCTION_ACCEPTANCE_PERFORMANCE_REQUESTS", "100").strip(),
        "--concurrency",
        os.getenv("PRODUCTION_ACCEPTANCE_PERFORMANCE_CONCURRENCY", "10").strip(),
        "--timeout",
        os.getenv("PRODUCTION_ACCEPTANCE_PERFORMANCE_TIMEOUT", "30").strip(),
        "--tenant",
        os.getenv("PRODUCTION_ACCEPTANCE_PERFORMANCE_TENANT", "tenant_demo").strip(),
        "--raw-output",
        raw_path,
        "--output",
        output_path,
    ]
    baseline = os.getenv("PRODUCTION_ACCEPTANCE_PERFORMANCE_BASELINE", "").strip()
    if baseline:
        command.extend(["--baseline", baseline])
    result = _run(command, timeout=600)
    result["evidence_path"] = str(ROOT / output_path)
    result["raw_output_path"] = str(ROOT / raw_path)
    return result


def _release_evidence() -> dict[str, object]:
    tracked = sorted(
        path
        for root in (
            "trpc_service",
            "scripts",
            "deployment",
            "Dockerfile",
            ".dockerignore",
            "alembic.ini",
            "README.md",
            "pyproject.toml",
            "uv.lock",
            "requirements.txt",
            "requirements-dev.txt",
        )
        for path in ((ROOT / root).rglob("*") if (ROOT / root).is_dir() else [ROOT / root])
        if path.is_file() and path.suffix != ".pyc"
    )
    digest = hashlib.sha256()
    for path in tracked:
        digest.update(str(path.relative_to(ROOT)).replace("\\", "/").encode())
        digest.update(path.read_bytes())
    image_ref = os.getenv("TRPC_LOCAL_IMAGE_REF", "").strip()
    local_image_id = ""
    local_repo_digests: list[str] = []
    image_labels: dict[str, str] = {}
    image_error = ""
    if image_ref and shutil.which("docker"):
        try:
            inspected = subprocess.run(
                ["docker", "image", "inspect", image_ref],
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=30,
                check=False,
            )
            if inspected.returncode == 0:
                records = json.loads(inspected.stdout)
                if records:
                    record = records[0]
                    local_image_id = str(record.get("Id", ""))
                    local_repo_digests = [str(item) for item in record.get("RepoDigests", [])]
                    image_labels = {
                        str(key): str(value)
                        for key, value in (record.get("Config", {}).get("Labels", {}) or {}).items()
                    }
            else:
                image_error = (inspected.stderr or inspected.stdout)[-1000:]
        except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
            image_error = str(exc)[:1000]
    sbom_path = os.getenv("TRPC_SBOM_PATH", "").strip()
    sbom_available = bool(sbom_path and (ROOT / sbom_path).is_file())
    image_fingerprint = image_labels.get("io.trpc.agent-service.source-fingerprint", "")
    fingerprint_matches = bool(local_image_id) and image_fingerprint == digest.hexdigest()
    configured_digest = os.getenv("TRPC_IMAGE_DIGEST", "").strip()
    digest_matches = not configured_digest or configured_digest == local_image_id
    evidence = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source_fingerprint": digest.hexdigest(),
        "image_digest": configured_digest,
        "local_image_ref": image_ref,
        "local_image_id": local_image_id,
        "local_repo_digests": local_repo_digests,
        "image_labels": image_labels,
        "image_fingerprint_matches": fingerprint_matches,
        "configured_digest_matches_local_id": digest_matches,
        "sbom": sbom_path if sbom_available else ("syft" if shutil.which("syft") else "not_available"),
    }
    if image_error:
        evidence["image_error"] = image_error
    if not evidence["image_digest"] and not local_image_id:
        return {
            "status": "not_run",
            "reason": "TRPC_IMAGE_DIGEST is not configured and no valid TRPC_LOCAL_IMAGE_REF was found",
            "evidence": evidence,
        }
    if not fingerprint_matches:
        return {
            "status": "fail",
            "reason": "image source fingerprint does not match the current source fingerprint",
            "evidence": evidence,
        }
    if not digest_matches:
        return {
            "status": "fail",
            "reason": "TRPC_IMAGE_DIGEST does not match the inspected local image ID",
            "evidence": evidence,
        }
    output = ROOT / "data" / "release-evidence.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    if not sbom_available:
        return {
            "status": "not_run",
            "reason": "image identity is recorded, but TRPC_SBOM_PATH does not point to an existing SBOM",
            "evidence": evidence,
            "path": str(output),
        }
    return {"status": "pass", "evidence": evidence, "path": str(output)}


def main() -> int:
    runtime = _runtime_probe()
    checks: dict[str, dict[str, object]] = {
        "1_concurrent_postgres_redis": runtime,
        "2_lease_takeover_and_fencing": runtime,
        "3_inbox_outbox_source_of_truth": runtime,
        "3b_session_mailbox_recovery_and_replay": _session_reliability_probe(),
        "4_tool_ledger_approval_ambiguity_budget": _run(
            [PYTHON, "-m", "pytest", "-q", "tests/test_production_depth.py"]
        ),
        "5_real_migration_phases": _migration_probe(),
        "5b_real_migration_rollback": _migration_rollback_probe(),
        "6_postgres_rls_and_worker_role": _rls_probe(),
        "7_toxiproxy_dependency_interruptions": _toxiproxy_probe(),
        "8_kubernetes_manifest": _kubernetes_manifest_probe(),
        "8_kubernetes_runtime": _kubernetes_probe(),
        "9_real_im_accounts": _im_probe(),
        "10_security_and_supply_chain": _security_probe(),
        "11_disaster_recovery": _disaster_recovery_probe(),
        "12_performance_acceptance": _performance_probe(),
        "13_release_evidence": _release_evidence(),
    }
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "branch": os.getenv("GIT_BRANCH", ""),
        "checks": checks,
        "summary": {
            "pass": sum(item["status"] == "pass" for item in checks.values()),
            "fail": sum(item["status"] == "fail" for item in checks.values()),
            "not_run": sum(item["status"] == "not_run" for item in checks.values()),
        },
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if report["summary"]["fail"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
