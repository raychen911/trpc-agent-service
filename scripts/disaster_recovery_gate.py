"""Disposable PostgreSQL backup and restore acceptance gate.

The command is intentionally opt-in. It never treats a missing database or
backup tool as a successful production recovery test.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4

from trpc_service.security.secrets import redact_secret_text

ROOT = Path(__file__).resolve().parents[1]
TABLE_COUNTS_QUERY = (
    "WITH table_names AS ("
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema='public' AND table_type='BASE TABLE' "
    "AND table_name <> 'alembic_version') "
    "SELECT COALESCE(jsonb_object_agg(table_name, row_count ORDER BY table_name)::text, '{}') "
    "FROM (SELECT table_name, "
    "(xpath('/table/row/count/text()', query_to_xml("
    "format('SELECT count(*) AS count FROM %I', table_name), true, false, '')))[1]"
    "::text::bigint AS row_count FROM table_names) counts"
)


def _not_run(reason: str) -> dict[str, object]:
    return {"status": "not_run", "reason": reason}


def _run(command: list[str], timeout: int = 180) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=timeout, check=False)


def _client_command(client: str, tool: str, *arguments: str) -> list[str]:
    if client:
        return ["docker", "exec", client, tool, *arguments]
    return [tool, *arguments]


def _table_counts(client: str, dsn: str) -> tuple[dict[str, int] | None, str]:
    result = _run(_client_command(client, "psql", dsn, "-Atqc", TABLE_COUNTS_QUERY), timeout=120)
    if result.returncode != 0:
        return None, result.stderr[-1000:]
    try:
        value = json.loads(result.stdout.strip() or "{}")
    except json.JSONDecodeError as exc:
        return None, f"table count output is invalid JSON: {exc}"
    if not isinstance(value, dict) or not all(
        isinstance(name, str) and isinstance(count, int) for name, count in value.items()
    ):
        return None, "table count output must be a JSON object of integer counts"
    return value, ""


def main() -> int:
    source = os.getenv("DISASTER_RECOVERY_SOURCE_DSN", "").strip()
    target = os.getenv("DISASTER_RECOVERY_TARGET_DSN", "").strip()
    allowed = os.getenv("DISASTER_RECOVERY_ALLOW_DESTRUCTIVE", "").lower()
    if not source or not target:
        print(json.dumps(_not_run("set DISASTER_RECOVERY_SOURCE_DSN and DISASTER_RECOVERY_TARGET_DSN")))
        return 0
    if allowed not in {"1", "true", "yes", "on"}:
        print(json.dumps(_not_run("set DISASTER_RECOVERY_ALLOW_DESTRUCTIVE=1 for disposable databases")))
        return 0
    client = os.getenv("DISASTER_RECOVERY_PGCLIENT_CONTAINER", "").strip()
    required_tools = ("docker",) if client else ("pg_dump", "pg_restore", "psql")
    if any(shutil.which(tool) is None for tool in required_tools):
        requirement = (
            "docker is required when DISASTER_RECOVERY_PGCLIENT_CONTAINER is configured"
            if client
            else "pg_dump, pg_restore, and psql are required"
        )
        print(json.dumps(_not_run(requirement)))
        return 0

    parser = argparse.ArgumentParser(description="Verify PostgreSQL backup and restore on disposable databases")
    parser.add_argument("--output", default="runs/phase6/disaster-recovery.json")
    args = parser.parse_args()
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    if client:
        dump_path = f"/tmp/trpc-agent-dr-{uuid4().hex}.dump"
    else:
        directory = tempfile.TemporaryDirectory(prefix="trpc-agent-dr-")
        dump_path = str(Path(directory.name) / "backup.dump")
    try:
        dump = _run(
            _client_command(client, "pg_dump", "--format=custom", "--file", dump_path, source),
            timeout=300,
        )
        restore = _run(
            _client_command(
                client,
                "pg_restore",
                "--clean",
                "--if-exists",
                "--no-owner",
                "--dbname",
                target,
                dump_path,
            ),
            timeout=300,
        )
        probe = _run(_client_command(client, "psql", target, "-Atqc", "SELECT 1"), timeout=60)
        source_counts, source_count_error = _table_counts(client, source)
        target_counts, target_count_error = _table_counts(client, target)
    finally:
        if client:
            _run(_client_command(client, "rm", "-f", dump_path), timeout=30)
        else:
            directory.cleanup()
    data_integrity = (
        source_counts is not None
        and target_counts is not None
        and source_counts == target_counts
    )
    result = {
        "status": (
            "pass"
            if dump.returncode == restore.returncode == probe.returncode == 0 and data_integrity
            else "fail"
        ),
        "dump_returncode": dump.returncode,
        "restore_returncode": restore.returncode,
        "probe_returncode": probe.returncode,
        "client_container": client or None,
        "source_table_counts": source_counts,
        "target_table_counts": target_counts,
        "data_integrity_match": data_integrity,
        "output": redact_secret_text(
            dump.stdout
            + dump.stderr
            + restore.stdout
            + restore.stderr
            + probe.stdout
            + probe.stderr
            + source_count_error
            + target_count_error
        )[-3000:],
    }
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({**result, "path": str(output)}, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
