"""Exercise two real loopback processes, shared SQLite, failover and restore.

All data and child logs live in a fresh OS temporary directory. Only synthetic
Web messages are sent. No WeCom connection or production backend is touched.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import os
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import zipfile
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import yaml

ROOT = Path(__file__).resolve().parents[1]
PYTHON = Path(sys.executable)
HIDDEN = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def sql_url(path: Path) -> str:
    return f"sqlite+aiosqlite:///{path.as_posix()}"


def sql_rows(path: Path, statement: str, parameters: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with sqlite3.connect(path) as connection:
        return connection.execute(statement, parameters).fetchall()


def command(arguments: list[str], env: dict[str, str], *, cwd: Path = ROOT) -> str:
    completed = subprocess.run(  # noqa: S603 - fixed CLI arguments, local fixture paths
        arguments,
        cwd=cwd,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=120,
        creationflags=HIDDEN,
    )
    if completed.returncode:
        raise RuntimeError(f"local command failed: {arguments[1:3]} (exit {completed.returncode})")
    return completed.stdout


class LocalCheck:
    def __init__(self, requests: int, concurrency: int) -> None:
        self.directory = Path(tempfile.mkdtemp(prefix="tap-local-runtime-"))
        self.requests, self.concurrency = requests, concurrency
        self.report: dict[str, Any] = {"scope": "loopback-two-process-sqlite", "checks": []}
        self.databases = {name: self.directory / f"{name}.db" for name in ("control", "data", "native")}
        self.env = dict(os.environ)
        self.env.update(
            TAP_ENVIRONMENT="test",
            TAP_BROKER_MODE="inline",
            TAP_AUTO_CREATE_SCHEMA="false",
            TAP_BOOTSTRAP_CONFIG_PATH=str(self.directory / "tenants.yaml"),
            TAP_CONTROL_DATABASE_URL=sql_url(self.databases["control"]),
            TAP_SESSION_HMAC_KEY=secrets.token_hex(32),
            TAP_ADMIN_BEARER_TOKEN=secrets.token_hex(32),
            TAP_INTERNAL_BEARER_TOKEN=secrets.token_hex(32),
            TAP_LOG_LEVEL="WARNING",
            TAP_MODEL_TIMEOUT_SECONDS="2",
            TAP_SESSION_LOCK_TIMEOUT_SECONDS="5",
            TAP_PROCESSING_LEASE_SECONDS="2",
            TAP_WORKER_POLL_MS="20",
            TAP_OUTBOX_POLL_SECONDS="0.05",
            TAP_SHUTDOWN_GRACE_SECONDS="3",
            PYTHONIOENCODING="utf-8",
        )
        self.nodes: list[subprocess.Popen[bytes] | None] = [None, None]
        self.logs: list[Any] = []
        self.ports: list[int] = []
        for _ in range(2):
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                self.ports.append(listener.getsockname()[1])

    def record(self, name: str, **evidence: Any) -> None:
        row = {"check": name, **evidence}
        self.report["checks"].append(row)
        print(json.dumps(row), flush=True)

    def provision(self) -> None:
        original = yaml.safe_load((ROOT / "config/tenants.example.yaml").read_text(encoding="utf-8"))
        tenants = []
        for tenant_id in ("alpha", "bravo"):
            tenant = copy.deepcopy(original["tenants"][0])
            prefix = f"TENANT_{tenant_id.upper()}"
            self.env[f"{prefix}_WEBHOOK_TOKEN"] = secrets.token_hex(24)
            self.env[f"{prefix}_DATA_SQL"] = sql_url(self.databases["data"])
            self.env[f"{prefix}_NATIVE_SQL"] = sql_url(self.databases["native"])
            tenant.update(tenant_id=tenant_id, display_name=f"Local {tenant_id}")
            binding = tenant["channels"][0]
            binding.update(binding_id=f"web-{tenant_id}-local", external_account_id=f"local-{tenant_id}")
            binding["credential_refs"] = {"webhook_token": {"uri": f"env://{prefix}_WEBHOOK_TOKEN"}}
            backend = {"kind": "sql", "dsn_ref": {"uri": f"env://{prefix}_DATA_SQL"}}
            tenant["data_backends"] = {
                resource: copy.deepcopy(backend)
                for resource in ("session", "memory", "summary", "artifact", "knowledge", "audit")
            }
            tenant["data_backends"]["session"]["native_dsn_ref"] = {"uri": f"env://{prefix}_NATIVE_SQL"}
            tenant["data_backends"]["audit"]["dsn_ref"] = {"uri": "env://TAP_CONTROL_DATABASE_URL"}
            tenant["models"]["offline"].update(context_window_tokens=16_384, max_output_tokens=1_024)
            tenant["governance"]["budget"].update(monthly_tokens=20_000_000, max_concurrent_sessions=100)
            tenants.append(tenant)
        (self.directory / "tenants.yaml").write_text(yaml.safe_dump({"tenants": tenants}), encoding="utf-8")
        for name in ("control", "data"):
            self.env["TAP_VALIDATION_SQL"] = sql_url(self.databases[name])
            command(
                [
                    str(PYTHON),
                    "-m",
                    "tenant_agent.cli",
                    "db-init",
                    "--database-url-env",
                    "TAP_VALIDATION_SQL",
                ],
                self.env,
            )
            version = sql_rows(self.databases[name], "select version_num from alembic_version")[0][0]
            assert version == "d4e5f607a1b2"
        self.env["TAP_VALIDATION_SQL"] = sql_url(self.databases["native"])
        command(
            [
                str(PYTHON),
                "-m",
                "tenant_agent.cli",
                "native-session-init",
                "--database-url-env",
                "TAP_VALIDATION_SQL",
            ],
            self.env,
        )
        self.record("fresh_platform_and_native_migrations", passed=True)

    def start(self, index: int, *, env: dict[str, str] | None = None) -> None:
        child_env = dict(env or self.env)
        child_env["TAP_NODE_ID"] = f"local-check-{index}"
        if index == 0:
            child_env["TAP_VALIDATION_PAUSE_MARKER"] = str(self.directory / "paused")
        log = (self.directory / f"node-{index}-{time.time_ns()}.log").open("wb")
        self.logs.append(log)
        self.nodes[index] = subprocess.Popen(  # noqa: S603 - only starts this fixture server
            [
                str(PYTHON),
                "-m",
                "uvicorn",
                "scripts.local_runtime_server:create_validation_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.ports[index]),
                "--no-access-log",
            ],
            cwd=ROOT,
            env=child_env,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=HIDDEN,
        )

    def kill(self, index: int) -> None:
        process = self.nodes[index]
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        self.nodes[index] = None

    async def ready(self, client: httpx.AsyncClient, index: int) -> None:
        for _ in range(150):
            process = self.nodes[index]
            if process is None or process.poll() is not None:
                raise RuntimeError(f"local node {index} exited during startup")
            try:
                response = await client.get(f"http://127.0.0.1:{self.ports[index]}/health/ready", timeout=2)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.1)
        raise TimeoutError("local startup timeout")

    async def send(
        self,
        client: httpx.AsyncClient,
        node: int,
        tenant: str,
        user: str,
        message: str,
        text: str = "local validation",
    ) -> dict[str, Any]:
        response = await client.post(
            f"http://127.0.0.1:{self.ports[node]}/v1/channels/web/web-{tenant}-local/webhook?synchronous=true",
            headers={"x-webhook-token": self.env[f"TENANT_{tenant.upper()}_WEBHOOK_TOKEN"]},
            json={"user_id": user, "conversation_id": user, "message_id": message, "text": text},
        )
        response.raise_for_status()
        return dict(response.json()["results"][0])

    async def scenarios(self) -> None:
        async with httpx.AsyncClient(timeout=90) as client:
            for index in range(2):
                self.start(index)
                await self.ready(client, index)
            first = await self.send(client, 0, "alpha", "same-user", "first")
            second = await self.send(client, 1, "alpha", "same-user", "second")
            other = await self.send(client, 1, "bravo", "same-user", "first")
            assert first["session_id"] == second["session_id"] != other["session_id"]
            denied = await client.get(
                f"http://127.0.0.1:{self.ports[1]}/admin/v1/tenants/bravo/sessions/{first['session_id']}",
                headers={"Authorization": f"Bearer {self.env['TAP_ADMIN_BEARER_TOKEN']}"},
            )
            assert denied.status_code == 404
            self.record(
                "two_process_session_routing_and_tenant_isolation",
                passed=True,
                pids=[p.pid for p in self.nodes if p],
            )

            duplicates = await asyncio.gather(
                *(self.send(client, index % 2, "alpha", "duplicate", "duplicate") for index in range(16))
            )
            session = duplicates[0]["session_id"]
            rows = sql_rows(
                self.databases["data"],
                "select count(*) from session_events where tenant_id=? and session_id=?",
                ("alpha", session),
            )
            assert rows[0][0] == 2
            self.record("cross_process_duplicate_delivery", passed=True, requests=16, committed_events=2)

            hot = await asyncio.gather(
                *(self.send(client, index % 2, "alpha", "hot", f"hot-{index}") for index in range(12))
            )
            assert all(row["status"] == "processed" for row in hot)
            sequences = sql_rows(
                self.databases["data"],
                "select sequence from session_events where tenant_id=? and session_id=? order by sequence",
                ("alpha", hot[0]["session_id"]),
            )
            assert [row[0] for row in sequences] == list(range(1, 25))
            self.record("cross_process_hot_session", passed=True, requests=12, contiguous_events=24)

            semaphore, statuses, errors, latencies = (
                asyncio.Semaphore(self.concurrency),
                Counter(),
                Counter(),
                [],
            )

            async def load(index: int) -> None:
                async with semaphore:
                    started = time.perf_counter()
                    try:
                        result = await self.send(
                            client,
                            index % 2,
                            "alpha" if index % 2 == 0 else "bravo",
                            f"load-{index}",
                            f"load-{index}",
                        )
                        statuses[result["status"]] += 1
                    except httpx.HTTPStatusError as exc:
                        errors[f"http:{exc.response.status_code}"] += 1
                    except Exception as exc:
                        errors[exc.__class__.__name__] += 1
                    latencies.append(time.perf_counter() - started)

            started = time.perf_counter()
            await asyncio.gather(*(load(index) for index in range(self.requests)))
            elapsed = time.perf_counter() - started
            latencies.sort()
            success = not errors and statuses["processed"] == self.requests
            self.record(
                "two_process_http_load",
                passed=success,
                requests=self.requests,
                concurrency=self.concurrency,
                statuses=dict(statuses),
                errors=dict(errors),
                throughput_rps=round(self.requests / elapsed, 2),
                p95_ms=round(latencies[int(len(latencies) * 0.95)] * 1000, 2),
            )

            pending = asyncio.create_task(
                self.send(client, 0, "alpha", "crash", "crash", "local-validation-pause")
            )
            for _ in range(100):
                if (self.directory / "paused").exists():
                    break
                await asyncio.sleep(0.05)
            assert (self.directory / "paused").exists(), "model fault injection was not reached"
            self.kill(0)
            await asyncio.gather(pending, return_exceptions=True)
            duplicate = await self.send(client, 1, "alpha", "crash", "crash", "local-validation-pause")
            assert duplicate["status"] == "duplicate_processing"
            expiry = sql_rows(
                self.databases["control"],
                "select lease_expires_at from inbound_receipts where status=?",
                ("processing",),
            )[0][0]
            deadline = datetime.fromisoformat(expiry).replace(tzinfo=UTC)
            print(
                json.dumps(
                    {
                        "check": "waiting_for_real_receipt_lease_expiry",
                        "seconds": round((deadline - datetime.now(UTC)).total_seconds()),
                    }
                ),
                flush=True,
            )
            while datetime.now(UTC) <= deadline:  # noqa: ASYNC110 - observes a real persisted lease deadline
                await asyncio.sleep(min(5, max(0.05, (deadline - datetime.now(UTC)).total_seconds())))
            recovered = await self.send(client, 1, "alpha", "crash", "crash", "local-validation-pause")
            assert recovered["status"] == "processed"
            crash_events = sql_rows(
                self.databases["data"],
                "select count(*) from session_events where tenant_id=? and session_id=?",
                ("alpha", recovered["session_id"]),
            )[0][0]
            assert crash_events == 2
            self.record(
                "killed_process_mid_turn_recovery",
                passed=True,
                committed_events=crash_events,
                clock_was_not_modified=True,
            )

            self.kill(1)
            restored = self.backup_restore()
            restored_env = dict(self.env)
            restored_env["TAP_CONTROL_DATABASE_URL"] = sql_url(restored["control"])
            for tenant in ("ALPHA", "BRAVO"):
                restored_env[f"TENANT_{tenant}_DATA_SQL"] = sql_url(restored["data"])
                restored_env[f"TENANT_{tenant}_NATIVE_SQL"] = sql_url(restored["native"])
            self.start(1, env=restored_env)
            await self.ready(client, 1)
            duplicate = await self.send(client, 1, "alpha", "crash", "crash", "local-validation-pause")
            assert duplicate["status"] == "duplicate_completed"
            continued = await self.send(client, 1, "alpha", "same-user", "after-restore")
            assert continued["session_id"] == first["session_id"]
            assert continued["status"] == "processed"
            self.record("restored_server_cached_reply_and_session_continuation", passed=True)

    def backup_restore(self) -> dict[str, Path]:
        restored = {}
        for name, source in self.databases.items():
            backup, destination = self.directory / f"{name}.backup.db", self.directory / f"{name}.restored.db"
            with sqlite3.connect(source) as original, sqlite3.connect(backup) as snapshot:
                original.backup(snapshot)
            with sqlite3.connect(backup) as snapshot, sqlite3.connect(destination) as target:
                snapshot.backup(target)
            with sqlite3.connect(source) as original, sqlite3.connect(destination) as target:
                assert target.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                assert (
                    hashlib.sha256("\n".join(original.iterdump()).encode()).digest()
                    == hashlib.sha256("\n".join(target.iterdump()).encode()).digest()
                )
            restored[name] = destination
        self.record("sqlite_online_backup_api_and_restore_integrity", passed=True, databases=len(restored))
        return restored

    def package(self) -> None:
        uv = shutil.which("uv")
        if uv is None:
            raise RuntimeError("uv is unavailable")
        destination = self.directory / "package"
        command([uv, "build", "--wheel", "--out-dir", str(destination)], self.env)
        wheel = next(destination.glob("*.whl"))
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
            installed = self.directory / "wheel-installed"
            archive.extractall(installed)
        assert "tenant_agent/cli.py" in names
        assert "tenant_agent/channels/wecom_bot.py" in names
        assert "tenant_agent/_resources/alembic.ini" in names
        assert "tenant_agent/_resources/config/tenant.wecom-bot.example.yaml" in names
        for migration in (ROOT / "migrations/versions").glob("*.py"):
            assert f"tenant_agent/_resources/migrations/versions/{migration.name}" in names
        assert not any(name.endswith((".env", ".db", ".pyc")) for name in names)
        self.record("wheel_build_and_payload", passed=True, wheel=wheel.name, entries=len(names))
        working = self.directory / "outside-checkout"
        working.mkdir()
        package_env = {
            key: value for key, value in self.env.items() if not key.startswith(("TAP_", "TENANT_"))
        }
        installed_database = working / "installed-control.db"
        package_env["TAP_CONTROL_DATABASE_URL"] = sql_url(installed_database)
        bootstrap = (
            "import sys; sys.path.insert(0, sys.argv[1]); "
            "from tenant_agent.resources import resource_root; "
            "assert resource_root().name == '_resources'; "
            "from tenant_agent.settings import Settings; "
            "assert Settings().bootstrap_config_path.is_file(); "
            "from tenant_agent.cli import app; sys.argv=['tenant-agent','db-init']; app()"
        )
        command([str(PYTHON), "-I", "-c", bootstrap, str(installed)], package_env, cwd=working)
        version = sql_rows(installed_database, "select version_num from alembic_version")[0][0]
        assert version == "d4e5f607a1b2"
        self.record("extracted_wheel_cli_outside_source_tree", passed=True, migration_head=version)

    async def run(self, *, package_only: bool = False) -> bool:
        try:
            if not package_only:
                await asyncio.to_thread(self.provision)
                await self.scenarios()
            else:
                self.report["scope"] = "extracted-wheel-outside-checkout"
            await asyncio.to_thread(self.package)
        except Exception as exc:
            self.record("runtime_check_exception", passed=False, error_type=exc.__class__.__name__)
        finally:
            self.kill(0)
            self.kill(1)
            for log in self.logs:
                log.close()
        self.report["all_passed"] = all(row.get("passed") for row in self.report["checks"])
        self.report["artifacts_directory"] = str(self.directory)
        (self.directory / "report.json").write_text(json.dumps(self.report, indent=2), encoding="utf-8")
        print(
            json.dumps(
                {"report": str(self.directory / "report.json"), "all_passed": self.report["all_passed"]}
            ),
            flush=True,
        )
        return bool(self.report["all_passed"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--package-only", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.requests <= 5_000 or not 1 <= args.concurrency <= 64:
        parser.error("requests must be 1..5000 and concurrency 1..64")
    check = LocalCheck(args.requests, args.concurrency)
    raise SystemExit(0 if asyncio.run(check.run(package_only=args.package_only)) else 1)


if __name__ == "__main__":
    main()
