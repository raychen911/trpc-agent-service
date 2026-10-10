from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from scripts.evidence_lineage import source_fingerprint
from scripts.real_runtime_gate import (
    FAULT_STAGE_NAMES,
    LOAD_STAGE_NAMES,
    MAX_KILL_TARGET_AGE_SECONDS,
    _active_duplicate_publish_probe,
    _active_turn_evidence,
    _allowed_runtime_project,
    _attach_compose_cleanup_evidence,
    _cleanup_owned_compose,
    _compose_command,
    _compose_project_container_ids,
    _connection_routes_match,
    _database_role_evidence,
    _dependency_fault,
    _envelope,
    _evidence_metadata,
    _fault_phase,
    _faults_skipped_after_load_failure,
    _inspect_container,
    _marker_times_within_run_window,
    _not_run_report,
    _parse_connection_environment,
    _parser,
    _preflight,
    _probe_stale_fencing_rejection,
    _production_scope_reasons,
    _proxy_field_matches,
    _proxy_ready,
    _role_evidence_check,
    _run_parameters,
    _run_real,
    _runtime_inputs_from_report,
    _set_proxy,
    _stage_marker,
    _status,
    _turn_state_evidence,
    _wait_for_batch,
    _wait_for_dlq,
    _wait_for_healthy_containers,
    _wait_for_takeover,
    _worker_container_for_owner,
    _worker_image_attestation,
    _write_report,
    main,
)
from trpc_service.storage.models import SessionSnapshot
from trpc_service.storage.protocols import FencingConflict
from trpc_service.tenant.models import Channel, TenantContext


@pytest.fixture(autouse=True)
def _clear_inherited_real_runtime_environment(monkeypatch) -> None:
    for name in tuple(os.environ):
        if name.startswith(("TRPC_RUN_REAL_", "TRPC_REAL_")):
            monkeypatch.delenv(name, raising=False)


def _current_source_fingerprint() -> str:
    result = source_fingerprint(Path(__file__).resolve().parents[2])
    assert result["status"] == "available"
    value = result.get("value")
    assert isinstance(value, str)
    return value


def _attested_workers(count: int = 4) -> list[dict[str, object]]:
    source = _current_source_fingerprint()
    return [
        {
            "container_id": f"worker-{index}",
            "pid": 100 + index,
            "image_id": "sha256:" + "a" * 64,
            "source_fingerprint": source,
        }
        for index in range(count)
    ]


def _role_evidence_fixture() -> dict[str, object]:
    from scripts.real_runtime_gate import (
        GLOBAL_WORKER_FUNCTION_SIGNATURES,
        RUNTIME_ROUTING_FUNCTION_SIGNATURES,
    )

    all_functions = (*RUNTIME_ROUTING_FUNCTION_SIGNATURES, *GLOBAL_WORKER_FUNCTION_SIGNATURES)

    return {
        "schema_version": 1,
        "status": "pass",
        "required_functions": list(GLOBAL_WORKER_FUNCTION_SIGNATURES),
        "runtime_allowed_functions": list(RUNTIME_ROUTING_FUNCTION_SIGNATURES),
        "runtime": {
            "expected_role": "trpc_runtime",
            "role_snapshot": {
                "current_user": "trpc_runtime",
                "session_user": "trpc_runtime",
                "role_name": "trpc_runtime",
                "role_superuser": False,
                "role_bypassrls": False,
                "functions": {
                    signature: {
                        "exists": True,
                        "execute": signature in RUNTIME_ROUTING_FUNCTION_SIGNATURES,
                    }
                    for signature in all_functions
                },
            },
            "global_function_probe": {
                "function": "public.list_channel_bindings(text)",
                "expected_access": "denied",
                "observed_access": "denied",
                "denied": True,
            },
        },
        "global_worker": {
            "expected_role": "trpc_worker",
            "role_snapshot": {
                "current_user": "trpc_worker",
                "session_user": "trpc_worker",
                "role_name": "trpc_worker",
                "role_superuser": False,
                "role_bypassrls": True,
                "functions": {
                    signature: {
                        "exists": True,
                        "execute": signature
                        in (
                            *RUNTIME_ROUTING_FUNCTION_SIGNATURES,
                            *GLOBAL_WORKER_FUNCTION_SIGNATURES,
                        ),
                    }
                    for signature in all_functions
                },
            },
            "global_function_probe": {
                "function": "public.list_channel_bindings(text)",
                "expected_access": "allowed",
                "observed_access": "allowed",
                "denied": False,
            },
        },
    }


class _RoleEvidenceConnection:
    def __init__(self, snapshot: dict[str, object], probe: dict[str, object]) -> None:
        self.snapshot = snapshot
        self.probe = probe
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _RoleEvidencePool:
    def __init__(self, connection: _RoleEvidenceConnection) -> None:
        self.connection = connection

    def acquire(self):
        connection = self.connection

        class _Acquire:
            async def __aenter__(self):
                return connection

            async def __aexit__(self, *_args):
                return None

        return _Acquire()


def _attested_participating_services(
    workers: list[dict[str, object]],
) -> dict[str, list[dict[str, object]]]:
    source = _current_source_fingerprint()
    image_id = "sha256:" + "a" * 64
    participating: dict[str, list[dict[str, object]]] = {
        "worker": [
            {
                **worker,
                "role": "worker",
                "status": "running",
                "health": "healthy",
            }
            for worker in workers
        ]
    }
    for index, role in enumerate(
        ("outbox-dispatcher", "channel-dispatcher", "post-turn-projector", "session-recovery"),
        start=1,
    ):
        participating[role] = [
            {
                "role": role,
                "container_id": f"{role}-{index}",
                "pid": 500 + index,
                "image_id": image_id,
                "source_fingerprint": source,
                "status": "running",
                "health": "healthy",
            }
        ]
    return participating


def test_real_runtime_probe_envelope_uses_only_provider_supported_metadata() -> None:
    envelope = _envelope(
        binding_account_id="account-1",
        message_id="message-1",
        user_id="user-1",
        text="probe",
    )

    assert envelope.channel is Channel.FEISHU
    assert envelope.provider_metadata == {}


@pytest.mark.parametrize(
    ("project", "allowed"),
    (
        ("trpc-agent-service", True),
        ("trpc-fault-20260824-145117", True),
        ("trpc-perf-20260824-145117", False),
        ("other-project", False),
        ("trpc-fault-../unsafe", False),
    ),
)
def test_runtime_project_guardrail_allows_only_fixed_or_dedicated_projects(
    project: str, allowed: bool
) -> None:
    assert _allowed_runtime_project(project) is allowed


def test_real_runtime_compose_commands_always_include_acceptance_safety_override() -> None:
    args = _parser().parse_args([])

    command = _compose_command(args, "ps", "-aq")

    assert any(
        str(item).replace("\\", "/").endswith("deploy/acceptance-runtime.override.yml")
        for item in command
    )
    assert "-v" not in command


def test_real_runtime_project_inventory_is_used_for_ownership_check(monkeypatch) -> None:
    args = _parser().parse_args([])
    captured: list[str] = []

    def fake_output(command, *, timeout):
        captured.extend(command)
        assert timeout == 30
        return {"status": "pass", "stdout": "container-a\ncontainer-b\n"}

    monkeypatch.setattr("scripts.real_runtime_gate._command_output", fake_output)

    assert _compose_project_container_ids(args) == ("container-a", "container-b")
    assert "ps" in captured and "-aq" in captured


def test_real_runtime_cleanup_is_owned_project_only_and_volume_preserving(monkeypatch) -> None:
    args = _parser().parse_args([])
    args._compose_started_by_gate = True
    calls: list[tuple[list[str], float]] = []

    def fake_result(command, *, timeout):
        calls.append((list(command), timeout))
        return {"status": "pass", "exit_code": 0}

    monkeypatch.setattr("scripts.real_runtime_gate._command_result", fake_result)

    assert _cleanup_owned_compose(args) == {"status": "pass", "exit_code": 0}
    assert args._compose_started_by_gate is False
    assert args._compose_cleanup_result == {"status": "pass", "exit_code": 0}
    assert len(calls) == 1
    command, timeout = calls[0]
    assert command[-2:] == ["down", "--remove-orphans"]
    assert "-v" not in command
    assert timeout == 180
    assert _cleanup_owned_compose(args) is None


def test_real_runtime_cleanup_failure_cannot_leave_a_pass_report() -> None:
    args = _parser().parse_args([])
    args._compose_cleanup_result = {"status": "fail", "exit_code": 1}
    report = {
        "candidate": {},
        "gate": "pass",
        "production_gate": "pass",
        "rejection_reasons": [],
        "production_rejection_reasons": [],
    }

    result = _attach_compose_cleanup_evidence(report, args)

    assert result["gate"] == "fail"
    assert result["production_gate"] == "fail"
    assert result["candidate"]["compose_cleanup"]["status"] == "fail"
    assert "cleanup did not complete" in result["rejection_reasons"][-1]


def test_real_runtime_compose_up_refuses_to_touch_preexisting_project(monkeypatch) -> None:
    args = _parser().parse_args(["--compose-up"])
    monkeypatch.setattr(
        "scripts.real_runtime_gate._compose_project_container_ids",
        lambda _args: ("caller-owned-container",),
    )
    monkeypatch.setattr(
        "scripts.real_runtime_gate._command_result",
        lambda *_args, **_kwargs: pytest.fail("must not run compose up"),
    )

    result = _preflight(args)

    assert result["status"] == "not_run"
    assert "caller-owned" in result["reason"]
    assert args._compose_started_by_gate is False


def test_real_runtime_prestarted_mode_is_explicit_and_bound() -> None:
    prestarted = _parser().parse_args(["--project", "trpc-fault-test", "--compose-prestarted"])
    parameters = _run_parameters(prestarted)

    assert parameters["compose_up"] is True
    assert parameters["compose_start_mode"] == "wrapper-prestarted-owned"
    assert _run_parameters(_parser().parse_args([]))["compose_start_mode"] == "none"


def test_real_runtime_prestarted_mode_requires_a_non_empty_dedicated_project(monkeypatch) -> None:
    args = _parser().parse_args(["--project", "trpc-fault-test", "--compose-prestarted"])
    monkeypatch.setattr(
        "scripts.real_runtime_gate._compose_project_container_ids", lambda _args: ()
    )
    monkeypatch.setattr(
        "scripts.real_runtime_gate._wait_for_healthy_containers",
        lambda *_args, **_kwargs: pytest.fail("must reject an empty wrapper project first"),
    )

    result = _preflight(args)

    assert result["status"] == "not_run"
    assert "non-empty wrapper-owned" in result["reason"]


def test_real_runtime_gate_is_not_run_without_explicit_opt_in(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRPC_RUN_REAL_MULTINODE", raising=False)
    monkeypatch.setattr(
        "scripts.real_runtime_gate._inspect_container",
        lambda *_args: pytest.fail("default mode must not inspect Docker"),
    )
    output = tmp_path / "real-runtime.json"

    assert main(["--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))

    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
    assert "--execute" in report["rejection_reasons"][0]


def test_missing_real_runtime_secrets_never_become_a_pass(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRPC_RUN_REAL_MULTINODE", "1")
    output = tmp_path / "real-runtime.json"

    assert main(["--execute", "--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))

    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
    assert any("TRPC_REAL_DATABASE_DSN" in reason for reason in report["rejection_reasons"])


def test_live_runtime_entry_requires_current_release_binding(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRPC_RUN_REAL_MULTINODE", "1")
    for name in (
        "TRPC_REAL_DATABASE_DSN",
        "TRPC_REAL_REDIS_URL",
        "TRPC_REAL_TENANT_ID",
        "TRPC_REAL_BINDING_ID",
        "TRPC_REAL_SESSION_HMAC_KEY",
        "TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN",
        "TRPC_REAL_GLOBAL_WORKER_DATABASE_ROLE",
    ):
        monkeypatch.setenv(name, "test-value-" + name.lower())
    monkeypatch.delenv("TRPC_RELEASE_ID", raising=False)
    monkeypatch.delenv("TRPC_RELEASE_NONCE", raising=False)
    output = tmp_path / "real-runtime.json"

    assert main(["--execute", "--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "not_run"
    assert any(
        "TRPC_RELEASE_ID and TRPC_RELEASE_NONCE" in reason for reason in report["rejection_reasons"]
    )


def test_database_role_evidence_is_fail_closed_and_recomputes_privileges() -> None:
    evidence = _role_evidence_fixture()

    assert _role_evidence_check(evidence) == ("pass", None)

    runtime = evidence["runtime"]
    assert isinstance(runtime, dict)
    snapshot = runtime["role_snapshot"]
    assert isinstance(snapshot, dict)
    functions = snapshot["functions"]
    assert isinstance(functions, dict)
    first_signature = next(
        signature for signature in functions if signature == "public.list_channel_bindings(text)"
    )
    functions[first_signature]["execute"] = True
    assert _role_evidence_check(evidence)[0] == "fail"


@pytest.mark.parametrize("role_key", ("runtime", "global_worker"))
def test_database_role_evidence_requires_the_deployed_role_names(role_key: str) -> None:
    evidence = _role_evidence_fixture()
    role = evidence[role_key]
    assert isinstance(role, dict)
    role["expected_role"] = "trpc_global_worker"

    status, reason = _role_evidence_check(evidence)

    assert status == "fail"
    assert reason and "must be" in reason


def test_database_role_evidence_rejects_superuser_and_missing_snapshots() -> None:
    evidence = _role_evidence_fixture()
    worker = evidence["global_worker"]
    assert isinstance(worker, dict)
    snapshot = worker["role_snapshot"]
    assert isinstance(snapshot, dict)
    snapshot["role_superuser"] = True
    assert _role_evidence_check(evidence)[0] == "fail"

    assert _role_evidence_check({"schema_version": 1, "status": "pass"})[0] == "not_run"


def test_database_role_evidence_keeps_routing_function_available_to_worker() -> None:
    evidence = _role_evidence_fixture()
    worker = evidence["global_worker"]
    assert isinstance(worker, dict)
    snapshot = worker["role_snapshot"]
    assert isinstance(snapshot, dict)
    functions = snapshot["functions"]
    assert isinstance(functions, dict)
    functions["public.resolve_channel_binding(text)"]["execute"] = False

    assert _role_evidence_check(evidence)[0] == "fail"


@pytest.mark.asyncio
async def test_database_role_evidence_requires_dedicated_worker_credentials(monkeypatch) -> None:
    monkeypatch.delenv("TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN", raising=False)
    monkeypatch.delenv("TRPC_REAL_GLOBAL_WORKER_DATABASE_ROLE", raising=False)

    result = await _database_role_evidence(object())

    assert result["status"] == "not_run"
    assert result["missing_variable_count"] == 2
    assert "dsn" not in json.dumps(result).lower()


@pytest.mark.asyncio
async def test_scoped_phase_stops_before_work_when_worker_role_evidence_does_not_pass(
    monkeypatch,
) -> None:
    args = _parser().parse_args(["--phase", "fault"])
    pool = _ClosablePool()

    monkeypatch.setattr(
        "scripts.real_runtime_gate._preflight",
        lambda _args: {
            "status": "pass",
            "worker_containers": _attested_workers(),
        },
    )

    async def fake_open_runtime(_args):
        return pool, object(), object()

    async def missing_role_evidence(_pool):
        return {
            "schema_version": 1,
            "status": "not_run",
            "reason": "dedicated global-worker database credentials are not configured",
        }

    async def unexpected_fault_phase(*_args):
        raise AssertionError("fault work must not start without passing worker role evidence")

    monkeypatch.setattr("scripts.real_runtime_gate._open_runtime", fake_open_runtime)
    monkeypatch.setattr("scripts.real_runtime_gate._database_role_evidence", missing_role_evidence)
    monkeypatch.setattr("scripts.real_runtime_gate._fault_phase", unexpected_fault_phase)

    report = await _run_real(args)

    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
    assert report["candidate"]["database_role_evidence"]["status"] == "not_run"
    assert pool.closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation", "expected_status"),
    (
        (None, "pass"),
        ("runtime_overaccess", "fail"),
        ("worker_missing_function", "fail"),
    ),
)
async def test_database_role_evidence_collector_reaches_terminal_contract(
    monkeypatch, mutation: str | None, expected_status: str
) -> None:
    evidence = _role_evidence_fixture()
    runtime = evidence["runtime"]
    worker = evidence["global_worker"]
    assert isinstance(runtime, dict) and isinstance(worker, dict)
    runtime_snapshot = runtime["role_snapshot"]
    worker_snapshot = worker["role_snapshot"]
    assert isinstance(runtime_snapshot, dict) and isinstance(worker_snapshot, dict)
    if mutation == "runtime_overaccess":
        runtime_snapshot["functions"]["public.list_channel_bindings(text)"]["execute"] = True
    elif mutation == "worker_missing_function":
        worker_snapshot["functions"]["public.list_channel_bindings(text)"]["execute"] = False

    runtime_connection = _RoleEvidenceConnection(
        deepcopy(runtime_snapshot),
        deepcopy(runtime["global_function_probe"]),
    )
    worker_connection = _RoleEvidenceConnection(
        deepcopy(worker_snapshot),
        deepcopy(worker["global_function_probe"]),
    )
    pool = _RoleEvidencePool(runtime_connection)

    async def fake_role_snapshot(connection, *, expected_functions):
        return deepcopy(connection.snapshot)

    async def fake_probe(connection, *, expected_access):
        return deepcopy(connection.probe)

    async def fake_connect(_dsn):
        return worker_connection

    monkeypatch.setenv("TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN", "postgresql://worker@db")
    monkeypatch.setenv("TRPC_REAL_GLOBAL_WORKER_DATABASE_ROLE", "trpc_worker")
    monkeypatch.setenv("TRPC_REAL_RUNTIME_DATABASE_ROLE", "trpc_runtime")
    monkeypatch.setattr("scripts.real_runtime_gate._role_snapshot", fake_role_snapshot)
    monkeypatch.setattr("scripts.real_runtime_gate._probe_global_function", fake_probe)
    monkeypatch.setattr("scripts.real_runtime_gate.asyncpg.connect", fake_connect)

    result = await _database_role_evidence(pool)

    assert result["status"] == expected_status
    assert worker_connection.closed is True


def test_status_propagates_failure_before_not_run() -> None:
    assert _status(({"status": "pass"}, {"status": "not_run"})) == "not_run"
    assert _status(({"status": "not_run"}, {"status": "fail"})) == "fail"
    assert _status(({"status": "pass"}, {"status": "pass"})) == "pass"


class _ClosablePool:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


async def _run_real_with_passing_phases(monkeypatch, args):
    pool = _ClosablePool()
    workers = _attested_workers()
    monkeypatch.setattr(
        "scripts.real_runtime_gate._preflight",
        lambda _args: {
            "status": "pass",
            "worker_containers": workers,
            "image_attestation": {
                "status": "pass",
                "image_id": "sha256:" + "a" * 64,
                "source_fingerprint": _current_source_fingerprint(),
            },
        },
    )

    async def open_runtime(_args):
        return pool, object(), object()

    async def passing_load(*_args):
        return {"status": "pass", "stage_markers": []}

    async def passing_faults(*_args):
        return {"status": "pass", "stage_markers": []}

    async def passing_role_evidence(_pool):
        return _role_evidence_fixture()

    monkeypatch.setattr("scripts.real_runtime_gate._open_runtime", open_runtime)
    monkeypatch.setattr("scripts.real_runtime_gate._database_role_evidence", passing_role_evidence)
    monkeypatch.setattr("scripts.real_runtime_gate._load_phase", passing_load)
    monkeypatch.setattr("scripts.real_runtime_gate._fault_phase", passing_faults)
    report = await _run_real(args)
    assert pool.closed is True
    return report


def _all_phase_args(*values: str) -> object:
    return _parser().parse_args(
        [
            "--phase",
            "all",
            "--kill-worker",
            "--republish-probe",
            *values,
        ]
    )


@pytest.mark.parametrize(
    ("messages", "duplicates", "fault_messages", "reason_fragment"),
    (
        (20, 2, 4, "messages >= 200"),
        (200, 2, 8, "duplicates >= 20"),
        (200, 20, 4, "fault_messages >= 8"),
        (200, 20, 8, "--republish-probe"),
    ),
)
@pytest.mark.asyncio
async def test_all_pass_below_any_production_scope_threshold_stays_not_run(
    monkeypatch, messages: int, duplicates: int, fault_messages: int, reason_fragment: str
) -> None:
    args = _all_phase_args(
        "--messages",
        str(messages),
        "--duplicates",
        str(duplicates),
        "--fault-messages",
        str(fault_messages),
    )
    if reason_fragment == "--republish-probe":
        args.republish_probe = False

    report = await _run_real_with_passing_phases(monkeypatch, args)

    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert any(reason_fragment in reason for reason in report["production_rejection_reasons"])


def test_production_scope_reasons_report_each_missing_requirement() -> None:
    args = _all_phase_args("--messages", "20", "--duplicates", "2", "--fault-messages", "4")
    args.republish_probe = False

    reasons = _production_scope_reasons(args)

    assert reasons == [
        "production acceptance requires messages >= 200; requested 20",
        "production acceptance requires duplicates >= 20; requested 2",
        "production acceptance requires fault_messages >= 8; requested 4",
        "production acceptance requires --republish-probe for real Redis "
        "duplicate-publish evidence",
    ]


@pytest.mark.asyncio
async def test_all_pass_at_production_scope_threshold_promotes_production_gate(monkeypatch) -> None:
    args = _all_phase_args(
        "--messages",
        "200",
        "--duplicates",
        "20",
        "--fault-messages",
        "8",
    )

    report = await _run_real_with_passing_phases(monkeypatch, args)

    assert report["gate"] == "pass"
    assert report["production_gate"] == "pass"
    assert not any(
        reason.startswith("production acceptance requires")
        for reason in report["production_rejection_reasons"]
    )


def test_fault_result_is_explicitly_not_run_after_load_failure() -> None:
    result = _faults_skipped_after_load_failure(
        {"status": "fail", "reason": "load timeout", "stage_markers": []}
    )

    assert result["status"] == "not_run"
    assert result["reason"] == "skipped_due_to_load_failure"
    assert result["load_status"] == "fail"
    assert result["load_reason"] == "load timeout"
    assert {marker["name"] for marker in result["stage_markers"]} == set(FAULT_STAGE_NAMES)
    assert all(marker["status"] == "not_run" for marker in result["stage_markers"])


@pytest.mark.asyncio
async def test_all_does_not_start_fault_phase_after_load_failure(monkeypatch) -> None:
    args = _all_phase_args("--messages", "200", "--duplicates", "20", "--fault-messages", "8")
    pool = _ClosablePool()
    workers = _attested_workers()
    monkeypatch.setattr(
        "scripts.real_runtime_gate._preflight",
        lambda _args: {
            "status": "pass",
            "worker_containers": workers,
            "image_attestation": {
                "status": "pass",
                "image_id": "sha256:" + "a" * 64,
                "source_fingerprint": _current_source_fingerprint(),
            },
        },
    )

    async def open_runtime(_args):
        return pool, object(), object()

    async def failed_load(*_args):
        return {"status": "fail", "reason": "load timeout", "stage_markers": []}

    async def unexpected_faults(*_args):
        raise AssertionError("fault phase must be skipped after load failure")

    async def passing_role_evidence(_pool):
        return _role_evidence_fixture()

    monkeypatch.setattr("scripts.real_runtime_gate._open_runtime", open_runtime)
    monkeypatch.setattr("scripts.real_runtime_gate._database_role_evidence", passing_role_evidence)
    monkeypatch.setattr("scripts.real_runtime_gate._load_phase", failed_load)
    monkeypatch.setattr("scripts.real_runtime_gate._fault_phase", unexpected_faults)

    report = await _run_real(args)

    assert pool.closed is True
    assert report["gate"] == "fail"
    assert report["production_gate"] == "fail"
    faults = report["candidate"]["faults"]
    assert faults["status"] == "not_run"
    assert faults["reason"] == "skipped_due_to_load_failure"
    assert all(marker["status"] == "not_run" for marker in faults["stage_markers"])


@pytest.mark.parametrize("phase", ("load", "fault"))
@pytest.mark.asyncio
async def test_scoped_load_or_fault_phase_never_promotes_production_gate(
    monkeypatch, phase: str
) -> None:
    args = _parser().parse_args(
        [
            "--phase",
            phase,
            "--messages",
            "200",
            "--duplicates",
            "20",
            "--fault-messages",
            "8",
            "--kill-worker",
            "--republish-probe",
        ]
    )

    report = await _run_real_with_passing_phases(monkeypatch, args)

    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert any(
        "load-only/fault-only execution is scoped evidence" in reason
        for reason in report["production_rejection_reasons"]
    )


def test_not_run_report_keeps_production_gate_not_run() -> None:
    report = _not_run_report(["Docker daemon is unavailable"])

    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
    assert report["production_rejection_reasons"] == ["Docker daemon is unavailable"]


def test_not_run_report_contains_explicit_unobserved_stage_markers() -> None:
    report = _not_run_report(["opt-in was not supplied"])

    markers = report["candidate"]["stage_markers"]
    assert {item["name"] for item in markers} == set(LOAD_STAGE_NAMES) | set(FAULT_STAGE_NAMES)
    assert all(item["status"] == "not_run" for item in markers)
    assert all("observed_at" in item for item in markers)


def test_default_report_attaches_unavailable_runtime_lineage_without_external_calls(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.delenv("TRPC_RUN_REAL_MULTINODE", raising=False)
    output = tmp_path / "real-runtime.json"

    assert main(["--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))

    assert report["evidence"]["producer"] == "scripts.real_runtime_gate"
    assert report["evidence"]["source_fingerprint"]["status"] == "available"
    assert report["evidence"]["runtime_fingerprint"]["status"] == "unavailable"
    assert report["run_id"] == report["evidence"]["run_id"]
    assert report["schema_version"] == 1
    assert len(report["run_nonce"]) == 32
    assert report["generated_at"].endswith("Z")
    assert report["started_at"].endswith("Z")
    assert report["ended_at"].endswith("Z")
    assert report["evidence"]["run_nonce"] == report["run_nonce"]


def test_real_runtime_evidence_becomes_available_only_for_complete_sample(tmp_path) -> None:
    args = _parser().parse_args(
        [
            "--phase",
            "load",
            "--redis-stream",
            "trpc:session-ready:v2",
            "--redis-group",
            "trpc-session-ready-v2",
        ]
    )
    report = {
        "gate": "pass",
        "candidate": {
            "mode": "real_compose_postgresql_redis",
            "preflight": {
                "status": "pass",
                "worker_containers": _attested_workers(),
                "image_attestation": {
                    "status": "pass",
                    "image_id": "sha256:" + "a" * 64,
                    "source_fingerprint": _current_source_fingerprint(),
                },
                "participating_services": _attested_participating_services(_attested_workers()),
            },
        },
    }

    inputs = _runtime_inputs_from_report(report, args=args)
    assert inputs is not None
    evidence = _evidence_metadata(report, args=args, run_id="real-runtime-test")

    assert evidence["runtime_fingerprint"]["status"] == "available"
    assert evidence["runtime_fingerprint"]["worker_count"] == 4
    assert evidence["runtime_fingerprint"]["participating_service_count"] == 5
    assert evidence["runtime_fingerprint"]["participating_container_count"] == 8
    assert len(evidence["runtime_fingerprint"]["participating_identity_summary_sha256"]) == 64
    rendered = json.dumps(evidence, ensure_ascii=False)
    assert "worker-a" not in rendered
    assert "trpc:session-ready:v2" not in rendered
    assert "trpc-session-ready-v2" not in rendered


def test_runtime_evidence_rejects_participating_service_from_mixed_image() -> None:
    args = _parser().parse_args(
        [
            "--phase",
            "load",
            "--redis-stream",
            "trpc:session-ready:v2",
            "--redis-group",
            "trpc-session-ready-v2",
        ]
    )
    workers = _attested_workers()
    participating = _attested_participating_services(workers)
    participating["outbox-dispatcher"][0]["image_id"] = "sha256:" + "b" * 64
    report = {
        "gate": "pass",
        "candidate": {
            "mode": "real_compose_postgresql_redis",
            "preflight": {
                "status": "pass",
                "worker_containers": workers,
                "image_attestation": {
                    "status": "pass",
                    "image_id": "sha256:" + "a" * 64,
                    "source_fingerprint": _current_source_fingerprint(),
                },
                "participating_services": participating,
            },
        },
    }

    assert _runtime_inputs_from_report(report, args=args) is None


def test_real_runtime_evidence_is_unavailable_without_worker_identity() -> None:
    args = _parser().parse_args(["--phase", "load"])
    report = {
        "gate": "pass",
        "candidate": {
            "mode": "real_compose_postgresql_redis",
            "preflight": {
                "status": "pass",
                "worker_containers": [],
                "image_attestation": {"status": "pass"},
            },
        },
    }

    assert _runtime_inputs_from_report(report, args=args) is None
    evidence = _evidence_metadata(report, args=args, run_id="missing-worker-test")
    assert evidence["runtime_fingerprint"]["status"] == "unavailable"


def test_real_runtime_evidence_is_unavailable_for_short_image_identity() -> None:
    args = _parser().parse_args(["--phase", "load"])
    workers = _attested_workers()
    workers[0]["image_id"] = "sha256:short"
    report = {
        "gate": "pass",
        "candidate": {
            "mode": "real_compose_postgresql_redis",
            "preflight": {
                "status": "pass",
                "worker_containers": workers,
                "image_attestation": {
                    "status": "pass",
                    "image_id": "sha256:short",
                    "source_fingerprint": _current_source_fingerprint(),
                },
            },
        },
    }

    assert _runtime_inputs_from_report(report, args=args) is None


def test_runtime_evidence_does_not_persist_sentinel_inputs(tmp_path) -> None:
    args = _parser().parse_args(["--phase", "fault"])
    report = {
        "gate": "pass",
        "candidate": {
            "mode": "real_compose_postgresql_redis",
            "preflight": {
                "status": "pass",
                "worker_containers": [
                    {
                        **worker,
                        "container_id": f"sentinel-worker-id-{index}",
                        "pid": 300 + index,
                    }
                    for index, worker in enumerate(_attested_workers(), start=1)
                ],
                "image_attestation": {
                    "status": "pass",
                    "image_id": "sha256:" + "a" * 64,
                    "source_fingerprint": _current_source_fingerprint(),
                },
                "participating_services": _attested_participating_services(
                    [
                        {
                            **worker,
                            "container_id": f"sentinel-worker-id-{index}",
                            "pid": 300 + index,
                        }
                        for index, worker in enumerate(_attested_workers(), start=1)
                    ]
                ),
            },
        },
    }

    _write_report(tmp_path / "real-runtime.json", report, args=args)
    evidence = report["evidence"]
    rendered = json.dumps(evidence, ensure_ascii=False)

    assert "sentinel-worker-id" not in rendered
    assert "TRPC_REAL_DATABASE_DSN" not in rendered
    assert "secret-token-value" not in rendered
    assert evidence["runtime_fingerprint"]["status"] == "available"


def test_write_boundary_replaces_historical_runtime_evidence(tmp_path) -> None:
    output = tmp_path / "historical-looking.json"
    report = {
        "evidence": {
            "kind": "historical",
            "producer": "old-script",
            "runtime_fingerprint": {"status": "available", "value": "old"},
        },
        "gate": "not_run",
        "production_gate": "not_run",
    }

    _write_report(output, report)
    rendered = json.loads(output.read_text(encoding="utf-8"))

    assert rendered["evidence"]["kind"] == "current_candidate"
    assert rendered["evidence"]["producer"] == "scripts.real_runtime_gate"
    assert rendered["evidence"]["runtime_fingerprint"]["status"] == "unavailable"


def test_write_boundary_rejects_symlink_output_parent(tmp_path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation is unavailable on this Windows runner")

    with pytest.raises(ValueError, match="symlink"):
        _write_report(link / "report.json", {"gate": "not_run"})


def test_stage_marker_does_not_add_sensitive_or_unrequested_fields() -> None:
    marker = _stage_marker("proxy.disabled", "pass", component="redis")

    assert marker["name"] == "proxy.disabled"
    assert marker["status"] == "pass"
    assert marker["component"] == "redis"
    assert "password" not in marker


def test_toxiproxy_boot_config_uses_supported_array_shape() -> None:
    config = json.loads(Path("deploy/toxiproxy.json").read_text(encoding="utf-8"))

    assert isinstance(config, list)
    assert {proxy["name"] for proxy in config} == {"postgres", "redis", "minio"}
    assert all(proxy["enabled"] is True for proxy in config)


@pytest.mark.parametrize(
    ("observed_listen", "matches"),
    (
        ("0.0.0.0:15432", True),
        ("[::]:15432", True),
        ("0.0.0.0:5432", False),
        ("127.0.0.1:15432", False),
        ("[::1]:15432", False),
        ("toxiproxy:15432", False),
    ),
)
def test_toxiproxy_wildcard_listen_normalization_is_port_and_host_strict(
    observed_listen: str, matches: bool
) -> None:
    assert _proxy_field_matches("listen", "0.0.0.0:15432", observed_listen) is matches


@pytest.mark.asyncio
async def test_toxiproxy_ready_keeps_upstream_validation_with_wildcard_listen(httpx_mock) -> None:
    httpx_mock.add_response(
        method="GET",
        url="http://toxiproxy:8474/proxies",
        json={
            "postgres": {
                "name": "postgres",
                "enabled": True,
                "listen": "[::]:15432",
                "upstream": "postgres:1543",
            },
            "redis": {
                "name": "redis",
                "enabled": True,
                "listen": "[::]:16379",
                "upstream": "redis:6379",
            },
        },
    )

    result = await _proxy_ready("http://toxiproxy:8474")

    assert result["status"] == "fail"
    assert result["invalid"]["postgres"]["upstream"]["expected"] == "postgres:5432"
    assert result["invalid"]["postgres"]["upstream"]["observed"] == "postgres:1543"


@pytest.mark.asyncio
async def test_toxiproxy_toggle_uses_v2_update_method(httpx_mock) -> None:
    httpx_mock.add_response(
        method="POST",
        url="http://toxiproxy:8474/proxies/redis",
        json={"name": "redis", "enabled": False},
    )
    httpx_mock.add_response(
        method="GET",
        url="http://toxiproxy:8474/proxies/redis",
        json={
            "name": "redis",
            "enabled": False,
            "listen": "0.0.0.0:16379",
            "upstream": "redis:6379",
        },
    )

    assert await _set_proxy("http://toxiproxy:8474", "redis", False) == {
        "status": "pass",
        "api_endpoint": "http://toxiproxy:8474",
        "name": "redis",
        "enabled": False,
        "listen": "0.0.0.0:16379",
        "upstream": "redis:6379",
    }


@pytest.mark.asyncio
async def test_toxiproxy_ready_requires_expected_proxy_configuration(httpx_mock) -> None:
    httpx_mock.add_response(
        method="GET",
        url="http://toxiproxy:8474/proxies",
        json={
            "postgres": {
                "name": "postgres",
                "enabled": True,
                "listen": "0.0.0.0:15432",
                "upstream": "postgres:5432",
            },
            "redis": {
                "name": "redis",
                "enabled": True,
                "listen": "0.0.0.0:16379",
                "upstream": "redis:6379",
            },
        },
    )

    result = await _proxy_ready("http://toxiproxy:8474")

    assert result["status"] == "pass"
    assert result["proxy_details"]["redis"]["upstream"] == "redis:6379"


@pytest.mark.asyncio
async def test_toxiproxy_toggle_rejects_readback_mismatch(httpx_mock) -> None:
    httpx_mock.add_response(
        method="POST",
        url="http://toxiproxy:8474/proxies/redis",
        json={"name": "redis", "enabled": False},
    )
    httpx_mock.add_response(
        method="GET",
        url="http://toxiproxy:8474/proxies/redis",
        json={
            "name": "redis",
            "enabled": True,
            "listen": "0.0.0.0:16379",
            "upstream": "redis:6379",
        },
    )

    result = await _set_proxy("http://toxiproxy:8474", "redis", False)

    assert result["status"] == "fail"
    assert "readback mismatch" in result["reason"]


@pytest.mark.asyncio
async def test_toxiproxy_toggle_rejects_readback_name_mismatch(httpx_mock) -> None:
    httpx_mock.add_response(
        method="POST",
        url="http://toxiproxy:8474/proxies/redis",
        json={"name": "redis", "enabled": False},
    )
    httpx_mock.add_response(
        method="GET",
        url="http://toxiproxy:8474/proxies/redis",
        json={
            "name": "other-proxy",
            "enabled": False,
            "listen": "0.0.0.0:16379",
            "upstream": "redis:6379",
        },
    )

    result = await _set_proxy("http://toxiproxy:8474", "redis", False)

    assert result["status"] == "fail"
    assert result["mismatch"]["name"]["expected"] == "redis"


@pytest.mark.asyncio
async def test_postgres_fault_accepts_before_disable_and_uses_direct_observer(
    monkeypatch,
) -> None:
    events: list[str] = []

    class RuntimePool:
        expired = False

        async def expire_connections(self) -> None:
            self.expired = True
            events.append("runtime_connections_expired")

    runtime_pool = RuntimePool()
    observation_pool = object()

    async def fake_accept_batch(*_args, **_kwargs):
        events.append("accept")
        return {
            "tenant_id": "tenant-test",
            "session_id": "session-test",
            "unique_inbound_ids": ["inbound-test"],
        }

    async def fake_set_proxy(_api, name, enabled):
        assert name == "postgres"
        events.append("restore" if enabled else "disable")
        return {"status": "pass"}

    async def fake_session_state(pool, **_kwargs):
        assert pool is observation_pool
        events.append("observe_while_down")
        return {"inbound_statuses": {"accepted": 1}}

    async def fake_wait_for_batch(pool, _batch, **_kwargs):
        assert pool is observation_pool
        events.append("observe_completion")
        return {"status": "pass", "state": {"turn_count": 1}}

    async def fake_sleep(_seconds):
        return None

    monkeypatch.setattr("scripts.real_runtime_gate._accept_batch", fake_accept_batch)
    monkeypatch.setattr("scripts.real_runtime_gate._set_proxy", fake_set_proxy)
    monkeypatch.setattr("scripts.real_runtime_gate._session_state", fake_session_state)
    monkeypatch.setattr("scripts.real_runtime_gate._wait_for_batch", fake_wait_for_batch)
    monkeypatch.setattr("scripts.real_runtime_gate.asyncio.sleep", fake_sleep)

    result = await _dependency_fault(
        SimpleNamespace(toxiproxy_api="http://127.0.0.1:8474", fault_messages=1, timeout_seconds=1),
        runtime_pool,
        object(),
        object(),
        proxy_name="postgres",
        prefix="postgres-test",
        observation_pool=observation_pool,
    )

    assert result["status"] == "pass"
    assert events.index("accept") < events.index("disable")
    assert events.index("disable") < events.index("observe_while_down")
    assert events.index("restore") < events.index("runtime_connections_expired")
    assert runtime_pool.expired is True


@pytest.mark.asyncio
async def test_fault_phase_never_disables_postgres_before_acceptance(monkeypatch) -> None:
    calls: list[tuple[str, object, bool]] = []

    class ObserverPool:
        closed = False

        async def close(self) -> None:
            self.closed = True

    observer = ObserverPool()

    async def fake_proxy_ready(_api):
        return {"status": "pass", "proxies": ["postgres", "redis"]}

    async def fake_open_observer(_args):
        return observer

    async def fake_dependency(*_args, **kwargs):
        calls.append(
            (
                kwargs["proxy_name"],
                kwargs["observation_pool"],
                bool(kwargs.get("disable_before_accept", False)),
            )
        )
        return {"status": "pass", "stage_markers": []}

    async def fake_seed(_pool, _tenant):
        return {"outbound_id": "outbound", "outbox_id": "outbox", "attempts_before": 4}

    async def fake_wait_dlq(*_args, **_kwargs):
        return {"status": "pass"}

    monkeypatch.setenv("TRPC_REAL_TENANT_ID", "tenant-test")
    monkeypatch.setattr("scripts.real_runtime_gate._proxy_ready", fake_proxy_ready)
    monkeypatch.setattr(
        "scripts.real_runtime_gate._open_global_worker_observer_pool", fake_open_observer
    )
    monkeypatch.setattr("scripts.real_runtime_gate._dependency_fault", fake_dependency)
    monkeypatch.setattr("scripts.real_runtime_gate._seed_dlq", fake_seed)
    monkeypatch.setattr("scripts.real_runtime_gate._wait_for_dlq", fake_wait_dlq)

    result = await _fault_phase(
        SimpleNamespace(
            use_toxiproxy=True,
            toxiproxy_api="http://127.0.0.1:8474",
            workers=4,
            timeout_seconds=1,
            republish_probe=False,
        ),
        object(),
        object(),
        object(),
    )

    assert result["status"] == "pass"
    assert calls == [("redis", observer, False), ("postgres", observer, False)]
    assert observer.closed is True


def test_connection_environment_uses_exact_urls_and_expected_routes() -> None:
    parsed = _parse_connection_environment(
        (
            "UNRELATED=toxiproxy:15432",
            "TRPC_SERVICE_ROLE=worker",
            "TRPC_SERVICE_DATABASE_DSN=postgresql+asyncpg://runtime@toxiproxy:15432/service",
            "TRPC_SERVICE_WORKER_DATABASE_DSN=postgresql+asyncpg://trpc_worker:secret@toxiproxy:15432/service",
            "TRPC_SERVICE_REDIS_URL=redis://:secret@toxiproxy:16379/0",
        )
    )

    assert parsed["valid"] is True
    assert parsed["database"] == {
        "scheme": "postgresql+asyncpg",
        "host": "toxiproxy",
        "port": 15432,
    }
    assert parsed["worker_database"] == {
        "scheme": "postgresql+asyncpg",
        "host": "toxiproxy",
        "port": 15432,
        "role": "trpc_worker",
    }
    assert (
        _connection_routes_match(
            {"connection_env": parsed}, use_toxiproxy=True, expected_role="worker"
        )
        is True
    )
    assert _connection_routes_match({"connection_env": parsed}, use_toxiproxy=False) is False


def test_connection_environment_requires_worker_role_route_for_participating_services() -> None:
    missing = _parse_connection_environment(
        (
            "TRPC_SERVICE_ROLE=worker",
            "TRPC_SERVICE_DATABASE_DSN=postgresql://runtime@toxiproxy:15432/service",
            "TRPC_SERVICE_REDIS_URL=redis://toxiproxy:16379/0",
        )
    )
    wrong_role = _parse_connection_environment(
        (
            "TRPC_SERVICE_ROLE=worker",
            "TRPC_SERVICE_DATABASE_DSN=postgresql://runtime@toxiproxy:15432/service",
            "TRPC_SERVICE_WORKER_DATABASE_DSN=postgresql://trpc_runtime@toxiproxy:15432/service",
            "TRPC_SERVICE_REDIS_URL=redis://toxiproxy:16379/0",
        )
    )

    assert missing["valid"] is False
    assert "TRPC_SERVICE_WORKER_DATABASE_DSN" in missing["reason"]
    assert wrong_role["valid"] is True
    assert (
        _connection_routes_match(
            {"connection_env": wrong_role}, use_toxiproxy=True, expected_role="worker"
        )
        is False
    )


def test_connection_environment_rejects_missing_or_malformed_exact_variables() -> None:
    missing = _parse_connection_environment(("UNRELATED=redis://toxiproxy:16379/0",))
    malformed = _parse_connection_environment(
        (
            "TRPC_SERVICE_DATABASE_DSN=not-a-dsn",
            "TRPC_SERVICE_REDIS_URL=redis://redis:6379/0",
        )
    )

    assert missing["valid"] is False
    assert malformed["valid"] is False


def test_preflight_does_not_count_unhealthy_workers(monkeypatch) -> None:
    repository_root = Path(__file__).resolve().parents[2]
    compose = repository_root / "docker-compose.yml"
    toxiproxy_override = repository_root / "deploy" / "toxiproxy-runtime.override.yml"
    args = type(
        "Args",
        (),
        {
            "compose_file": compose,
            "toxiproxy_override": toxiproxy_override,
            "use_toxiproxy": False,
            "project": "trpc-agent-service",
            "workers": 4,
            "messages": 1,
            "duplicates": 0,
            "fault_messages": 1,
            "timeout_seconds": 1.0,
            "redis_stream": "trpc:session-ready:v2",
            "redis_group": "trpc-session-ready-v2",
            "compose_up": False,
        },
    )()
    healthy_routes = _parse_connection_environment(
        (
            "TRPC_SERVICE_DATABASE_DSN=postgresql://runtime@postgres:5432/service",
            "TRPC_SERVICE_REDIS_URL=redis://redis:6379/0",
        )
    )
    containers = {
        f"worker-{index}": {
            "container_id": f"worker-{index}",
            "pid": index,
            "status": "running",
            "health": "healthy" if index != 2 else "none",
            "connection_env": healthy_routes,
        }
        for index in range(1, 5)
    }
    monkeypatch.setattr("scripts.real_runtime_gate._worker_ids", lambda _args: tuple(containers))
    monkeypatch.setattr(
        "scripts.real_runtime_gate._inspect_container",
        lambda _args, container_id: containers[container_id],
    )

    result = _preflight(args)

    assert result["status"] == "not_run"
    assert "running and healthy" in result["reason"]


def test_health_preflight_polls_starting_workers_until_healthy(monkeypatch) -> None:
    args = type("Args", (), {"timeout_seconds": 5.0})()
    attempts = 0

    def inspect(_args, container_id):
        nonlocal attempts
        attempts += 1
        return {
            "container_id": container_id,
            "pid": 100,
            "status": "running",
            "health": "starting" if attempts == 1 else "healthy",
        }

    monkeypatch.setattr("scripts.real_runtime_gate._inspect_container", inspect)
    monkeypatch.setattr("scripts.real_runtime_gate.time.sleep", lambda _seconds: None)

    observed = _wait_for_healthy_containers(
        args,
        service="worker",
        minimum=1,
        discover=lambda _args: ("worker-1",),
    )

    assert attempts == 2
    assert observed[0]["health"] == "healthy"


def test_worker_image_attestation_requires_four_workers_and_one_current_image() -> None:
    result = _worker_image_attestation(_attested_workers())

    assert result == {
        "status": "pass",
        "worker_count": 4,
        "image_id": "sha256:" + "a" * 64,
        "image_digest_verified": True,
        "algorithm": "sha256",
        "source_fingerprint": _current_source_fingerprint(),
    }


@pytest.mark.parametrize(
    ("mutation", "reason"),
    (
        (lambda workers: workers[:3], "at least 4"),
        (
            lambda workers: [
                *workers[:3],
                {**workers[3], "image_id": "sha256:" + "b" * 64},
            ],
            "mixed candidate image IDs",
        ),
        (
            lambda workers: [
                *workers[:3],
                {**workers[3], "source_fingerprint": "0" * 64},
            ],
            "missing or stale",
        ),
        (
            lambda workers: [
                *workers[:3],
                {**workers[3], "image_id": None},
            ],
            "image ID or source label is missing",
        ),
    ),
)
def test_worker_image_attestation_fails_closed_for_unprovable_workers(mutation, reason) -> None:
    result = _worker_image_attestation(mutation(_attested_workers()))

    assert result["status"] == "not_run"
    assert reason in result["reason"]


def test_inspect_container_extracts_only_safe_image_attestation_fields(monkeypatch) -> None:
    class FakeCompleted:
        returncode = 0
        stdout = json.dumps(
            {
                "Id": "sha256:container-id",
                "Image": "sha256:" + "a" * 64,
                "State": {
                    "Pid": 1234,
                    "Status": "running",
                    "Health": {"Status": "healthy"},
                },
                "Config": {
                    "Labels": {
                        "io.trpc.agent-service.source-fingerprint": "a" * 64,
                    },
                    "Env": [
                        "TRPC_SERVICE_ROLE=worker",
                        "TRPC_SERVICE_DATABASE_DSN=postgresql://runtime:secret@postgres:5432/service",
                        "TRPC_SERVICE_WORKER_DATABASE_DSN=postgresql://trpc_worker:secret@postgres:5432/service",
                        "TRPC_SERVICE_REDIS_URL=redis://:secret@redis:6379/0",
                    ],
                },
            }
        )

    calls: list[list[str]] = []

    def fake_run(command, **_kwargs):
        calls.append(command)
        return FakeCompleted()

    monkeypatch.setattr("scripts.real_runtime_gate.shutil.which", lambda _name: "docker")
    monkeypatch.setattr("scripts.real_runtime_gate.subprocess.run", fake_run)

    result = _inspect_container(None, "short-container-id")

    assert result["container_id"] == "sha256:container-id"
    assert result["image_id"] == "sha256:" + "a" * 64
    assert result["source_fingerprint"] == "a" * 64
    assert result["connection_env"]["database"] == {
        "scheme": "postgresql",
        "host": "postgres",
        "port": 5432,
    }
    assert result["connection_env"]["worker_database"] == {
        "scheme": "postgresql",
        "host": "postgres",
        "port": 5432,
        "role": "trpc_worker",
    }
    assert "secret" not in json.dumps(result)
    assert calls[0][1:3] == ["inspect", "--format"]


@pytest.mark.asyncio
async def test_wait_for_batch_rejects_more_turns_than_unique_inbounds(monkeypatch) -> None:
    async def state(*_args, **_kwargs):
        return {
            "inbound_statuses": {"committed": 2},
            "turn_count": 3,
            "lease_owner_present": False,
            "event_sequences_contiguous": True,
            "published_inbound_outbox": 2,
            "published_scheduler_outbox": 1,
            "mailbox_v2_completion": {"status": "pass", "schema_version": 2},
        }

    monkeypatch.setattr("scripts.real_runtime_gate._session_state", state)
    result = await _wait_for_batch(
        None,
        {"tenant_id": "tenant", "session_id": "session", "unique_inbound_ids": ["a", "b"]},
        wait_seconds=1,
    )

    assert result["status"] == "fail"
    assert "more turns" in result["reason"]


@pytest.mark.asyncio
async def test_wait_for_batch_requires_exact_turn_count(monkeypatch) -> None:
    async def state(*_args, **_kwargs):
        return {
            "inbound_statuses": {"committed": 2},
            "turn_count": 2,
            "lease_owner_present": False,
            "event_sequences_contiguous": True,
            "published_inbound_outbox": 2,
            "published_scheduler_outbox": 1,
            "mailbox_v2_completion": {"status": "pass", "schema_version": 2},
        }

    monkeypatch.setattr("scripts.real_runtime_gate._session_state", state)
    result = await _wait_for_batch(
        None,
        {"tenant_id": "tenant", "session_id": "session", "unique_inbound_ids": ["a", "b"]},
        wait_seconds=1,
    )

    assert result["status"] == "pass"


@pytest.mark.asyncio
async def test_wait_for_batch_is_not_run_without_mailbox_v2_completion(monkeypatch) -> None:
    async def state(*_args, **_kwargs):
        return {
            "inbound_statuses": {"committed": 1},
            "turn_count": 1,
            "lease_owner_present": False,
            "event_sequences_contiguous": True,
            "published_inbound_outbox": 1,
        }

    monkeypatch.setattr("scripts.real_runtime_gate._session_state", state)
    result = await _wait_for_batch(
        None,
        {"tenant_id": "tenant", "session_id": "session", "unique_inbound_ids": ["a"]},
        wait_seconds=0.01,
    )

    assert result["status"] == "fail"


def test_stage_marker_window_rejects_stale_or_future_observations() -> None:
    report = {
        "started_at": "2026-08-24T00:00:00.000000Z",
        "ended_at": "2026-08-24T00:01:00.000000Z",
        "candidate": {
            "stage_markers": [
                {
                    "name": "acceptance.persisted",
                    "status": "pass",
                    "observed_at": "2026-08-24T00:02:00.000000Z",
                }
            ]
        },
    }

    assert _marker_times_within_run_window(report) is False


class DuplicatePublishRedisDouble:
    def __init__(self, *, turn_count: int = 1) -> None:
        self.turn_count = turn_count
        self.last_delivered = "0-0"
        self.closed = False

    async def xinfo_groups(self, _stream):
        return [{"name": b"workers", "last-delivered-id": self.last_delivered.encode()}]

    async def xadd(self, _stream, _fields):
        self.last_delivered = "2-0"
        return b"2-0"

    async def xpending_range(self, _stream, _group, _start, _end, _count):
        return []

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
async def test_active_duplicate_publish_requires_group_progress_and_one_turn(monkeypatch) -> None:
    redis = DuplicatePublishRedisDouble()
    monkeypatch.setenv("TRPC_REAL_REDIS_URL", "redis://test-only")
    monkeypatch.setattr("scripts.real_runtime_gate.redis_async.from_url", lambda *_a, **_k: redis)
    monkeypatch.setattr(
        "scripts.real_runtime_gate._scoped_fetch",
        lambda *_a, **_k: _async_rows(
            {
                "outbox_id": "outbox-1",
                "tenant_id": "tenant",
                "event_type": "session.ready.v2",
                "aggregate_id": "session",
                "payload_json": {
                    "inbound_id": "inbound-1",
                    "generation": 1,
                    "priority": 0,
                    "trace_id": "trace-1",
                    "created_at": "2026-08-24T00:00:00.000000Z",
                },
                "trace_headers": {},
            }
        ),
    )
    monkeypatch.setattr(
        "scripts.real_runtime_gate._session_state",
        lambda *_a, **_k: _async_state({"turn_count": redis.turn_count}),
    )

    result = await _active_duplicate_publish_probe(
        None,
        {
            "tenant_id": "tenant",
            "session_id": "session",
            "unique_inbound_ids": ["inbound-1"],
        },
        stream="stream",
        group="workers",
        wait_seconds=1,
    )

    assert result["status"] == "pass"
    assert result["duplicate_stream_id"] == "2-0"
    assert result["pending_duplicate"] is False
    assert result["turn_count_exactly_one"] is True
    assert redis.closed is True
    assert "payload" not in result


@pytest.mark.asyncio
async def test_active_duplicate_publish_fails_when_it_creates_second_turn(monkeypatch) -> None:
    redis = DuplicatePublishRedisDouble(turn_count=2)
    monkeypatch.setenv("TRPC_REAL_REDIS_URL", "redis://test-only")
    monkeypatch.setattr("scripts.real_runtime_gate.redis_async.from_url", lambda *_a, **_k: redis)
    monkeypatch.setattr(
        "scripts.real_runtime_gate._scoped_fetch",
        lambda *_a, **_k: _async_rows(
            {
                "outbox_id": "outbox-1",
                "tenant_id": "tenant",
                "event_type": "session.ready.v2",
                "aggregate_id": "session",
                "payload_json": {
                    "inbound_id": "inbound-1",
                    "generation": 1,
                    "priority": 0,
                    "trace_id": "trace-1",
                    "created_at": "2026-08-24T00:00:00.000000Z",
                },
                "trace_headers": {},
            }
        ),
    )
    monkeypatch.setattr(
        "scripts.real_runtime_gate._session_state",
        lambda *_a, **_k: _async_state({"turn_count": redis.turn_count}),
    )

    result = await _active_duplicate_publish_probe(
        None,
        {
            "tenant_id": "tenant",
            "session_id": "session",
            "unique_inbound_ids": ["inbound-1"],
        },
        stream="stream",
        group="workers",
        wait_seconds=1,
    )

    assert result["status"] == "fail"
    assert "more than one" in result["reason"]


async def _async_rows(row):
    return [row]


async def _async_state(state):
    return state


class StaleProbeRepositoryDouble:
    def __init__(self, *, conflict: bool) -> None:
        self.conflict = conflict
        self.commit_calls = []
        self.context = TenantContext(
            tenant_id="tenant",
            app_id="app",
            config_version=1,
            channel_binding_id="binding",
            principal_id="principal",
            session_id="session",
            request_id="request",
            trace_id="trace",
        )
        self.acceptance = SimpleNamespace(context=self.context)
        self.snapshot = SessionSnapshot(
            tenant_id="tenant",
            app_id="app",
            session_id="session",
            principal_id="principal",
        )

    async def get_acceptance(self, _tenant_id, _inbound_id):
        return self.acceptance

    async def get_session_snapshot(self, _tenant_id, _session_id):
        return self.snapshot

    async def commit(self, commit):
        self.commit_calls.append(commit)
        if self.conflict:
            raise FencingConflict("stale worker")
        return SimpleNamespace()


@pytest.mark.asyncio
async def test_stale_fencing_probe_requires_conflict_and_current_replacement(monkeypatch) -> None:
    observations = [
        {"lease_owner": "worker-new", "lease_epoch": 2, "turn_id": "turn-1"},
        {"lease_owner": "worker-new", "lease_epoch": 2, "turn_id": "turn-1"},
    ]

    async def evidence(*_args, **_kwargs):
        return observations.pop(0)

    monkeypatch.setattr("scripts.real_runtime_gate._active_turn_evidence", evidence)
    repository = StaleProbeRepositoryDouble(conflict=True)
    result = await _probe_stale_fencing_rejection(
        None,
        repository,
        batch={
            "tenant_id": "tenant",
            "session_id": "session",
            "unique_inbound_ids": ["inbound-1"],
        },
        old_evidence={
            "lease_owner": "worker-old",
            "lease_epoch": 1,
            "turn_id": "turn-1",
            "inbound_id": "inbound-1",
            "attempt": 1,
        },
        takeover_observation={"status": "pass", "takeover_owner": "worker-new"},
    )

    assert result["status"] == "pass"
    assert result["fencing_conflict_caught"] is True
    assert result["owner_still_current"] is True
    assert len(repository.commit_calls) == 1


@pytest.mark.asyncio
async def test_stale_fencing_probe_fails_if_old_commit_succeeds(monkeypatch) -> None:
    async def evidence(*_args, **_kwargs):
        return {"lease_owner": "worker-new", "lease_epoch": 2, "turn_id": "turn-1"}

    monkeypatch.setattr("scripts.real_runtime_gate._active_turn_evidence", evidence)
    repository = StaleProbeRepositoryDouble(conflict=False)
    result = await _probe_stale_fencing_rejection(
        None,
        repository,
        batch={
            "tenant_id": "tenant",
            "session_id": "session",
            "unique_inbound_ids": ["inbound-1"],
        },
        old_evidence={
            "lease_owner": "worker-old",
            "lease_epoch": 1,
            "turn_id": "turn-1",
            "inbound_id": "inbound-1",
            "attempt": 1,
        },
        takeover_observation={"status": "pass", "takeover_owner": "worker-new"},
    )

    assert result["status"] == "fail"
    assert "unexpectedly succeeded" in result["reason"]


@pytest.mark.asyncio
async def test_active_turn_owner_requires_batch_scope(monkeypatch) -> None:
    called = False

    async def evidence(*_args, **_kwargs):
        nonlocal called
        called = True
        return {"lease_owner": "worker-a"}

    monkeypatch.setattr("scripts.real_runtime_gate._active_turn_evidence", evidence)
    from scripts.real_runtime_gate import _active_turn_owner

    assert await _active_turn_owner(None, "tenant") is None
    assert not called
    assert await _active_turn_owner(None, "tenant", "session", ("inbound",)) == "worker-a"
    assert called


@pytest.mark.asyncio
async def test_takeover_requires_new_epoch_different_owner_and_attempt(monkeypatch) -> None:
    observations = [
        None,
        {
            "lease_owner": "worker-b",
            "lease_epoch": 2,
            "attempt": 2,
            "turn_id": "turn-1",
        },
    ]

    calls = []

    async def evidence(*_args, **kwargs):
        calls.append(kwargs)
        return observations.pop(0) if observations else observations[-1]

    monkeypatch.setattr("scripts.real_runtime_gate._active_turn_evidence", evidence)
    result = await _wait_for_takeover(
        None,
        tenant_id="tenant",
        session_id="session",
        inbound_ids=("inbound",),
        killed_owner="worker-a",
        previous_epoch=1,
        target_turn_id="00000000-0000-0000-0000-000000000001",
        target_inbound_id="00000000-0000-0000-0000-000000000002",
        wait_seconds=1,
    )

    assert result["status"] == "pass"
    assert result["lease_epoch_monotonic"] is True
    assert result["takeover_owner_differs"] is True
    assert all(call["turn_id"].endswith("0001") for call in calls)
    assert all(call["inbound_id"].endswith("0002") for call in calls)
    assert all(call["max_started_age_seconds"] == MAX_KILL_TARGET_AGE_SECONDS for call in calls)


@pytest.mark.asyncio
async def test_active_turn_evidence_selects_only_freshest_processing_turn(monkeypatch) -> None:
    captured = {}

    async def fetch(_pool, _tenant_id, query, *args):
        captured["query"] = query
        captured["args"] = args
        return []

    monkeypatch.setattr("scripts.real_runtime_gate._scoped_fetch", fetch)
    result = await _active_turn_evidence(
        None,
        "tenant",
        "session",
        ("00000000-0000-0000-0000-000000000003",),
        max_started_age_seconds=MAX_KILL_TARGET_AGE_SECONDS,
    )

    assert result is None
    assert "ORDER BY turn.started_at DESC" in captured["query"]
    assert "turn.started_at >=" in captured["query"]
    assert "turn.fencing_token=session.lease_epoch" in captured["query"]
    assert captured["args"][-1] == MAX_KILL_TARGET_AGE_SECONDS


@pytest.mark.asyncio
async def test_active_turn_evidence_normalizes_database_types_for_reports(monkeypatch) -> None:
    async def fetch(*_args):
        return [
            {
                "lease_owner": "worker-a",
                "lease_epoch": 2,
                "turn_id": UUID("00000000-0000-0000-0000-000000000006"),
                "inbound_id": UUID("00000000-0000-0000-0000-000000000007"),
                "attempt": 2,
                "fencing_token": 2,
                "started_at": datetime(2026, 9, 1, tzinfo=UTC),
                "processing_age_seconds": Decimal("0.125"),
            }
        ]

    monkeypatch.setattr("scripts.real_runtime_gate._scoped_fetch", fetch)
    result = await _active_turn_evidence(
        None,
        "tenant",
        "session",
        ("00000000-0000-0000-0000-000000000007",),
    )

    assert result is not None
    assert result["turn_id"].endswith("0006")
    assert result["inbound_id"].endswith("0007")
    assert result["started_at"] == "2026-09-01T00:00:00.000000Z"
    assert result["processing_age_seconds"] == 0.125
    json.dumps(result, allow_nan=False)


@pytest.mark.asyncio
async def test_turn_state_evidence_is_scoped_to_exact_target(monkeypatch) -> None:
    captured = {}

    async def fetch(_pool, _tenant_id, query, *args):
        captured["query"] = query
        captured["args"] = args
        return [
            {
                "turn_id": args[-2],
                "inbound_id": args[-1],
                "status": "committed",
                "attempt": 1,
                "started_at": None,
                "committed_at": None,
                "lease_owner": None,
                "lease_epoch": 1,
            }
        ]

    monkeypatch.setattr("scripts.real_runtime_gate._scoped_fetch", fetch)
    result = await _turn_state_evidence(
        None,
        tenant_id="tenant",
        session_id="session",
        turn_id="00000000-0000-0000-0000-000000000004",
        inbound_id="00000000-0000-0000-0000-000000000005",
    )

    assert result is not None
    assert result["status"] == "committed"
    assert isinstance(result["turn_id"], str)
    assert isinstance(result["inbound_id"], str)
    json.dumps(result, allow_nan=False)
    assert "turn.turn_id=$3::uuid" in captured["query"]
    assert str(captured["args"][-2]).endswith("0004")
    assert str(captured["args"][-1]).endswith("0005")


@pytest.mark.asyncio
async def test_dlq_evidence_requires_attempt_increment(monkeypatch) -> None:
    async def fetch(_pool, _tenant_id, query, *_args):
        if "dead_letters" in query:
            return [{"status": "open", "reason": "binding_unavailable", "source_id": "outbound"}]
        return [{"attempts": 5, "last_error_type": "binding_unavailable"}]

    monkeypatch.setattr("scripts.real_runtime_gate._scoped_fetch", fetch)
    result = await _wait_for_dlq(None, "tenant", "outbound", "outbox", 4, 1)

    assert result["status"] == "pass"
    assert result["retry_attempts_increased"] is True
    assert result["terminal_path"] == "exhausted_retry_terminal_path"


def test_active_lease_owner_maps_to_exact_worker_container() -> None:
    workers = (
        {"container_id": "abc123456789-full"},
        {"container_id": "def987654321-full"},
    )

    assert _worker_container_for_owner(workers, "worker-def987654321") == workers[1]
    assert _worker_container_for_owner(workers, "worker-missing") is None
