from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.evidence_lineage import canonical_sha256
from scripts.fault_injection_gate import (
    FAULT_STAGE_REQUIRED_MARKERS,
    FAULT_STAGE_REQUIRED_STAGES,
    SCENARIO_STAGE_MARKERS,
    SCENARIOS,
    _fault_stage_worker_image_attestation,
    _missing_real_environment,
    _not_run_report,
    _parent_worker_preflight,
    _production_contract,
    _real_command,
    _run_fault_stage_acceptance,
    _run_real_scenario,
    _runbook,
    _runtime_fencing_worker_evidence,
    _runtime_worker_ids,
    _runtime_worker_recovery,
    _scenario_stage_markers,
    _status,
)
from scripts.fault_injection_gate import main as fault_main
from tests.conftest import envelope, repository
from trpc_service.agent.worker import ProcessStatus, WorkerResult
from trpc_service.channels.dispatcher import ChannelDispatcher
from trpc_service.channels.envelopes import DeliveryReceipt, DeliveryStatus
from trpc_service.queue.dispatcher import OutboxDispatcher
from trpc_service.queue.redis_streams import QueueMessage
from trpc_service.queue.worker_consumer import WorkerConsumer
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import OutboxRecord, StoredEvent, TurnCommit
from trpc_service.storage.protocols import FencingConflict
from trpc_service.tenant.models import Channel, ToolRisk
from trpc_service.tool.execution import (
    ExecutionStatus,
    HumanReviewRequired,
    InMemoryExecutionLedger,
    ToolExecutor,
)


class SimulatedWorkerKill(RuntimeError):
    """Test-only stand-in for SIGKILL at a declared stage."""


class QueueDouble:
    def __init__(self) -> None:
        self.acked: list[QueueMessage] = []
        self.deferred: list[QueueMessage] = []

    async def ack(self, message: QueueMessage) -> None:
        self.acked.append(message)

    async def defer(self, message: QueueMessage, *, consumer_id: str) -> bool:
        self.deferred.append(message)
        return False

    async def heartbeat(self, _message: QueueMessage, *, consumer_id: str, stop_event) -> bool:
        await stop_event.wait()
        return True


class RepositoryDouble:
    def __init__(self, acceptance) -> None:
        self.acceptance = acceptance

    async def get_acceptance(self, tenant_id: str, inbound_id: str):
        if (
            tenant_id == self.acceptance.context.tenant_id
            and inbound_id == self.acceptance.inbound_id
        ):
            return self.acceptance
        return None


class CrashThenCommitWorker:
    def __init__(self) -> None:
        self.calls = 0

    async def process(self, _acceptance):
        self.calls += 1
        if self.calls == 1:
            raise SimulatedWorkerKill("enqueue-stage kill")
        return WorkerResult(ProcessStatus.COMMITTED)


@pytest.mark.asyncio
async def test_enqueue_stage_kill_leaves_delivery_unacked_for_reclaim() -> None:
    acceptance = await TenantRuntime(repository(), routing_key=b"q" * 32).accept(
        "binding-unpredictable-a", envelope("enqueue-kill")
    )
    message = QueueMessage(
        stream_id="1-0",
        outbox_id="outbox",
        tenant_id=acceptance.context.tenant_id,
        event_type="inbound.accepted",
        aggregate_id=acceptance.inbound_id,
        payload={},
        trace_headers={},
    )
    queue = QueueDouble()
    consumer = WorkerConsumer(
        RepositoryDouble(acceptance), queue, CrashThenCommitWorker(), consumer_id="worker-a"
    )

    with pytest.raises(SimulatedWorkerKill):
        await consumer.process_message(message)
    assert queue.acked == []

    await consumer.process_message(message)
    assert queue.acked == [message]


def _context():
    return (
        TenantRuntime(repository(), routing_key=b"t" * 32)
        .prepare(repository()._routes["binding-unpredictable-a"], envelope())
        .context
    )


@pytest.mark.asyncio
async def test_tool_stage_kill_marks_non_idempotent_result_ambiguous() -> None:
    context = _context()
    ledger = InMemoryExecutionLedger()
    executor = ToolExecutor(b"e" * 32, ledger)
    calls = 0

    async def side_effect() -> str:
        nonlocal calls
        calls += 1
        raise SimulatedWorkerKill("tool-stage kill")

    with pytest.raises(HumanReviewRequired):
        await executor.execute(
            context,
            turn_id="turn-1",
            tool_name="charge",
            arguments={"amount": 1},
            risk=ToolRisk.NON_IDEMPOTENT,
            call=side_effect,
        )

    key = executor.key_for(
        context,
        turn_id="turn-1",
        tool_name="charge",
        arguments={"amount": 1},
    )
    assert ledger.records[key].status == ExecutionStatus.AMBIGUOUS
    with pytest.raises(HumanReviewRequired):
        await executor.execute(
            context,
            turn_id="turn-1",
            tool_name="charge",
            arguments={"amount": 1},
            risk=ToolRisk.NON_IDEMPOTENT,
            call=side_effect,
        )
    assert calls == 1


@pytest.mark.asyncio
async def test_tool_stage_kill_allows_idempotent_retry_with_same_key() -> None:
    context = _context()
    ledger = InMemoryExecutionLedger()
    executor = ToolExecutor(b"e" * 32, ledger)
    calls = 0

    async def idempotent_call() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise SimulatedWorkerKill("idempotent tool-stage kill")
        return "ok"

    with pytest.raises(SimulatedWorkerKill):
        await executor.execute(
            context,
            turn_id="turn-2",
            tool_name="lookup",
            arguments={"key": "a"},
            risk=ToolRisk.IDEMPOTENT,
            call=idempotent_call,
        )
    assert (
        await executor.execute(
            context,
            turn_id="turn-2",
            tool_name="lookup",
            arguments={"key": "a"},
            risk=ToolRisk.IDEMPOTENT,
            call=idempotent_call,
        )
        == "ok"
    )
    assert calls == 2


@pytest.mark.asyncio
async def test_commit_stage_stale_fencing_token_cannot_publish() -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=b"c" * 32).accept(
        "binding-unpredictable-a", envelope("commit-kill")
    )
    old = await repo.acquire(
        acceptance=accepted,
        worker_id="worker-old",
        lease_for=timedelta(milliseconds=1),
    )
    assert old is not None
    await asyncio.sleep(0.01)
    replacement = await repo.acquire(
        acceptance=accepted,
        worker_id="worker-new",
        lease_for=timedelta(seconds=1),
    )
    assert replacement is not None
    with pytest.raises(FencingConflict):
        await repo.commit(
            TurnCommit(
                context=accepted.context,
                lease=old,
                state={"stale": True},
                events=(StoredEvent(event_id="stale", author="agent", timestamp=1, event={}),),
            )
        )
    result = await repo.commit(
        TurnCommit(
            context=accepted.context,
            lease=replacement,
            state={"fresh": True},
            events=(StoredEvent(event_id="fresh", author="agent", timestamp=2, event={}),),
        )
    )
    assert result.last_sequence == 1


class PublishQueueDouble:
    def __init__(self) -> None:
        self.available = False
        self.published: list[OutboxRecord] = []

    async def publish(self, record: OutboxRecord) -> str:
        if not self.available:
            raise ConnectionError("redis unavailable")
        self.published.append(record)
        return "1-0"


class OutboxRepositoryDouble:
    def __init__(self, record: OutboxRecord) -> None:
        self.record = record
        self.released: list[dict[str, object]] = []
        self.marked: list[str] = []

    async def claim_outbox(self, **_kwargs):
        return (self.record,)

    async def release_outbox(self, _tenant_id, outbox_id, **kwargs):
        self.released.append({"outbox_id": outbox_id, **kwargs})

    async def mark_outbox_published(self, _tenant_id, outbox_id, **_kwargs):
        self.marked.append(outbox_id)


@pytest.mark.asyncio
async def test_redis_connection_recovery_republishes_durable_outbox() -> None:
    record = OutboxRecord(
        outbox_id="outbox-redis",
        tenant_id="tenant-a",
        event_type="inbound.accepted",
        aggregate_id="inbound",
        payload={"inbound_id": "inbound"},
    )
    repo = OutboxRepositoryDouble(record)
    queue = PublishQueueDouble()
    dispatcher = OutboxDispatcher(repo, queue, owner_id="dispatcher")

    assert await dispatcher.dispatch_once() == 0
    assert repo.released and not repo.marked
    queue.available = True
    assert await dispatcher.dispatch_once() == 1
    assert repo.marked == [record.outbox_id]
    assert len(queue.published) == 1


class DeliveryRepositoryDouble:
    def __init__(self, record: OutboxRecord) -> None:
        self.record = record
        self.route = repository()._routes["binding-unpredictable-a"]
        self.receipts: list[DeliveryReceipt] = []
        self.dead_letters: list[str] = []

    async def claim_outbox(self, **_kwargs):
        return (self.record,)

    async def resolve_binding(self, _binding_id):
        return self.route

    async def record_delivery(self, _tenant_id, receipt, **_kwargs):
        self.receipts.append(receipt)

    async def dead_letter_outbox(self, _record, **kwargs):
        self.dead_letters.append(str(kwargs["reason"]))


@pytest.mark.asyncio
async def test_ambiguous_delivery_goes_to_dlq_without_automatic_replay() -> None:
    envelope = {
        "outbound_id": "outbound-ambiguous",
        "tenant_id": "tenant-a",
        "binding_id": "binding-unpredictable-a",
        "channel": "feishu",
        "target_id": "user",
        "session_id": "session",
        "text": "reply",
    }
    record = OutboxRecord(
        outbox_id="outbox-ambiguous",
        tenant_id="tenant-a",
        event_type="outbound.feishu.ready",
        aggregate_id="outbound-ambiguous",
        payload=envelope,
    )
    repo = DeliveryRepositoryDouble(record)
    receipt = DeliveryReceipt(
        outbound_id="outbound-ambiguous",
        status=DeliveryStatus.AMBIGUOUS,
        provider_code="transport_unknown",
    )
    dispatcher = ChannelDispatcher(
        repo,
        {Channel.FEISHU: SimpleNamespace(send=lambda *_args: _async_receipt(receipt))},
        owner_id="channel",
        event_type="outbound.feishu.ready",
    )
    assert await dispatcher.dispatch_once() == 1
    assert repo.dead_letters == ["transport_unknown"]
    assert repo.receipts[0].status == DeliveryStatus.AMBIGUOUS


async def _async_receipt(receipt: DeliveryReceipt) -> DeliveryReceipt:
    return receipt


def test_fault_gate_template_keeps_production_not_run() -> None:
    args = SimpleNamespace(
        scenario="all",
        workers=4,
        output="runs/multitenant/fault-injection.json",
        project="trpc-agent-service",
    )
    report = _not_run_report(["test-only preflight"], args)
    assert tuple(report["baseline"]["scenario_names"]) == tuple(SCENARIOS)
    assert report["production_gate"] == "not_run"
    assert set(report["candidate"]["scenarios"]) == set(SCENARIOS)
    assert all(
        marker["status"] == "not_run"
        for scenario in report["candidate"]["scenarios"].values()
        for marker in scenario["stage_markers"]
    )


def test_fault_gate_status_and_scenario_inventory_are_explicit() -> None:
    assert _status(["pass", "not_run"]) == "not_run"
    assert _status(["pass", "fail"]) == "fail"
    assert set(SCENARIOS) == {
        "redis_interrupt",
        "worker_enqueue",
        "worker_tool",
        "worker_commit",
        "fencing",
        "republish",
        "dlq",
        "ambiguous",
    }
    assert set(SCENARIO_STAGE_MARKERS) == set(SCENARIOS)
    for scenario, names in SCENARIO_STAGE_MARKERS.items():
        markers = _scenario_stage_markers(
            scenario,
            status="simulated",
            reason="offline contract",
        )
        assert tuple(item["name"] for item in markers) == names
        assert all(item["status"] == "simulated" for item in markers)


def test_fault_report_contract_publishes_fixed_production_inventory() -> None:
    contract = _production_contract()

    assert contract["schema_version"] == 1
    assert contract["mode"] == "real_compose_fault_injection"
    assert set(contract["required_scenarios"]) == set(SCENARIOS)
    assert contract["marker_requirements"]["status"] == "pass"
    assert contract["marker_requirements"]["duplicate_names"] == "reject"


def test_fault_runbook_uses_the_all_scenario_selector() -> None:
    runbook = _runbook(
        SimpleNamespace(project="trpc-agent-service", workers=4, output=Path("fault.json"))
    )

    command = runbook["fault_stage_command_template"]
    assert command[command.index("--scenario") + 1] == "all"


def test_fault_runbook_starts_session_recovery_for_fault_stage_takeover() -> None:
    runbook = _runbook(
        SimpleNamespace(project="trpc-agent-service", workers=4, output=Path("fault.json"))
    )

    assert "session-recovery" in runbook["toxiproxy_start"]


def test_fault_stage_only_gate_does_not_require_normal_runtime_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        monkeypatch.delenv(name, raising=False)

    assert _missing_real_environment(require_runtime_credentials=False) == []
    assert "TRPC_REAL_DATABASE_DSN" in _missing_real_environment(require_runtime_credentials=True)
    assert "TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN" in _missing_real_environment(
        require_runtime_credentials=True
    )


def test_real_fault_entry_requires_current_release_binding(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRPC_RUN_REAL_MULTINODE", "1")
    monkeypatch.delenv("TRPC_RELEASE_ID", raising=False)
    monkeypatch.delenv("TRPC_RELEASE_NONCE", raising=False)
    output = tmp_path / "fault-injection.json"

    assert (
        fault_main(
            [
                "--execute",
                "--scenario",
                "worker_enqueue",
                "--require-production",
                "--output",
                str(output),
            ]
        )
        == 1
    )
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "not_run"
    assert any(
        "TRPC_RELEASE_ID and TRPC_RELEASE_NONCE" in reason for reason in report["rejection_reasons"]
    )


def test_final_fault_wrapper_requires_complete_production_gate_before_cleanup() -> None:
    wrapper = (
        Path(__file__).resolve().parents[2] / "runs" / "multitenant" / "run-final-fault-gate.ps1"
    )
    source = wrapper.read_text(encoding="utf-8")

    assert "--require-production" in source
    gate_guard = 'if ($fault.gate -ne "pass" -or $fault.production_gate -ne "pass")'
    guard_offset = source.index(gate_guard)
    cleanup_offset = source.index("docker compose @stageCompose down")
    assert guard_offset < cleanup_offset
    assert 'status -ne "not_run"' not in source


def test_final_fault_wrapper_uses_normal_runtime_compose_ports() -> None:
    wrapper = (
        Path(__file__).resolve().parents[2] / "runs" / "multitenant" / "run-final-fault-gate.ps1"
    )
    lines = wrapper.read_text(encoding="utf-8").splitlines()
    database_line = next(line for line in lines if "$env:TRPC_REAL_DATABASE_DSN =" in line)
    redis_line = next(line for line in lines if "$env:TRPC_REAL_REDIS_URL =" in line)

    assert "$runtimeDbPort" in database_line
    assert ":15432/" not in database_line
    assert "$runtimeRedisPort" in redis_line
    assert ":16379/" not in redis_line


def test_real_command_forwards_isolated_compose_and_toxiproxy_selectors(tmp_path) -> None:
    args = SimpleNamespace(
        compose_file=tmp_path / "compose.yml",
        toxiproxy_override=tmp_path / "toxiproxy.yml",
        project="fault-project",
        toxiproxy_api="http://127.0.0.1:18474",
        workers=4,
        timeout_seconds=12.0,
        fault_messages=3,
        messages=5,
        duplicates=1,
    )
    command = _real_command(args, "redis_interrupt", tmp_path / "report.json")
    assert "--require-production" not in command
    assert "--use-toxiproxy" in command
    assert ["--project", "fault-project"] == command[
        command.index("--project") : command.index("--project") + 2
    ]
    assert "--fault-messages" in command
    assert "http://127.0.0.1:18474" in command
    republish = _real_command(args, "republish", tmp_path / "republish.json")
    assert "--republish-probe" in republish
    fencing = _real_command(args, "fencing", tmp_path / "fencing.json")
    assert "--use-toxiproxy" in fencing
    ambiguous = _real_command(args, "ambiguous", tmp_path / "ambiguous.json")
    assert Path(ambiguous[1]).name == "ambiguous_provider_acceptance.py"
    assert "--execute" in ambiguous
    assert ["--project", "fault-project"] == ambiguous[
        ambiguous.index("--project") : ambiguous.index("--project") + 2
    ]


def test_ambiguous_command_forwards_explicit_provider_url(tmp_path: Path) -> None:
    args = SimpleNamespace(
        project="trpc-fault-runtime-unit",
        timeout_seconds=12.0,
        ambiguous_provider_url="http://127.0.0.1:18791",
    )

    command = _real_command(args, "ambiguous", tmp_path / "ambiguous.json")

    assert command[command.index("--provider-url") + 1] == "http://127.0.0.1:18791"


def test_parent_worker_preflight_binds_ambiguous_endpoint_to_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = "trpc-fault-runtime-unit"
    source = "a" * 64
    image = "sha256:" + "b" * 64
    workers = tuple(f"worker-{index}" for index in range(1, 5))
    monkeypatch.setenv("TRPC_REAL_IMAGE_DIGEST", image)
    monkeypatch.setattr(
        "scripts.fault_injection_gate.source_fingerprint",
        lambda _root: {"status": "available", "value": source},
    )
    monkeypatch.setattr(
        "scripts.fault_injection_gate._runtime_worker_ids",
        lambda _project: workers,
    )
    monkeypatch.setattr(
        "scripts.fault_injection_gate._runtime_worker_inspect",
        lambda worker: {
            "container_id": worker,
            "project": project,
            "service": "worker",
            "source_fingerprint": source,
            "image_id": image,
            "status": "running",
            "health": "healthy",
        },
    )

    result = _parent_worker_preflight(project, 4)

    assert result["status"] == "pass"
    assert result["worker_count"] == 4
    assert result["healthy_worker_count"] == 4
    assert result["independent_processes"] is True


def test_runtime_worker_ids_requests_full_container_ids(monkeypatch) -> None:
    worker_ids = ("a" * 64, "b" * 64, "c" * 64, "d" * 64)
    calls: list[list[str]] = []

    def fake_run(command, **_kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout="\n".join(worker_ids) + "\n")

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)

    assert _runtime_worker_ids("trpc-fault-test") == worker_ids
    assert calls[0][1:4] == ["ps", "-aq", "--no-trunc"]


def test_runtime_worker_recovery_starts_only_retained_container_with_attestation(
    monkeypatch,
) -> None:
    project = "trpc-fault-runtime-unit"
    source = "a" * 64
    image = "sha256:" + "b" * 64
    worker_ids = [f"worker-{index}" for index in range(1, 5)]
    states = {
        worker_id: ("exited", "unhealthy") if worker_id == "worker-4" else ("running", "healthy")
        for worker_id in worker_ids
    }
    calls: list[list[str]] = []

    def fake_run(command, **_kwargs):
        calls.append(command)
        if command[1] == "inspect":
            worker_id = command[-1]
            status, health = states[worker_id]
            labels = {
                "com.docker.compose.project": project,
                "com.docker.compose.service": "worker",
                "io.trpc.agent-service.source-fingerprint": source,
            }
            return SimpleNamespace(
                returncode=0,
                stdout=f"{json.dumps(labels)}\t{image}\t{status}\t{health}\n",
            )
        if command[1] == "start":
            assert command[2] == "worker-4"
            states["worker-4"] = ("running", "healthy")
            return SimpleNamespace(returncode=0, stdout="worker-4\n")
        raise AssertionError(f"unexpected Docker command: {command}")

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _runtime_worker_recovery(
        SimpleNamespace(project=project, workers=4, timeout_seconds=1.0),
        {
            "worker_preflight": {
                "status": "pass",
                "source_fingerprint": source,
                "image_id": image,
            },
            "_runtime_worker_containers": worker_ids,
            "_runtime_killed_worker_container": "worker-4",
            "_runtime_surviving_healthy_worker_containers": tuple(worker_ids[:3]),
        },
    )

    assert result["status"] == "pass"
    assert result["healthy_worker_count"] == 4
    assert result["started_count"] == 1
    assert [command[2] for command in calls if command[1] == "start"] == ["worker-4"]
    assert not any(command[1] in {"up", "down"} for command in calls)


def test_runtime_fencing_worker_evidence_accepts_sequences_and_kill_record() -> None:
    worker_ids = tuple(f"worker-{index}" for index in range(1, 5))
    child = {
        "candidate": {
            "preflight": {
                "worker_containers": tuple({"container_id": item} for item in worker_ids),
            },
            "load": {
                "worker_kill": {"killed_container_id": worker_ids[-1]},
                "fencing_takeover": {
                    "surviving_healthy_worker_containers": tuple(worker_ids[:3]),
                },
            },
        }
    }

    retained, killed, survivors = _runtime_fencing_worker_evidence(child)

    assert retained == worker_ids
    assert killed == worker_ids[-1]
    assert survivors == worker_ids[:3]


def test_runtime_worker_recovery_fails_closed_without_kill_identity(monkeypatch) -> None:
    calls: list[list[str]] = []

    def unexpected_run(command, **_kwargs):
        calls.append(command)
        raise AssertionError("recovery must not inspect or recreate without kill identity")

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", unexpected_run)
    result = _runtime_worker_recovery(
        SimpleNamespace(
            project="trpc-fault-runtime-unit",
            workers=4,
            timeout_seconds=1.0,
        ),
        {
            "worker_preflight": {
                "status": "pass",
                "source_fingerprint": "a" * 64,
                "image_id": "sha256:" + "b" * 64,
            },
            "_runtime_worker_containers": [f"worker-{index}" for index in range(1, 5)],
            "_runtime_surviving_healthy_worker_containers": [
                f"worker-{index}" for index in range(1, 4)
            ],
        },
    )

    assert result["status"] == "not_run"
    assert "killed" in result["reason"]
    assert calls == []


def test_runtime_worker_recovery_rejects_mismatched_release_without_starting(monkeypatch) -> None:
    project = "trpc-fault-runtime-unit"
    source = "a" * 64
    expected_image = "sha256:" + "b" * 64
    stale_image = "sha256:" + "c" * 64
    calls: list[list[str]] = []

    def fake_run(command, **_kwargs):
        calls.append(command)
        if command[1:3] == ["ps", "-aq"]:
            return SimpleNamespace(returncode=0, stdout="worker-1\n")
        if command[1] == "inspect":
            labels = {
                "com.docker.compose.project": project,
                "com.docker.compose.service": "worker",
                "io.trpc.agent-service.source-fingerprint": source,
            }
            return SimpleNamespace(
                returncode=0,
                stdout=f"{json.dumps(labels)}\t{stale_image}\trunning\thealthy\n",
            )
        if command[1] == "start":
            raise AssertionError("a mismatched release must never be started")
        raise AssertionError(f"unexpected Docker command: {command}")

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _runtime_worker_recovery(
        SimpleNamespace(project=project, workers=4, timeout_seconds=1.0),
        {
            "worker_preflight": {
                "status": "pass",
                "source_fingerprint": source,
                "image_id": expected_image,
            },
            "_runtime_worker_containers": ["worker-1"],
        },
    )

    assert result["status"] == "not_run"
    assert "image" in result["reason"] or "retained" in result["reason"]
    assert not any(command[1] == "start" for command in calls)


def test_real_scenario_uses_unique_child_report_and_validates_phase(monkeypatch, tmp_path) -> None:
    compose = tmp_path / "compose.yml"
    override = tmp_path / "toxiproxy.yml"
    compose.write_text("services: {}", encoding="utf-8")
    override.write_text("services: {}", encoding="utf-8")
    args = SimpleNamespace(
        compose_file=compose,
        toxiproxy_override=override,
        project="fault-project",
        toxiproxy_api="http://127.0.0.1:18474",
        workers=4,
        timeout_seconds=12.0,
        fault_messages=3,
        messages=5,
        duplicates=1,
        allow_process_kill=False,
        output=tmp_path / "fault-report.json",
    )

    def fake_run(command, *, env, **_kwargs):
        output = command[command.index("--output") + 1]
        markers = [
            {"name": name, "status": "pass", "observed_at": "now"}
            for name in SCENARIO_STAGE_MARKERS["redis_interrupt"]
        ]
        child = {
            "run_id": env["TRPC_REAL_RUN_ID"],
            "run_nonce": "child-nonce-redis",
            "started_at": "2026-08-24T00:00:00Z",
            "ended_at": "2026-08-24T00:00:01Z",
            "gate": "pass",
            "production_gate": "not_run",
            "candidate": {
                "faults": {
                    "status": "pass",
                    "stage_markers": markers,
                    "redis": {"status": "pass", "stage_markers": markers},
                }
            },
        }
        with open(output, "w", encoding="utf-8") as handle:
            json.dump(child, handle)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_real_scenario(args, "redis_interrupt")

    assert result["status"] == "pass"
    assert result["run_id"] == result["child_run_id"]
    assert result["child_phase"] == "fault"
    assert Path(result["child_report"]).parent.parent.name == "fault-evidence"
    assert Path(result["child_report"]).name == "redis_interrupt.child.json"
    child = json.loads(Path(result["child_report"]).read_text(encoding="utf-8"))
    assert result["child_report_sha256"] == canonical_sha256(child)
    assert result["child_nonce_sha256"] == hashlib.sha256(b"child-nonce-redis").hexdigest()
    assert result["child_report_started_at"] == "2026-08-24T00:00:00Z"
    assert result["child_report_ended_at"] == "2026-08-24T00:00:01Z"
    assert result["observed_exit_code"] == 0
    assert result["child_report_path_confined"] is True


def test_real_ambiguous_scenario_uses_response_drop_child(monkeypatch, tmp_path) -> None:
    compose = tmp_path / "compose.yml"
    override = tmp_path / "toxiproxy.yml"
    compose.write_text("services: {}", encoding="utf-8")
    override.write_text("services: {}", encoding="utf-8")
    args = SimpleNamespace(
        compose_file=compose,
        toxiproxy_override=override,
        project="trpc-fault-runtime-unit",
        toxiproxy_api="http://127.0.0.1:18474",
        ambiguous_provider_url="",
        workers=4,
        timeout_seconds=12.0,
        fault_messages=3,
        messages=5,
        duplicates=1,
        allow_process_kill=False,
        output=tmp_path / "fault-report.json",
    )
    markers = [
        {"name": name, "status": "pass", "observed_at": "2026-08-25T00:00:00Z"}
        for name in SCENARIO_STAGE_MARKERS["ambiguous"]
    ]

    def fake_run(command, *, env, **_kwargs):
        output = command[command.index("--output") + 1]
        child = {
            "run_id": env["TRPC_REAL_RUN_ID"],
            "run_nonce_sha256": "a" * 64,
            "started_at": "2026-08-25T00:00:00Z",
            "ended_at": "2026-08-25T00:00:01Z",
            "gate": "pass",
            "production_gate": "not_run",
            "candidate": {
                "ambiguous": {
                    "status": "pass",
                    "manual_confirmation_required": True,
                    "automatic_replay_count": 0,
                    "confirmed_replay_status": "pass",
                    "provider_ledger": {
                        "accepted_count": 1,
                        "side_effect_count": 1,
                        "duplicate_replay_count": 1,
                    },
                    "stage_markers": markers,
                }
            },
        }
        with open(output, "w", encoding="utf-8") as handle:
            json.dump(child, handle)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    monkeypatch.setattr(
        "scripts.fault_injection_gate._parent_worker_preflight",
        lambda _project, _workers: {
            "status": "pass",
            "worker_count": 4,
            "healthy_worker_count": 4,
            "independent_processes": True,
        },
    )

    result = _run_real_scenario(args, "ambiguous")

    assert result["status"] == "pass"
    assert result["child_phase"] == "ambiguous"
    assert result["evidence"]["automatic_replay_count"] == 0
    assert result["evidence"]["provider_ledger"]["side_effect_count"] == 1
    assert [marker["name"] for marker in result["stage_markers"]] == list(
        SCENARIO_STAGE_MARKERS["ambiguous"]
    )


def test_real_scenario_rejects_scoped_child_that_claims_production_pass(
    monkeypatch, tmp_path
) -> None:
    compose = tmp_path / "compose.yml"
    override = tmp_path / "toxiproxy.yml"
    compose.write_text("services: {}", encoding="utf-8")
    override.write_text("services: {}", encoding="utf-8")
    args = SimpleNamespace(
        compose_file=compose,
        toxiproxy_override=override,
        project="fault-project",
        toxiproxy_api="http://127.0.0.1:18474",
        workers=4,
        timeout_seconds=12.0,
        fault_messages=3,
        messages=5,
        duplicates=1,
        allow_process_kill=False,
        output=tmp_path / "fault-report.json",
    )

    def fake_run(command, *, env, **_kwargs):
        output = command[command.index("--output") + 1]
        markers = [
            {"name": name, "status": "pass", "observed_at": "now"}
            for name in SCENARIO_STAGE_MARKERS["redis_interrupt"]
        ]
        child = {
            "run_id": env["TRPC_REAL_RUN_ID"],
            "run_nonce": "child-nonce-invalid-production",
            "started_at": "2026-08-24T00:00:00Z",
            "ended_at": "2026-08-24T00:00:01Z",
            "gate": "pass",
            "production_gate": "pass",
            "candidate": {
                "faults": {
                    "status": "pass",
                    "stage_markers": markers,
                    "redis": {"status": "pass", "stage_markers": markers},
                }
            },
        }
        with open(output, "w", encoding="utf-8") as handle:
            json.dump(child, handle)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)

    result = _run_real_scenario(args, "redis_interrupt")

    assert result["status"] == "fail"
    assert "scoped child production_gate must be not_run" in result["reason"]


def test_real_scenario_missing_required_marker_never_passes(monkeypatch, tmp_path) -> None:
    compose = tmp_path / "compose.yml"
    override = tmp_path / "toxiproxy.yml"
    compose.write_text("services: {}", encoding="utf-8")
    override.write_text("services: {}", encoding="utf-8")
    args = SimpleNamespace(
        compose_file=compose,
        toxiproxy_override=override,
        project="fault-project",
        toxiproxy_api="http://127.0.0.1:18474",
        workers=4,
        timeout_seconds=12.0,
        fault_messages=3,
        messages=5,
        duplicates=1,
        allow_process_kill=False,
        output=tmp_path / "fault-report.json",
    )

    def fake_run(command, *, env, **_kwargs):
        output = command[command.index("--output") + 1]
        child = {
            "run_id": env["TRPC_REAL_RUN_ID"],
            "run_nonce": "child-nonce-missing-marker",
            "started_at": "2026-08-24T00:00:00Z",
            "ended_at": "2026-08-24T00:00:01Z",
            "gate": "pass",
            "candidate": {
                "faults": {
                    "status": "pass",
                    "redis": {"status": "pass", "stage_markers": []},
                }
            },
        }
        with open(output, "w", encoding="utf-8") as handle:
            json.dump(child, handle)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_real_scenario(args, "redis_interrupt")

    assert result["status"] == "not_run"
    assert "missing required stage markers" in result["reason"]
    assert "scoped child production_gate must be not_run; observed missing" in result["reason"]


def test_republish_requires_active_duplicate_publish_probe(monkeypatch, tmp_path) -> None:
    compose = tmp_path / "compose.yml"
    override = tmp_path / "toxiproxy.yml"
    compose.write_text("services: {}", encoding="utf-8")
    override.write_text("services: {}", encoding="utf-8")
    args = SimpleNamespace(
        compose_file=compose,
        toxiproxy_override=override,
        project="fault-project",
        toxiproxy_api="http://127.0.0.1:18474",
        workers=4,
        timeout_seconds=12.0,
        fault_messages=3,
        messages=5,
        duplicates=1,
        allow_process_kill=False,
        output=tmp_path / "fault-report.json",
    )

    def fake_run(command, *, env, **_kwargs):
        output = command[command.index("--output") + 1]
        markers = [
            {"name": name, "status": "pass", "observed_at": "now"}
            for name in SCENARIO_STAGE_MARKERS["republish"]
            if name != "duplicate_publish_verified"
        ]
        child = {
            "run_id": env["TRPC_REAL_RUN_ID"],
            "run_nonce": "child-nonce-republish",
            "started_at": "2026-08-24T00:00:00Z",
            "ended_at": "2026-08-24T00:00:01Z",
            "gate": "pass",
            "production_gate": "not_run",
            "candidate": {
                "faults": {
                    "status": "pass",
                    "stage_markers": markers,
                    "redis": {"status": "pass", "stage_markers": markers},
                }
            },
        }
        with open(output, "w", encoding="utf-8") as handle:
            json.dump(child, handle)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_real_scenario(args, "republish")

    assert result["status"] == "not_run"
    assert "active duplicate Redis publish probe" in result["reason"]


def test_republish_promotes_active_duplicate_publish_probe_to_contract_field(
    monkeypatch, tmp_path
) -> None:
    compose = tmp_path / "compose.yml"
    override = tmp_path / "toxiproxy.yml"
    compose.write_text("services: {}", encoding="utf-8")
    override.write_text("services: {}", encoding="utf-8")
    args = SimpleNamespace(
        compose_file=compose,
        toxiproxy_override=override,
        project="fault-project",
        toxiproxy_api="http://127.0.0.1:18474",
        workers=4,
        timeout_seconds=12.0,
        fault_messages=3,
        messages=5,
        duplicates=1,
        allow_process_kill=False,
        output=tmp_path / "fault-report.json",
    )
    probe = {
        "status": "pass",
        "duplicate_stream_id": "42-0",
        "turn_count": 1,
        "turn_count_exactly_one": True,
        "pending_duplicate": False,
    }
    markers = [
        {"name": name, "status": "pass", "observed_at": "now"}
        for name in SCENARIO_STAGE_MARKERS["republish"]
    ]

    def fake_run(command, *, env, **_kwargs):
        output = command[command.index("--output") + 1]
        child = {
            "run_id": env["TRPC_REAL_RUN_ID"],
            "run_nonce": "child-nonce-republish-pass",
            "started_at": "2026-08-24T00:00:00Z",
            "ended_at": "2026-08-24T00:00:01Z",
            "gate": "pass",
            "production_gate": "not_run",
            "candidate": {
                "faults": {
                    "status": "pass",
                    "stage_markers": markers,
                    "redis": {"status": "pass", "stage_markers": markers},
                },
                "load": {},
            },
        }
        child["candidate"]["faults"]["republish_duplicate_publish_probe"] = probe
        with open(output, "w", encoding="utf-8") as handle:
            json.dump(child, handle)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_real_scenario(args, "republish")

    assert result["status"] == "pass"
    assert result["duplicate_publish_probe"] == probe
    assert result["evidence"]["duplicate_publish_probe"] == probe


def test_real_scenario_rejects_child_phase_that_did_not_pass(monkeypatch, tmp_path) -> None:
    compose = tmp_path / "compose.yml"
    override = tmp_path / "toxiproxy.yml"
    compose.write_text("services: {}", encoding="utf-8")
    override.write_text("services: {}", encoding="utf-8")
    args = SimpleNamespace(
        compose_file=compose,
        toxiproxy_override=override,
        project="fault-project",
        toxiproxy_api="http://127.0.0.1:18474",
        workers=4,
        timeout_seconds=12.0,
        fault_messages=3,
        messages=5,
        duplicates=1,
        allow_process_kill=False,
        output=tmp_path / "fault-report.json",
    )

    def fake_run(command, *, env, **_kwargs):
        output = command[command.index("--output") + 1]
        child = {
            "run_id": env["TRPC_REAL_RUN_ID"],
            "run_nonce": "child-nonce-phase",
            "started_at": "2026-08-24T00:00:00Z",
            "ended_at": "2026-08-24T00:00:01Z",
            "gate": "pass",
            "production_gate": "not_run",
            "candidate": {
                "faults": {
                    "status": "not_run",
                    "redis": {"status": "pass", "stage_markers": []},
                }
            },
        }
        with open(output, "w", encoding="utf-8") as handle:
            json.dump(child, handle)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_real_scenario(args, "redis_interrupt")

    assert result["status"] == "not_run"
    assert "child fault phase did not pass" in result["reason"]


def _fault_stage_args(output, **overrides):
    values = {
        "output": output,
        "timeout_seconds": 30.0,
        "allow_process_kill": True,
        "fault_project": "trpc-fault-test-run",
        "fault_worker_container": "worker-container-verified",
        "fault_termination": "kill",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _fault_stage_child(
    run_id: str,
    *,
    gate: str = "pass",
    provenance: dict[str, object] | None = None,
) -> dict[str, object]:
    cases = []
    for stage in FAULT_STAGE_REQUIRED_STAGES:
        status = "pass" if gate == "pass" else "not_run"
        cases.append(
            {
                "stage": stage,
                "status": status,
                "case": {
                    "case_id": f"case-{stage}",
                    "run_id": run_id,
                    "tenant_id": f"fault-{stage}",
                    "session_id": f"session-{stage}",
                    "inbound_id": f"inbound-{stage}",
                    "message_id": f"message-{stage}",
                },
                "control_id": f"control-{stage}",
                "killed_container_id": "worker-container-verified",
                "markers": [
                    {"name": marker, "status": "pass", "observed_at": "now"}
                    for marker in FAULT_STAGE_REQUIRED_MARKERS[stage]
                ]
                if status == "pass"
                else [{"name": "case.planned", "status": "not_run"}],
            }
        )
    report = {
        "schema_version": 1,
        "mode": "fault_stage_acceptance",
        "run_id": run_id,
        "started_at": "2026-08-24T00:00:00Z",
        "ended_at": "2026-08-24T00:00:01Z",
        "gate": gate,
        "production_gate": gate,
        "cases": cases,
    }
    if provenance is not None:
        report["execution_provenance"] = provenance
        report["run_nonce_sha256"] = provenance.get("nonce_sha256")
    return report


def test_fault_stage_worker_image_attestation_requires_one_current_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = "trpc-fault-test-run"
    source = "a" * 64
    image = "sha256:" + "b" * 64
    worker_ids = tuple(f"worker-{index}" for index in range(1, 5))

    monkeypatch.setattr(
        "scripts.fault_injection_gate._runtime_worker_ids",
        lambda _project: worker_ids,
    )
    monkeypatch.setattr(
        "scripts.fault_injection_gate._runtime_worker_inspect",
        lambda worker_id: {
            "project": project,
            "service": "worker",
            "status": "running",
            "health": "healthy",
            "source_fingerprint": source,
            "image_id": image,
            "worker_id": worker_id,
        },
    )

    result = _fault_stage_worker_image_attestation(project, worker_ids[0], source)

    assert result == {
        "status": "pass",
        "worker_count": 4,
        "image_count": 1,
        "image_id": image,
        "source_fingerprint": source,
        "source_fingerprint_matches": True,
    }


def test_fault_stage_worker_image_attestation_rejects_stale_or_mixed_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = "trpc-fault-test-run"
    source = "a" * 64
    image = "sha256:" + "b" * 64
    worker_ids = tuple(f"worker-{index}" for index in range(1, 5))
    inspected = {
        worker_id: {
            "project": project,
            "service": "worker",
            "status": "running",
            "health": "healthy",
            "source_fingerprint": source,
            "image_id": image,
            "worker_id": worker_id,
        }
        for worker_id in worker_ids
    }

    monkeypatch.setattr(
        "scripts.fault_injection_gate._runtime_worker_ids",
        lambda _project: worker_ids,
    )
    monkeypatch.setattr(
        "scripts.fault_injection_gate._runtime_worker_inspect",
        lambda worker_id: inspected[worker_id],
    )

    inspected[worker_ids[0]]["source_fingerprint"] = "c" * 64
    stale = _fault_stage_worker_image_attestation(project, worker_ids[0], source)
    assert stale["status"] == "not_run"
    assert "mismatched" in stale["reason"]

    inspected[worker_ids[0]]["source_fingerprint"] = source
    inspected[worker_ids[-1]]["image_id"] = "sha256:" + "d" * 64
    mixed = _fault_stage_worker_image_attestation(project, worker_ids[0], source)
    assert mixed["status"] == "not_run"
    assert "mixed" in mixed["reason"]


def test_fault_stage_child_runs_once_and_maps_all_three_exact_cases(monkeypatch, tmp_path) -> None:
    output = tmp_path / "fault-injection.json"
    args = _fault_stage_args(output)
    monkeypatch.setenv("TRPC_FAULT_RUN_ID", "run-fault-stage-test")
    calls = []

    def fake_run(command, *, env, **_kwargs):
        calls.append(command)
        child_output = command[command.index("--output") + 1]
        with open(child_output, "w", encoding="utf-8") as handle:
            json.dump(
                _fault_stage_child(
                    env["TRPC_FAULT_RUN_ID"],
                    provenance={
                        "schema_version": 1,
                        "run_id": env["TRPC_FAULT_RUN_ID"],
                        "project": env["TRPC_FAULT_PROJECT"],
                        "worker_container": env["TRPC_FAULT_WORKER_CONTAINER"],
                        "scheduler_version": env["TRPC_FAULT_SCHEDULER_VERSION"],
                        "redis_stream": env["TRPC_FAULT_REDIS_STREAM"],
                        "redis_group": env["TRPC_FAULT_REDIS_GROUP"],
                        "nonce_sha256": hashlib.sha256(
                            env["TRPC_FAULT_EVIDENCE_NONCE"].encode()
                        ).hexdigest(),
                        "pid": 1234,
                    },
                ),
                handle,
            )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_fault_stage_acceptance(args)

    assert len(calls) == 1
    command = calls[0]
    assert Path(command[1]).name == "fault_stage_acceptance.py"
    assert command[command.index("--project") + 1] == "trpc-fault-test-run"
    assert command[command.index("--worker-container") + 1] == "worker-container-verified"
    assert "--allow-process-kill" in command
    assert set(result) == {"worker_enqueue", "worker_tool", "worker_commit"}
    assert all(item["status"] == "pass" for item in result.values())
    assert result["worker_tool"]["stage_markers"][0]["name"] == ("preflight.workers_verified")


def test_fault_stage_child_preserves_completed_case_when_later_case_is_not_run(
    monkeypatch, tmp_path
) -> None:
    output = tmp_path / "fault-injection.json"
    args = _fault_stage_args(output)
    monkeypatch.setenv("TRPC_FAULT_RUN_ID", "run-fault-stage-partial")

    def fake_run(command, *, env, **_kwargs):
        child_output = command[command.index("--output") + 1]
        provenance = {
            "schema_version": 1,
            "run_id": env["TRPC_FAULT_RUN_ID"],
            "project": env["TRPC_FAULT_PROJECT"],
            "worker_container": env["TRPC_FAULT_WORKER_CONTAINER"],
            "scheduler_version": env["TRPC_FAULT_SCHEDULER_VERSION"],
            "redis_stream": env["TRPC_FAULT_REDIS_STREAM"],
            "redis_group": env["TRPC_FAULT_REDIS_GROUP"],
            "nonce_sha256": hashlib.sha256(env["TRPC_FAULT_EVIDENCE_NONCE"].encode()).hexdigest(),
            "pid": 1234,
        }
        child = _fault_stage_child(env["TRPC_FAULT_RUN_ID"], gate="not_run", provenance=provenance)
        child["cases"][0]["status"] = "pass"
        child["cases"][0]["markers"] = [
            {"name": marker, "status": "pass", "observed_at": "now"}
            for marker in FAULT_STAGE_REQUIRED_MARKERS["enqueue"]
        ]
        child["cases"][0]["reason"] = None
        child["cases"][1]["reason"] = "lease takeover was not observed before the hard timeout"
        child["cases"][2]["reason"] = "lease takeover was not observed before the hard timeout"
        with open(child_output, "w", encoding="utf-8") as handle:
            json.dump(child, handle)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_fault_stage_acceptance(args)

    assert result["worker_enqueue"]["status"] == "pass"
    assert result["worker_tool"]["status"] == "not_run"
    assert result["worker_commit"]["status"] == "not_run"
    assert "lease takeover" in result["worker_tool"]["reason"]


def test_fault_stage_acceptance_attaches_current_image_attestation(monkeypatch, tmp_path) -> None:
    output = tmp_path / "fault-injection.json"
    args = _fault_stage_args(output, scenario="worker_enqueue")
    source = "a" * 64
    image = "sha256:" + "b" * 64
    monkeypatch.setenv("TRPC_FAULT_RUN_ID", "run-fault-stage-attestation")
    attestation_calls = []

    def fake_attestation(project, container, expected_source):
        attestation_calls.append((project, container, expected_source))
        return {
            "status": "pass",
            "worker_count": 4,
            "image_count": 1,
            "image_id": image,
            "source_fingerprint": expected_source,
            "source_fingerprint_matches": True,
        }

    def fake_run(command, *, env, **_kwargs):
        child_output = command[command.index("--output") + 1]
        child = _fault_stage_child(
            env["TRPC_FAULT_RUN_ID"],
            provenance={
                "schema_version": 1,
                "run_id": env["TRPC_FAULT_RUN_ID"],
                "project": env["TRPC_FAULT_PROJECT"],
                "worker_container": env["TRPC_FAULT_WORKER_CONTAINER"],
                "scheduler_version": env["TRPC_FAULT_SCHEDULER_VERSION"],
                "redis_stream": env["TRPC_FAULT_REDIS_STREAM"],
                "redis_group": env["TRPC_FAULT_REDIS_GROUP"],
                "nonce_sha256": hashlib.sha256(
                    env["TRPC_FAULT_EVIDENCE_NONCE"].encode()
                ).hexdigest(),
                "pid": 1234,
            },
        )
        child["worker_preflight"] = {
            "status": "pass",
            "worker_count": 4,
            "healthy_worker_count": 4,
            "independent_processes": True,
            "positive_pid_count": 4,
        }
        with open(child_output, "w", encoding="utf-8") as handle:
            json.dump(child, handle)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(
        "scripts.fault_injection_gate._fault_stage_worker_image_attestation",
        fake_attestation,
    )
    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)

    result = _run_fault_stage_acceptance(
        args,
        expected_source_fingerprint={"value": source},
    )

    assert attestation_calls == [("trpc-fault-test-run", "worker-container-verified", source)]
    preflight = result["worker_enqueue"]["worker_preflight"]
    assert preflight["image_id"] == image
    assert preflight["source_fingerprint"] == source
    assert preflight["image_attestation"]["source_fingerprint_matches"] is True


def test_fault_stage_child_passes_selected_scenario_and_maps_one_case(
    monkeypatch, tmp_path
) -> None:
    output = tmp_path / "fault-injection.json"
    args = _fault_stage_args(output, scenario="worker_enqueue")
    monkeypatch.setenv("TRPC_FAULT_RUN_ID", "run-fault-stage-selected")
    calls = []

    def fake_run(command, *, env, **_kwargs):
        calls.append(command)
        assert command[command.index("--scenario") + 1] == "worker_enqueue"
        child_output = command[command.index("--output") + 1]
        child = _fault_stage_child(
            env["TRPC_FAULT_RUN_ID"],
            provenance={
                "schema_version": 1,
                "run_id": env["TRPC_FAULT_RUN_ID"],
                "project": env["TRPC_FAULT_PROJECT"],
                "worker_container": env["TRPC_FAULT_WORKER_CONTAINER"],
                "scheduler_version": env["TRPC_FAULT_SCHEDULER_VERSION"],
                "redis_stream": env["TRPC_FAULT_REDIS_STREAM"],
                "redis_group": env["TRPC_FAULT_REDIS_GROUP"],
                "nonce_sha256": hashlib.sha256(
                    env["TRPC_FAULT_EVIDENCE_NONCE"].encode()
                ).hexdigest(),
                "pid": 1234,
            },
        )
        child["cases"] = [child["cases"][0]]
        child["requested_stages"] = ["enqueue"]
        child["production_gate"] = "not_run"
        with open(child_output, "w", encoding="utf-8") as handle:
            json.dump(child, handle)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_fault_stage_acceptance(args)

    assert len(calls) == 1
    assert set(result) == {"worker_enqueue"}
    assert result["worker_enqueue"]["status"] == "pass"


def test_fault_stage_timeout_attempts_bounded_worker_restore(monkeypatch, tmp_path) -> None:
    output = tmp_path / "fault-injection.json"
    args = _fault_stage_args(output, scenario="worker_enqueue")
    monkeypatch.setenv("TRPC_FAULT_RUN_ID", "run-fault-stage-restore")
    calls: list[list[str]] = []
    started_ids: set[str] = set()

    def fake_run(command, **_kwargs):
        calls.append(command)
        if command[1:3] == ["ps", "-aq"]:
            return SimpleNamespace(
                returncode=0,
                stdout="worker-container-verified\nworker-container-2\nworker-container-3\nworker-container-4\n",
            )
        if command[1] == "inspect":
            container_id = command[-1]
            labels = {
                "com.docker.compose.project": "trpc-fault-test-run",
                "com.docker.compose.service": "worker",
            }
            status = "running" if container_id in started_ids else "exited"
            health = "healthy" if container_id in started_ids else "unhealthy"
            return SimpleNamespace(
                returncode=0,
                stdout=f"{json.dumps(labels)}\tsha256:{'a' * 64}\t{status}\t{health}\n",
            )
        if command[1] == "start":
            started_ids.add(command[2])
            return SimpleNamespace(returncode=0, stdout="worker-container-verified\n")
        raise subprocess.TimeoutExpired(cmd=command, timeout=1)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_fault_stage_acceptance(args)

    assert result["worker_enqueue"]["status"] == "fail"
    assert "timed out" in result["worker_enqueue"]["reason"]
    assert any(command[1] == "start" for command in calls)


def test_fault_stage_nonzero_exit_attempts_bounded_worker_restore(monkeypatch, tmp_path) -> None:
    output = tmp_path / "fault-injection.json"
    args = _fault_stage_args(output, scenario="worker_enqueue")
    monkeypatch.setenv("TRPC_FAULT_RUN_ID", "run-fault-stage-nonzero")
    calls: list[list[str]] = []
    started_ids: set[str] = set()

    def fake_run(command, **_kwargs):
        calls.append(command)
        if command[1].endswith("fault_stage_acceptance.py"):
            return SimpleNamespace(returncode=3)
        if command[1:3] == ["ps", "-aq"]:
            return SimpleNamespace(
                returncode=0,
                stdout="worker-container-verified\nworker-container-2\nworker-container-3\nworker-container-4\n",
            )
        if command[1] == "inspect":
            container_id = command[-1]
            labels = {
                "com.docker.compose.project": "trpc-fault-test-run",
                "com.docker.compose.service": "worker",
            }
            status = "running" if container_id in started_ids else "exited"
            health = "healthy" if container_id in started_ids else "unhealthy"
            return SimpleNamespace(
                returncode=0,
                stdout=f"{json.dumps(labels)}\tsha256:{'a' * 64}\t{status}\t{health}\n",
            )
        if command[1] == "start":
            started_ids.add(command[2])
            return SimpleNamespace(returncode=0, stdout="worker-container-verified\n")
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_fault_stage_acceptance(args)

    assert result["worker_enqueue"]["status"] == "fail"
    assert "exited with code 3" in result["worker_enqueue"]["reason"]
    assert "worker containers were restored" in result["worker_enqueue"]["reason"]
    assert any(command[1] == "start" for command in calls)


def test_fault_stage_child_invalid_schema_fails_closed(monkeypatch, tmp_path) -> None:
    output = tmp_path / "fault-injection.json"
    args = _fault_stage_args(output)
    monkeypatch.setenv("TRPC_FAULT_RUN_ID", "run-fault-stage-invalid")

    def fake_run(command, **_kwargs):
        child_output = command[command.index("--output") + 1]
        with open(child_output, "w", encoding="utf-8") as handle:
            json.dump({"schema_version": 0, "mode": "deterministic_contract"}, handle)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_fault_stage_acceptance(args)

    assert all(item["status"] == "fail" for item in result.values())
    assert all("schema_version" in item["reason"] for item in result.values())


def test_fault_stage_child_foreign_provenance_fails_closed(monkeypatch, tmp_path) -> None:
    output = tmp_path / "fault-injection.json"
    args = _fault_stage_args(output)
    monkeypatch.setenv("TRPC_FAULT_RUN_ID", "run-fault-stage-provenance")

    def fake_run(command, *, env, **_kwargs):
        child_output = command[command.index("--output") + 1]
        provenance = {
            "schema_version": 1,
            "run_id": env["TRPC_FAULT_RUN_ID"],
            "project": env["TRPC_FAULT_PROJECT"],
            "worker_container": env["TRPC_FAULT_WORKER_CONTAINER"],
            "scheduler_version": env["TRPC_FAULT_SCHEDULER_VERSION"],
            "redis_stream": env["TRPC_FAULT_REDIS_STREAM"],
            "redis_group": env["TRPC_FAULT_REDIS_GROUP"],
            "nonce_sha256": "foreign-report",
            "pid": 1234,
        }
        with open(child_output, "w", encoding="utf-8") as handle:
            json.dump(
                _fault_stage_child(env["TRPC_FAULT_RUN_ID"], provenance=provenance),
                handle,
            )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_fault_stage_acceptance(args)

    assert all(item["status"] == "fail" for item in result.values())
    assert all("nonce_sha256" in item["reason"] for item in result.values())


def test_fault_stage_child_timeout_fails_closed(monkeypatch, tmp_path) -> None:
    output = tmp_path / "fault-injection.json"
    args = _fault_stage_args(output)
    monkeypatch.setenv("TRPC_FAULT_RUN_ID", "run-fault-stage-timeout")

    def fake_run(_command, **_kwargs):
        raise subprocess.TimeoutExpired(cmd="fault-stage", timeout=1)

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", fake_run)
    result = _run_fault_stage_acceptance(args)

    assert all(item["status"] == "fail" for item in result.values())
    assert all("timed out" in item["reason"] for item in result.values())


def test_fault_stage_child_not_run_report_does_not_start_process_without_selectors(
    monkeypatch, tmp_path
) -> None:
    output = tmp_path / "fault-injection.json"
    args = _fault_stage_args(output, fault_project=None, fault_worker_container=None)
    monkeypatch.setenv("TRPC_FAULT_RUN_ID", "run-fault-stage-not-run")
    called = False

    def unexpected_run(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("fault-stage child must not start without explicit selectors")

    monkeypatch.setattr("scripts.fault_injection_gate.subprocess.run", unexpected_run)
    result = _run_fault_stage_acceptance(args)

    assert called is False
    assert all(item["status"] == "not_run" for item in result.values())
