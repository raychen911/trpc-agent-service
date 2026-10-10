from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.fault_stage_acceptance as fault_stage_acceptance
from scripts.fault_stage_acceptance import (
    AcceptanceEvidence,
    CaseIdentity,
    CaseStatus,
    ControlEvidence,
    FaultStageAcceptanceRunner,
    ProcessingEvidence,
    RecoveryEvidence,
    StageAcceptanceError,
    StageNotRun,
    StageResult,
    WorkerContainer,
    _execute,
    _parse_offline_agent_delay,
    _parser,
    _required_fault_stage_env,
    _validate_recovery,
    build_execution_report,
    build_not_run_report,
    validate_project,
    validate_worker_container,
)
from trpc_service.config.settings import SchedulerVersion
from trpc_service.faults import FaultStage, FaultStageEvent

PROJECT = "trpc-fault-test-run"


def _worker(container_id: str, worker_id: str) -> WorkerContainer:
    return WorkerContainer(
        container_id=container_id,
        project=PROJECT,
        service="worker",
        status="running",
        health="healthy",
        worker_id=worker_id,
    )


class FakeCompose:
    def __init__(self, *, target: WorkerContainer | None = None) -> None:
        target = target or _worker("worker-a", "worker-a")
        self.containers: dict[str, WorkerContainer] = {
            target.container_id: target,
            "worker-b": _worker("worker-b", "worker-b"),
        }
        self.calls: list[tuple[str, str]] = []
        self.target = target

    def inspect(self, container: str) -> WorkerContainer:
        return self.containers[container]

    def list_workers(self, project: str) -> tuple[WorkerContainer, ...]:
        assert project == PROJECT
        return tuple(self.containers.values())

    def stop(self, container_id: str) -> None:
        self.calls.append(("stop", container_id))
        old = self.containers[container_id]
        self.containers[container_id] = WorkerContainer(
            container_id=old.container_id,
            project=old.project,
            service=old.service,
            status="exited",
            health=None,
            worker_id=old.worker_id,
            pid=old.pid,
        )

    def start(self, container_id: str) -> None:
        self.calls.append(("start", container_id))
        old = self.containers[container_id]
        self.containers[container_id] = WorkerContainer(
            container_id=old.container_id,
            project=old.project,
            service=old.service,
            status="running",
            health="healthy",
            worker_id=old.worker_id,
            pid=old.pid,
        )

    def terminate(self, container_id: str, *, mode: str) -> None:
        self.calls.append((mode, container_id))
        old = self.containers[container_id]
        self.containers[container_id] = WorkerContainer(
            container_id=old.container_id,
            project=old.project,
            service=old.service,
            status="exited",
            health=None,
            worker_id=old.worker_id,
            pid=old.pid,
        )


class FourWorkerCompose(FakeCompose):
    def __init__(self) -> None:
        super().__init__()
        self.containers.update(
            {
                "worker-c": _worker("worker-c", "worker-c"),
                "worker-d": _worker("worker-d", "worker-d"),
            }
        )
        for pid, container_id in enumerate(self.containers, start=1):
            worker = self.containers[container_id]
            self.containers[container_id] = WorkerContainer(
                container_id=worker.container_id,
                project=worker.project,
                service=worker.service,
                status=worker.status,
                health=worker.health,
                worker_id=worker.worker_id,
                pid=pid,
            )


class FakeController:
    def __init__(self, backend: FakeBackend) -> None:
        self.backend = backend
        self.events: list[FaultStageEvent] = []
        self.releases: list[str] = []
        self.release_result = True

    async def arm(self, event: FaultStageEvent) -> str:
        self.events.append(event)
        self.backend.event = event
        return f"control-{len(self.events)}"

    async def release(self, control_id: str, *, tenant_id: str) -> bool:
        assert tenant_id.startswith("fault-")
        self.releases.append(control_id)
        return self.release_result


class FakeBackend:
    def __init__(self, compose: FakeCompose) -> None:
        self.compose = compose
        self.event: FaultStageEvent | None = None
        self.cleaned: list[CaseIdentity] = []
        self.accepted: list[CaseIdentity] = []
        self.recovery_stale_rejected = True
        self.marker_mismatch = False
        self.processing_owner = "worker-a"
        self.omit_execution_key = False

    async def provision_case(self, case: CaseIdentity) -> None:
        assert case.tenant_id.startswith("fault-")

    async def accept_and_publish(self, case: CaseIdentity) -> AcceptanceEvidence:
        self.accepted.append(case)
        return AcceptanceEvidence(
            case.inbound_id,
            f"stream-{case.case_id}",
            session_id=case.session_id,
            outbox_id=f"outbox-{case.case_id}",
            generation=1,
        )

    async def observe_processing(
        self,
        case: CaseIdentity,
        accepted: AcceptanceEvidence,
        *,
        require_execution_key: bool = False,
    ) -> ProcessingEvidence:
        return ProcessingEvidence(
            inbound_id=accepted.inbound_id,
            session_id=case.session_id,
            turn_id=f"turn-{case.case_id}",
            worker_id=self.processing_owner,
            fencing_token=7,
            execution_key=(
                None
                if self.omit_execution_key
                else (f"execution-{case.case_id}" if require_execution_key else None)
            ),
        )

    async def read_control(self, tenant_id: str, control_id: str) -> ControlEvidence:
        assert tenant_id.startswith("fault-")
        assert self.event is not None
        event = self.event
        return ControlEvidence(
            control_id=control_id,
            status="entered",
            marker_status="entered",
            marker_worker_id=event.worker_id,
            marker_inbound_id=event.inbound_id,
            marker_turn_id=event.turn_id if not self.marker_mismatch else "wrong-turn",
            marker_execution_key=event.execution_key,
            marker_stream_id=event.stream_id,
            marker_fencing_token=event.fencing_token,
        )

    async def wait_for_recovery(
        self,
        case: CaseIdentity,
        *,
        old_worker_id: str,
        old_fencing_token: int,
        killed_container_id: str,
    ) -> RecoveryEvidence:
        assert case.tenant_id.startswith("fault-")
        assert old_worker_id
        assert old_fencing_token >= 0
        # The real backend returns survivor inventory after the recovery turn
        # has completed.  Wait for the concurrently started fake survivor too,
        # so the fixture does not manufacture an invalid stopped-survivor
        # evidence snapshot merely because the observer starts before health.
        while not any(  # noqa: ASYNC110 - yield until the concurrent survivor start completes
            key != killed_container_id and value.status == "running" and value.health == "healthy"
            for key, value in self.compose.containers.items()
        ):
            await asyncio.sleep(0)
        survivors = tuple(
            value for key, value in self.compose.containers.items() if key != killed_container_id
        )
        return RecoveryEvidence(
            owner_worker_id="worker-b",
            lease_epoch=old_fencing_token + 1,
            survivors=survivors,
            turn_count=1,
            sequences=(1, 2, 3),
            stale_token_rejected=self.recovery_stale_rejected,
        )

    async def verify_final(self, case: CaseIdentity, recovery: RecoveryEvidence) -> None:
        assert case.inbound_id
        assert recovery.turn_count == 1

    async def cleanup_case(self, case: CaseIdentity) -> None:
        self.cleaned.append(case)


def _runner(
    compose: FakeCompose,
    backend: FakeBackend,
    *,
    allow_process_kill: bool = True,
    controller: FakeController | None = None,
    run_id: str | None = None,
) -> FaultStageAcceptanceRunner:
    controller = controller or FakeController(backend)
    return FaultStageAcceptanceRunner(
        compose=compose,
        backend=backend,
        controller=controller,
        project=PROJECT,
        worker_container="worker-a",
        allow_process_kill=allow_process_kill,
        run_id=run_id,
        timeout_seconds=0.2,
        sleeper=lambda _delay: asyncio.sleep(0),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("observation", ("healthy", "terminated"))
async def test_docker_inspect_polling_is_bounded_and_backed_off(observation: str) -> None:
    class InspectCompose:
        def __init__(self) -> None:
            self.inspect_count = 0

        def inspect(self, container_id: str) -> WorkerContainer:
            self.inspect_count += 1
            ready = self.inspect_count >= (3 if observation == "healthy" else 2)
            if observation == "healthy":
                status, health = "running", "healthy" if ready else "starting"
            else:
                status, health = ("exited", None) if ready else ("running", "healthy")
            return WorkerContainer(
                container_id=container_id,
                project=PROJECT,
                service="worker",
                status=status,
                health=health,
                worker_id="worker-a",
            )

        def start(self, _container_id: str) -> None:
            return None

    compose = InspectCompose()
    clock = 0.0
    sleeps: list[float] = []

    def monotonic() -> float:
        return clock

    async def sleeper(delay: float) -> None:
        nonlocal clock
        sleeps.append(delay)
        clock += delay

    runner = FaultStageAcceptanceRunner(
        compose=compose,
        backend=object(),
        controller=object(),
        project=PROJECT,
        worker_container="worker-a",
        timeout_seconds=5.0,
        sleeper=sleeper,
        monotonic=monotonic,
    )

    if observation == "healthy":
        await runner._start_and_wait("worker-a", "worker-a")
    else:
        result = await runner._wait_for_terminated("worker-a", expected_worker_id="worker-a")
        assert result.status == "exited"

    assert sleeps
    assert sleeps[0] >= 0.5
    assert sleeps == sorted(sleeps)
    assert all(0.5 <= delay <= 2.0 for delay in sleeps)


def test_default_report_is_not_run_and_cases_are_unique(tmp_path: Path) -> None:
    report = build_not_run_report(reason="offline by default")
    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
    cases = report["cases"]
    assert [item["status"] for item in cases] == ["not_run", "not_run", "not_run"]
    assert cases[0]["case"]["case_id"] != cases[1]["case"]["case_id"]
    output = tmp_path / "report.json"
    output.write_text(json.dumps(report), encoding="utf-8")
    assert json.loads(output.read_text(encoding="utf-8"))["gate"] == "not_run"


def test_fault_stage_cli_timeout_defaults_to_ninety_seconds() -> None:
    args = _parser().parse_args([])
    assert args.timeout_seconds == 90.0
    assert args.scheduler_version == "v2"
    assert args.scenario == "all"


def test_fault_stage_case_tenant_is_bound_to_run_id() -> None:
    case = CaseIdentity.create(FaultStage.ENQUEUE, run_id="run-selection")
    assert case.tenant_id.startswith("fault-run-selection-")


@pytest.mark.asyncio
async def test_fault_stage_runner_executes_only_selected_stage() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    results = await _runner(compose, backend).run_all((FaultStage.ENQUEUE,))
    assert [result.stage for result in results] == [FaultStage.ENQUEUE]
    assert backend.cleaned


@pytest.mark.asyncio
async def test_fault_stage_all_cases_reconciles_four_workers_between_cases() -> None:
    compose = FourWorkerCompose()

    class RunningSurvivorBackend(FakeBackend):
        async def wait_for_recovery(self, *args, **kwargs) -> RecoveryEvidence:
            recovery = await super().wait_for_recovery(*args, **kwargs)
            return RecoveryEvidence(
                owner_worker_id=recovery.owner_worker_id,
                lease_epoch=recovery.lease_epoch,
                survivors=tuple(
                    worker
                    for worker in recovery.survivors
                    if worker.status == "running" and worker.health == "healthy"
                ),
                turn_count=recovery.turn_count,
                sequences=recovery.sequences,
                stale_token_rejected=recovery.stale_token_rejected,
            )

    backend = RunningSurvivorBackend(compose)
    results = await _runner(compose, backend).run_all(
        (FaultStage.ENQUEUE, FaultStage.TOOL, FaultStage.COMMIT_TXN_OPEN)
    )

    assert [result.status for result in results] == [CaseStatus.PASS] * 3
    assert all(
        worker.status == "running" and worker.health == "healthy"
        for worker in compose.containers.values()
    )


@pytest.mark.asyncio
async def test_fault_stage_retries_transient_empty_worker_inventory() -> None:
    class TransientEmptyInventoryCompose(FakeCompose):
        def __init__(self) -> None:
            super().__init__()
            self.inventory_calls = 0

        def list_workers(self, project: str) -> tuple[WorkerContainer, ...]:
            self.inventory_calls += 1
            if self.inventory_calls == 1:
                return ()
            return super().list_workers(project)

    compose = TransientEmptyInventoryCompose()
    backend = FakeBackend(compose)
    result = await _runner(compose, backend).run_case(FaultStage.ENQUEUE)

    assert result.status == CaseStatus.PASS
    assert compose.inventory_calls >= 2


@pytest.mark.asyncio
async def test_fault_stage_persistent_empty_worker_inventory_fails_closed() -> None:
    class EmptyInventoryCompose(FakeCompose):
        def list_workers(self, project: str) -> tuple[WorkerContainer, ...]:
            assert project == PROJECT
            return ()

    compose = EmptyInventoryCompose()
    backend = FakeBackend(compose)
    runner = FaultStageAcceptanceRunner(
        compose=compose,
        backend=backend,
        controller=FakeController(backend),
        project=PROJECT,
        worker_container="worker-a",
        allow_process_kill=True,
        timeout_seconds=0.01,
        sleeper=lambda _delay: asyncio.sleep(0),
    )
    result = await runner.run_case(FaultStage.ENQUEUE)

    assert result.status == CaseStatus.NOT_RUN
    assert result.reason == "no worker containers were observed"
    assert compose.calls == []
    assert backend.cleaned


@pytest.mark.asyncio
async def test_fault_stage_cleanup_retries_transient_failure() -> None:
    class TransientCleanupBackend(FakeBackend):
        def __init__(self, compose: FakeCompose) -> None:
            super().__init__(compose)
            self.cleanup_attempts = 0

        async def cleanup_case(self, case: CaseIdentity) -> None:
            self.cleanup_attempts += 1
            if self.cleanup_attempts == 1:
                raise RuntimeError("transient database race")
            await super().cleanup_case(case)

    compose = FakeCompose()
    backend = TransientCleanupBackend(compose)
    result = await _runner(compose, backend).run_case(FaultStage.ENQUEUE)
    assert result.status == CaseStatus.PASS
    assert backend.cleanup_attempts == 2
    assert backend.cleaned == [result.case]


@pytest.mark.asyncio
async def test_fault_stage_cleanup_persistent_failure_stays_failed() -> None:
    class PersistentCleanupBackend(FakeBackend):
        def __init__(self, compose: FakeCompose) -> None:
            super().__init__(compose)
            self.cleanup_attempts = 0

        async def cleanup_case(self, case: CaseIdentity) -> None:
            del case
            self.cleanup_attempts += 1
            raise RuntimeError("persistent database failure")

    compose = FakeCompose()
    backend = PersistentCleanupBackend(compose)
    result = await _runner(compose, backend).run_case(FaultStage.ENQUEUE)
    assert result.status == CaseStatus.FAIL
    assert result.reason == "test tenant cleanup failed"
    # One immediate attempt plus the three bounded backoff retries.
    assert backend.cleanup_attempts == 4
    # Cleanup is attempted before restoration, but restoration remains
    # mandatory even when every exact tenant delete attempt fails.
    assert ("start", "worker-a") in compose.calls


@pytest.mark.asyncio
async def test_fault_stage_cleanup_has_independent_bounded_timeout() -> None:
    class HangingCleanupBackend(FakeBackend):
        def __init__(self, compose: FakeCompose) -> None:
            super().__init__(compose)
            self.cleanup_attempts = 0

        async def cleanup_case(self, case: CaseIdentity) -> None:
            del case
            self.cleanup_attempts += 1
            await asyncio.sleep(10)

    compose = FakeCompose()
    backend = HangingCleanupBackend(compose)
    result = await _runner(compose, backend).run_case(FaultStage.ENQUEUE)

    assert result.status == CaseStatus.FAIL
    assert result.reason == "test tenant cleanup failed"
    assert backend.cleanup_attempts == 4
    assert ("start", "worker-a") in compose.calls


@pytest.mark.asyncio
async def test_fault_stage_cleans_tenant_before_restoring_terminated_worker() -> None:
    class OrderedCleanupBackend(FakeBackend):
        async def cleanup_case(self, case: CaseIdentity) -> None:
            self.compose.calls.append(("cleanup", case.case_id))
            await super().cleanup_case(case)

    compose = FakeCompose()
    backend = OrderedCleanupBackend(compose)
    result = await _runner(compose, backend).run_case(FaultStage.ENQUEUE)

    assert result.status == CaseStatus.PASS
    cleanup_index = next(i for i, call in enumerate(compose.calls) if call[0] == "cleanup")
    kill_index = next(i for i, call in enumerate(compose.calls) if call == ("kill", "worker-a"))
    target_restore_indices = [
        i for i, call in enumerate(compose.calls) if call == ("start", "worker-a")
    ]
    assert len(target_restore_indices) >= 2
    assert kill_index < cleanup_index < target_restore_indices[-1]


def test_scoped_execution_report_does_not_claim_production() -> None:
    case = CaseIdentity.create(FaultStage.ENQUEUE, run_id="run-selection")
    report = build_execution_report(
        (StageResult(FaultStage.ENQUEUE, CaseStatus.PASS, case),),
        required_stages=(FaultStage.ENQUEUE,),
    )
    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert report["requested_stages"] == ["enqueue"]
    assert [item["stage"] for item in report["cases"]] == ["enqueue"]


def test_scheduler_transport_is_version_pinned() -> None:
    assert fault_stage_acceptance._scheduler_transport(SchedulerVersion.V2) == (
        "trpc:session-ready:v2",
        "trpc-session-ready-v2",
    )
    assert fault_stage_acceptance._scheduler_transport(SchedulerVersion.V1) == (
        "trpc:inbound:v1",
        "trpc-workers-v1",
    )
    with pytest.raises(StageNotRun, match="stream/group"):
        fault_stage_acceptance.PostgresRuntimeStageBackend(
            object(),
            object(),
            compose=object(),
            project=PROJECT,
            routing_key=b"r" * 32,
            binding_seed="binding",
            scheduler_version=SchedulerVersion.V2,
            stream="trpc:inbound:v1",
            group="trpc-workers-v1",
            offline_agent_delay_seconds=1,
        )


def test_worker_transport_must_match_scheduler() -> None:
    worker = WorkerContainer(
        "worker-a",
        PROJECT,
        "worker",
        "running",
        "healthy",
        "worker-a",
        "v2",
        "trpc:session-ready:v2",
        "trpc-session-ready-v2",
    )
    fault_stage_acceptance.validate_worker_transport(
        worker,
        scheduler_version=SchedulerVersion.V2,
        stream="trpc:session-ready:v2",
        group="trpc-session-ready-v2",
    )
    with pytest.raises(StageNotRun, match="scheduler transport"):
        fault_stage_acceptance.validate_worker_transport(
            worker,
            scheduler_version=SchedulerVersion.V1,
            stream="trpc:inbound:v1",
            group="trpc-workers-v1",
        )


def test_fault_stage_compose_override_matches_recovery_timing() -> None:
    override = Path("deploy/fault-stage-runtime.override.yml").read_text(encoding="utf-8")
    assert 'restart: "no"' in override
    assert 'TRPC_SERVICE_LEASE_SECONDS: "10"' in override
    assert 'TRPC_SERVICE_REDIS_RECLAIM_AFTER_MS: "1000"' in override
    assert 'TRPC_SERVICE_WORKER_POLL_SECONDS: "0.05"' in override


@pytest.mark.asyncio
async def test_fault_stage_cli_rejects_short_timeout_before_runtime_access() -> None:
    args = _parser().parse_args(["--execute", "--timeout-seconds", "29"])
    report = await _execute(args)
    assert report["gate"] == "not_run"
    assert "between 30 and 600" in report["reason"]


def test_production_gate_requires_both_real_cases() -> None:
    first = CaseIdentity.create(FaultStage.ENQUEUE)
    second = CaseIdentity.create(FaultStage.COMMIT_TXN_OPEN)
    not_run = build_execution_report(
        (
            # Constructing results directly keeps this aggregation test
            # independent from the Docker/PG fakes below.
            StageResult(FaultStage.ENQUEUE, CaseStatus.PASS, first),
            StageResult(FaultStage.COMMIT_TXN_OPEN, CaseStatus.NOT_RUN, second),
        )
    )
    assert not_run["production_gate"] == "not_run"


@pytest.mark.asyncio
async def test_runtime_case_report_uses_fixed_fault_run_id() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    result = await _runner(compose, backend, run_id="TRPC-FAULT-RUN").run_case(FaultStage.ENQUEUE)
    assert result.case.run_id == "TRPC-FAULT-RUN"


def test_project_and_worker_guardrails_fail_closed() -> None:
    with pytest.raises(StageNotRun):
        validate_project("trpc-agent-service")
    with pytest.raises(StageAcceptanceError):
        validate_worker_container(
            _worker("worker-a", "worker-a"),
            project=PROJECT,
            explicit_container="worker-b",
        )
    with pytest.raises(StageAcceptanceError):
        validate_worker_container(
            WorkerContainer("worker-a", "other-project", "worker", "running", "healthy", "w"),
            project=PROJECT,
            explicit_container="worker-a",
        )
    with pytest.raises(StageNotRun):
        validate_worker_container(
            WorkerContainer("worker-a", PROJECT, "worker", "running", None, "w"),
            project=PROJECT,
            explicit_container="worker-a",
        )
    with pytest.raises(StageNotRun, match="<=63"):
        validate_project("trpc-fault-" + "a" * 64)
    with pytest.raises(StageNotRun, match="container"):
        fault_stage_acceptance._validate_container_selector("worker/a")


def test_docker_compose_controller_lists_full_container_ids(monkeypatch) -> None:
    container_id = "a" * 64
    controller = fault_stage_acceptance.DockerComposeController(project=PROJECT)
    calls: list[tuple[str, ...]] = []
    payload = {
        "Id": container_id,
        "Config": {
            "Labels": {
                "com.docker.compose.project": PROJECT,
                "com.docker.compose.service": "worker",
            },
            "Env": [],
            "Hostname": "fault-worker",
        },
        "State": {"Status": "running", "Health": {"Status": "healthy"}, "Pid": 123},
    }

    def fake_run(*args: str) -> str:
        calls.append(args)
        if args[1] == "ps":
            return container_id + "\n"
        if args[1] == "inspect":
            assert args[-1] == container_id
            return json.dumps(payload)
        raise AssertionError(args)

    monkeypatch.setattr(controller, "_run", fake_run)

    workers = controller.list_workers(PROJECT)

    assert workers[0].container_id == container_id
    assert calls[0][1:4] == ("ps", "-a", "--no-trunc")


@pytest.mark.asyncio
async def test_v2_enqueue_without_claim_checkpoint_is_not_run_without_side_effect() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    backend.scheduler_version = SchedulerVersion.V2
    backend.supports_enqueue_checkpoint = False
    result = await _runner(compose, backend).run_case(FaultStage.ENQUEUE)
    assert result.status == CaseStatus.NOT_RUN
    assert "claim-before checkpoint" in (result.reason or "")
    assert compose.calls == []
    assert backend.cleaned


@pytest.mark.asyncio
async def test_v2_processing_requires_explicit_ack_evidence_before_kill() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    backend.scheduler_version = SchedulerVersion.V2
    backend.supports_enqueue_checkpoint = True
    result = await _runner(compose, backend).run_case(FaultStage.COMMIT_TXN_OPEN)
    assert result.status == CaseStatus.NOT_RUN
    assert "ACK before execution" in (result.reason or "")
    assert not any(call[0] == "kill" for call in compose.calls)


@pytest.mark.asyncio
async def test_v2_exact_ack_probe_requires_empty_pel_and_matching_payload() -> None:
    class RedisDouble:
        def __init__(self, pending: bool) -> None:
            self.pending = pending
            self.xrange_calls = 0

        async def xpending_range(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            return ["pending"] if self.pending else []

        async def xrange(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            self.xrange_calls += 1
            return [
                (
                    "42-0",
                    {
                        "event_id": "event-case",
                        "tenant_id": "fault-case",
                        "session_id": "session-case",
                        "generation": "1",
                        "priority": "0",
                        "trace_id": "trace-case",
                        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    },
                )
            ]

    state = SimpleNamespace(
        case=SimpleNamespace(tenant_id="fault-case", session_id="session-case"),
        accepted=AcceptanceEvidence(
            inbound_id="inbound-case",
            stream_id="42-0",
            session_id="session-case",
            outbox_id="event-case",
            generation=1,
        ),
    )
    for pending, expected in ((True, False), (False, True)):
        redis = RedisDouble(pending)
        backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
        backend._redis = redis
        backend._queue = SimpleNamespace(
            stream="trpc:session-ready:v2", group="trpc-session-ready-v2"
        )
        backend._scheduler_version = SchedulerVersion.V2
        assert await backend._v2_delivery_acknowledged(state, state.accepted) is expected
        assert redis.xrange_calls == (0 if pending else 1)


@pytest.mark.asyncio
async def test_v2_exact_ack_probe_accepts_xack_xdel_absent_entry() -> None:
    class RedisDouble:
        async def xpending_range(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            return []

        async def xrange(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            return []

    state = SimpleNamespace(
        case=SimpleNamespace(tenant_id="fault-case", session_id="session-case"),
        accepted=AcceptanceEvidence(
            inbound_id="inbound-case",
            stream_id="42-0",
            session_id="session-case",
            outbox_id="event-case",
            generation=1,
        ),
    )
    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._redis = RedisDouble()
    backend._queue = SimpleNamespace(stream="trpc:session-ready:v2", group="trpc-session-ready-v2")
    backend._scheduler_version = SchedulerVersion.V2

    assert await backend._v2_delivery_acknowledged(state, state.accepted) is True


@pytest.mark.asyncio
async def test_termination_reinspects_worker_identity_before_destructive_call() -> None:
    class RecreatedCompose(FakeCompose):
        def __init__(self) -> None:
            super().__init__()
            self.inspect_calls = 0

        def inspect(self, container: str) -> WorkerContainer:
            self.inspect_calls += 1
            value = super().inspect(container)
            # ENQUEUE calls inspect for preflight, start readiness twice, then
            # once more immediately before terminate.  Simulate replacement
            # of the selected resource at that last boundary.
            if self.inspect_calls == 4:
                return WorkerContainer(
                    value.container_id,
                    value.project,
                    value.service,
                    value.status,
                    value.health,
                    "replacement-worker",
                )
            return value

    compose = RecreatedCompose()
    backend = FakeBackend(compose)
    result = await _runner(compose, backend).run_case(FaultStage.ENQUEUE)
    assert result.status == CaseStatus.FAIL
    assert "identity changed" in (result.reason or "")
    assert not any(call[0] == "kill" for call in compose.calls)


@pytest.mark.asyncio
async def test_enqueue_requires_exact_marker_before_termination() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    backend.marker_mismatch = True
    controller = FakeController(backend)
    result = await _runner(compose, backend, controller=controller).run_case(FaultStage.ENQUEUE)
    assert result.status == CaseStatus.FAIL
    assert "mismatched" in (result.reason or "")
    assert not any(call[0] == "kill" and call[1] == "worker-a" for call in compose.calls)
    assert backend.cleaned
    assert controller.releases == [result.control_id]


@pytest.mark.asyncio
async def test_enqueue_success_starts_only_survivors_before_recovery_and_restores() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    controller = FakeController(backend)
    result = await _runner(compose, backend, controller=controller).run_case(FaultStage.ENQUEUE)
    assert result.status == CaseStatus.PASS
    assert result.killed_container_id == "worker-a"
    calls = compose.calls
    assert calls[:2] == [("stop", "worker-a"), ("stop", "worker-b")]
    assert ("kill", "worker-a") in calls
    # worker-b is started for takeover and all preflight workers are restored.
    assert calls.count(("start", "worker-b")) >= 1
    assert calls.count(("start", "worker-a")) >= 1
    assert backend.cleaned and controller.releases == [result.control_id]


@pytest.mark.asyncio
async def test_recovery_observer_starts_before_survivor_health_and_keeps_fast_evidence() -> None:
    class DelayedHealthCompose(FakeCompose):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.health_probe_seen = False
            self.health_ready = False

        def start(self, container_id: str) -> None:
            old = self.containers[container_id]
            self.calls.append(("start", container_id))
            self.containers[container_id] = WorkerContainer(
                container_id=old.container_id,
                project=old.project,
                service=old.service,
                status="running",
                health="starting",
                worker_id=old.worker_id,
                pid=old.pid,
            )
            self.started.set()

        def inspect(self, container: str) -> WorkerContainer:
            value = super().inspect(container)
            if container == "worker-b" and value.status == "running":
                if not self.health_probe_seen:
                    self.health_probe_seen = True
                    return value
                self.health_ready = True
                return WorkerContainer(
                    container_id=value.container_id,
                    project=value.project,
                    service=value.service,
                    status=value.status,
                    health="healthy",
                    worker_id=value.worker_id,
                    pid=value.pid,
                )
            return value

    class FastBeforeHealthyBackend(FakeBackend):
        def __init__(self, compose: DelayedHealthCompose) -> None:
            super().__init__(compose)
            self.observer_started = asyncio.Event()
            self.returned_before_health = False

        async def wait_for_recovery(self, case: CaseIdentity, **kwargs: object) -> RecoveryEvidence:
            del kwargs
            self.observer_started.set()
            await self.compose.started.wait()
            self.returned_before_health = not self.compose.health_ready
            survivors = tuple(
                worker for worker in case_workers_before_health if worker.container_id != "worker-a"
            )
            return RecoveryEvidence(
                owner_worker_id="worker-b",
                lease_epoch=1,
                survivors=survivors,
                turn_count=1,
                sequences=(1, 2, 3),
            )

    compose = DelayedHealthCompose()
    compose.stop("worker-b")
    case_workers_before_health = tuple(compose.list_workers(PROJECT))
    case = fault_stage_acceptance._MutableCase(
        CaseIdentity.create(FaultStage.ENQUEUE, run_id="recovery-fast-before-health")
    )
    case.workers_before = case_workers_before_health
    case.stopped_ids = ["worker-b"]
    backend = FastBeforeHealthyBackend(compose)
    recovery = await _runner(compose, backend)._recover_with_survivors(
        case,
        old_worker_id="worker-a",
        old_fencing_token=0,
        killed_container_id="worker-a",
    )

    assert backend.observer_started.is_set()
    assert backend.returned_before_health is True
    assert compose.health_ready is True
    assert recovery.owner_worker_id == "worker-b"


@pytest.mark.asyncio
async def test_recovery_observer_budget_covers_takeover_and_final_commit_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        fault_stage_acceptance,
        "_SURVIVOR_SCHEDULING_TIMEOUT_SECONDS",
        0.05,
    )
    monkeypatch.setattr(
        fault_stage_acceptance,
        "_WORKER_INVENTORY_CONVERGENCE_TIMEOUT_SECONDS",
        0.01,
    )

    class TwoPhaseRecoveryBackend(FakeBackend):
        async def wait_for_recovery(self, *args: object, **kwargs: object) -> RecoveryEvidence:
            await asyncio.sleep(0.06)
            return await super().wait_for_recovery(*args, **kwargs)

    compose = FakeCompose()
    compose.stop("worker-b")
    case = fault_stage_acceptance._MutableCase(
        CaseIdentity.create(FaultStage.ENQUEUE, run_id="recovery-two-phase-budget")
    )
    case.workers_before = tuple(compose.list_workers(PROJECT))
    case.stopped_ids = ["worker-b"]
    backend = TwoPhaseRecoveryBackend(compose)

    recovery = await _runner(compose, backend)._recover_with_survivors(
        case,
        old_worker_id="worker-a",
        old_fencing_token=0,
        killed_container_id="worker-a",
    )

    assert recovery.owner_worker_id == "worker-b"


@pytest.mark.asyncio
async def test_recovery_rotates_only_after_exact_takeover_timeout() -> None:
    class RotatingRecoveryBackend(FakeBackend):
        def __init__(self, compose: FourWorkerCompose) -> None:
            super().__init__(compose)
            self.attempts = 0

        async def wait_for_recovery(self, *args: object, **kwargs: object) -> RecoveryEvidence:
            self.attempts += 1
            if self.attempts == 1:
                raise StageNotRun(fault_stage_acceptance._TAKEOVER_NOT_OBSERVED_REASON)
            return await super().wait_for_recovery(*args, **kwargs)

    compose = FourWorkerCompose()
    compose.stop("worker-b")
    compose.stop("worker-c")
    case = fault_stage_acceptance._MutableCase(
        CaseIdentity.create(FaultStage.ENQUEUE, run_id="recovery-rotate-takeover")
    )
    case.workers_before = tuple(compose.list_workers(PROJECT))
    case.stopped_ids = ["worker-b", "worker-c"]
    backend = RotatingRecoveryBackend(compose)

    recovery = await _runner(compose, backend)._recover_with_survivors(
        case,
        old_worker_id="worker-a",
        old_fencing_token=0,
        killed_container_id="worker-a",
    )

    assert backend.attempts == 2
    assert recovery.owner_worker_id == "worker-b"
    assert ("start", "worker-b") in compose.calls
    assert ("start", "worker-c") in compose.calls


@pytest.mark.asyncio
async def test_recovery_final_commit_timeout_remains_fail_closed() -> None:
    class FinalCommitFailureBackend(FakeBackend):
        def __init__(self, compose: FourWorkerCompose) -> None:
            super().__init__(compose)
            self.attempts = 0

        async def wait_for_recovery(self, *args: object, **kwargs: object) -> RecoveryEvidence:
            self.attempts += 1
            raise StageNotRun("final committed turn was not observed before the hard timeout")

    compose = FourWorkerCompose()
    compose.stop("worker-b")
    compose.stop("worker-c")
    case = fault_stage_acceptance._MutableCase(
        CaseIdentity.create(FaultStage.ENQUEUE, run_id="recovery-final-commit-failure")
    )
    case.workers_before = tuple(compose.list_workers(PROJECT))
    case.stopped_ids = ["worker-b", "worker-c"]
    backend = FinalCommitFailureBackend(compose)

    with pytest.raises(StageNotRun, match="final committed turn"):
        await _runner(compose, backend)._recover_with_survivors(
            case,
            old_worker_id="worker-a",
            old_fencing_token=0,
            killed_container_id="worker-a",
        )

    assert backend.attempts == 1
    assert ("start", "worker-b") in compose.calls
    assert ("start", "worker-c") not in compose.calls


@pytest.mark.asyncio
async def test_recovery_observer_owner_epoch_survive_final_lease_clear() -> None:
    case = CaseIdentity.create(FaultStage.ENQUEUE, run_id="recovery-owner-retention")
    state = fault_stage_acceptance._RuntimeCaseState(
        case=case,
        binding_id="binding-recovery-owner",
        app_id="app-recovery-owner",
        account_id="account-recovery-owner",
        acceptance=SimpleNamespace(
            context=SimpleNamespace(
                tenant_id=case.tenant_id,
                session_id=case.session_id,
            ),
            inbound_id=case.inbound_id,
        ),
    )
    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._cases = {case.case_id: state}
    backend._scheduler_version = SchedulerVersion.V2
    backend._timeout_seconds = 0.2
    backend._project = PROJECT

    class TransientEmptyInventoryCompose(FakeCompose):
        def __init__(self) -> None:
            super().__init__()
            self.inventory_calls = 0

        def list_workers(self, project: str) -> tuple[WorkerContainer, ...]:
            self.inventory_calls += 1
            if self.inventory_calls == 1:
                return ()
            return super().list_workers(project)

    compose = TransientEmptyInventoryCompose()
    backend._compose = compose
    rows = iter(
        (
            {
                "status": "processing",
                "lease_owner": "worker-b",
                "lease_epoch": 2,
                "mailbox_status": "RUNNING",
                "mailbox_processing_inbound_id": case.inbound_id,
                "mailbox_lease_owner": "worker-b",
                "mailbox_lease_epoch": 2,
            },
            {"status": "committed", "lease_owner": None, "lease_epoch": 2},
        )
    )

    async def turn_row(*args: object, **kwargs: object) -> object:
        del args, kwargs
        return next(rows)

    async def mailbox_completed(*args: object, **kwargs: object) -> bool:
        del args, kwargs
        return True

    async def session_sequences(*args: object, **kwargs: object) -> tuple[int, ...]:
        del args, kwargs
        return (1, 2, 3)

    async def turn_count(*args: object, **kwargs: object) -> int:
        del args, kwargs
        return 1

    backend._turn_row = turn_row  # type: ignore[method-assign]
    backend._mailbox_item_completed = mailbox_completed  # type: ignore[method-assign]
    backend._session_sequences = session_sequences  # type: ignore[method-assign]
    backend._turn_count = turn_count  # type: ignore[method-assign]

    recovery = await backend.wait_for_recovery(
        case,
        old_worker_id="worker-a",
        old_fencing_token=1,
        killed_container_id="worker-a",
    )

    assert recovery.owner_worker_id == "worker-b"
    assert recovery.lease_epoch == 2
    assert compose.inventory_calls >= 2


@pytest.mark.asyncio
async def test_final_survivor_inventory_persistent_empty_keeps_original_reason() -> None:
    class EmptyInventoryCompose(FakeCompose):
        def list_workers(self, project: str) -> tuple[WorkerContainer, ...]:
            assert project == PROJECT
            return ()

    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._compose = EmptyInventoryCompose()
    backend._project = PROJECT
    backend._timeout_seconds = 0.01

    with pytest.raises(StageNotRun, match=r"^no worker containers were observed$"):
        await backend._healthy_survivor_inventory()


@pytest.mark.asyncio
async def test_final_survivor_inventory_retries_nonempty_starting_workers() -> None:
    class StartingThenHealthyCompose(FakeCompose):
        def __init__(self) -> None:
            super().__init__()
            self.inventory_calls = 0

        def list_workers(self, project: str) -> tuple[WorkerContainer, ...]:
            self.inventory_calls += 1
            workers = super().list_workers(project)
            if self.inventory_calls == 1:
                return tuple(replace(worker, health="starting") for worker in workers)
            return workers

    compose = StartingThenHealthyCompose()
    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._compose = compose
    backend._project = PROJECT
    backend._timeout_seconds = 0.2

    survivors = await backend._healthy_survivor_inventory()

    assert survivors == tuple(compose.containers.values())
    assert compose.inventory_calls >= 2


@pytest.mark.asyncio
async def test_final_survivor_inventory_persistent_unhealthy_keeps_original_reason() -> None:
    class UnhealthyInventoryCompose(FakeCompose):
        def list_workers(self, project: str) -> tuple[WorkerContainer, ...]:
            return tuple(
                replace(worker, health="starting") for worker in super().list_workers(project)
            )

    compose = UnhealthyInventoryCompose()
    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._compose = compose
    backend._project = PROJECT
    backend._timeout_seconds = 0.01

    with pytest.raises(StageNotRun, match=r"^no worker containers were observed$"):
        await backend._healthy_survivor_inventory()


def test_final_survivor_inventory_does_not_accept_only_killed_target() -> None:
    recovery = RecoveryEvidence(
        owner_worker_id="worker-b",
        lease_epoch=8,
        survivors=(_worker("worker-a", "worker-a"),),
        turn_count=1,
        sequences=(1, 2),
    )

    with pytest.raises(StageNotRun, match="no healthy survivor remains"):
        _validate_recovery(
            recovery,
            old_worker_id="worker-old",
            old_fencing_token=7,
            killed_container_id="worker-a",
            project=PROJECT,
            require_stale_rejection=False,
        )


@pytest.mark.asyncio
async def test_recovery_observer_is_cancelled_when_survivor_start_fails() -> None:
    class FailingSurvivorStartCompose(FakeCompose):
        def start(self, container_id: str) -> None:
            if container_id == "worker-b":
                raise RuntimeError("survivor start failed")
            super().start(container_id)

    class HangingRecoveryBackend(FakeBackend):
        def __init__(self, compose: FakeCompose) -> None:
            super().__init__(compose)
            self.started = asyncio.Event()
            self.cancelled = False

        async def wait_for_recovery(self, *args: object, **kwargs: object) -> RecoveryEvidence:
            del args, kwargs
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            raise AssertionError("recovery observer unexpectedly returned")

    compose = FailingSurvivorStartCompose()
    compose.stop("worker-b")
    case = fault_stage_acceptance._MutableCase(
        CaseIdentity.create(FaultStage.ENQUEUE, run_id="recovery-start-failure")
    )
    case.workers_before = tuple(compose.list_workers(PROJECT))
    case.stopped_ids = ["worker-b"]
    backend = HangingRecoveryBackend(compose)

    with pytest.raises(RuntimeError, match="survivor start failed"):
        await _runner(compose, backend)._recover_with_survivors(
            case,
            old_worker_id="worker-a",
            old_fencing_token=0,
            killed_container_id="worker-a",
        )

    assert backend.started.is_set()
    assert backend.cancelled is True


@pytest.mark.asyncio
async def test_recovery_observer_is_cancelled_after_timeout() -> None:
    class HangingRecoveryBackend(FakeBackend):
        def __init__(self, compose: FakeCompose) -> None:
            super().__init__(compose)
            self.started = asyncio.Event()
            self.cancelled = False

        async def wait_for_recovery(self, *args: object, **kwargs: object) -> RecoveryEvidence:
            del args, kwargs
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            raise AssertionError("recovery observer unexpectedly returned")

    compose = FakeCompose()
    compose.stop("worker-b")
    case = fault_stage_acceptance._MutableCase(
        CaseIdentity.create(FaultStage.ENQUEUE, run_id="recovery-timeout")
    )
    case.workers_before = tuple(compose.list_workers(PROJECT))
    case.stopped_ids = ["worker-b"]
    backend = HangingRecoveryBackend(compose)

    with pytest.raises(StageNotRun, match="lease takeover was not observed"):
        await _runner(compose, backend)._recover_with_survivors(
            case,
            old_worker_id="worker-a",
            old_fencing_token=0,
            killed_container_id="worker-a",
        )

    assert backend.started.is_set()
    assert backend.cancelled is True


@pytest.mark.asyncio
async def test_recovery_observer_is_cancelled_with_recovery_runner() -> None:
    class HangingRecoveryBackend(FakeBackend):
        def __init__(self, compose: FakeCompose) -> None:
            super().__init__(compose)
            self.started = asyncio.Event()
            self.cancelled = False

        async def wait_for_recovery(self, *args: object, **kwargs: object) -> RecoveryEvidence:
            del args, kwargs
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            raise AssertionError("recovery observer unexpectedly returned")

    compose = FakeCompose()
    compose.stop("worker-b")
    case = fault_stage_acceptance._MutableCase(
        CaseIdentity.create(FaultStage.ENQUEUE, run_id="recovery-cancellation")
    )
    case.workers_before = tuple(compose.list_workers(PROJECT))
    case.stopped_ids = ["worker-b"]
    backend = HangingRecoveryBackend(compose)
    task = asyncio.create_task(
        _runner(compose, backend)._recover_with_survivors(
            case,
            old_worker_id="worker-a",
            old_fencing_token=0,
            killed_container_id="worker-a",
        )
    )
    await backend.started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert backend.cancelled is True


@pytest.mark.asyncio
async def test_termination_requires_observed_exited_container() -> None:
    class NoOpTerminateCompose(FakeCompose):
        def terminate(self, container_id: str, *, mode: str) -> None:
            self.calls.append((mode, container_id))

    compose = NoOpTerminateCompose()
    backend = FakeBackend(compose)
    result = await _runner(compose, backend).run_case(FaultStage.ENQUEUE)

    assert result.status == CaseStatus.NOT_RUN
    assert "still running" in (result.reason or "")
    assert "worker.terminated" not in {item["name"] for item in result.markers}
    assert backend.cleaned


@pytest.mark.asyncio
async def test_commit_requires_explicit_stale_token_rejection() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    backend.recovery_stale_rejected = False
    result = await _runner(compose, backend).run_case(FaultStage.COMMIT_TXN_OPEN)
    assert result.status == CaseStatus.NOT_RUN
    assert "stale fencing" in (result.reason or "")
    assert any(call == ("kill", "worker-a") for call in compose.calls)


@pytest.mark.asyncio
async def test_release_false_is_a_failed_control_evidence() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    controller = FakeController(backend)
    controller.release_result = False
    result = await _runner(compose, backend, controller=controller).run_case(FaultStage.ENQUEUE)
    assert result.status == CaseStatus.FAIL
    assert "release was not acknowledged" in (result.reason or "")


@pytest.mark.asyncio
async def test_commit_success_has_takeover_stale_rejection_and_contiguous_sequence() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    result = await _runner(compose, backend).run_case(FaultStage.COMMIT_TXN_OPEN)
    assert result.status == CaseStatus.PASS
    assert compose.calls[0] == ("stop", "worker-b")
    assert ("stop", "worker-a") not in compose.calls[:1]
    names = [item["name"] for item in result.markers]
    assert "marker.entered" in names
    assert "stale_token_rejection_verified" in names
    assert "turn.single_contiguous_verified" in names


@pytest.mark.asyncio
async def test_tool_stage_arms_exact_execution_key_and_verifies_recovery() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    controller = FakeController(backend)
    result = await _runner(compose, backend, controller=controller).run_case(FaultStage.TOOL)
    assert result.status == CaseStatus.PASS
    assert result.killed_container_id == "worker-a"
    assert controller.events[0].stage is FaultStage.TOOL
    assert controller.events[0].turn_id == f"turn-{result.case.case_id}"
    assert controller.events[0].execution_key == f"execution-{result.case.case_id}"
    assert "tool.idempotent_execution_verified" in {item["name"] for item in result.markers}
    assert ("kill", "worker-a") in compose.calls


@pytest.mark.asyncio
async def test_tool_stage_without_execution_key_is_not_run_without_termination() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    backend.omit_execution_key = True
    result = await _runner(compose, backend).run_case(FaultStage.TOOL)
    assert result.status == CaseStatus.NOT_RUN
    assert "execution key" in (result.reason or "")
    assert not any(call[0] == "kill" for call in compose.calls)


@pytest.mark.asyncio
async def test_kill_acknowledgement_is_required_and_does_not_terminate() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    result = await _runner(compose, backend, allow_process_kill=False).run_case(FaultStage.ENQUEUE)
    assert result.status == CaseStatus.NOT_RUN
    assert "termination requires" in (result.reason or "")
    assert not any(call[0] == "kill" for call in compose.calls)
    assert backend.cleaned


@pytest.mark.asyncio
async def test_wrong_processing_owner_is_not_run_without_termination() -> None:
    compose = FakeCompose()
    backend = FakeBackend(compose)
    backend.processing_owner = "worker-b"
    result = await _runner(compose, backend).run_case(FaultStage.COMMIT_TXN_OPEN)
    assert result.status == CaseStatus.NOT_RUN
    assert "explicit worker" in (result.reason or "")
    assert not any(call[0] == "kill" for call in compose.calls)


def test_fault_stage_controller_uses_worker_configured_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[str, str]] = []

    class ControllerDouble:
        def __init__(self, pool: object, *, run_id: str, run_token: str) -> None:
            del pool
            observed.append((run_id, run_token))

        async def arm(self, event: FaultStageEvent) -> str:
            del event
            return "control-fixed-run"

        async def release(self, control_id: str, *, tenant_id: str) -> bool:
            del control_id, tenant_id
            return True

    monkeypatch.setattr(fault_stage_acceptance, "PostgresFaultStageController", ControllerDouble)
    controller = fault_stage_acceptance.PostgresPerCaseStageController(
        object(), "r" * 32, "worker-config-run-id"
    )
    event = FaultStageEvent(
        stage=FaultStage.ENQUEUE,
        tenant_id="fault-tenant",
        worker_id="worker-a",
        inbound_id="inbound-1",
        stream_id="stream-1",
    )
    assert asyncio.run(controller.arm(event)) == "control-fixed-run"
    assert observed == [("worker-config-run-id", "r" * 32)]


def test_recovery_requires_monotonic_lease_epoch_and_healthy_survivor() -> None:
    recovery = RecoveryEvidence(
        owner_worker_id="worker-b",
        lease_epoch=7,
        survivors=(_worker("worker-b", "worker-b"),),
        turn_count=1,
        sequences=(1, 2),
    )
    with pytest.raises(StageAcceptanceError, match="lease epoch"):
        _validate_recovery(
            recovery,
            old_worker_id="worker-a",
            old_fencing_token=7,
            killed_container_id="worker-a",
            project=PROJECT,
            require_stale_rejection=False,
        )


def test_fault_cli_does_not_fallback_to_general_service_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in fault_stage_acceptance.FAULT_STAGE_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TRPC_SERVICE_DATABASE_DSN", "postgresql://ordinary")
    monkeypatch.setenv("TRPC_REAL_REDIS_URL", "redis://ordinary")
    with pytest.raises(StageNotRun) as error:
        _required_fault_stage_env()
    message = str(error.value)
    assert "TRPC_FAULT_DATABASE_DSN" in message
    assert "TRPC_FAULT_REDIS_URL" in message
    assert "ordinary" not in message


@pytest.mark.asyncio
async def test_fault_cli_reports_only_missing_dedicated_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in fault_stage_acceptance.FAULT_STAGE_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TRPC_RUN_FAULT_STAGE_ACCEPTANCE", "1")
    monkeypatch.setenv("TRPC_SERVICE_ENVIRONMENT", "test")
    monkeypatch.setenv("TRPC_FAULT_STAGE_ALLOW_KILL", "1")
    monkeypatch.setenv("TRPC_SERVICE_DATABASE_DSN", "postgresql://ordinary")
    args = _parser().parse_args(
        [
            "--execute",
            "--project",
            PROJECT,
            "--worker-container",
            "worker-a",
            "--allow-process-kill",
        ]
    )
    report = await _execute(args)
    assert report["gate"] == "not_run"
    assert "TRPC_FAULT_DATABASE_DSN" in report["reason"]
    assert "TRPC_FAULT_RUN_TOKEN" in report["reason"]
    assert "ordinary" not in json.dumps(report)


def test_offline_agent_delay_is_explicit_and_positive() -> None:
    assert _parse_offline_agent_delay("1.5") == 1.5
    with pytest.raises(StageNotRun, match="OFFLINE_AGENT_DELAY"):
        _parse_offline_agent_delay("0")
    with pytest.raises(StageNotRun, match="OFFLINE_AGENT_DELAY"):
        _parse_offline_agent_delay("nan")
    with pytest.raises(StageNotRun, match="OFFLINE_AGENT_DELAY"):
        _parse_offline_agent_delay("not-a-number")


@pytest.mark.asyncio
async def test_redis_delivery_cleanup_is_exact_case_scoped() -> None:
    class RedisDouble:
        def __init__(self) -> None:
            self.deleted: list[tuple[str, str]] = []
            self.dedupe_deleted: list[str] = []

        async def xpending_range(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            return []

        async def xrange(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            return [("42-0", {"outbox_id": "outbox-case"})]

        async def xdel(self, stream: str, stream_id: str) -> int:
            self.deleted.append((stream, stream_id))
            return 1

        async def exists(self, key: str) -> int:
            assert key == "trpc:published:outbox-case"
            return 1

        async def delete(self, key: str) -> int:
            self.dedupe_deleted.append(key)
            return 1

    redis = RedisDouble()
    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._redis = redis
    backend._queue = SimpleNamespace(stream="trpc:inbound:v1", group="trpc-workers-v1")
    backend._timeout_seconds = 1.0
    state = SimpleNamespace(
        accepted=AcceptanceEvidence(
            inbound_id="inbound-case",
            stream_id="42-0",
            outbox_id="outbox-case",
        )
    )
    await backend._verify_and_clear_redis_delivery(state)
    assert redis.deleted == [("trpc:inbound:v1", "42-0")]
    assert redis.dedupe_deleted == ["trpc:published:outbox-case"]


@pytest.mark.asyncio
async def test_v2_redis_delivery_cleanup_validates_session_ready_without_dedupe_key() -> None:
    class RedisDouble:
        def __init__(self) -> None:
            self.deleted: list[tuple[str, str]] = []

        async def xpending_range(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            return []

        async def xrange(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            return [
                (
                    "42-0",
                    {
                        "event_id": "event-case",
                        "tenant_id": "fault-case",
                        "session_id": "session-case",
                        "generation": "1",
                        "priority": "0",
                        "trace_id": "trace-case",
                        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    },
                )
            ]

        async def xdel(self, stream: str, stream_id: str) -> int:
            self.deleted.append((stream, stream_id))
            return 1

        async def exists(self, key: str) -> int:
            raise AssertionError(f"v2 must not inspect dedupe key {key}")

        async def delete(self, key: str) -> int:
            raise AssertionError(f"v2 must not delete dedupe key {key}")

    redis = RedisDouble()
    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._redis = redis
    backend._queue = SimpleNamespace(stream="trpc:session-ready:v2", group="trpc-session-ready-v2")
    backend._scheduler_version = SchedulerVersion.V2
    backend._timeout_seconds = 1.0
    state = SimpleNamespace(
        case=SimpleNamespace(tenant_id="fault-case", session_id="session-case"),
        accepted=AcceptanceEvidence(
            inbound_id="inbound-case",
            stream_id="42-0",
            session_id="session-case",
            outbox_id="event-case",
            generation=1,
        ),
    )
    await backend._verify_and_clear_redis_delivery(state)
    assert redis.deleted == [("trpc:session-ready:v2", "42-0")]


@pytest.mark.asyncio
async def test_v2_redis_delivery_ack_and_delete_is_complete_when_exact_entry_is_absent() -> None:
    class RedisDouble:
        def __init__(self) -> None:
            self.deleted: list[tuple[str, str]] = []
            self.xrange_calls = 0

        async def xpending_range(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            return []

        async def xrange(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            self.xrange_calls += 1
            return []

        async def xdel(self, stream: str, stream_id: str) -> int:
            self.deleted.append((stream, stream_id))
            return 1

    redis = RedisDouble()
    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._redis = redis
    backend._queue = SimpleNamespace(stream="trpc:session-ready:v2", group="trpc-session-ready-v2")
    backend._scheduler_version = SchedulerVersion.V2
    backend._timeout_seconds = 1.0
    state = SimpleNamespace(
        case=SimpleNamespace(tenant_id="fault-case", session_id="session-case"),
        accepted=AcceptanceEvidence(
            inbound_id="inbound-case",
            stream_id="42-0",
            session_id="session-case",
            outbox_id="event-case",
            generation=1,
        ),
    )

    await backend._verify_and_clear_redis_delivery(state)

    assert redis.xrange_calls == 1
    assert redis.deleted == []


@pytest.mark.asyncio
async def test_v2_redis_delivery_pending_entry_does_not_pass_after_timeout() -> None:
    class RedisDouble:
        def __init__(self) -> None:
            self.xpending_calls = 0
            self.deleted: list[tuple[str, str]] = []

        async def xpending_range(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            self.xpending_calls += 1
            return [("42-0", "consumer", 1, 1)]

        async def xrange(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            raise AssertionError("a pending delivery must not be read as ACK evidence")

        async def xdel(self, stream: str, stream_id: str) -> int:
            self.deleted.append((stream, stream_id))
            return 1

    redis = RedisDouble()
    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._redis = redis
    backend._queue = SimpleNamespace(stream="trpc:session-ready:v2", group="trpc-session-ready-v2")
    backend._scheduler_version = SchedulerVersion.V2
    backend._timeout_seconds = 0.01
    state = SimpleNamespace(
        case=SimpleNamespace(tenant_id="fault-case", session_id="session-case"),
        accepted=AcceptanceEvidence(
            inbound_id="inbound-case",
            stream_id="42-0",
            session_id="session-case",
            outbox_id="event-case",
            generation=1,
        ),
    )

    with pytest.raises(StageNotRun, match="before timeout"):
        await backend._verify_and_clear_redis_delivery(state)

    assert redis.xpending_calls >= 1
    assert redis.deleted == []


@pytest.mark.asyncio
async def test_v2_redis_delivery_present_entry_requires_strict_session_ready_fields() -> None:
    class RedisDouble:
        def __init__(self) -> None:
            self.deleted: list[tuple[str, str]] = []

        async def xpending_range(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            return []

        async def xrange(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            # Missing ``created_at`` must not be accepted as a seven-field v2
            # notice merely because the stream id and event id match.
            return [
                (
                    "42-0",
                    {
                        "event_id": "event-case",
                        "tenant_id": "fault-case",
                        "session_id": "session-case",
                        "generation": "1",
                        "priority": "0",
                        "trace_id": "trace-case",
                    },
                )
            ]

        async def xdel(self, stream: str, stream_id: str) -> int:
            self.deleted.append((stream, stream_id))
            return 1

    redis = RedisDouble()
    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._redis = redis
    backend._queue = SimpleNamespace(stream="trpc:session-ready:v2", group="trpc-session-ready-v2")
    backend._scheduler_version = SchedulerVersion.V2
    backend._timeout_seconds = 1.0
    state = SimpleNamespace(
        case=SimpleNamespace(tenant_id="fault-case", session_id="session-case"),
        accepted=AcceptanceEvidence(
            inbound_id="inbound-case",
            stream_id="42-0",
            session_id="session-case",
            outbox_id="event-case",
            generation=1,
        ),
    )

    with pytest.raises(StageNotRun, match="valid SessionReady"):
        await backend._verify_and_clear_redis_delivery(state)

    assert redis.deleted == []


@pytest.mark.asyncio
async def test_postgres_cleanup_retains_case_until_redis_cleanup_succeeds() -> None:
    case = CaseIdentity.create(FaultStage.ENQUEUE, run_id="cleanup-state")
    state = fault_stage_acceptance._RuntimeCaseState(
        case=case,
        binding_id="binding-case",
        app_id="app-case",
        account_id="account-case",
    )

    class ConnectionDouble:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[str, str, str]]] = []

        async def fetchval(self, query: str, *args: str) -> dict[str, int]:
            assert len(args) == 3
            self.calls.append((query, (args[0], args[1], args[2])))
            return {"tenants": 1}

    connection = ConnectionDouble()
    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._cases = {case.case_id: state}
    backend._scheduler_version = SchedulerVersion.V2

    @asynccontextmanager
    async def fake_tenant_transaction(tenant_id: str) -> AsyncIterator[ConnectionDouble]:
        assert tenant_id == case.tenant_id
        yield connection

    backend._tenant_transaction = fake_tenant_transaction  # type: ignore[method-assign]
    redis_attempts = 0

    async def cleanup_redis(_state: object) -> None:
        nonlocal redis_attempts
        redis_attempts += 1
        if redis_attempts == 1:
            raise StageAcceptanceError("temporary Redis failure")

    backend._cleanup_redis_delivery = cleanup_redis  # type: ignore[assignment]

    with pytest.raises(StageAcceptanceError, match="exact Redis fault delivery cleanup failed"):
        await backend.cleanup_case(case)
    assert case.case_id in backend._cases
    assert connection.calls == []

    await backend.cleanup_case(case)
    assert case.case_id not in backend._cases
    assert connection.calls == [
        (
            "SELECT public.cleanup_fault_stage_fixture($1, $2, $3)",
            (case.tenant_id, case.run_id, case.case_id),
        )
    ]
    assert redis_attempts == 2


@pytest.mark.asyncio
async def test_postgres_cleanup_accepts_authoritative_ids_after_acceptance() -> None:
    original = CaseIdentity.create(FaultStage.ENQUEUE, run_id="cleanup-authoritative")
    state = fault_stage_acceptance._RuntimeCaseState(
        case=original,
        binding_id="binding-authoritative",
        app_id="app-authoritative",
        account_id="account-authoritative",
    )
    accepted = replace(
        original,
        session_id="s1_authoritative",
        inbound_id="2f8c50a0-6dc0-46a8-8c6c-42b1e8a2fb6e",
    )

    class ConnectionDouble:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[str, str, str]]] = []

        async def fetchval(self, query: str, *args: str) -> str:
            assert len(args) == 3
            self.calls.append((query, (args[0], args[1], args[2])))
            return '{"tenants":1}'

    connection = ConnectionDouble()
    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._cases = {original.case_id: state}
    backend._scheduler_version = SchedulerVersion.V2

    @asynccontextmanager
    async def fake_tenant_transaction(tenant_id: str) -> AsyncIterator[ConnectionDouble]:
        assert tenant_id == original.tenant_id
        yield connection

    backend._tenant_transaction = fake_tenant_transaction  # type: ignore[method-assign]

    async def cleanup_redis(_state: object) -> None:
        return None

    backend._cleanup_redis_delivery = cleanup_redis  # type: ignore[assignment]

    await backend.cleanup_case(accepted)
    assert accepted.case_id not in backend._cases
    assert connection.calls == [
        (
            "SELECT public.cleanup_fault_stage_fixture($1, $2, $3)",
            (original.tenant_id, original.run_id, original.case_id),
        )
    ]


@pytest.mark.asyncio
async def test_postgres_cleanup_retains_authority_when_tenant_count_is_not_one() -> None:
    case = CaseIdentity.create(FaultStage.ENQUEUE, run_id="cleanup-count")
    state = fault_stage_acceptance._RuntimeCaseState(
        case=case,
        binding_id="binding-count",
        app_id="app-count",
        account_id="account-count",
    )

    class ConnectionDouble:
        async def fetchval(self, query: str, *args: str) -> dict[str, int]:
            assert query == "SELECT public.cleanup_fault_stage_fixture($1, $2, $3)"
            assert args == (case.tenant_id, case.run_id, case.case_id)
            return {"tenants": 0}

    backend = object.__new__(fault_stage_acceptance.PostgresRuntimeStageBackend)
    backend._cases = {case.case_id: state}

    @asynccontextmanager
    async def fake_tenant_transaction(tenant_id: str) -> AsyncIterator[ConnectionDouble]:
        assert tenant_id == case.tenant_id
        yield ConnectionDouble()

    backend._tenant_transaction = fake_tenant_transaction  # type: ignore[method-assign]

    async def cleanup_redis(_state: object) -> None:
        return None

    backend._cleanup_redis_delivery = cleanup_redis  # type: ignore[assignment]

    with pytest.raises(StageAcceptanceError, match="exactly one tenant"):
        await backend.cleanup_case(case)
    assert case.case_id in backend._cases
