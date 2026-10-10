#!/usr/bin/env python3
"""Marker-driven, opt-in worker fault acceptance.

This command is intentionally stricter than a timing-based kill test.  It only
terminates the explicitly selected worker after a PostgreSQL fault-stage row
contains an exact ``entered`` marker.  The default invocation is side-effect
free and writes a JSON ``not_run`` report.

The real runtime-specific operations are supplied through ``StageBackend``.
Keeping those operations behind a small protocol makes the safety guards
testable without Docker, PostgreSQL, Redis, or a process kill.  The production
runtime uses the same ``PostgresFaultStageController`` that workers use; the
``PostgresStageMarkerReader`` below provides the read side of its marker table.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import inspect
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

import asyncpg
import redis.asyncio as redis_async
from redis.exceptions import RedisError

from trpc_service.channels.envelopes import InboundEnvelope, PayloadKind
from trpc_service.config.settings import Environment, SchedulerVersion
from trpc_service.faults import (
    FaultStage,
    FaultStageEvent,
    PostgresFaultStageController,
)
from trpc_service.queue.redis_streams import RedisStreamQueue
from trpc_service.queue.session_ready import (
    SESSION_READY_GROUP_V2,
    SESSION_READY_STREAM_V2,
    SessionReadyCodec,
    SessionReadyQueue,
)
from trpc_service.queue.session_ready_outbox import (
    SESSION_READY_EVENT_V2,
    SessionReadyOutboxQueue,
)
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import Acceptance, OutboxRecord, SessionLease, TurnCommit
from trpc_service.storage.postgres import PostgresRuntimeRepository
from trpc_service.storage.protocols import FencingConflict
from trpc_service.tenant.control import PostgresControlPlaneRepository
from trpc_service.tenant.models import Channel, ChannelBinding, ConversationKind
from trpc_service.tool.test_tool import DETERMINISTIC_FAULT_TOOL_NAME

try:  # Keep direct script-path and module invocations equivalent.
    from scripts.report_io import atomic_write_json
except ModuleNotFoundError:  # pragma: no cover - direct script startup fallback
    from report_io import atomic_write_json  # type: ignore[import-not-found,no-redef]

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "runs" / "multitenant" / "fault-stage-acceptance.json"
OPT_IN_ENV = "TRPC_RUN_FAULT_STAGE_ACCEPTANCE"
KILL_OPT_IN_ENV = "TRPC_FAULT_STAGE_ALLOW_KILL"
PROJECT_PREFIX = "trpc-fault-"
_PROJECT_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}\Z")
_CONTAINER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_EVIDENCE_NONCE_ENV = "TRPC_FAULT_EVIDENCE_NONCE"
_EVIDENCE_PROJECT_ENV = "TRPC_FAULT_PROJECT"
_EVIDENCE_CONTAINER_ENV = "TRPC_FAULT_WORKER_CONTAINER"
_EVIDENCE_SCHEDULER_ENV = "TRPC_FAULT_SCHEDULER_VERSION"
_EVIDENCE_STREAM_ENV = "TRPC_FAULT_REDIS_STREAM"
_EVIDENCE_GROUP_ENV = "TRPC_FAULT_REDIS_GROUP"
# Keep the order aligned with the runtime pipeline.  All three cases are
# required for a production fault-acceptance claim; a partial run is
# deliberately reported as ``not_run``.
REQUIRED_STAGES = (FaultStage.ENQUEUE, FaultStage.TOOL, FaultStage.COMMIT_TXN_OPEN)
SCENARIO_STAGES = {
    "worker_enqueue": FaultStage.ENQUEUE,
    "worker_tool": FaultStage.TOOL,
    "worker_commit": FaultStage.COMMIT_TXN_OPEN,
}
WORKER_SERVICE = "worker"
# A restored worker/dispatcher can briefly still hold a transaction or Redis
# connection for this case.  Retrying only the already case-scoped cleanup is
# safe; broad deletion or an unbounded wait would make a fault run less safe.
# Bounded exponential backoff covers the 0.5-1s transient transaction race
# window while retaining a hard upper bound.  Cleanup remains tenant/run
# scoped on every attempt and never broadens its deletion predicate.
_CLEANUP_RETRY_DELAYS_SECONDS = (0.5, 1.0, 2.0)
# Cleanup is a recovery/teardown operation, not a production acceptance
# observation.  Reusing the per-case runtime timeout here allowed a blocked
# DELETE/pool acquire to consume the full 300-second case budget on every
# retry; the parent then timed out the all-three child before it could write a
# report.  Keep cleanup bounded independently while retaining all four exact,
# tenant-scoped attempts and fail-closed status on exhaustion.
_CLEANUP_TIMEOUT_SECONDS = 30.0
# A worker pool must be ready before the next case is allowed to arm a marker.
# This is deliberately shorter than the per-case evidence timeout: a failed
# restore must fail closed promptly instead of making the next case wait on a
# marker that no worker can consume.
_WORKER_RESTORE_TIMEOUT_SECONDS = 30.0
# ``run_all`` is one child invocation.  Keep a small bounded allowance for
# cleanup and worker restoration while preventing an all-three run from
# inheriting an unbounded sequence of per-case waits.
_RUN_ALL_GRACE_SECONDS = 30.0
# Start survivors one at a time.  Starting every survivor before observing the
# takeover can let the first survivor claim and finish the turn before the
# recovery poll begins, which loses the owner evidence and waits a full case
# timeout.  A short scheduling window is enough to rotate to another healthy
# survivor without allowing a marker wait to become permanent.
_SURVIVOR_SCHEDULING_TIMEOUT_SECONDS = 30.0
# Docker can briefly return an empty worker inventory while a just-restored
# Compose project is settling.  Retry only that empty observation for a short,
# fixed window; a persistent empty inventory must still fail closed at the
# caller's existing validation boundary.
_WORKER_INVENTORY_CONVERGENCE_TIMEOUT_SECONDS = 5.0
_WORKER_INVENTORY_RETRY_DELAYS_SECONDS = (0.1, 0.25, 0.5, 1.0, 2.0)
_TAKEOVER_NOT_OBSERVED_REASON = "lease takeover was not observed before the hard timeout"
# Docker inspect is a host/Docker-Desktop API call, not an in-process wait.
# A 50ms loop can issue hundreds of daemon requests per case and amplify host
# pressure during a fault run.  Keep the first observation at >=500ms and use
# bounded exponential backoff while preserving the same hard deadline.
_DOCKER_INSPECT_POLL_MIN_SECONDS = 0.5
_DOCKER_INSPECT_POLL_MAX_SECONDS = 2.0


def _scheduler_transport(version: SchedulerVersion) -> tuple[str, str]:
    """Return the only Redis transport permitted for one scheduler version."""

    if version == SchedulerVersion.V2:
        return SESSION_READY_STREAM_V2, SESSION_READY_GROUP_V2
    return "trpc:inbound:v1", "trpc-workers-v1"


def _tenant_run_tag(run_id: str) -> str:
    """Return a readable, bounded, collision-resistant run namespace."""

    return f"{run_id[:40]}-{hashlib.sha256(run_id.encode('utf-8')).hexdigest()[:12]}"


class CaseStatus(StrEnum):
    PASS = "pass"  # noqa: S105 - a report status, not a credential
    FAIL = "fail"
    NOT_RUN = "not_run"


class StageAcceptanceError(RuntimeError):
    """An execution error after the required runtime preconditions passed."""


class StageNotRun(StageAcceptanceError):
    """A required observable or safe precondition is unavailable."""


@dataclass(frozen=True, slots=True)
class WorkerContainer:
    """Only the inspected, label-verified container identity is actionable."""

    container_id: str
    project: str
    service: str
    status: str
    health: str | None
    worker_id: str
    scheduler_version: str | None = None
    redis_stream: str | None = None
    redis_group: str | None = None
    pid: int | None = None


@dataclass(frozen=True, slots=True)
class CaseIdentity:
    stage: FaultStage
    case_id: str
    run_id: str
    tenant_id: str
    session_id: str
    inbound_id: str
    message_id: str

    @classmethod
    def create(cls, stage: FaultStage, *, run_id: str | None = None) -> CaseIdentity:
        suffix = uuid4().hex
        # These identifiers are deliberately unique per case.  They contain no
        # message body, account secret, or external provider identity.
        selected_run_id = run_id.strip() if run_id is not None else f"run-{suffix}"
        if not selected_run_id:
            raise ValueError("fault-stage run id cannot be blank")
        return cls(
            stage=stage,
            case_id=f"case-{suffix}",
            run_id=selected_run_id,
            # Bind the synthetic tenant to the invocation run.  This makes
            # recovery/cleanup able to identify only this run's resources;
            # a timeout must never justify deleting every ``fault-*`` tenant.
            tenant_id=f"fault-{_tenant_run_tag(selected_run_id)}-{suffix}",
            session_id=f"session-{suffix}",
            inbound_id=f"inbound-{suffix}",
            message_id=f"message-{suffix}",
        )


@dataclass(frozen=True, slots=True)
class AcceptanceEvidence:
    inbound_id: str
    stream_id: str
    session_id: str | None = None
    outbox_id: str | None = None
    generation: int | None = None


@dataclass(frozen=True, slots=True)
class ProcessingEvidence:
    inbound_id: str
    session_id: str
    turn_id: str
    worker_id: str
    fencing_token: int
    execution_key: str | None = None
    # v2 sets this only after the exact stream entry is no longer pending.
    # It is deliberately false by default so a fake/legacy v1 observation
    # cannot be mistaken for claim -> ACK -> execute evidence.
    acknowledged: bool = False


@dataclass(frozen=True, slots=True)
class ControlEvidence:
    """Content-free read of one ``fault_stage_controls`` row."""

    control_id: str
    status: str
    marker_status: str | None
    marker_worker_id: str | None
    marker_inbound_id: str | None
    marker_turn_id: str | None
    marker_execution_key: str | None
    marker_stream_id: str | None
    marker_fencing_token: int | None


@dataclass(frozen=True, slots=True)
class RecoveryEvidence:
    owner_worker_id: str
    lease_epoch: int
    survivors: tuple[WorkerContainer, ...]
    turn_count: int
    sequences: tuple[int, ...]
    stale_token_rejected: bool = False


@dataclass(frozen=True, slots=True)
class StageBackend(Protocol):
    """Database/runtime evidence needed by the safety state machine.

    Implementations must not return message text or provider secrets.  The
    acceptance runner only serializes the identifiers and counters below.
    """

    async def provision_case(self, case: CaseIdentity) -> None: ...

    async def accept_and_publish(self, case: CaseIdentity) -> AcceptanceEvidence: ...

    async def observe_processing(
        self,
        case: CaseIdentity,
        accepted: AcceptanceEvidence,
        *,
        require_execution_key: bool = False,
    ) -> ProcessingEvidence: ...

    async def read_control(self, tenant_id: str, control_id: str) -> ControlEvidence | None: ...

    async def wait_for_recovery(
        self,
        case: CaseIdentity,
        *,
        old_worker_id: str,
        old_fencing_token: int,
        killed_container_id: str,
    ) -> RecoveryEvidence: ...

    async def verify_final(self, case: CaseIdentity, recovery: RecoveryEvidence) -> None: ...

    async def cleanup_case(self, case: CaseIdentity) -> None: ...


class ComposeController(Protocol):
    """Small Docker facade; tests provide a fake and never invoke Docker."""

    def inspect(self, container: str) -> WorkerContainer: ...

    def list_workers(self, project: str) -> tuple[WorkerContainer, ...]: ...

    def stop(self, container_id: str) -> None: ...

    def start(self, container_id: str) -> None: ...

    def terminate(self, container_id: str, *, mode: str) -> None: ...


class StageController(Protocol):
    async def arm(self, event: FaultStageEvent) -> str: ...

    async def release(self, control_id: str, *, tenant_id: str) -> bool: ...


@dataclass(frozen=True, slots=True)
class StageResult:
    stage: FaultStage
    status: CaseStatus
    case: CaseIdentity
    markers: tuple[dict[str, Any], ...] = ()
    reason: str | None = None
    control_id: str | None = None
    killed_container_id: str | None = None

    def as_report(self) -> dict[str, Any]:
        payload = {
            "stage": self.stage.value,
            "status": self.status.value,
            "case": {
                "case_id": self.case.case_id,
                "run_id": self.case.run_id,
                "tenant_id": self.case.tenant_id,
                "session_id": self.case.session_id,
                "inbound_id": self.case.inbound_id,
                "message_id": self.case.message_id,
            },
            "markers": list(self.markers),
        }
        if self.reason:
            payload["reason"] = self.reason
        if self.control_id:
            payload["control_id"] = self.control_id
        if self.killed_container_id:
            payload["killed_container_id"] = self.killed_container_id
        return payload


@dataclass
class _MutableCase:
    case: CaseIdentity
    accepted: AcceptanceEvidence | None = None
    processing: ProcessingEvidence | None = None
    control_id: str | None = None
    target: WorkerContainer | None = None
    workers_before: tuple[WorkerContainer, ...] = ()
    stopped_ids: list[str] = field(default_factory=list)
    marker_rows: list[dict[str, Any]] = field(default_factory=list)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _marker(name: str, status: CaseStatus, **details: Any) -> dict[str, Any]:
    value: dict[str, Any] = {"name": name, "status": status.value, "observed_at": _now()}
    value.update({key: item for key, item in details.items() if item is not None})
    return value


def _redis_text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _field(value: object, name: str, default: object = None) -> object:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _coerce_container(value: object) -> WorkerContainer:
    if isinstance(value, WorkerContainer):
        return value
    if not isinstance(value, Mapping):
        raise TypeError("container inspection must be a mapping or WorkerContainer")
    return WorkerContainer(
        container_id=str(value.get("container_id", "")),
        project=str(value.get("project", "")),
        service=str(value.get("service", "")),
        status=str(value.get("status", "")),
        health=None if value.get("health") is None else str(value["health"]),
        worker_id=str(value.get("worker_id", "")),
        scheduler_version=(
            None if value.get("scheduler_version") is None else str(value["scheduler_version"])
        ),
        redis_stream=None if value.get("redis_stream") is None else str(value["redis_stream"]),
        redis_group=None if value.get("redis_group") is None else str(value["redis_group"]),
        pid=(
            int(value["pid"])
            if isinstance(value.get("pid"), int) and not isinstance(value.get("pid"), bool)
            else None
        ),
    )


def validate_project(project: str | None) -> str:
    if not project or not project.strip():
        raise StageNotRun("an explicit Compose project is required")
    value = project.strip()
    if not value.startswith(PROJECT_PREFIX) or len(value) <= len(PROJECT_PREFIX):
        raise StageNotRun(f"Compose project must use the unique {PROJECT_PREFIX}<run> prefix")
    # Compose project names are resource selectors, not arbitrary labels.  A
    # bounded lower-case name prevents accidental targeting of the normal
    # stack and keeps the value safe for Docker labels and report paths.
    if _PROJECT_PATTERN.fullmatch(value) is None:
        raise StageNotRun(
            "Compose project must be <=63 chars and contain only lower-case letters, "
            "digits, '-' or '_'"
        )
    return value


def _validate_container_selector(container: str | None) -> str:
    value = (container or "").strip()
    if _CONTAINER_PATTERN.fullmatch(value) is None:
        raise StageNotRun(
            "worker container must be a bounded Docker id/name without whitespace or path chars"
        )
    return value


def validate_worker_container(
    container: WorkerContainer,
    *,
    project: str,
    explicit_container: str,
) -> WorkerContainer:
    """Fail closed before any stop/kill operation."""

    if not container.container_id or container.container_id != explicit_container:
        raise StageAcceptanceError("inspected worker is not the explicitly selected container")
    if container.project != project:
        raise StageAcceptanceError(
            "worker Compose project label does not match the requested project"
        )
    if container.service != WORKER_SERVICE:
        raise StageAcceptanceError("selected container is not the Compose worker service")
    if container.status.lower() != "running":
        raise StageNotRun("selected worker container is not running")
    if (container.health or "").lower() != "healthy":
        raise StageNotRun("selected worker container is not healthy or has no health status")
    if not container.worker_id.strip():
        raise StageNotRun("selected worker has no stable worker_id")
    return container


def validate_worker_transport(
    container: WorkerContainer,
    *,
    scheduler_version: SchedulerVersion,
    stream: str,
    group: str,
) -> None:
    """Require inspected workers to advertise the same scheduler wire contract."""

    if (
        container.scheduler_version != scheduler_version.value
        or container.redis_stream != stream
        or container.redis_group != group
    ):
        raise StageNotRun(
            f"worker {container.container_id} scheduler transport does not match "
            f"{scheduler_version.value}"
        )


def _validate_workers(
    workers: Sequence[WorkerContainer],
    *,
    project: str,
    killed_id: str | None = None,
    minimum: int = 1,
    require_independent: bool = False,
) -> tuple[WorkerContainer, ...]:
    if minimum < 1:
        raise ValueError("minimum worker count must be positive")
    if not workers:
        raise StageNotRun("no worker containers were observed")
    valid: list[WorkerContainer] = []
    container_ids: set[str] = set()
    worker_ids: set[str] = set()
    for worker in workers:
        if killed_id is not None and worker.container_id == killed_id:
            continue
        if worker.project != project or worker.service != WORKER_SERVICE:
            raise StageAcceptanceError(
                "worker list contains a container from another service/project"
            )
        if worker.status.lower() != "running" or (worker.health or "").lower() != "healthy":
            raise StageNotRun("every survivor must be running and healthy")
        if not worker.worker_id.strip():
            raise StageNotRun("every survivor must expose a stable worker_id")
        if worker.container_id in container_ids or worker.worker_id in worker_ids:
            raise StageAcceptanceError(
                "worker inventory contains duplicate container/worker identity"
            )
        container_ids.add(worker.container_id)
        worker_ids.add(worker.worker_id)
        valid.append(worker)
    if not valid:
        raise StageNotRun("no healthy survivor remains after the selected worker termination")
    if len(valid) < minimum:
        raise StageNotRun(f"at least {minimum} healthy worker containers are required")
    if require_independent:
        pids = [worker.pid for worker in valid]
        if any(pid is None or pid <= 0 for pid in pids):
            raise StageNotRun("every production worker must expose a positive process PID")
        if len(set(pids)) != len(pids):
            raise StageNotRun("production workers must have independent process identities")
    return tuple(valid)


def _control_matches(control: object, event: FaultStageEvent, control_id: str) -> bool:
    """Require the full control/marker identity, including nullable fields."""

    expected: dict[str, object] = {
        "worker_id": event.worker_id,
        "inbound_id": event.inbound_id,
        "turn_id": event.turn_id,
        "execution_key": event.execution_key,
        "stream_id": event.stream_id,
        "fencing_token": event.fencing_token,
    }
    if str(_field(control, "control_id", "")) != control_id:
        return False
    if _field(control, "status") != "entered" or _field(control, "marker_status") != "entered":
        return False
    return all(_field(control, f"marker_{name}") == value for name, value in expected.items())


def _validate_recovery(
    recovery: RecoveryEvidence,
    *,
    old_worker_id: str,
    old_fencing_token: int,
    killed_container_id: str,
    project: str,
    require_stale_rejection: bool,
) -> None:
    if recovery.owner_worker_id == old_worker_id:
        raise StageAcceptanceError("lease takeover owner is the terminated worker")
    if recovery.lease_epoch <= old_fencing_token:
        raise StageAcceptanceError("lease epoch did not increase after worker takeover")
    survivors = _validate_workers(
        recovery.survivors, project=project, killed_id=killed_container_id
    )
    if any(item.container_id == killed_container_id for item in survivors):
        raise StageAcceptanceError("terminated worker is still counted as a survivor")
    if recovery.owner_worker_id not in {item.worker_id for item in survivors}:
        raise StageAcceptanceError("takeover owner does not map to a healthy survivor")
    if require_stale_rejection and not recovery.stale_token_rejected:
        raise StageNotRun("stale fencing-token rejection was not explicitly observed")
    if recovery.turn_count != 1:
        raise StageAcceptanceError("recovery did not produce exactly one session turn")
    if not recovery.sequences:
        raise StageNotRun("no committed event sequence was observed")
    expected = tuple(range(recovery.sequences[0], recovery.sequences[-1] + 1))
    if recovery.sequences != expected:
        raise StageAcceptanceError("committed event sequence is not contiguous")


async def _maybe_await(value: object) -> object:
    if inspect.isawaitable(value):
        return await cast(Awaitable[object], value)
    return value


class FaultStageAcceptanceRunner:
    """Execute all required exact-marker cases with cleanup in every path."""

    def __init__(
        self,
        *,
        compose: ComposeController,
        backend: StageBackend,
        controller: StageController,
        project: str,
        worker_container: str,
        termination: str = "kill",
        timeout_seconds: float = 90.0,
        allow_process_kill: bool = False,
        run_id: str | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._compose = compose
        self._backend = backend
        self._controller = controller
        self._project = validate_project(project)
        self._worker_container = _validate_container_selector(worker_container)
        if termination not in {"stop", "kill"}:
            raise ValueError("termination must be stop or kill")
        self._termination = termination
        if timeout_seconds <= 0 or timeout_seconds > 600:
            raise ValueError("timeout_seconds must be between 0 and 600")
        self._timeout_seconds = timeout_seconds
        self._allow_process_kill = allow_process_kill
        self._run_id = run_id.strip() if run_id is not None else None
        if run_id is not None and not self._run_id:
            raise StageNotRun("fault-stage run id cannot be blank")
        self._sleep = sleeper
        self._monotonic = monotonic

    async def run_all(
        self, stages: Sequence[FaultStage] = REQUIRED_STAGES
    ) -> tuple[StageResult, ...]:
        selected = tuple(stages)
        if not selected or any(stage not in REQUIRED_STAGES for stage in selected):
            raise ValueError("fault-stage run must select one or more required stages")
        if len(set(selected)) != len(selected):
            raise ValueError("fault-stage run cannot select duplicate stages")
        deadline = self._monotonic() + (
            self._timeout_seconds * len(selected) + _RUN_ALL_GRACE_SECONDS
        )
        results: list[StageResult] = []
        for index, stage in enumerate(selected):
            if index:
                await self._ensure_worker_pool()
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise StageNotRun("fault-stage all-case budget was exhausted before the next case")
            try:
                result = await asyncio.wait_for(self.run_case(stage), timeout=remaining)
            except TimeoutError as error:
                raise StageNotRun("fault-stage all-case budget was exceeded") from error
            results.append(result)
        return tuple(results)

    async def run_case(self, stage: FaultStage) -> StageResult:
        if stage not in REQUIRED_STAGES:
            raise ValueError(f"unsupported acceptance stage: {stage}")
        case = _MutableCase(CaseIdentity.create(stage, run_id=self._run_id))
        markers: list[dict[str, Any]] = []
        result_status = CaseStatus.FAIL
        reason: str | None = None
        killed_id: str | None = None
        control_released = False
        try:
            # The v2 runtime deliberately has no claim-before checkpoint in
            # SessionWorkerConsumer yet.  Never turn a timeout into a pass or
            # fall back to the v1 PEL/BUSY hook for this case.  The guard is
            # capability based so unit fakes and the legacy v1 path remain
            # usable while the production v2 result stays honestly not_run.
            if (
                stage == FaultStage.ENQUEUE
                and self._backend_scheduler_version() == SchedulerVersion.V2
                and getattr(self._backend, "supports_enqueue_checkpoint", True) is False
            ):
                raise StageNotRun(
                    "v2 claim-before checkpoint is not wired into SessionWorkerConsumer; "
                    "no enqueue kill evidence is trusted"
                )
            await self._bounded(self._backend.provision_case(case.case))
            target = _coerce_container(self._compose.inspect(self._worker_container))
            case.target = validate_worker_container(
                target, project=self._project, explicit_container=self._worker_container
            )
            workers = await self._list_worker_inventory()
            _validate_workers(workers, project=self._project)
            if sum(item.container_id == case.target.container_id for item in workers) != 1:
                raise StageAcceptanceError(
                    "explicit worker container is not exactly one member of the worker service"
                )
            case.workers_before = workers
            markers.append(_marker("preflight.workers_verified", CaseStatus.PASS))

            if stage == FaultStage.ENQUEUE:
                await self._stop_workers(case, workers)
                case.accepted = cast(
                    AcceptanceEvidence,
                    await self._bounded(self._backend.accept_and_publish(case.case)),
                )
                self._require_acceptance(case)
                markers.append(
                    _marker(
                        "acceptance.persisted",
                        CaseStatus.PASS,
                        phase="claim_before",
                        inbound_id=case.accepted.inbound_id,
                        session_id=case.accepted.session_id,
                        stream_id=case.accepted.stream_id,
                        outbox_id=case.accepted.outbox_id,
                        generation=case.accepted.generation,
                    )
                )
                event = FaultStageEvent(
                    stage=stage,
                    tenant_id=case.case.tenant_id,
                    worker_id=case.target.worker_id,
                    # SessionReady carries the outbox event id, not the
                    # inbound-message id.  For v2 ENQUEUE this exact event id
                    # is what the consumer presents at the pre-claim hook;
                    # v1 retains its historical inbound-id contract.
                    inbound_id=(
                        case.accepted.outbox_id
                        if self._backend_scheduler_version() == SchedulerVersion.V2
                        else case.accepted.inbound_id
                    ),
                    stream_id=case.accepted.stream_id,
                )
                case.control_id = cast(str, await _maybe_await(self._controller.arm(event)))
                markers.append(_marker("control.armed", CaseStatus.PASS))
                await self._start_and_wait(case.target.container_id, case.target.worker_id)
            else:
                # Leave only the explicitly selected worker consuming this
                # case.  Otherwise Redis scheduling can assign the turn to a
                # different healthy worker and the marker would be forced to
                # report not_run after a nondeterministic race.
                await self._stop_workers(
                    case, workers, exclude_container_id=case.target.container_id
                )
                case.accepted = cast(
                    AcceptanceEvidence,
                    await self._bounded(self._backend.accept_and_publish(case.case)),
                )
                self._require_acceptance(case)
                markers.append(
                    _marker(
                        "acceptance.persisted",
                        CaseStatus.PASS,
                        phase="claim_before",
                        inbound_id=case.accepted.inbound_id,
                        session_id=case.accepted.session_id,
                        stream_id=case.accepted.stream_id,
                        outbox_id=case.accepted.outbox_id,
                        generation=case.accepted.generation,
                    )
                )
                case.processing = cast(
                    ProcessingEvidence,
                    await self._bounded(
                        self._backend.observe_processing(
                            case.case,
                            case.accepted,
                            require_execution_key=stage == FaultStage.TOOL,
                        )
                    ),
                )
                self._require_processing(case)
                if case.processing.worker_id != case.target.worker_id:
                    raise StageNotRun(
                        "processing owner does not map to the explicit worker container"
                    )
                markers.append(
                    _marker(
                        "turn.processing_observed",
                        CaseStatus.PASS,
                        phase="ack_after_claim",
                        inbound_id=case.processing.inbound_id,
                        session_id=case.processing.session_id,
                        turn_id=case.processing.turn_id,
                        worker_id=case.processing.worker_id,
                        fencing_token=case.processing.fencing_token,
                        execution_key=case.processing.execution_key,
                        acknowledged=case.processing.acknowledged,
                    )
                )
                if self._backend_scheduler_version() == SchedulerVersion.V2:
                    markers.append(
                        _marker(
                            "v2.ack_before_execute",
                            CaseStatus.PASS,
                            stream_id=case.accepted.stream_id,
                            acknowledged=case.processing.acknowledged,
                        )
                    )
                if stage == FaultStage.TOOL:
                    if not case.processing.execution_key:
                        raise StageNotRun("tool processing marker lacks an execution key")
                    # Match ToolExecutor's exact event shape.  It does not
                    # include inbound/fencing fields at this checkpoint, so
                    # adding them here would make the marker fail closed.
                    event = FaultStageEvent(
                        stage=stage,
                        tenant_id=case.case.tenant_id,
                        worker_id=case.processing.worker_id,
                        turn_id=case.processing.turn_id,
                        execution_key=case.processing.execution_key,
                    )
                else:
                    event = FaultStageEvent(
                        stage=stage,
                        tenant_id=case.case.tenant_id,
                        worker_id=case.processing.worker_id,
                        inbound_id=case.processing.inbound_id,
                        turn_id=case.processing.turn_id,
                        fencing_token=case.processing.fencing_token,
                    )
                case.control_id = cast(str, await _maybe_await(self._controller.arm(event)))
                markers.append(_marker("control.armed", CaseStatus.PASS))

            await self._wait_for_exact_marker(case, event)
            markers.append(_marker("marker.entered", CaseStatus.PASS))
            if (
                stage == FaultStage.ENQUEUE
                and self._backend_scheduler_version() == SchedulerVersion.V2
            ):
                markers.append(
                    _marker(
                        "v2.claim_before_observed",
                        CaseStatus.PASS,
                        inbound_id=event.inbound_id,
                        stream_id=event.stream_id,
                    )
                )
            if not self._allow_process_kill:
                raise StageNotRun("process termination requires the explicit kill acknowledgement")
            # This is the only call site that can terminate a worker.  It is
            # reached only after exact marker evidence has been read back.
            # Re-inspect immediately before the destructive operation.  A
            # stale preflight identity must never authorize killing a newly
            # recreated container that happens to reuse the selector.
            current_target = _coerce_container(self._compose.inspect(case.target.container_id))
            case.target = validate_worker_container(
                current_target,
                project=self._project,
                explicit_container=self._worker_container,
            )
            if case.target.worker_id != event.worker_id:
                raise StageAcceptanceError(
                    "worker identity changed after the fault marker; refusing termination"
                )
            self._compose.terminate(case.target.container_id, mode=self._termination)
            killed_id = case.target.container_id
            terminated = await self._wait_for_terminated(
                case.target.container_id,
                expected_worker_id=event.worker_id,
            )
            markers.append(
                _marker(
                    "worker.terminated",
                    CaseStatus.PASS,
                    container_status=terminated.status,
                    pid=terminated.pid,
                )
            )
            if case.control_id:
                await self._release_control(
                    self._controller.release(case.control_id, tenant_id=case.case.tenant_id)
                )
                control_released = True
            old_fencing_token = (
                case.processing.fencing_token
                if stage == FaultStage.TOOL and case.processing is not None
                else event.fencing_token or 0
            )
            # Start one known survivor and observe its takeover before starting
            # the rest.  Starting the complete survivor set first lets a
            # survivor process and commit so quickly that the subsequent
            # recovery query misses the transient lease owner and waits for the
            # full hard timeout.  The finally block still restores every
            # preflight container before this case returns.
            recovery = await self._recover_with_survivors(
                case,
                old_worker_id=event.worker_id,
                old_fencing_token=old_fencing_token,
                killed_container_id=killed_id,
            )
            _validate_recovery(
                recovery,
                old_worker_id=event.worker_id,
                old_fencing_token=old_fencing_token,
                killed_container_id=killed_id,
                project=self._project,
                require_stale_rejection=stage in {FaultStage.TOOL, FaultStage.COMMIT_TXN_OPEN},
            )
            markers.append(_marker("worker.survivors_observed", CaseStatus.PASS))
            if stage in {FaultStage.TOOL, FaultStage.COMMIT_TXN_OPEN}:
                markers.append(_marker("stale_token_rejection_verified", CaseStatus.PASS))
            markers.append(_marker("turn.single_contiguous_verified", CaseStatus.PASS))
            await self._bounded(self._backend.verify_final(case.case, recovery))
            markers.append(_marker("turn.commit_verified", CaseStatus.PASS))
            markers.append(_marker("outbound.intent_verified", CaseStatus.PASS))
            if stage == FaultStage.TOOL:
                markers.append(_marker("tool.idempotent_execution_verified", CaseStatus.PASS))
            result_status = CaseStatus.PASS
        except StageNotRun as error:
            result_status = CaseStatus.NOT_RUN
            reason = str(error)
        except StageAcceptanceError as error:
            result_status = CaseStatus.FAIL
            reason = str(error)
        except Exception as error:
            result_status = CaseStatus.FAIL
            reason = f"{type(error).__name__}: execution failed"
        finally:
            if case.control_id and not control_released:
                try:
                    await self._release_control(
                        self._controller.release(case.control_id, tenant_id=case.case.tenant_id)
                    )
                    control_released = True
                except Exception:
                    if reason is None:
                        result_status = CaseStatus.FAIL
                        reason = "fault control release was not acknowledged"
            # Clean the generated tenant while the deliberately terminated
            # worker is still stopped.  Restoring it first can let that old
            # process reconnect and race the tenant-scoped DELETEs (or retain
            # a lease/connection across the cleanup retry window).  Cleanup is
            # exact and idempotent; it must happen before restoration, but a
            # cleanup failure must never prevent container restoration.
            try:
                await self._cleanup_case_with_retry(case.case)
            except Exception:
                if reason is None:
                    result_status = CaseStatus.FAIL
                    reason = "test tenant cleanup failed"
            finally:
                # Restore only containers observed before this case, including
                # an explicitly terminated target that was intentionally left
                # running for COMMIT ownership.  Never use ``down -v`` or
                # remove a Compose volume as part of acceptance.
                restore_ids = list(case.stopped_ids)
                if killed_id and killed_id not in restore_ids:
                    restore_ids.append(killed_id)
                for container_id in reversed(restore_ids):
                    try:
                        expected_worker_id = next(
                            (
                                worker.worker_id
                                for worker in case.workers_before
                                if worker.container_id == container_id
                            ),
                            None,
                        )
                        await self._start_and_wait(container_id, expected_worker_id)
                    except Exception:
                        if reason is None:
                            result_status = CaseStatus.FAIL
                            reason = "worker cleanup failed"
        return StageResult(
            stage=stage,
            status=result_status,
            case=case.case,
            markers=tuple(markers),
            reason=reason,
            control_id=case.control_id,
            killed_container_id=killed_id,
        )

    async def _stop_workers(
        self,
        case: _MutableCase,
        workers: Sequence[WorkerContainer],
        *,
        exclude_container_id: str | None = None,
    ) -> None:
        for worker in workers:
            if worker.container_id == exclude_container_id:
                continue
            self._compose.stop(worker.container_id)
            case.stopped_ids.append(worker.container_id)

    async def _ensure_worker_pool(self) -> None:
        """Prove that the next case starts with the original healthy pool."""

        workers = await self._list_worker_inventory()
        if not workers:
            raise StageNotRun("worker pool inventory is empty before the next fault-stage case")
        identities: set[str] = set()
        for worker in workers:
            if worker.project != self._project or worker.service != WORKER_SERVICE:
                raise StageAcceptanceError("worker pool inventory is outside the fault project")
            if not worker.worker_id.strip() or worker.container_id in identities:
                raise StageAcceptanceError("worker pool inventory has an invalid identity")
            identities.add(worker.container_id)
        for worker in workers:
            if worker.status.lower() == "running" and worker.health == "healthy":
                continue
            try:
                await asyncio.wait_for(
                    self._start_and_wait(worker.container_id, worker.worker_id),
                    timeout=min(self._timeout_seconds, _WORKER_RESTORE_TIMEOUT_SECONDS),
                )
            except TimeoutError as error:
                raise StageNotRun(
                    "worker pool was not restored before the next fault-stage case"
                ) from error
        restored = await self._list_worker_inventory()
        _validate_workers(
            restored,
            project=self._project,
            minimum=4,
            require_independent=True,
        )

    async def _list_worker_inventory(self) -> tuple[WorkerContainer, ...]:
        """Read a non-empty worker inventory within one bounded convergence window.

        Docker's project/service listing can transiently be empty immediately
        after a worker restore even though the preflight identities are still
        valid.  Only an empty result is retried; malformed, partial, or
        cross-project inventories continue to the existing fail-closed
        validation checks.  Returning an empty tuple after the deadline keeps
        the caller's stage-specific reason intact for a persistent outage.
        """

        deadline = self._monotonic() + min(
            self._timeout_seconds, _WORKER_INVENTORY_CONVERGENCE_TIMEOUT_SECONDS
        )
        for delay in (0.0, *_WORKER_INVENTORY_RETRY_DELAYS_SECONDS):
            workers = tuple(
                _coerce_container(item) for item in self._compose.list_workers(self._project)
            )
            if workers:
                return workers
            if delay == 0.0:
                continue
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                break
            await self._sleep(min(delay, remaining))
        return ()

    async def _recover_with_survivors(
        self,
        case: _MutableCase,
        *,
        old_worker_id: str,
        old_fencing_token: int,
        killed_container_id: str,
    ) -> RecoveryEvidence:
        """Rotate healthy survivors until one provides takeover evidence."""

        survivors = tuple(
            worker
            for worker in case.workers_before
            if worker.container_id != killed_container_id
            and worker.container_id in case.stopped_ids
        )
        if not survivors:
            raise StageNotRun("no stopped survivor was available after worker termination")
        for worker in survivors:
            # Start the recovery observer before Docker health becomes ready.
            # The worker can reclaim an already-idle PEL entry as soon as its
            # event loop starts, and a fast deterministic turn may claim and
            # commit before the dependency-only health probe returns.  The
            # observer task therefore owns the whole takeover window; it is
            # always drained on startup failure, timeout, or cancellation.
            recovery_task = asyncio.create_task(
                self._backend.wait_for_recovery(
                    case.case,
                    old_worker_id=old_worker_id,
                    old_fencing_token=old_fencing_token,
                    killed_container_id=killed_container_id,
                ),
                name=f"fault-stage-recovery:{case.case.case_id}:{worker.worker_id}",
            )
            try:
                # A scheduling turn is required because a running+healthy
                # fake/container inspection can otherwise return without an
                # await, defeating the ordering guarantee above.
                await asyncio.sleep(0)
                await self._start_and_wait(worker.container_id, worker.worker_id)
            except BaseException:
                if not recovery_task.done():
                    recovery_task.cancel()
                await asyncio.gather(recovery_task, return_exceptions=True)
                raise
            try:
                return cast(
                    RecoveryEvidence,
                    await asyncio.wait_for(
                        recovery_task,
                        # ``wait_for_recovery`` has one bounded window for
                        # takeover, a second one for final commit, and a
                        # bounded final inventory convergence window.  The
                        # outer observer budget must cover all three or a
                        # takeover near the first deadline is cancelled before
                        # its commit/evidence inventory can be observed.
                        timeout=min(
                            self._timeout_seconds,
                            2 * _SURVIVOR_SCHEDULING_TIMEOUT_SECONDS
                            + _WORKER_INVENTORY_CONVERGENCE_TIMEOUT_SECONDS,
                        ),
                    ),
                )
            except TimeoutError as error:
                # The backend itself has bounded takeover and final-commit
                # phases.  Reaching the larger outer budget therefore means
                # recovery did not complete safely; do not treat an observer
                # timeout as permission to rotate another survivor.
                if not recovery_task.done():
                    recovery_task.cancel()
                await asyncio.gather(recovery_task, return_exceptions=True)
                raise StageNotRun(
                    "lease takeover was not observed from a healthy survivor"
                ) from error
            except StageNotRun as error:
                if not recovery_task.done():
                    recovery_task.cancel()
                await asyncio.gather(recovery_task, return_exceptions=True)
                if str(error) == _TAKEOVER_NOT_OBSERVED_REASON:
                    # This survivor did not become the owner within its
                    # bounded scheduling window.  Keep it running and give
                    # the next healthy survivor a chance.  A final-commit
                    # StageNotRun, or any other reason, remains fail-closed.
                    continue
                raise
            except BaseException:
                if not recovery_task.done():
                    recovery_task.cancel()
                await asyncio.gather(recovery_task, return_exceptions=True)
                raise
        raise StageNotRun("lease takeover was not observed from a healthy survivor")

    async def _start_and_wait(
        self, container_id: str, expected_worker_id: str | None = None
    ) -> None:
        """Start one preflight container and prove it is healthy before use.

        A successful ``docker start`` only means the process was requested; it
        does not prove the worker passed its health check.  Polling the same
        container identity prevents the next case from racing a still-starting
        worker and keeps survivor evidence tied to the preflight worker ID.
        """

        deadline = self._monotonic() + self._timeout_seconds
        inspected = _coerce_container(self._compose.inspect(container_id))
        self._validate_restore_identity(inspected, container_id, expected_worker_id)
        if inspected.status.lower() != "running":
            self._compose.start(container_id)
        poll_delay = _DOCKER_INSPECT_POLL_MIN_SECONDS
        while self._monotonic() < deadline:
            inspected = _coerce_container(self._compose.inspect(container_id))
            self._validate_restore_identity(inspected, container_id, expected_worker_id)
            if inspected.status.lower() == "running" and inspected.health == "healthy":
                return
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                break
            await self._sleep(min(poll_delay, remaining))
            poll_delay = min(poll_delay * 2, _DOCKER_INSPECT_POLL_MAX_SECONDS)
        raise StageNotRun("worker did not become running and healthy before timeout")

    async def _wait_for_terminated(
        self, container_id: str, *, expected_worker_id: str
    ) -> WorkerContainer:
        """Prove the destructive Docker operation actually stopped the target."""

        deadline = self._monotonic() + self._timeout_seconds
        poll_delay = _DOCKER_INSPECT_POLL_MIN_SECONDS
        while self._monotonic() < deadline:
            inspected = _coerce_container(self._compose.inspect(container_id))
            self._validate_restore_identity(inspected, container_id, expected_worker_id)
            if inspected.status.lower() in {"exited", "dead"}:
                return inspected
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                break
            await self._sleep(min(poll_delay, remaining))
            poll_delay = min(poll_delay * 2, _DOCKER_INSPECT_POLL_MAX_SECONDS)
        raise StageNotRun("worker termination was requested but the container is still running")

    def _backend_scheduler_version(self) -> SchedulerVersion | None:
        value = getattr(self._backend, "scheduler_version", None)
        if value is None:
            return None
        try:
            return SchedulerVersion(value)
        except (TypeError, ValueError):
            raise StageAcceptanceError("backend exposed an unsupported scheduler version") from None

    def _validate_restore_identity(
        self,
        inspected: WorkerContainer,
        container_id: str,
        expected_worker_id: str | None,
    ) -> None:
        if inspected.container_id != container_id:
            raise StageAcceptanceError("Docker selector resolved to a different container")
        if inspected.project != self._project or inspected.service != WORKER_SERVICE:
            raise StageAcceptanceError("restored container is outside the fault worker project")
        if expected_worker_id and inspected.worker_id != expected_worker_id:
            raise StageAcceptanceError("worker identity changed while restoring container")

    def _require_acceptance(self, case: _MutableCase) -> None:
        if case.accepted is None or not case.accepted.inbound_id.strip():
            raise StageNotRun("acceptance did not return this case's inbound id")
        if not case.accepted.stream_id.strip():
            raise StageNotRun("acceptance did not return a Redis stream id")
        if self._backend_scheduler_version() == SchedulerVersion.V2:
            if not case.accepted.session_id or not case.accepted.session_id.strip():
                raise StageNotRun("v2 acceptance lacks the authoritative session id")
            if not case.accepted.outbox_id or not case.accepted.outbox_id.strip():
                raise StageNotRun("v2 acceptance lacks the authoritative outbox event id")
            if case.accepted.generation is None or case.accepted.generation < 1:
                raise StageNotRun("v2 acceptance lacks a positive mailbox generation")
        if case.accepted.session_id:
            case.case = replace(case.case, session_id=case.accepted.session_id)
        case.case = replace(case.case, inbound_id=case.accepted.inbound_id)

    def _require_processing(self, case: _MutableCase) -> None:
        processing = case.processing
        if processing is None:
            raise StageNotRun("processing marker is unavailable")
        if processing.inbound_id != case.case.inbound_id:
            raise StageNotRun("processing owner is for a different inbound")
        if processing.session_id != case.case.session_id:
            raise StageNotRun("processing owner is for a different session")
        if not processing.turn_id.strip() or processing.fencing_token < 1:
            raise StageNotRun("processing marker lacks turn or fencing identity")
        if self._backend_scheduler_version() == SchedulerVersion.V2 and not processing.acknowledged:
            raise StageNotRun("v2 processing evidence did not prove ACK before execution")

    async def _wait_for_exact_marker(self, case: _MutableCase, event: FaultStageEvent) -> None:
        if case.control_id is None:
            raise StageAcceptanceError("control was not armed")
        deadline = self._monotonic() + self._timeout_seconds
        saw_mismatch = False
        while self._monotonic() < deadline:
            row = await self._bounded(
                self._backend.read_control(case.case.tenant_id, case.control_id)
            )
            if row is not None:
                if _control_matches(row, event, case.control_id):
                    return
                saw_mismatch = True
                if _field(row, "status") == "entered":
                    raise StageAcceptanceError("fault marker entered with mismatched identity")
            await self._sleep(0.05)
        if saw_mismatch:
            raise StageAcceptanceError("fault marker never matched the armed identity")
        raise StageNotRun("exact fault-stage marker was not observed before timeout")

    async def _bounded(self, value: object) -> object:
        """Apply the case hard timeout to every injected async operation."""

        return await asyncio.wait_for(_maybe_await(value), timeout=self._timeout_seconds)

    async def _cleanup_case_with_retry(self, case: CaseIdentity) -> None:
        """Retry only this generated case's cleanup after transient races.

        The backend owns the tenant-scoped, run-bound delete set.  Calling it
        afresh on every attempt is important because the first attempt may
        have been interrupted while a restored worker still held a row or
        connection.  The final exception is deliberately handled by
        ``run_case`` as a generic, secret-free report reason.
        """

        delays = (0.0, *_CLEANUP_RETRY_DELAYS_SECONDS)
        for attempt, delay in enumerate(delays):
            if delay:
                await self._sleep(delay)
            try:
                await asyncio.wait_for(
                    _maybe_await(self._backend.cleanup_case(case)),
                    timeout=min(self._timeout_seconds, _CLEANUP_TIMEOUT_SECONDS),
                )
            except Exception:
                if attempt == len(delays) - 1:
                    raise
            else:
                return

    async def _release_control(self, value: object) -> None:
        released = await self._bounded(value)
        if not bool(released):
            raise StageAcceptanceError("fault control release was not acknowledged")


class DockerComposeController:
    """Minimal Docker facade with label and status checks at the caller."""

    def __init__(self, *, project: str) -> None:
        self.project = validate_project(project)

    @staticmethod
    def _run(*args: str) -> str:
        completed = subprocess.run(  # noqa: S603 - executable and args are fixed by this module
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if completed.returncode != 0:
            raise StageAcceptanceError(completed.stderr.strip() or "Docker command failed")
        return completed.stdout

    def inspect(self, container: str) -> WorkerContainer:
        raw = self._run("docker", "inspect", "--format", "{{json .}}", container).strip()
        try:
            payload = json.loads(raw)
            labels = payload["Config"]["Labels"] or {}
            environment = {
                item.split("=", 1)[0]: item.split("=", 1)[1]
                for item in (payload["Config"].get("Env") or [])
                if "=" in item
            }
            state = payload["State"]
            health = state.get("Health")
            pid = state.get("Pid")
            if not isinstance(pid, int) or isinstance(pid, bool):
                raise StageAcceptanceError("Docker inspection did not expose a process PID")
            hostname = str(payload["Config"].get("Hostname") or payload["Id"][:12])
            worker_id = str(labels.get("trpc.worker_id") or f"worker-{hostname}")
            return WorkerContainer(
                container_id=str(payload["Id"]),
                project=str(labels.get("com.docker.compose.project", "")),
                service=str(labels.get("com.docker.compose.service", "")),
                status=str(state.get("Status", "")),
                health=None if health is None else str(health.get("Status", "")),
                worker_id=worker_id,
                scheduler_version=environment.get("TRPC_SERVICE_SCHEDULER_VERSION"),
                redis_stream=environment.get("TRPC_SERVICE_REDIS_STREAM"),
                redis_group=environment.get("TRPC_SERVICE_REDIS_CONSUMER_GROUP"),
                pid=pid,
            )
        except (KeyError, TypeError, ValueError) as error:
            raise StageAcceptanceError(
                "Docker inspection did not expose safe worker identity"
            ) from error

    def list_workers(self, project: str) -> tuple[WorkerContainer, ...]:
        project = validate_project(project)
        output = self._run(
            "docker",
            "ps",
            "-a",
            "--no-trunc",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--filter",
            "label=com.docker.compose.service=worker",
            "--format",
            "{{.ID}}",
        )
        return tuple(self.inspect(item) for item in output.splitlines() if item.strip())

    def stop(self, container_id: str) -> None:
        self._run("docker", "stop", "--time", "10", container_id)

    def start(self, container_id: str) -> None:
        self._run("docker", "start", container_id)

    def terminate(self, container_id: str, *, mode: str) -> None:
        if mode == "stop":
            self.stop(container_id)
        elif mode == "kill":
            self._run("docker", "kill", "--signal", "SIGKILL", container_id)
        else:
            raise ValueError("termination must be stop or kill")


class PostgresStageMarkerReader:
    """Read exact marker rows under the same tenant RLS context as the controller."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def read_control(self, tenant_id: str, control_id: str) -> ControlEvidence | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            row = await connection.fetchrow(
                """
                SELECT control_id,status,marker_status,marker_worker_id,marker_inbound_id,
                       marker_turn_id,marker_execution_key,marker_stream_id,marker_fencing_token
                  FROM fault_stage_controls
                 WHERE tenant_id=$1 AND control_id=$2
                """,
                tenant_id,
                control_id,
            )
        if row is None:
            return None
        return ControlEvidence(
            control_id=str(row["control_id"]),
            status=str(row["status"]),
            marker_status=None if row["marker_status"] is None else str(row["marker_status"]),
            marker_worker_id=row["marker_worker_id"],
            marker_inbound_id=row["marker_inbound_id"],
            marker_turn_id=row["marker_turn_id"],
            marker_execution_key=row["marker_execution_key"],
            marker_stream_id=row["marker_stream_id"],
            marker_fencing_token=row["marker_fencing_token"],
        )


@dataclass
class _RuntimeCaseState:
    case: CaseIdentity
    binding_id: str
    app_id: str
    account_id: str
    acceptance: Acceptance | None = None
    accepted: AcceptanceEvidence | None = None
    processing: ProcessingEvidence | None = None


class PostgresRuntimeStageBackend:
    """Concrete PostgreSQL/Redis backend for the marker-driven cases.

    The backend creates one short-lived tenant, app, revision, and Feishu
    binding per case.  It uses :class:`TenantRuntime` for the authoritative
    acceptance transaction, updates only that case's outbox row, and waits on
    the authoritative session/turn rows.  No content is returned as evidence.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        redis: Any,
        *,
        compose: ComposeController,
        project: str,
        routing_key: bytes,
        binding_seed: str,
        scheduler_version: SchedulerVersion = SchedulerVersion.V2,
        stream: str | None = None,
        group: str | None = None,
        offline_agent_delay_seconds: float,
        timeout_seconds: float = 90.0,
    ) -> None:
        if len(routing_key) < 32:
            raise ValueError("routing HMAC key must contain at least 32 bytes")
        if not binding_seed.strip():
            raise ValueError("fault-stage binding seed cannot be blank")
        if offline_agent_delay_seconds <= 0 or offline_agent_delay_seconds > 5:
            raise StageNotRun(
                "COMMIT_TXN_OPEN requires a positive test-only offline agent delay (<=5s)"
            )
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0 or timeout_seconds > 600:
            raise StageNotRun("fault-stage backend timeout must be finite, positive, and <=600s")
        self._pool = pool
        self._redis = redis
        self._compose = compose
        self._project = validate_project(project)
        self._routing_key = routing_key
        self._binding_seed = binding_seed.strip()
        try:
            self._scheduler_version = SchedulerVersion(scheduler_version)
        except ValueError as error:
            raise StageNotRun("fault-stage scheduler version must be v1 or v2") from error
        expected_stream, expected_group = _scheduler_transport(self._scheduler_version)
        selected_stream = expected_stream if stream is None else stream
        selected_group = expected_group if group is None else group
        if selected_stream != expected_stream or selected_group != expected_group:
            raise StageNotRun("fault-stage scheduler version and Redis stream/group do not match")
        if self._scheduler_version == SchedulerVersion.V2:
            self._queue: Any = SessionReadyQueue(
                redis, stream=selected_stream, group=selected_group
            )
            self._publisher: Any = SessionReadyOutboxQueue(self._queue)
        else:
            self._queue = RedisStreamQueue(redis, stream=selected_stream, group=selected_group)
            self._publisher = self._queue
        self._repository = PostgresRuntimeRepository(pool)
        self._control_plane = PostgresControlPlaneRepository(pool)
        self._marker_reader = PostgresStageMarkerReader(pool)
        self._cases: dict[str, _RuntimeCaseState] = {}
        self._offline_agent_delay_seconds = offline_agent_delay_seconds
        self._timeout_seconds = timeout_seconds

    @property
    def scheduler_version(self) -> SchedulerVersion:
        """Expose the wire contract to the acceptance state machine."""

        return self._scheduler_version

    @property
    def supports_enqueue_checkpoint(self) -> bool:
        """Whether the selected runtime has a v2 claim-before checkpoint.

        The SessionReady consumer exposes this checkpoint only when the worker
        is started with the explicit test-only fault controller wiring.  The
        capability is reported here so the acceptance runner can distinguish a
        v2 runtime that supports the checkpoint from an older image; the
        worker/container checks still fail closed when that wiring is absent.
        """

        return True

    @asynccontextmanager
    async def _tenant_transaction(self, tenant_id: str) -> Any:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            yield connection

    def _state(self, case: CaseIdentity) -> _RuntimeCaseState:
        try:
            return self._cases[case.case_id]
        except KeyError as error:
            raise StageAcceptanceError("fault-stage case was not provisioned") from error

    async def provision_case(self, case: CaseIdentity) -> None:
        suffix = case.case_id.removeprefix("case-")
        state = _RuntimeCaseState(
            case=case,
            binding_id=f"{self._binding_seed}-{suffix[:48]}",
            app_id=f"fault-app-{suffix[:48]}",
            account_id=f"fault-account-{suffix[:48]}",
        )
        self._cases[case.case_id] = state
        actor = "fault-stage-acceptance"
        request_hash = hashlib.sha256(case.case_id.encode()).hexdigest()
        created = await self._control_plane.create_tenant(
            tenant_id=case.tenant_id,
            display_name="fault-stage-acceptance",
            actor=actor,
            idempotency_key=f"{case.case_id}:tenant",
            request_hash=request_hash,
        )
        # Every real fault case uses the same bounded deterministic model/tool
        # wiring.  The tool is registered by the worker only when the worker
        # is in Environment.TEST with fault injection explicitly enabled.
        # Its execution ledger row is the durable idempotency evidence for the
        # TOOL case; it never receives or echoes the inbound text.
        config = {
            "model": {"provider": "offline", "model": "offline"},
            "storage": {"profile_id": "fault-stage"},
            "tools": {
                "allow": [DETERMINISTIC_FAULT_TOOL_NAME],
                "classifications": {DETERMINISTIC_FAULT_TOOL_NAME: "idempotent"},
            },
        }
        revision = await self._control_plane.create_config_revision(
            tenant_id=case.tenant_id,
            app_id=state.app_id,
            config=config,
            actor=actor,
            expected_version=int(created["control_version"]),
            idempotency_key=f"{case.case_id}:config",
            request_hash=hashlib.sha256(f"{case.case_id}:config".encode()).hexdigest(),
        )
        binding = ChannelBinding(
            binding_id=state.binding_id,
            tenant_id=case.tenant_id,
            app_id=state.app_id,
            channel=Channel.FEISHU,
            account_id=state.account_id,
            capabilities=frozenset({"text"}),
        )
        await self._control_plane.put_binding(
            tenant_id=case.tenant_id,
            binding_id=state.binding_id,
            binding=binding,
            actor=actor,
            expected_version=int(revision["tenant_control_version"]),
            idempotency_key=f"{case.case_id}:binding",
            request_hash=hashlib.sha256(f"{case.case_id}:binding".encode()).hexdigest(),
        )

    async def accept_and_publish(self, case: CaseIdentity) -> AcceptanceEvidence:
        state = self._state(case)
        envelope = InboundEnvelope(
            channel=Channel.FEISHU,
            account_id=state.account_id,
            external_message_id=case.message_id,
            external_user_id=f"fault-user-{case.case_id.removeprefix('case-')}",
            conversation_kind=ConversationKind.DIRECT,
            payload_kind=PayloadKind.TEXT,
            # This value is test input only and is never included in a report.
            text="fault-stage acceptance probe",
        )
        acceptance = await TenantRuntime(
            self._repository,
            routing_key=self._routing_key,
            scheduler_version=self._scheduler_version,
        ).accept(state.binding_id, envelope)
        state.acceptance = acceptance
        owner_id = f"fault-stage-{uuid4().hex}"
        generation: int | None = None
        scheduler_version = getattr(self, "_scheduler_version", SchedulerVersion.V1)
        if scheduler_version == SchedulerVersion.V2:
            generation = await self._case_mailbox_generation(
                acceptance.context.tenant_id, acceptance.context.session_id
            )
        row = await self._claim_case_outbox(
            tenant_id=acceptance.context.tenant_id,
            inbound_id=acceptance.inbound_id,
            session_id=acceptance.context.session_id,
            generation=generation,
            owner_id=owner_id,
        )
        if row is None:
            raise StageAcceptanceError("durable inbound outbox row was not claimable")
        await self._publisher.ensure_group()
        stream_id = await self._publisher.publish(row)
        if not stream_id:
            raise StageAcceptanceError("Redis Streams did not return a stream id")
        await self._repository.mark_outbox_published(
            acceptance.context.tenant_id, row.outbox_id, owner_id=owner_id
        )
        evidence = AcceptanceEvidence(
            inbound_id=acceptance.inbound_id,
            stream_id=stream_id,
            session_id=acceptance.context.session_id,
            outbox_id=row.outbox_id,
            generation=(
                _payload_int(row.payload, "generation")
                if self._scheduler_version == SchedulerVersion.V2
                else None
            ),
        )
        state.accepted = evidence
        return evidence

    async def _case_mailbox_generation(self, tenant_id: str, session_id: str) -> int:
        async with self._tenant_transaction(tenant_id) as connection:
            value = await connection.fetchval(
                """
                SELECT queue_generation FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                """,
                tenant_id,
                session_id,
            )
        if value is None or int(value) < 1:
            raise StageNotRun("v2 acceptance did not create a ready mailbox generation")
        return int(value)

    async def _claim_case_outbox(
        self,
        *,
        tenant_id: str,
        inbound_id: str,
        owner_id: str,
        session_id: str | None = None,
        generation: int | None = None,
    ) -> OutboxRecord | None:
        if self._scheduler_version == SchedulerVersion.V2:
            if not session_id or generation is None or generation < 1:
                raise StageNotRun("v2 outbox claim lacks session generation identity")
            event_type = SESSION_READY_EVENT_V2
            aggregate_id = session_id
        else:
            event_type = "inbound.accepted"
            aggregate_id = inbound_id
        async with self._tenant_transaction(tenant_id) as connection:
            if self._scheduler_version == SchedulerVersion.V2:
                row = await connection.fetchrow(
                    """
                    UPDATE outbox_events
                       SET claimed_by=$3, claim_expires_at=now()+interval '60 seconds',
                           attempts=attempts+1
                     WHERE tenant_id=$1 AND aggregate_id=$2
                       AND event_type=$4 AND published_at IS NULL
                       AND (claim_expires_at IS NULL OR claim_expires_at<=now())
                       AND (payload_json->>'generation')::bigint=$5
                    RETURNING outbox_id,tenant_id,event_type,aggregate_id,payload_json,
                              trace_headers,attempts
                    """,
                    tenant_id,
                    aggregate_id,
                    owner_id,
                    event_type,
                    generation,
                )
            else:
                row = await connection.fetchrow(
                    """
                UPDATE outbox_events
                   SET claimed_by=$3, claim_expires_at=now()+interval '60 seconds',
                       attempts=attempts+1
                 WHERE tenant_id=$1 AND aggregate_id=$2
                   AND event_type=$4 AND published_at IS NULL
                   AND (claim_expires_at IS NULL OR claim_expires_at<=now())
                RETURNING outbox_id,tenant_id,event_type,aggregate_id,payload_json,
                          trace_headers,attempts
                """,
                    tenant_id,
                    aggregate_id,
                    owner_id,
                    event_type,
                )
        if row is None:
            return None
        payload = row["payload_json"]
        trace_headers = row["trace_headers"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        if isinstance(trace_headers, str):
            trace_headers = json.loads(trace_headers)
        if not isinstance(payload, dict) or not isinstance(trace_headers, dict):
            raise StageAcceptanceError("outbox evidence is not a JSON object")
        return OutboxRecord(
            outbox_id=str(row["outbox_id"]),
            tenant_id=str(row["tenant_id"]),
            event_type=str(row["event_type"]),
            aggregate_id=str(row["aggregate_id"]),
            payload=payload,
            trace_headers=trace_headers,
            attempts=int(row["attempts"]),
        )

    async def observe_processing(
        self,
        case: CaseIdentity,
        accepted: AcceptanceEvidence,
        *,
        require_execution_key: bool = False,
    ) -> ProcessingEvidence:
        state = self._state(case)
        if state.acceptance is None:
            raise StageNotRun("acceptance authority is unavailable")
        deadline = time.monotonic() + self._timeout_seconds
        while time.monotonic() < deadline:
            row = await self._turn_row(
                state.acceptance.context.tenant_id,
                accepted.inbound_id,
                require_execution_key=require_execution_key,
            )
            if (
                row is not None
                and row["status"] == "processing"
                and (
                    self._scheduler_version == SchedulerVersion.V1
                    or (
                        row["mailbox_status"] == "RUNNING"
                        and str(row["mailbox_processing_inbound_id"] or "") == accepted.inbound_id
                        and int(row["mailbox_lease_epoch"] or 0) == int(row["lease_epoch"] or 0)
                    )
                )
                and (not require_execution_key or row["execution_key"])
            ):
                acknowledged = False
                if self._scheduler_version == SchedulerVersion.V2:
                    acknowledged = await self._v2_delivery_acknowledged(state, accepted)
                    if not acknowledged:
                        await asyncio.sleep(0.05)
                        continue
                processing = ProcessingEvidence(
                    inbound_id=accepted.inbound_id,
                    session_id=state.acceptance.context.session_id,
                    turn_id=str(row["turn_id"]),
                    worker_id=str(row["lease_owner"] or ""),
                    fencing_token=int(row["fencing_token"]),
                    execution_key=(
                        str(row["execution_key"]) if row["execution_key"] is not None else None
                    ),
                    acknowledged=acknowledged,
                )
                if processing.worker_id and processing.fencing_token > 0:
                    state.processing = processing
                    return processing
            await asyncio.sleep(0.05)
        raise StageNotRun("processing turn was not observed before the hard timeout")

    async def _v2_delivery_acknowledged(
        self,
        state: _RuntimeCaseState,
        accepted: AcceptanceEvidence,
    ) -> bool:
        """Prove the exact wake-up was ACKed before a turn can execute.

        ``session_turns.status=processing`` proves the PostgreSQL claim.  The
        exact PEL lookup then proves the Redis claim window has ended.  The
        payload is decoded as v2 as well, so an unrelated stream id cannot
        satisfy this phase.  This is intentionally read-only; cleanup is
        performed only after final commit or in the case-scoped cleanup path.
        """

        try:
            pending = await self._redis.xpending_range(
                self._queue.stream,
                self._queue.group,
                min=accepted.stream_id,
                max=accepted.stream_id,
                count=1,
            )
            if pending:
                return False
            fields = await self._read_exact_delivery_fields(state)
            if fields is not None:
                return True
            # SessionReadyQueue XACKs and then XDELs the exact v2 entry.  Once
            # the exact PEL lookup is empty, an absent entry is therefore the
            # successful ACK+XDEL state, not missing evidence.  This mirrors
            # the final-delivery verifier below and prevents the processing
            # observation from waiting until the hard timeout after a normal
            # worker ACK.
            return self._scheduler_version == SchedulerVersion.V2
        except StageNotRun:
            raise
        except (AttributeError, NotImplementedError, RedisError, TypeError, ValueError) as error:
            raise StageNotRun("v2 ACK evidence operations are unavailable") from error

    async def _read_exact_delivery_fields(
        self, state: _RuntimeCaseState
    ) -> Mapping[object, object] | None:
        accepted = state.accepted
        if accepted is None:
            raise StageNotRun("Redis evidence lacks the accepted delivery")
        entries = await self._redis.xrange(
            self._queue.stream,
            min=accepted.stream_id,
            max=accepted.stream_id,
            count=1,
        )
        if not entries or _redis_text(entries[0][0]) != accepted.stream_id:
            return None
        candidate = entries[0][1]
        if not isinstance(candidate, Mapping):
            raise StageNotRun("Redis stream fields are unavailable for this case")
        scheduler_version = getattr(self, "_scheduler_version", SchedulerVersion.V1)
        if scheduler_version == SchedulerVersion.V2:
            try:
                ready = SessionReadyCodec.decode(candidate)
            except ValueError as error:
                raise StageNotRun(
                    "Redis stream entry is not a valid SessionReady v2 notice"
                ) from error
            if (
                ready.event_id != accepted.outbox_id
                or ready.tenant_id != state.case.tenant_id
                or ready.session_id != (accepted.session_id or state.case.session_id)
                or ready.generation != accepted.generation
            ):
                raise StageNotRun("Redis SessionReady entry does not match this case")
        else:
            outbox_value = candidate.get("outbox_id", candidate.get(b"outbox_id"))
            if _redis_text(outbox_value) != accepted.outbox_id:
                raise StageNotRun("Redis stream entry does not match this case outbox")
        return candidate

    async def _turn_row(
        self,
        tenant_id: str,
        inbound_id: str,
        *,
        require_execution_key: bool = False,
    ) -> Any | None:
        async with self._tenant_transaction(tenant_id) as connection:
            return await connection.fetchrow(
                """
                SELECT t.turn_id,t.status,t.fencing_token,t.attempt,
                       s.session_id,s.lease_owner,s.lease_epoch,s.lease_expires_at,
                       m.status AS mailbox_status,
                       m.processing_inbound_id AS mailbox_processing_inbound_id,
                       m.processing_sequence AS mailbox_processing_sequence,
                       m.lease_owner AS mailbox_lease_owner,
                       m.lease_epoch AS mailbox_lease_epoch,
                       m.resolved_sequence AS mailbox_resolved_sequence,
                       te.execution_key,te.status AS tool_status
                  FROM session_turns t
                  JOIN sessions s ON s.tenant_id=t.tenant_id AND s.session_id=t.session_id
                  LEFT JOIN session_mailboxes m
                    ON m.tenant_id=t.tenant_id AND m.session_id=t.session_id
                  LEFT JOIN tool_executions te
                    ON te.tenant_id=t.tenant_id AND te.turn_id=t.turn_id
                   AND te.status='started'
                 WHERE t.tenant_id=$1 AND t.inbound_id=$2::uuid
                   AND ($3::boolean = false OR te.execution_key IS NOT NULL)
                 ORDER BY t.attempt DESC
                 LIMIT 1
                """,
                tenant_id,
                inbound_id,
                require_execution_key,
            )

    async def read_control(self, tenant_id: str, control_id: str) -> ControlEvidence | None:
        return await self._marker_reader.read_control(tenant_id, control_id)

    async def wait_for_recovery(
        self,
        case: CaseIdentity,
        *,
        old_worker_id: str,
        old_fencing_token: int,
        killed_container_id: str,
    ) -> RecoveryEvidence:
        state = self._state(case)
        acceptance = state.acceptance
        if acceptance is None:
            raise StageNotRun("acceptance authority is unavailable")
        # Takeover and final-commit observation are separate bounded phases.
        # A takeover observed near the end of its window must still receive a
        # complete final-commit window; the owner/epoch captured below remains
        # valid even after commit clears the live session lease.
        phase_timeout = min(self._timeout_seconds, _SURVIVOR_SCHEDULING_TIMEOUT_SECONDS)
        takeover_deadline = time.monotonic() + phase_timeout
        takeover: Any | None = None
        while time.monotonic() < takeover_deadline:
            row = await self._turn_row(acceptance.context.tenant_id, acceptance.inbound_id)
            if (
                row is not None
                and str(row["lease_owner"] or "")
                and str(row["lease_owner"]) != old_worker_id
                and int(row["lease_epoch"]) > old_fencing_token
                and (
                    self._scheduler_version == SchedulerVersion.V1
                    or (
                        row["mailbox_status"] == "RUNNING"
                        and str(row["mailbox_processing_inbound_id"] or "") == acceptance.inbound_id
                        and str(row["mailbox_lease_owner"] or "") == str(row["lease_owner"])
                        and int(row["mailbox_lease_epoch"] or 0) == int(row["lease_epoch"])
                    )
                )
            ):
                takeover = row
                break
            await asyncio.sleep(0.05)
        if takeover is None:
            raise StageNotRun("lease takeover was not observed before the hard timeout")

        takeover_owner = str(takeover["lease_owner"])
        takeover_epoch = int(takeover["lease_epoch"])

        stale_rejected = False
        if state.processing is not None:
            stale_rejected = await self._probe_stale_commit(state, state.processing)

        final: Any | None = None
        final_deadline = time.monotonic() + phase_timeout
        while time.monotonic() < final_deadline:
            row = await self._turn_row(acceptance.context.tenant_id, acceptance.inbound_id)
            if (
                row is not None
                and row["status"] == "committed"
                and (
                    self._scheduler_version == SchedulerVersion.V1
                    or await self._mailbox_item_completed(
                        acceptance.context.tenant_id,
                        acceptance.context.session_id,
                        acceptance.inbound_id,
                    )
                )
            ):
                final = row
                break
            await asyncio.sleep(0.05)
        if final is None:
            raise StageNotRun("final committed turn was not observed before the hard timeout")
        # Recovery is intentionally observed while the runner starts one
        # survivor at a time.  Do not include preflight containers that are
        # still stopped in the evidence set; doing so would make a valid first
        # takeover fail validation before the finally block restores the full
        # pool for the next case.
        survivors = await self._healthy_survivor_inventory()
        sequences = await self._session_sequences(
            acceptance.context.tenant_id, acceptance.context.session_id
        )
        turn_count = await self._turn_count(
            acceptance.context.tenant_id, acceptance.context.session_id, acceptance.inbound_id
        )
        return RecoveryEvidence(
            # Do not reread the live lease here: a successful commit clears
            # sessions.lease_owner and mailbox.lease_owner by design.  These
            # values are the exact survivor identity observed at takeover.
            owner_worker_id=takeover_owner,
            lease_epoch=takeover_epoch,
            survivors=survivors,
            turn_count=turn_count,
            sequences=sequences,
            stale_token_rejected=stale_rejected,
        )

    async def _healthy_survivor_inventory(self) -> tuple[WorkerContainer, ...]:
        """Converge an empty final worker inventory without relaxing validation.

        The recovery observer starts one survivor before the final commit can
        be observed.  Docker may briefly return an empty or not-yet-healthy
        project inventory at that boundary.  Retry while the filtered
        running+healthy survivor set is empty for a bounded window; any
        survivor returned remains subject to the existing identity checks in
        ``_validate_recovery``.
        """

        deadline = time.monotonic() + min(
            self._timeout_seconds, _WORKER_INVENTORY_CONVERGENCE_TIMEOUT_SECONDS
        )
        for delay in (0.0, *_WORKER_INVENTORY_RETRY_DELAYS_SECONDS):
            inventory = tuple(
                _coerce_container(item) for item in self._compose.list_workers(self._project)
            )
            survivors = tuple(
                worker
                for worker in inventory
                if worker.status.lower() == "running" and worker.health == "healthy"
            )
            if survivors:
                return survivors
            if delay == 0.0:
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(min(delay, remaining))
        raise StageNotRun("no worker containers were observed")

    async def _probe_stale_commit(
        self, state: _RuntimeCaseState, processing: ProcessingEvidence
    ) -> bool:
        acceptance = state.acceptance
        if acceptance is None:
            raise StageNotRun("acceptance authority is unavailable")
        snapshot = await self._repository.get_session_snapshot(
            acceptance.context.tenant_id, acceptance.context.session_id
        )
        if snapshot is None:
            raise StageNotRun("session snapshot for stale-token probe is unavailable")
        old_lease = SessionLease(
            tenant_id=acceptance.context.tenant_id,
            session_id=acceptance.context.session_id,
            turn_id=processing.turn_id,
            inbound_id=processing.inbound_id,
            worker_id=processing.worker_id,
            fencing_token=processing.fencing_token,
            expires_at=datetime.now(UTC) + timedelta(seconds=60),
            attempt=1,
            snapshot=snapshot,
        )
        try:
            commit = TurnCommit(
                context=acceptance.context,
                lease=old_lease,
                state=snapshot.state,
                events=(),
            )
            if self._scheduler_version == SchedulerVersion.V2:
                await self._repository.commit_session_ready(commit)
            else:
                await self._repository.commit(commit)
        except FencingConflict:
            return True
        raise StageAcceptanceError("stale fencing token unexpectedly committed")

    async def _mailbox_item_completed(
        self, tenant_id: str, session_id: str, inbound_id: str
    ) -> bool:
        async with self._tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT m.status,m.processing_inbound_id,i.resolved_at,
                       m.resolved_sequence,i.sequence
                  FROM session_mailboxes m
                  JOIN session_mailbox_items i
                    ON i.tenant_id=m.tenant_id AND i.session_id=m.session_id
                 WHERE m.tenant_id=$1 AND m.session_id=$2
                   AND i.inbound_id=$3::uuid
                """,
                tenant_id,
                session_id,
                inbound_id,
            )
        if row is None or row["resolved_at"] is None:
            return False
        if row["processing_inbound_id"] is not None:
            return False
        if row["status"] == "RUNNING":
            return False
        return int(row["resolved_sequence"]) >= int(row["sequence"])

    async def _session_sequences(self, tenant_id: str, session_id: str) -> tuple[int, ...]:
        async with self._tenant_transaction(tenant_id) as connection:
            rows = await connection.fetch(
                """
                SELECT sequence FROM session_events
                 WHERE tenant_id=$1 AND session_id=$2 ORDER BY sequence
                """,
                tenant_id,
                session_id,
            )
        return tuple(int(row["sequence"]) for row in rows)

    async def _turn_count(self, tenant_id: str, session_id: str, inbound_id: str) -> int:
        async with self._tenant_transaction(tenant_id) as connection:
            value = await connection.fetchval(
                """
                SELECT count(*) FROM session_turns
                 WHERE tenant_id=$1 AND session_id=$2 AND inbound_id=$3::uuid
                """,
                tenant_id,
                session_id,
                inbound_id,
            )
        return int(value or 0)

    async def verify_final(self, case: CaseIdentity, recovery: RecoveryEvidence) -> None:
        state = self._state(case)
        if state.acceptance is None:
            raise StageNotRun("acceptance authority is unavailable")
        row = await self._turn_row(state.acceptance.context.tenant_id, state.acceptance.inbound_id)
        if row is None or row["status"] != "committed":
            raise StageAcceptanceError("final session turn is not committed")
        mailbox_completed = (
            await self._mailbox_item_completed(
                state.acceptance.context.tenant_id,
                state.acceptance.context.session_id,
                state.acceptance.inbound_id,
            )
            if self._scheduler_version == SchedulerVersion.V2
            else True
        )
        if not mailbox_completed:
            raise StageAcceptanceError("final mailbox item is not resolved")
        actual_count = await self._turn_count(
            state.acceptance.context.tenant_id,
            state.acceptance.context.session_id,
            state.acceptance.inbound_id,
        )
        if recovery.turn_count != actual_count:
            raise StageAcceptanceError("final turn count changed after recovery evidence")
        actual_sequences = await self._session_sequences(
            state.acceptance.context.tenant_id, state.acceptance.context.session_id
        )
        if actual_sequences != recovery.sequences:
            raise StageAcceptanceError("final event sequence changed after recovery evidence")
        if case.stage == FaultStage.TOOL:
            await self._verify_tool_execution(state)
        await self._verify_outbound_intent(state)
        await self._verify_and_clear_redis_delivery(state)

    async def _verify_outbound_intent(self, state: _RuntimeCaseState) -> None:
        """Verify the commit atomically created exactly one outbound intent."""

        acceptance = state.acceptance
        if acceptance is None:
            raise StageNotRun("outbound evidence lacks the accepted inbound")
        async with self._tenant_transaction(acceptance.context.tenant_id) as connection:
            rows = await connection.fetch(
                """
                SELECT outbound_id,binding_id,session_id,in_reply_to,status
                  FROM outbound_messages
                 WHERE tenant_id=$1 AND session_id=$2 AND in_reply_to=$3
                 ORDER BY outbound_id
                """,
                acceptance.context.tenant_id,
                acceptance.context.session_id,
                acceptance.envelope.external_message_id,
            )
        if len(rows) != 1:
            raise StageAcceptanceError(
                "commit did not produce exactly one outbound intent for this inbound"
            )
        row = rows[0]
        if str(row["binding_id"]) != state.binding_id:
            raise StageAcceptanceError("outbound intent is bound to a different channel binding")
        if str(row["session_id"]) != acceptance.context.session_id:
            raise StageAcceptanceError("outbound intent is bound to a different session")
        if str(row["in_reply_to"]) != acceptance.envelope.external_message_id:
            raise StageAcceptanceError("outbound intent is not correlated to this inbound")
        if str(row["status"]) not in {"pending", "sending", "delivered", "failed", "ambiguous"}:
            raise StageAcceptanceError("outbound intent has an unknown delivery status")

    async def _verify_tool_execution(self, state: _RuntimeCaseState) -> None:
        """Verify one idempotent ledger entry, without storing tool content."""

        acceptance = state.acceptance
        processing = state.processing
        if acceptance is None or processing is None or not processing.execution_key:
            raise StageNotRun("tool execution identity is unavailable")
        async with self._tenant_transaction(acceptance.context.tenant_id) as connection:
            rows = await connection.fetch(
                """
                SELECT execution_key,turn_id,tool_name,classification,status
                  FROM tool_executions
                 WHERE tenant_id=$1 AND turn_id=$2::uuid
                 ORDER BY execution_key
                """,
                acceptance.context.tenant_id,
                processing.turn_id,
            )
        if len(rows) != 1:
            raise StageAcceptanceError(
                "idempotent tool execution ledger does not contain exactly one row"
            )
        row = rows[0]
        if str(row["execution_key"]) != processing.execution_key:
            raise StageAcceptanceError("tool execution key changed during takeover")
        if str(row["tool_name"]) != DETERMINISTIC_FAULT_TOOL_NAME:
            raise StageAcceptanceError("unexpected tool identity in fault-stage ledger")
        if str(row["classification"]) != "idempotent":
            raise StageAcceptanceError("fault-stage tool is not classified idempotent")
        if str(row["status"]) != "succeeded":
            raise StageNotRun("idempotent tool did not reach a durable succeeded status")

    async def _verify_and_clear_redis_delivery(self, state: _RuntimeCaseState) -> None:
        """Prove this case was ACKed and clear only its exact stream entry.

        Redis does not retain an ACK history.  A committed turn proves the
        worker consumed the corresponding stream entry; the targeted pending
        lookup then proves that entry is no longer pending.  v2 validates the
        seven-field SessionReady payload when it is still present; an absent
        exact entry is the expected post-ACK XDEL state.  v1 retains its
        historical requirement for an observable entry and never uses the v2
        absent-entry shortcut.  If the deployment cannot provide the exact
        Redis commands, the case remains not_run and no broad stream trim is
        attempted.
        """

        accepted = state.accepted
        if accepted is None or not accepted.outbox_id:
            raise StageNotRun("Redis acknowledgement evidence lacks this case outbox id")
        stream_id = accepted.stream_id
        scheduler_version = getattr(self, "_scheduler_version", SchedulerVersion.V1)
        try:
            # The worker acquires the authoritative PostgreSQL claim before
            # XACK, then executes and commits the turn.  Poll only this exact
            # stream id after the final PostgreSQL checks above; no broad
            # pending scan or stream trim is permitted here.
            deadline = time.monotonic() + min(self._timeout_seconds, 10.0)
            fields: Mapping[object, object] | None = None
            while time.monotonic() < deadline:
                pending = await self._redis.xpending_range(
                    self._queue.stream,
                    self._queue.group,
                    min=stream_id,
                    max=stream_id,
                    count=1,
                )
                if pending:
                    await asyncio.sleep(0.05)
                    continue
                fields = await self._read_exact_delivery_fields(state)
                if fields is not None:
                    break
                # SessionReady v2 deletes the exact stream entry immediately
                # after XACK.  Once the PostgreSQL turn/mailbox completion
                # checks above have passed and the exact PEL is empty, an
                # absent entry is therefore the successful ACK+XDEL state.
                # Keep v1's historical requirement for an observable entry;
                # its ACK evidence still relies on validating then deleting
                # the retained published record below.
                if scheduler_version == SchedulerVersion.V2:
                    return
                await asyncio.sleep(0.05)
            if fields is None:
                raise StageNotRun(
                    "Redis stream ACK evidence was not observed for this case before timeout"
                )
            if int(await self._redis.xdel(self._queue.stream, stream_id)) != 1:
                raise StageNotRun("Redis stream cleanup was not acknowledged for this case")
            if scheduler_version == SchedulerVersion.V1:
                dedupe_key = f"trpc:published:{accepted.outbox_id}"
                if int(await self._redis.exists(dedupe_key)):
                    if int(await self._redis.delete(dedupe_key)) != 1:
                        raise StageNotRun("Redis dedupe cleanup was not acknowledged for this case")
        except StageNotRun:
            raise
        except (AttributeError, NotImplementedError, RedisError, TypeError, ValueError) as error:
            raise StageNotRun("Redis exact ACK/cleanup operations are unavailable") from error

    async def cleanup_case(self, case: CaseIdentity) -> None:
        # The generated case identity is the sole cleanup authority.  Never
        # accept a caller-supplied tenant id that is not the state we created.
        state = self._cases.get(case.case_id)
        if state is None:
            return
        # ``run_case`` replaces the synthetic session/inbound identifiers with
        # the authoritative values returned by ``accept_and_publish`` before
        # teardown.  Those two fields are intentionally mutable within one
        # case; ownership must be tied to the immutable invocation scope and
        # never to the pre-acceptance placeholder values retained in state.
        if (
            state.case.case_id != case.case_id
            or state.case.stage != case.stage
            or state.case.run_id != case.run_id
            or state.case.tenant_id != case.tenant_id
            or state.case.message_id != case.message_id
            or not case.case_id.startswith("case-")
            or not case.tenant_id.startswith(f"fault-{_tenant_run_tag(case.run_id)}-")
        ):
            raise StageAcceptanceError("refusing cleanup for an unowned fault-stage case")

        try:
            await self._cleanup_redis_delivery(state)
        except Exception as error:
            # SQL cleanup must not run until the exact Redis delivery has been
            # removed.  Retain the in-memory authority so the bounded caller
            # can retry this same case without broadening either scope.
            raise StageAcceptanceError("exact Redis fault delivery cleanup failed") from error
        async with self._tenant_transaction(case.tenant_id) as connection:
            raw_counts = await connection.fetchval(
                "SELECT public.cleanup_fault_stage_fixture($1, $2, $3)",
                case.tenant_id,
                case.run_id,
                case.case_id,
            )
            if isinstance(raw_counts, str):
                try:
                    deleted_counts = json.loads(raw_counts)
                except json.JSONDecodeError as error:
                    raise StageAcceptanceError(
                        "fault-stage fixture cleanup result is invalid"
                    ) from error
            else:
                deleted_counts = raw_counts
            if not isinstance(deleted_counts, Mapping):
                raise StageAcceptanceError("fault-stage fixture cleanup result is invalid")
            tenant_count = deleted_counts.get("tenants")
            if isinstance(tenant_count, bool) or tenant_count != 1:
                raise StageAcceptanceError(
                    "fault-stage fixture cleanup did not delete exactly one tenant"
                )
        # Keep the in-memory authority until both exact Redis cleanup and the
        # database-owned atomic cleanup have completed and been acknowledged.
        self._cases.pop(case.case_id, None)

    async def _cleanup_redis_delivery(self, state: _RuntimeCaseState) -> None:
        """Delete only this case's validated stream entry during teardown."""

        accepted = state.accepted
        if accepted is None:
            return
        try:
            fields = await self._read_exact_delivery_fields(state)
            if fields is not None:
                if int(await self._redis.xdel(self._queue.stream, accepted.stream_id)) != 1:
                    raise StageAcceptanceError(
                        "Redis stream cleanup was not acknowledged for this case"
                    )
            if self._scheduler_version == SchedulerVersion.V1 and accepted.outbox_id:
                dedupe_key = f"trpc:published:{accepted.outbox_id}"
                if int(await self._redis.exists(dedupe_key)):
                    if int(await self._redis.delete(dedupe_key)) != 1:
                        raise StageAcceptanceError(
                            "Redis dedupe cleanup was not acknowledged for this case"
                        )
        except StageAcceptanceError:
            raise
        except (AttributeError, NotImplementedError, RedisError, TypeError, ValueError) as error:
            raise StageAcceptanceError(
                "exact Redis fault delivery cleanup is unavailable"
            ) from error

    async def close(self) -> None:
        await self._redis.aclose()
        await self._pool.close()


class PostgresPerCaseStageController:
    """Use the worker's fixed run id so its checkpoint can match our control."""

    def __init__(self, pool: asyncpg.Pool, run_token: str, run_id: str) -> None:
        self._pool = pool
        self._run_token = run_token
        if not run_id.strip():
            raise ValueError("fault-stage run id cannot be blank")
        self._run_id = run_id.strip()
        self._controller: PostgresFaultStageController | None = None

    async def arm(self, event: FaultStageEvent) -> str:
        self._controller = PostgresFaultStageController(
            self._pool,
            run_id=self._run_id,
            run_token=self._run_token,
        )
        return str(await self._controller.arm(event))

    async def release(self, control_id: str, *, tenant_id: str) -> bool:
        if self._controller is None:
            return False
        return bool(await self._controller.release(control_id, tenant_id=tenant_id))


def _planned_case(stage: FaultStage, *, reason: str) -> StageResult:
    return StageResult(
        stage=stage,
        status=CaseStatus.NOT_RUN,
        case=CaseIdentity.create(stage),
        markers=(_marker("case.planned", CaseStatus.NOT_RUN, reason=reason),),
        reason=reason,
    )


def _requested_stages(name: str) -> tuple[FaultStage, ...]:
    if name == "all":
        return REQUIRED_STAGES
    try:
        return (SCENARIO_STAGES[name],)
    except KeyError as error:
        raise ValueError(f"unsupported fault-stage scenario: {name}") from error


def build_not_run_report(
    *, reason: str, required_stages: Sequence[FaultStage] = REQUIRED_STAGES
) -> dict[str, Any]:
    selected = tuple(required_stages)
    cases = [_planned_case(stage, reason=reason).as_report() for stage in selected]
    return {
        "schema_version": 1,
        "generated_at": _now(),
        "mode": "fault_stage_acceptance",
        "gate": "not_run",
        "production_gate": "not_run",
        "reason": reason,
        "requested_stages": [stage.value for stage in selected],
        "cases": cases,
    }


def build_execution_report(
    results: Sequence[StageResult],
    *,
    required_stages: Sequence[FaultStage] = REQUIRED_STAGES,
    worker_preflight: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Aggregate only the selected exact cases; planned results never pass."""

    by_stage = {result.stage: result for result in results}
    selected = tuple(required_stages)
    if not selected or len(set(selected)) != len(selected):
        raise ValueError("fault-stage report must select unique required stages")
    if set(by_stage) != set(selected):
        gate = CaseStatus.NOT_RUN
        reason = "one or more selected fault-stage cases were not executed"
    elif all(by_stage[stage].status == CaseStatus.PASS for stage in selected):
        gate = CaseStatus.PASS
        reason = None
    elif any(by_stage[stage].status == CaseStatus.FAIL for stage in selected):
        gate = CaseStatus.FAIL
        reason = "one or more selected fault-stage cases failed"
    else:
        gate = CaseStatus.NOT_RUN
        reason = "one or more selected fault-stage cases were not run"
    production_gate = (
        CaseStatus.PASS
        if gate == CaseStatus.PASS and set(selected) == set(REQUIRED_STAGES)
        else CaseStatus.NOT_RUN
    )
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": _now(),
        "mode": "fault_stage_acceptance",
        "gate": gate.value,
        # A production claim is possible only when all three exact cases pass.
        "production_gate": production_gate.value,
        "requested_stages": [stage.value for stage in selected],
        "cases": [by_stage[stage].as_report() for stage in selected if stage in by_stage],
    }
    if reason:
        report["reason"] = reason
    if worker_preflight is not None:
        report["worker_preflight"] = dict(worker_preflight)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--scenario",
        choices=("all", *SCENARIO_STAGES),
        default="all",
        help="execute one worker fault stage, or all three (default: all)",
    )
    parser.add_argument(
        "--project", default=None, help=f"explicit unique project ({PROJECT_PREFIX}<run>)"
    )
    parser.add_argument("--worker-container", default=None, help="one explicit worker container id")
    parser.add_argument("--termination", choices=("stop", "kill"), default="kill")
    parser.add_argument("--allow-process-kill", action="store_true")
    parser.add_argument(
        "--scheduler-version",
        choices=tuple(version.value for version in SchedulerVersion),
        default=SchedulerVersion.V2.value,
        help="scheduler wire version; v2 is the production default, v1 is legacy-only",
    )
    parser.add_argument("--timeout-seconds", type=float, default=90.0)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def _write_report(path: Path, report: Mapping[str, Any]) -> None:
    absolute = path.expanduser().absolute()
    current = absolute
    while True:
        if current.is_symlink():
            raise StageAcceptanceError("refusing to write a fault-stage report through a symlink")
        parent = current.parent
        if parent == current:
            break
        current = parent
    atomic_write_json(path, dict(report))


def _execution_provenance() -> dict[str, Any] | None:
    """Return invocation-bound, secret-free child evidence when requested.

    The parent gate supplies a fresh nonce for every child invocation.  Only
    its SHA-256 is emitted, together with exact selectors and transport names;
    this prevents a stale/foreign JSON report from being accepted without
    placing a run token or message body in the report.
    """

    nonce = os.getenv(_EVIDENCE_NONCE_ENV, "").strip()
    if not nonce:
        return None
    run_id = os.getenv("TRPC_FAULT_RUN_ID", "").strip()
    project = os.getenv(_EVIDENCE_PROJECT_ENV, "").strip()
    container = os.getenv(_EVIDENCE_CONTAINER_ENV, "").strip()
    scheduler = os.getenv(_EVIDENCE_SCHEDULER_ENV, "").strip()
    stream = os.getenv(_EVIDENCE_STREAM_ENV, "").strip()
    group = os.getenv(_EVIDENCE_GROUP_ENV, "").strip()
    if not all((run_id, project, container, scheduler, stream, group)):
        raise StageNotRun("fault-stage evidence provenance environment is incomplete")
    return {
        "schema_version": 1,
        "run_id": run_id,
        "project": project,
        "worker_container": container,
        "scheduler_version": scheduler,
        "redis_stream": stream,
        "redis_group": group,
        "nonce_sha256": hashlib.sha256(nonce.encode("utf-8")).hexdigest(),
        "pid": os.getpid(),
    }


def _connection_dsn(raw: str) -> str:
    return raw.replace("postgresql+asyncpg://", "postgresql://", 1)


def _payload_int(payload: Mapping[str, Any], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise StageNotRun(f"v2 outbox payload lacks a valid {name}")
    return value


def _secret_bytes(raw: str) -> bytes:
    value = raw.strip()
    for decoder in (base64.urlsafe_b64decode, base64.b64decode):
        try:
            decoded = decoder(value + "=" * (-len(value) % 4))
        except (ValueError, TypeError):
            continue
        if len(decoded) >= 32:
            return decoded
    encoded = value.encode()
    if len(encoded) < 32:
        raise StageNotRun("session HMAC key must contain at least 32 bytes")
    return encoded


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()
    return None


FAULT_STAGE_ENV_NAMES = (
    "TRPC_FAULT_DATABASE_DSN",
    "TRPC_FAULT_REDIS_URL",
    "TRPC_FAULT_BINDING_SEED",
    "TRPC_FAULT_SESSION_HMAC_KEY",
    "TRPC_FAULT_RUN_ID",
    "TRPC_FAULT_RUN_TOKEN",
    "TRPC_FAULT_OFFLINE_AGENT_DELAY_SECONDS",
)


def _required_fault_stage_env() -> dict[str, str]:
    """Read only dedicated fault-test variables and fail closed.

    The real acceptance command must never fall back to ordinary service or
    performance-test DSNs.  A missing value is reported by variable name only;
    values (including secrets) never enter the JSON report.
    """

    values = {name: _env_first(name) for name in FAULT_STAGE_ENV_NAMES}
    missing = tuple(name for name, value in values.items() if not value)
    if missing:
        raise StageNotRun(
            "missing dedicated fault-stage environment variables: " + ", ".join(missing)
        )
    return cast(dict[str, str], values)


def _parse_offline_agent_delay(raw: str) -> float:
    try:
        delay = float(raw)
    except (TypeError, ValueError) as error:
        raise StageNotRun(
            "TRPC_FAULT_OFFLINE_AGENT_DELAY_SECONDS must be a positive number <=5"
        ) from error
    if not math.isfinite(delay) or delay <= 0 or delay > 5:
        raise StageNotRun("TRPC_FAULT_OFFLINE_AGENT_DELAY_SECONDS must be a positive number <=5")
    return delay


async def _execute(args: argparse.Namespace) -> dict[str, Any]:
    required_stages = _requested_stages(args.scenario)
    if not args.execute:
        return build_not_run_report(
            reason="pass --execute to enable fault-stage acceptance",
            required_stages=required_stages,
        )
    if (
        not math.isfinite(args.timeout_seconds)
        or args.timeout_seconds < 30
        or args.timeout_seconds > 600
    ):
        return build_not_run_report(
            reason="fault-stage acceptance timeout must be finite, between 30 and 600 seconds",
            required_stages=required_stages,
        )
    if os.getenv(OPT_IN_ENV, "") != "1":
        return build_not_run_report(
            reason=f"{OPT_IN_ENV}=1 is required", required_stages=required_stages
        )
    if os.getenv("TRPC_SERVICE_ENVIRONMENT", "") != Environment.TEST.value:
        return build_not_run_report(
            reason="fault-stage acceptance is restricted to Environment.TEST",
            required_stages=required_stages,
        )
    if not args.allow_process_kill or os.getenv(KILL_OPT_IN_ENV, "") != "1":
        return build_not_run_report(
            reason=f"--allow-process-kill and {KILL_OPT_IN_ENV}=1 are required",
            required_stages=required_stages,
        )
    pool: asyncpg.Pool | None = None
    redis: Any | None = None
    backend: PostgresRuntimeStageBackend | None = None
    try:
        project = validate_project(args.project)
        try:
            scheduler_version = SchedulerVersion(args.scheduler_version)
        except (AttributeError, TypeError, ValueError) as error:
            raise StageNotRun("fault-stage scheduler version must be v1 or v2") from error
        expected_stream, expected_group = _scheduler_transport(scheduler_version)
        stream = os.getenv("TRPC_FAULT_REDIS_STREAM", expected_stream)
        group = os.getenv("TRPC_FAULT_REDIS_GROUP", expected_group)
        if stream != expected_stream or group != expected_group:
            raise StageNotRun("fault-stage scheduler version and Redis stream/group do not match")
        if not args.worker_container:
            raise StageNotRun("--worker-container is required")
        # Validate the selector before any Docker API call.  The subsequent
        # project/service/health checks still bind the termination target to
        # the dedicated worker, but an unsafe selector must never reach the
        # external command wrapper even as a read-only inspect.
        worker_selector = _validate_container_selector(args.worker_container)
        fault_env = _required_fault_stage_env()
        if shutil.which("docker") is None:
            raise StageNotRun("Docker is not installed")
        database_dsn = fault_env["TRPC_FAULT_DATABASE_DSN"]
        redis_url = fault_env["TRPC_FAULT_REDIS_URL"]
        binding_seed = fault_env["TRPC_FAULT_BINDING_SEED"]
        routing_key_raw = fault_env["TRPC_FAULT_SESSION_HMAC_KEY"]
        run_id = fault_env["TRPC_FAULT_RUN_ID"]
        run_token = fault_env["TRPC_FAULT_RUN_TOKEN"]
        offline_agent_delay_seconds = _parse_offline_agent_delay(
            fault_env["TRPC_FAULT_OFFLINE_AGENT_DELAY_SECONDS"]
        )
        routing_key = _secret_bytes(routing_key_raw)
        if len(run_token.encode()) < 32:
            raise StageNotRun("fault run token must contain at least 32 bytes")
        compose = DockerComposeController(project=project)
        pool = await asyncpg.create_pool(
            _connection_dsn(database_dsn), min_size=1, max_size=8, command_timeout=30
        )
        if not await pool.fetchval("SELECT 1"):
            raise StageNotRun("PostgreSQL readiness probe failed")
        # Keep Redis operations bounded.  v2 XREAD uses a short finite block;
        # the socket timeout remains above that block while ACK/cleanup are
        # independently bounded by the worker/acceptance code.
        redis = cast(
            Any,
            redis_async.from_url(
                redis_url,
                decode_responses=False,
                socket_connect_timeout=3.0,
                socket_timeout=10.0,
                retry_on_timeout=False,
            ),
        )
        if not bool(await _maybe_await(redis.ping())):
            raise StageNotRun("Redis readiness probe failed")
        target = compose.inspect(worker_selector)
        workers = compose.list_workers(project)
        verified_workers = _validate_workers(
            workers,
            project=project,
            minimum=4,
            require_independent=True,
        )
        worker_preflight = {
            "status": "pass",
            "worker_count": len(verified_workers),
            "healthy_worker_count": len(verified_workers),
            "independent_processes": True,
            "positive_pid_count": len(
                {worker.pid for worker in verified_workers if worker.pid is not None}
            ),
        }
        for worker in workers:
            validate_worker_transport(
                worker,
                scheduler_version=scheduler_version,
                stream=stream,
                group=group,
            )
        backend = PostgresRuntimeStageBackend(
            pool,
            redis,
            compose=compose,
            project=project,
            routing_key=routing_key,
            binding_seed=binding_seed,
            scheduler_version=scheduler_version,
            offline_agent_delay_seconds=offline_agent_delay_seconds,
            stream=stream,
            group=group,
            timeout_seconds=args.timeout_seconds,
        )
        controller = PostgresPerCaseStageController(pool, run_token, run_id)
        results = await FaultStageAcceptanceRunner(
            compose=compose,
            backend=cast(StageBackend, backend),
            controller=controller,
            project=project,
            worker_container=target.container_id,
            termination=args.termination,
            timeout_seconds=args.timeout_seconds,
            allow_process_kill=True,
            run_id=run_id,
        ).run_all(required_stages)
        return build_execution_report(
            results,
            required_stages=required_stages,
            worker_preflight=worker_preflight,
        )
    except StageNotRun as error:
        return build_not_run_report(reason=str(error), required_stages=required_stages)
    except StageAcceptanceError as error:
        report = build_not_run_report(reason=str(error), required_stages=required_stages)
        report["gate"] = "fail"
        report["production_gate"] = "not_run"
        return report
    except Exception as error:
        report = build_not_run_report(
            reason=f"{type(error).__name__}: execution failed",
            required_stages=required_stages,
        )
        report["gate"] = "fail"
        report["production_gate"] = "not_run"
        return report
    finally:
        if backend is not None:
            await backend.close()
        elif redis is not None:
            await redis.aclose()
        if pool is not None and backend is None:
            await pool.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    started_at = _now()
    report = asyncio.run(_execute(args))
    ended_at = _now()
    run_id = os.getenv("TRPC_FAULT_RUN_ID", "").strip()
    if run_id:
        report["run_id"] = run_id
    report["started_at"] = started_at
    report["ended_at"] = ended_at
    provenance = _execution_provenance()
    if provenance is not None:
        report["execution_provenance"] = provenance
        report["run_nonce_sha256"] = provenance["nonce_sha256"]
    _write_report(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    if report.get("gate") == "fail":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = [
    "AcceptanceEvidence",
    "CaseIdentity",
    "CaseStatus",
    "ComposeController",
    "ControlEvidence",
    "DockerComposeController",
    "FaultStageAcceptanceRunner",
    "PostgresFaultStageController",
    "PostgresPerCaseStageController",
    "PostgresRuntimeStageBackend",
    "PostgresStageMarkerReader",
    "ProcessingEvidence",
    "RecoveryEvidence",
    "StageAcceptanceError",
    "StageBackend",
    "StageController",
    "StageNotRun",
    "StageResult",
    "WorkerContainer",
    "build_execution_report",
    "build_not_run_report",
    "main",
    "validate_project",
    "validate_worker_container",
]
