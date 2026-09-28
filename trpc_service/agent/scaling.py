"""Desired-state Worker Pool control and replaceable capacity adapters."""

import asyncio
from dataclasses import dataclass
from datetime import datetime
import logging
import os
from pathlib import Path
import re
import ssl
import subprocess
from typing import BinaryIO, Protocol

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.agent.nodes import PostgreSQLRuntimeNodeRegistry, RuntimeNodeHealth
from trpc_service.storage.runtime_orm import WorkerPoolControlRow

logger = logging.getLogger(__name__)
_POOL_NAME = "agent-worker"
_KUBERNETES_NAME = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")


class WorkerPoolGenerationConflict(RuntimeError):
    """Raised when an administrator updates an out-of-date pool view."""


class WorkerPoolScaleRequest(BaseModel):
    """Generation-guarded desired capacity submitted by a platform administrator."""

    desired_nodes: int = Field(ge=1, le=64)
    expected_generation: int = Field(ge=1)


class WorkerPoolTarget(BaseModel):
    """Durable desired state consumed by local or Kubernetes reconcilers."""

    pool_name: str
    desired_nodes: int
    generation: int
    updated_by: str
    updated_at: datetime


class WorkerPoolStatus(WorkerPoolTarget):
    """Desired and observed Worker capacity shown in the administrator console."""

    active_nodes: int
    draining_nodes: int
    stale_nodes: int
    reconciling: bool
    scaler_mode: str


class PostgreSQLWorkerPoolStore:
    """Persist the singleton Worker Pool target in the primary database."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def ensure(
        self,
        *,
        initial_desired_nodes: int,
        session: AsyncSession | None = None,
    ) -> WorkerPoolTarget:
        """Create the initial target once, retaining later administrator choices."""

        if not 1 <= initial_desired_nodes <= 64:
            raise ValueError("initial desired Worker count must be between 1 and 64")
        if session is not None:
            return await self._ensure_in_session(session, initial_desired_nodes)
        async with self._sessions.begin() as database:
            return await self._ensure_in_session(database, initial_desired_nodes)

    async def get(self) -> WorkerPoolTarget | None:
        """Return the current target without creating control-plane state."""

        async with self._sessions() as database:
            row = await database.get(WorkerPoolControlRow, _POOL_NAME)
            return None if row is None else self._target(row)

    async def scale(
        self,
        *,
        desired_nodes: int,
        expected_generation: int,
        updated_by: str,
        session: AsyncSession | None = None,
    ) -> WorkerPoolTarget:
        """Change desired capacity once using optimistic concurrency control."""

        if not 1 <= desired_nodes <= 64:
            raise ValueError("desired Worker count must be between 1 and 64")
        if expected_generation < 1 or not updated_by.strip():
            raise ValueError("Worker Pool generation or actor is invalid")
        if session is not None:
            return await self._scale_in_session(
                session,
                desired_nodes,
                expected_generation,
                updated_by,
            )
        async with self._sessions.begin() as database:
            return await self._scale_in_session(
                database,
                desired_nodes,
                expected_generation,
                updated_by,
            )

    async def _ensure_in_session(
        self,
        database: AsyncSession,
        initial_desired_nodes: int,
    ) -> WorkerPoolTarget:
        # PostgreSQL and supported SQLite both provide this conflict form. It
        # closes the first-start race between Gateway and Supervisor without
        # allowing either process to overwrite an administrator's later value.
        await database.execute(
            text("""
                INSERT INTO worker_pool_control
                    (pool_name, desired_nodes, generation, updated_by)
                VALUES (:pool_name, :desired_nodes, 1, 'bootstrap')
                ON CONFLICT (pool_name) DO NOTHING
            """),
            {
                "pool_name": _POOL_NAME,
                "desired_nodes": initial_desired_nodes,
            },
        )
        row = await database.get(WorkerPoolControlRow, _POOL_NAME, with_for_update=True)
        if row is None:
            raise RuntimeError("Worker Pool target could not be initialized")
        return self._target(row)

    async def _scale_in_session(
        self,
        database: AsyncSession,
        desired_nodes: int,
        expected_generation: int,
        updated_by: str,
    ) -> WorkerPoolTarget:
        row = await database.get(WorkerPoolControlRow, _POOL_NAME, with_for_update=True)
        if row is None:
            raise LookupError("Worker Pool target has not been initialized")
        if row.generation != expected_generation:
            raise WorkerPoolGenerationConflict("Worker Pool target changed; refresh and retry")
        if row.desired_nodes != desired_nodes:
            row.desired_nodes = desired_nodes
            row.generation += 1
            row.updated_by = updated_by
            await database.flush()
        return self._target(row)

    @staticmethod
    def _target(row: WorkerPoolControlRow) -> WorkerPoolTarget:
        return WorkerPoolTarget(
            pool_name=row.pool_name,
            desired_nodes=row.desired_nodes,
            generation=row.generation,
            updated_by=row.updated_by,
            updated_at=row.updated_at,
        )


class WorkerProcess(Protocol):
    """Minimal process surface owned by the local capacity adapter."""

    index: int
    node_id: str

    @property
    def returncode(self) -> int | None:
        ...

    def request_drain(self) -> None:
        ...

    async def wait(self) -> int:
        ...

    def close(self) -> None:
        ...


class WorkerProcessLauncher(Protocol):
    """Create one independently addressable local Worker process."""

    async def launch(self, index: int) -> WorkerProcess:
        ...


class WorkerCapacity(Protocol):
    """Reconcile one runtime-specific capacity implementation."""

    async def reconcile(self, desired_nodes: int) -> None:
        ...

    async def close(self) -> None:
        ...


class WorkerPoolTargetStore(Protocol):
    """Read and initialize desired capacity for a runtime controller."""

    async def get(self) -> WorkerPoolTarget | None:
        ...

    async def ensure(self, *, initial_desired_nodes: int) -> WorkerPoolTarget:
        ...


class LocalWorkerCapacity:
    """Manage local Worker processes and drain surplus nodes before removal."""

    def __init__(self, launcher: WorkerProcessLauncher) -> None:
        self._launcher = launcher
        self._processes: dict[int, WorkerProcess] = {}
        self._draining: set[int] = set()

    @property
    def managed_nodes(self) -> int:
        """Return processes not yet reaped, including nodes currently draining."""

        return len(self._processes)

    async def reconcile(self, desired_nodes: int) -> None:
        """Start missing indices and signal surplus indices without awaiting them."""

        if not 1 <= desired_nodes <= 64:
            raise ValueError("desired Worker count must be between 1 and 64")
        self._reap_finished()
        desired_indices = set(range(1, desired_nodes + 1))
        for index in sorted(set(self._processes) - desired_indices, reverse=True):
            if index in self._draining:
                continue
            process = self._processes[index]
            # The Worker first closes queue intake and then publishes its own
            # draining heartbeat. Keeping that order avoids showing a node as
            # drained while it can still claim a new task.
            process.request_drain()
            self._draining.add(index)
        for index in sorted(desired_indices - set(self._processes)):
            self._processes[index] = await self._launcher.launch(index)

    async def close(self) -> None:
        """Drain every child and wait until all in-flight Agent work finishes."""

        for index in sorted(self._processes, reverse=True):
            process = self._processes[index]
            if index not in self._draining:
                process.request_drain()
                self._draining.add(index)
        for process in self._processes.values():
            await process.wait()
            process.close()
        self._processes.clear()
        self._draining.clear()

    def _reap_finished(self) -> None:
        for index, process in tuple(self._processes.items()):
            if process.returncode is None:
                continue
            process.close()
            self._processes.pop(index)
            self._draining.discard(index)


class WorkerPoolController:
    """Continuously converge a capacity adapter to the durable desired count."""

    def __init__(
        self,
        store: WorkerPoolTargetStore,
        capacity: WorkerCapacity,
        *,
        initial_desired_nodes: int,
        reconcile_interval_seconds: float,
    ) -> None:
        if reconcile_interval_seconds <= 0:
            raise ValueError("Worker Pool reconcile interval must be positive")
        self._store = store
        self._capacity = capacity
        self._initial_desired_nodes = initial_desired_nodes
        self._interval = reconcile_interval_seconds
        self._stop = asyncio.Event()

    async def run(self) -> None:
        """Reconcile until shutdown, retrying transient database/runtime failures."""

        while not self._stop.is_set():
            try:
                target = await self._store.get()
                if target is None:
                    target = await self._store.ensure(
                        initial_desired_nodes=self._initial_desired_nodes)
                await self._capacity.reconcile(target.desired_nodes)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.error("Worker Pool reconciliation failed with %s", type(error).__name__)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
            except TimeoutError:
                pass

    def stop_reconciling(self) -> None:
        """Wake the polling loop without terminating adapter-owned work."""

        self._stop.set()

    async def close(self) -> None:
        """Stop reconciliation and gracefully drain adapter-owned capacity."""

        self._stop.set()
        await self._capacity.close()


@dataclass(slots=True)
class LocalWorkerProcess:
    """One child Worker and the local files owned for its lifetime."""

    index: int
    node_id: str
    process: asyncio.subprocess.Process
    pid_file: Path
    log_stream: BinaryIO

    @property
    def returncode(self) -> int | None:
        return self.process.returncode

    def request_drain(self) -> None:
        """Deliver SIGTERM once; the Worker stops claims and finishes active work."""

        if self.process.returncode is None:
            self.process.terminate()

    async def wait(self) -> int:
        return await self.process.wait()

    def close(self) -> None:
        """Close the log and remove only this process's still-current PID file."""

        self.log_stream.close()
        try:
            if self.pid_file.read_text(encoding="utf-8").strip() == str(self.process.pid):
                self.pid_file.unlink()
        except FileNotFoundError:
            pass


class LocalWorkerProcessLauncher:
    """Launch host-Python Workers without exposing process control to the Web API."""

    def __init__(
        self,
        *,
        executable: Path,
        run_dir: Path,
        log_dir: Path,
        concurrency_per_node: int,
    ) -> None:
        if concurrency_per_node < 1:
            raise ValueError("Worker concurrency per node must be positive")
        self._executable = executable.expanduser().resolve()
        self._run_dir = run_dir.expanduser().resolve()
        self._log_dir = log_dir.expanduser().resolve()
        self._concurrency = concurrency_per_node

    async def launch(self, index: int) -> LocalWorkerProcess:
        """Start one stable-index Worker and record its PID for operational scripts."""

        if index < 1 or index > 64:
            raise ValueError("local Worker index must be between 1 and 64")
        self._run_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._log_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        node_id = f"local-worker-{index}"
        log_path = self._log_dir / f"agent-worker-{index}.log"
        pid_file = self._run_dir / f"agent-worker-{index}.pid"
        log_stream = log_path.open("ab", buffering=0)
        environment = os.environ.copy()
        environment.update({
            "TRPC_SERVICE_RUNTIME_ROLE": "worker",
            "TRPC_SERVICE_NODE_ID": node_id,
            "TRPC_SERVICE_WORKER_CONCURRENCY": str(self._concurrency),
            "TRPC_SERVICE_LOG_FILE": str(log_path),
        })
        try:
            process = await asyncio.create_subprocess_exec(
                str(self._executable),
                stdin=subprocess.DEVNULL,
                stdout=log_stream,
                stderr=subprocess.STDOUT,
                env=environment,
                start_new_session=True,
            )
        except Exception:
            log_stream.close()
            raise
        pid_file.write_text(f"{process.pid}\n", encoding="utf-8")
        return LocalWorkerProcess(
            index=index,
            node_id=node_id,
            process=process,
            pid_file=pid_file,
            log_stream=log_stream,
        )


class KubernetesWorkerCapacity:
    """Reconcile the Worker Deployment scale subresource with minimal RBAC."""

    def __init__(
        self,
        *,
        namespace: str,
        deployment: str,
        client: httpx.AsyncClient | None = None,
        api_url: str = "https://kubernetes.default.svc",
        token_file: Path | None = None,
        ca_file: Path | None = None,
    ) -> None:
        if (_KUBERNETES_NAME.fullmatch(namespace) is None
                or _KUBERNETES_NAME.fullmatch(deployment) is None):
            raise ValueError("Kubernetes namespace or Deployment name is invalid")
        self._path = f"/apis/apps/v1/namespaces/{namespace}/deployments/{deployment}/scale"
        self._owns_client = client is None
        if client is not None:
            self._client = client
            return
        if token_file is None or ca_file is None:
            raise ValueError("Kubernetes scaler requires service-account token and CA files")
        token = token_file.read_text(encoding="utf-8").strip()
        if not token:
            raise ValueError("Kubernetes service-account token is empty")
        tls = ssl.create_default_context(cafile=str(ca_file))
        self._client = httpx.AsyncClient(
            base_url=api_url,
            verify=tls,
            timeout=10,
            headers={"Authorization": f"Bearer {token}"},
        )

    async def reconcile(self, desired_nodes: int) -> None:
        """Patch only when observed replicas differ from desired capacity."""

        if not 1 <= desired_nodes <= 64:
            raise ValueError("desired Worker count must be between 1 and 64")
        response = await self._client.get(self._path)
        response.raise_for_status()
        payload = response.json()
        spec = payload.get("spec") if isinstance(payload, dict) else None
        if not isinstance(spec, dict) or not isinstance(spec.get("replicas"), int):
            raise ValueError("Kubernetes scale response has no integer replica count")
        current = spec["replicas"]
        if current == desired_nodes:
            return
        updated = await self._client.patch(
            self._path,
            json={"spec": {
                "replicas": desired_nodes
            }},
            headers={"Content-Type": "application/merge-patch+json"},
        )
        updated.raise_for_status()

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


async def describe_worker_pool(
    target: WorkerPoolTarget,
    registry: PostgreSQLRuntimeNodeRegistry,
    *,
    scaler_mode: str,
) -> WorkerPoolStatus:
    """Combine desired state with fresh Worker heartbeat observations."""

    nodes = await registry.list()
    workers = [node for node in nodes.items if node.role in {"worker", "api_worker"}]
    active = sum(node.health is RuntimeNodeHealth.ACTIVE for node in workers)
    draining = sum(node.health is RuntimeNodeHealth.DRAINING for node in workers)
    stale = sum(node.health is RuntimeNodeHealth.STALE for node in workers)
    return WorkerPoolStatus(
        **target.model_dump(),
        active_nodes=active,
        draining_nodes=draining,
        stale_nodes=stale,
        reconciling=active != target.desired_nodes or draining > 0,
        scaler_mode=scaler_mode,
    )
