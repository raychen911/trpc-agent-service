import asyncio
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from trpc_service.agent.scaling import (
    LocalWorkerCapacity,
    LocalWorkerProcessLauncher,
    KubernetesWorkerCapacity,
    PostgreSQLWorkerPoolStore,
    WorkerPoolGenerationConflict,
    WorkerPoolController,
    WorkerPoolTarget,
    describe_worker_pool,
)
from trpc_service.agent.nodes import (
    RuntimeNodeHealth,
    RuntimeNodeList,
    RuntimeNodeRead,
)
from trpc_service.storage.orm import Base


class FakeWorkerProcess:
    """Controllable child process used to verify delayed scale-in."""

    def __init__(self, index: int) -> None:
        self.index = index
        self.node_id = f"local-worker-{index}"
        self.returncode: int | None = None
        self.drain_requested = False
        self.drain_requests = 0

    def request_drain(self) -> None:
        self.drain_requested = True
        self.drain_requests += 1

    async def wait(self) -> int:
        self.returncode = 0
        return 0

    def close(self) -> None:
        pass


class FakeWorkerLauncher:

    def __init__(self) -> None:
        self.started: list[FakeWorkerProcess] = []

    async def launch(self, index: int) -> FakeWorkerProcess:
        process = FakeWorkerProcess(index)
        self.started.append(process)
        return process


class StaticWorkerPoolStore:

    def __init__(self, desired_nodes: int) -> None:
        self.desired_nodes = desired_nodes

    async def get(self) -> WorkerPoolTarget:
        return WorkerPoolTarget(
            pool_name="agent-worker",
            desired_nodes=self.desired_nodes,
            generation=1,
            updated_by="test",
            updated_at=datetime.now(timezone.utc),
        )

    async def ensure(self, *, initial_desired_nodes: int) -> WorkerPoolTarget:
        """Match the controller port while keeping this fake deterministic."""

        del initial_desired_nodes
        return await self.get()


class RecordingCapacity:

    def __init__(self) -> None:
        self.targets: list[int] = []
        self.closed = False

    async def reconcile(self, desired_nodes: int) -> None:
        self.targets.append(desired_nodes)

    async def close(self) -> None:
        self.closed = True


class InitializingWorkerPoolStore:
    """Return no target once so controller bootstrap remains covered."""

    def __init__(self) -> None:
        self.ensure_calls: list[int] = []
        self.target: WorkerPoolTarget | None = None

    async def get(self) -> WorkerPoolTarget | None:
        return self.target

    async def ensure(self, *, initial_desired_nodes: int) -> WorkerPoolTarget:
        self.ensure_calls.append(initial_desired_nodes)
        self.target = WorkerPoolTarget(
            pool_name="agent-worker",
            desired_nodes=initial_desired_nodes,
            generation=1,
            updated_by="bootstrap",
            updated_at=datetime.now(timezone.utc),
        )
        return self.target


@pytest.mark.anyio
async def test_worker_pool_store_uses_optimistic_generation(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'scaling.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgreSQLWorkerPoolStore(sessions)

    initial = await store.ensure(initial_desired_nodes=2)
    assert await store.get() == initial
    expanded = await store.scale(
        desired_nodes=5,
        expected_generation=initial.generation,
        updated_by="platform-admin",
    )
    unchanged = await store.scale(
        desired_nodes=5,
        expected_generation=expanded.generation,
        updated_by="platform-admin",
    )

    assert initial.desired_nodes == 2
    assert expanded.desired_nodes == 5
    assert expanded.generation == initial.generation + 1
    assert unchanged.generation == expanded.generation
    with pytest.raises(WorkerPoolGenerationConflict):
        await store.scale(
            desired_nodes=3,
            expected_generation=initial.generation,
            updated_by="stale-admin",
        )

    await engine.dispose()


@pytest.mark.anyio
async def test_worker_pool_store_rejects_invalid_or_uninitialized_updates(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'invalid-scaling.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = PostgreSQLWorkerPoolStore(async_sessionmaker(engine, expire_on_commit=False))

    with pytest.raises(ValueError):
        await store.ensure(initial_desired_nodes=0)
    with pytest.raises(ValueError):
        await store.scale(desired_nodes=65, expected_generation=1, updated_by="admin")
    with pytest.raises(ValueError):
        await store.scale(desired_nodes=1, expected_generation=0, updated_by="admin")
    with pytest.raises(ValueError):
        await store.scale(desired_nodes=1, expected_generation=1, updated_by=" ")
    with pytest.raises(LookupError):
        await store.scale(desired_nodes=1, expected_generation=1, updated_by="admin")

    await engine.dispose()


@pytest.mark.anyio
async def test_worker_pool_controller_observes_updated_desired_state() -> None:
    store = StaticWorkerPoolStore(2)
    capacity = RecordingCapacity()
    controller = WorkerPoolController(
        store,
        capacity,
        initial_desired_nodes=2,
        reconcile_interval_seconds=0.01,
    )
    task = asyncio.create_task(controller.run())
    for _ in range(20):
        if capacity.targets:
            break
        await asyncio.sleep(0.005)
    store.desired_nodes = 5
    for _ in range(20):
        if 5 in capacity.targets:
            break
        await asyncio.sleep(0.005)
    controller.stop_reconciling()
    await task
    await controller.close()

    assert capacity.targets[0] == 2
    assert 5 in capacity.targets
    assert capacity.closed


@pytest.mark.anyio
async def test_worker_pool_controller_bootstraps_missing_target() -> None:
    store = InitializingWorkerPoolStore()
    capacity = RecordingCapacity()
    controller = WorkerPoolController(
        store,
        capacity,
        initial_desired_nodes=3,
        reconcile_interval_seconds=0.01,
    )
    task = asyncio.create_task(controller.run())
    for _ in range(20):
        if capacity.targets:
            break
        await asyncio.sleep(0.005)
    controller.stop_reconciling()
    await task

    assert store.ensure_calls == [3]
    assert capacity.targets == [3]


def test_worker_pool_controller_rejects_nonpositive_interval() -> None:
    with pytest.raises(ValueError):
        WorkerPoolController(
            StaticWorkerPoolStore(2),
            RecordingCapacity(),
            initial_desired_nodes=2,
            reconcile_interval_seconds=0,
        )


@pytest.mark.anyio
async def test_local_capacity_expands_and_delays_removal_until_workers_exit() -> None:
    launcher = FakeWorkerLauncher()
    capacity = LocalWorkerCapacity(launcher)  # type: ignore[arg-type]

    await capacity.reconcile(2)
    await capacity.reconcile(5)
    await capacity.reconcile(3)

    assert [process.index for process in launcher.started] == [1, 2, 3, 4, 5]
    assert launcher.started[3].drain_requested
    assert launcher.started[4].drain_requested
    assert capacity.managed_nodes == 5

    launcher.started[3].returncode = 0
    launcher.started[4].returncode = 0
    await capacity.reconcile(3)

    assert capacity.managed_nodes == 3
    await capacity.close()


@pytest.mark.anyio
async def test_local_capacity_validates_target_and_does_not_redrain() -> None:
    launcher = FakeWorkerLauncher()
    capacity = LocalWorkerCapacity(launcher)  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        await capacity.reconcile(0)
    await capacity.reconcile(2)
    await capacity.reconcile(1)
    await capacity.reconcile(1)

    assert launcher.started[1].drain_requested
    assert launcher.started[1].drain_requests == 1
    await capacity.close()


@pytest.mark.anyio
async def test_kubernetes_capacity_updates_only_the_worker_scale_subresource() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"spec": {"replicas": 2}})
        return httpx.Response(200, json={"spec": {"replicas": 5}})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://kubernetes.test",
    )
    capacity = KubernetesWorkerCapacity(
        namespace="trpc-agent-service",
        deployment="agent-worker",
        client=client,
    )

    await capacity.reconcile(2)
    await capacity.reconcile(5)
    await capacity.close()

    assert [request.method for request in requests] == ["GET", "GET", "PATCH"]
    assert all(request.url.path.endswith("/deployments/agent-worker/scale") for request in requests)
    assert requests[-1].headers["content-type"] == "application/merge-patch+json"
    assert requests[-1].content == b'{"spec":{"replicas":5}}'
    await client.aclose()


@pytest.mark.anyio
async def test_kubernetes_capacity_validates_configuration_and_response() -> None:
    with pytest.raises(ValueError):
        KubernetesWorkerCapacity(namespace="Bad Namespace", deployment="worker")
    with pytest.raises(ValueError):
        KubernetesWorkerCapacity(namespace="default", deployment="worker")

    async def malformed(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"spec": {"replicas": "two"}})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(malformed),
        base_url="https://kubernetes.test",
    )
    capacity = KubernetesWorkerCapacity(
        namespace="default",
        deployment="worker",
        client=client,
    )
    with pytest.raises(ValueError):
        await capacity.reconcile(0)
    with pytest.raises(ValueError):
        await capacity.reconcile(2)
    await capacity.close()
    await client.aclose()


@pytest.mark.anyio
async def test_local_launcher_sets_isolated_worker_identity_and_pid_file(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class Process:
        pid = 4321
        returncode: int | None = None

        def terminate(self) -> None:
            captured["terminated"] = True

        async def wait(self) -> int:
            self.returncode = 0
            return 0

    async def create_process(executable: str, **options: object) -> Process:
        captured["executable"] = executable
        captured.update(options)
        return Process()

    monkeypatch.setattr("trpc_service.agent.scaling.asyncio.create_subprocess_exec", create_process)
    launcher = LocalWorkerProcessLauncher(
        executable=tmp_path / "venv" / "bin" / "trpc-agent-service",
        run_dir=tmp_path / "run",
        log_dir=tmp_path / "logs",
        concurrency_per_node=3,
    )

    worker = await launcher.launch(4)
    environment = captured["env"]

    assert isinstance(environment, dict)
    assert environment["TRPC_SERVICE_RUNTIME_ROLE"] == "worker"
    assert environment["TRPC_SERVICE_NODE_ID"] == "local-worker-4"
    assert environment["TRPC_SERVICE_WORKER_CONCURRENCY"] == "3"
    assert worker.pid_file.read_text(encoding="utf-8") == "4321\n"
    worker.request_drain()
    await worker.wait()
    worker.close()
    assert captured["terminated"]
    assert not worker.pid_file.exists()


@pytest.mark.anyio
async def test_local_launcher_validates_inputs_and_closes_log_on_failure(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError):
        LocalWorkerProcessLauncher(
            executable=tmp_path / "service",
            run_dir=tmp_path / "run",
            log_dir=tmp_path / "logs",
            concurrency_per_node=0,
        )
    launcher = LocalWorkerProcessLauncher(
        executable=tmp_path / "service",
        run_dir=tmp_path / "run",
        log_dir=tmp_path / "logs",
        concurrency_per_node=1,
    )
    with pytest.raises(ValueError):
        await launcher.launch(0)

    async def fail_to_start(*_: object, **__: object) -> None:
        raise OSError("process unavailable")

    monkeypatch.setattr(
        "trpc_service.agent.scaling.asyncio.create_subprocess_exec",
        fail_to_start,
    )
    with pytest.raises(OSError):
        await launcher.launch(1)


@pytest.mark.anyio
async def test_describe_worker_pool_reports_observed_health() -> None:
    now = datetime.now(timezone.utc)

    class Registry:

        async def list(self) -> RuntimeNodeList:
            return RuntimeNodeList(
                items=[
                    RuntimeNodeRead(
                        node_id="worker-1",
                        role="worker",
                        health=RuntimeNodeHealth.ACTIVE,
                        worker_concurrency=2,
                        started_at=now,
                        heartbeat_at=now,
                        stopped_at=None,
                    ),
                    RuntimeNodeRead(
                        node_id="worker-2",
                        role="worker",
                        health=RuntimeNodeHealth.DRAINING,
                        worker_concurrency=2,
                        started_at=now,
                        heartbeat_at=now,
                        stopped_at=None,
                    ),
                    RuntimeNodeRead(
                        node_id="worker-old",
                        role="worker",
                        health=RuntimeNodeHealth.STALE,
                        worker_concurrency=2,
                        started_at=now,
                        heartbeat_at=now,
                        stopped_at=None,
                    ),
                ],
                total=3,
            )

    target = WorkerPoolTarget(
        pool_name="agent-worker",
        desired_nodes=1,
        generation=2,
        updated_by="admin",
        updated_at=now,
    )
    status = await describe_worker_pool(
        target,
        Registry(),  # type: ignore[arg-type]
        scaler_mode="local_process",
    )

    assert status.active_nodes == 1
    assert status.draining_nodes == 1
    assert status.stale_nodes == 1
    assert status.reconciling
