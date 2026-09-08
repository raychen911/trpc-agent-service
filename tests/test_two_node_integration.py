"""Real two-process integration test backed by PostgreSQL and Redis."""

import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.integration


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _get_json(url: str, token: str) -> Any:
    request = urllib.request.Request(url, headers={"x-gateway-token": token})
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.load(response)


def _wait_ready(base_url: str, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/health/ready", timeout=1) as response:
                if response.status == 200:
                    return
        except Exception:
            time.sleep(0.2)
    raise AssertionError(f"node did not become ready: {base_url}")


def _wait_for_failover(
    base_url: str,
    query: str,
    token: str,
    previous_node_id: str,
    timeout: float = 20,
) -> dict[str, Any]:
    """Tolerate transient connection errors while the dead node TTL expires."""

    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    last_route: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        try:
            last_route = _get_json(
                f"{base_url}/gateway/v1/routes/resolve?{query}",
                token,
            )
            if last_route["node_id"] != previous_node_id:
                return last_route
        except (OSError, TimeoutError) as error:
            last_error = error
        time.sleep(0.5)
    raise AssertionError(
        "route did not fail over before the deadline; "
        f"last_route={last_route!r}, last_error={last_error!r}"
    )


def test_real_two_nodes_register_route_and_fail_over() -> None:
    database_url = os.getenv("TRPC_INTEGRATION_DATABASE_URL")
    redis_url = os.getenv("TRPC_INTEGRATION_REDIS_URL")
    if not database_url or not redis_url:
        pytest.skip("set TRPC_INTEGRATION_DATABASE_URL and TRPC_INTEGRATION_REDIS_URL")
    if not database_url.startswith("postgresql"):
        pytest.fail("TRPC_INTEGRATION_DATABASE_URL must use PostgreSQL")

    root = Path(__file__).resolve().parents[1]
    token = "two-node-integration-secret"
    base_environment = os.environ.copy()
    base_environment.update(
        {
            "TRPC_SERVICE_ENVIRONMENT": "test",
            "TRPC_SERVICE_DATABASE_URL": database_url,
            "TRPC_SERVICE_AUTO_CREATE_SCHEMA": "false",
            "TRPC_SERVICE_CONVERSATION_BACKEND": "sql",
            "TRPC_SERVICE_COORDINATION_BACKEND": "redis",
            "TRPC_SERVICE_REDIS_URL": redis_url,
            "TRPC_SERVICE_GATEWAY_INTERNAL_SECRET": token,
            "TRPC_SERVICE_OUTBOX_WORKER_ENABLED": "false",
            "TRPC_SERVICE_INBOUND_WORKER_ENABLED": "false",
            "TRPC_SERVICE_NODE_TTL_SECONDS": "4",
            "TRPC_SERVICE_NODE_HEARTBEAT_SECONDS": "1",
        }
    )
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=root,
        env=base_environment,
        check=True,
        timeout=60,
    )
    from sqlalchemy import select

    from trpc_service.storage.database import Database
    from trpc_service.storage.models import Tenant
    from trpc_service.storage.tenant_context import tenant_database_scope

    database = Database(database_url)
    suffix = uuid.uuid4().hex[:12]
    with database.session_factory.begin() as session:
        first = Tenant(
            slug=f"rls-a-{suffix}",
            name="RLS A",
            audit_policy={},
            key_namespace=f"rls/a/{suffix}",
        )
        second = Tenant(
            slug=f"rls-b-{suffix}",
            name="RLS B",
            audit_policy={},
            key_namespace=f"rls/b/{suffix}",
        )
        session.add_all((first, second))
        session.flush()
        first_id, second_id = first.id, second.id
    with tenant_database_scope(first_id), database.session_factory() as session:
        visible = set(session.scalars(select(Tenant.id)))
        assert first_id in visible
        assert second_id not in visible
    database.dispose()
    ports = (_free_port(), _free_port())
    processes: list[subprocess.Popen[bytes]] = []
    failed_over: dict[str, Any] | None = None
    try:
        for index, port in enumerate(ports, start=1):
            environment = dict(base_environment)
            environment.update(
                {
                    "TRPC_SERVICE_NODE_ID": f"integration-node-{index}",
                    "TRPC_SERVICE_NODE_BASE_URL": f"http://127.0.0.1:{port}",
                }
            )
            processes.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "uvicorn",
                        "trpc_service.web.app:app",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                    ],
                    cwd=root,
                    env=environment,
                    # An unread PIPE can fill and block Uvicorn on Windows.
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT,
                )
            )
        bases = tuple(f"http://127.0.0.1:{port}" for port in ports)
        for base in bases:
            _wait_ready(base)
        deadline = time.monotonic() + 10
        nodes: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            nodes = _get_json(f"{bases[0]}/gateway/v1/nodes", token)
            if len(nodes) == 2:
                break
            time.sleep(0.2)
        assert {node["node_id"] for node in nodes} == {
            "integration-node-1",
            "integration-node-2",
        }

        query = "tenant_id=t1&agent_app_id=a1&session_id=shared-session"
        route = _get_json(f"{bases[0]}/gateway/v1/routes/resolve?{query}", token)
        repeated = _get_json(f"{bases[1]}/gateway/v1/routes/resolve?{query}", token)
        assert repeated["node_id"] == route["node_id"]

        selected_index = int(route["node_id"].rsplit("-", 1)[1]) - 1
        processes[selected_index].terminate()
        processes[selected_index].wait(timeout=10)
        survivor = bases[1 - selected_index]
        assert processes[1 - selected_index].poll() is None
        failed_over = _wait_for_failover(survivor, query, token, route["node_id"])
        assert failed_over["node_id"] != route["node_id"]
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
