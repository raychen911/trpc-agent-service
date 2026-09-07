from __future__ import annotations

import asyncio
import socket
from pathlib import Path

import httpx
import pytest
import uvicorn
import yaml

from tenant_agent.main import create_app
from tenant_agent.settings import Settings
from tests.helpers import make_tenant


@pytest.mark.asyncio
async def test_real_loopback_server_readiness_and_signed_webhook(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TENANT_ALPHA_WEBHOOK_TOKEN", "live-http-token")
    tenant = make_tenant()
    bootstrap = tmp_path / "tenants.yaml"
    bootstrap.write_text(
        yaml.safe_dump({"tenants": [tenant.model_dump(mode="json")]}, sort_keys=False),
        encoding="utf-8",
    )
    app = create_app(
        Settings(
            control_database_url="inmemory://",
            bootstrap_config_path=bootstrap,
            session_hmac_key="a-long-enough-test-session-hmac-key",
            admin_bearer_token="admin-test-token",
            internal_bearer_token="internal-test-token",
            worker_poll_ms=20,
            outbox_poll_seconds=0.01,
        )
    )
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = int(listener.getsockname()[1])
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            log_config=None,
            lifespan="on",
            timeout_graceful_shutdown=2,
        )
    )
    server_task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(0.01)
        assert server.started
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
            ready = await client.get("/health/ready")
            assert ready.status_code == 200
            response = await client.post(
                f"/v1/channels/web/{tenant.channels[0].binding_id}/webhook?synchronous=true",
                headers={"x-webhook-token": "live-http-token"},
                json={
                    "user_id": "socket-user",
                    "conversation_id": "socket-conversation",
                    "message_id": "socket-message",
                    "text": "real HTTP",
                },
            )
            assert response.status_code == 200
            assert response.json()["results"][0]["status"] == "processed"
    finally:
        server.should_exit = True
        await asyncio.wait_for(server_task, timeout=5)
