from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import httpx
import pytest

from trpc_service._cli import build_signed_request
from trpc_service.web.app import create_shared_app


@pytest.mark.shared_backend
async def test_two_http_nodes_expose_processing_cached_and_conflict_without_backend_details(
    runtime_secret_env: dict[str, str], shared_redis_url: str, shared_database_url: str
) -> None:
    del shared_redis_url, shared_database_url
    base = dict(os.environ)
    base.update(runtime_secret_env)
    app_a = create_shared_app({**base, "TRPC_NODE_ID": "http-a"})
    app_b = create_shared_app({**base, "TRPC_NODE_ID": "http-b"})

    class Args:
        url = "http://127.0.0.1"
        binding_id = "binding-alpha"
        secret_env = "TRPC_DEMO_ALPHA_SECRET"
        # The shared database intentionally survives test processes. Keep this
        # scenario unique across runs while reusing the value inside this run
        # to exercise processing/cached/conflict semantics.
        external_message_id = f"http-duplicate-{uuid4().hex}"
        external_user_id = "http-user"
        conversation_type = "direct"
        external_conversation_id = "http-conversation"
        text = "Remember validation token ALPHA."
        trace_id = None

    request = build_signed_request(Args, base)
    async with app_a.router.lifespan_context(app_a), app_b.router.lifespan_context(app_b):
        clients = [httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") for app in (app_a, app_b)]
        first = await asyncio.gather(*[client.post(request.url, content=request.content, headers=request.headers) for client in clients])
        payloads = [response.json() for response in first]
        assert sum(item["status"] == "succeeded" for item in payloads) == 1
        assert sum(item["status"] in {"processing", "duplicate"} for item in payloads) == 1
        assert sum(item.get("data") and item["data"]["delivery_action"] == "deliver" for item in payloads) == 1
        assert app_a.state.runtime.worker.call_count + app_b.state.runtime.worker.call_count == 1
        cached = (await clients[1].post(request.url, content=request.content, headers=request.headers)).json()
        assert cached["status"] == "duplicate" and cached["data"]["delivery_action"] == "suppress"

        Args.text = "Recall the validation token."
        conflict_request = build_signed_request(Args, base)
        conflict_response = await clients[0].post(conflict_request.url, content=conflict_request.content, headers=conflict_request.headers)
        assert conflict_response.status_code == 409
        body = conflict_response.text.lower()
        assert "redis" not in body and "postgres" not in body and "dsn" not in body
        for client in clients:
            await client.aclose()
