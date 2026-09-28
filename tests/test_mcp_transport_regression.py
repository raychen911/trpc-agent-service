"""Exercise the real HTTPX transport entry, not a mock of its stream checks."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpcore
import httpx
from mcp.client.session_group import StreamableHttpParameters
import pytest

from trpc_service.mcp.transport import PinnedMCPTransport, PinnedMCPSessionManager


@pytest.mark.anyio
@pytest.mark.parametrize("body_kind", ["empty", "json", "stream"])
async def test_pinned_transport_preserves_async_stream_for_httpcore(monkeypatch, body_kind):
    observed = []

    async def response_body():
        yield b'{}'

    async def send(request):
        body = b"".join([chunk async for chunk in request.stream])
        observed.append((request.url, request.headers, body, request.extensions))
        return httpcore.Response(200, content=response_body())

    async def upload():
        yield b'{"method":'
        yield b'"initialize"}'

    transport = PinnedMCPTransport("https://mcp.example:8443/mcp", ("8.8.8.8", ))
    monkeypatch.setattr(transport._pool, "handle_async_request", send)
    async with httpx.AsyncClient(transport=transport) as client:
        if body_kind == "empty":
            response = await client.get("https://mcp.example:8443/mcp")
        elif body_kind == "json":
            response = await client.post("https://mcp.example:8443/mcp",
                                         json={"method": "initialize"})
        else:
            response = await client.post("https://mcp.example:8443/mcp", content=upload())
    assert response.status_code == 200
    url, headers, body, extensions = observed[0]
    assert url.host == b"8.8.8.8" and url.port == 8443
    assert (b"Host", b"mcp.example:8443") in headers
    assert extensions["sni_hostname"] == "mcp.example"
    assert body == (b"" if body_kind == "empty" else b'{"method":"initialize"}')


@pytest.mark.anyio
async def test_sdk_session_failure_never_formats_authentication_headers(monkeypatch, caplog):
    token = "sensitive-mcp-token-for-regression"
    manager = PinnedMCPSessionManager(
        StreamableHttpParameters(url="https://mcp.example/mcp",
                                 headers={"Authorization": f"Bearer {token}"}), ("8.8.8.8", ))

    @asynccontextmanager
    async def unavailable(headers=None) -> AsyncIterator[object]:
        raise httpx.ConnectError("connection unavailable")
        yield

    monkeypatch.setattr(manager, "_create_client", unavailable)
    try:
        with pytest.raises(RuntimeError) as failure:
            await manager.create_session()
        assert token not in str(failure.value)
        assert token not in caplog.text
        assert token not in repr(manager._connection_params)
        # Authentication must still be sent to the provider, not removed.
        assert manager._merge_headers()["Authorization"] == f"Bearer {token}"
    finally:
        await manager.close()
