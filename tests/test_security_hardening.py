import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from threading import Event

import httpx
import pytest
from fastapi import HTTPException
from mcp.client.session_group import StreamableHttpParameters

from trpc_service.admin.login_guard import LoginGuard, password_work
from trpc_service.config import Settings
from trpc_service.mcp.transport import PinnedMCPSessionManager, PinnedMCPTransport


@pytest.mark.anyio
async def test_mcp_connection_uses_checked_ip_and_original_tls_identity(
    monkeypatch: pytest.MonkeyPatch, ) -> None:
    observed: list[httpx.Request] = []

    async def socket_send(self: object, request: httpx.Request) -> httpx.Response:
        observed.append(request)
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", socket_send)
    async with PinnedMCPTransport("https://mcp.example:8443/mcp", ("8.8.8.8", )) as transport:
        request = httpx.Request("POST", "https://mcp.example:8443/mcp?session=1", json={})
        await transport.handle_async_request(request)
        with pytest.raises(PermissionError, match="authorized origin"):
            await transport.handle_async_request(httpx.Request("GET", "https://127.0.0.1/"))
        with pytest.raises(PermissionError, match="authorized origin"):
            await transport.handle_async_request(httpx.Request("GET", "http://mcp.example/"))
    assert len(observed) == 1
    assert observed[0].url.host == "8.8.8.8"
    assert observed[0].url.port == 8443
    assert observed[0].headers["host"] == "mcp.example:8443"
    assert observed[0].extensions["sni_hostname"] == "mcp.example"
    assert observed[0].url.query == b"session=1"


@pytest.mark.anyio
async def test_login_capacity_is_not_released_when_kdf_caller_is_cancelled() -> None:
    guard = LoginGuard(1, 10)
    started, release = Event(), Event()

    def kdf() -> bool:
        started.set()
        assert release.wait(5)
        return True

    async def login() -> None:
        async with guard.admit("source"):
            await password_work(kdf)

    task = asyncio.create_task(login())
    try:
        assert await asyncio.to_thread(started.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        with pytest.raises(HTTPException) as failure:
            async with guard.admit("other"):
                pytest.fail("cancelled KDF must still hold its capacity")
        assert failure.value.status_code == 429
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    async with guard.admit("other"):
        pass


@pytest.mark.anyio
async def test_unknown_login_sources_are_rate_limited_without_unbounded_counters() -> None:
    guard = LoginGuard(2, 2)
    for _ in range(2):
        async with guard.admit("unknown"):
            pass
    with pytest.raises(HTTPException) as failure:
        async with guard.admit("unknown"):
            pytest.fail("source must be limited")
    assert failure.value.headers == {"Retry-After": "60"}


def test_execution_facts_must_share_the_delivery_database() -> None:
    settings = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://trpc@db:5432/facts",
        storage_backends={
            "primary": {
                "kind": "postgresql",
                "url": "postgresql+asyncpg://trpc@db/facts"
            },
            "other": {
                "kind": "postgresql",
                "url": "postgresql+asyncpg://trpc@db/other"
            },
            "local": {
                "kind": "inmemory"
            },
        },
        storage_profile={"session": "primary"},
    )
    settings.validate_execution_backends({})
    with pytest.raises(ValueError, match="share the primary"):
        settings.validate_execution_backends({"session": "other"})
    with pytest.raises(ValueError, match="primary PostgreSQL"):
        settings.validate_execution_backends({"session": "local"})


def test_production_rejects_insecure_browser_sessions() -> None:
    with pytest.raises(ValueError, match="secure cookies"):
        Settings(_env_file=None, environment="production")


@pytest.mark.parametrize("environment", ["production", "Production", "PRODUCTION"])
def test_production_never_exempts_sqlite_facts(environment: str) -> None:
    with pytest.raises(ValueError, match="primary PostgreSQL"):
        Settings(_env_file=None,
                 environment=environment,
                 secure_cookies=True,
                 database_url="sqlite+aiosqlite:///:memory:")


@pytest.mark.anyio
async def test_mcp_fails_over_checked_ips_only_before_sending_request(
    monkeypatch: pytest.MonkeyPatch, ) -> None:
    addresses = ("2606:4700:4700::1111", "8.8.8.8")
    attempts: list[str] = []
    failure: type[httpx.TransportError] = httpx.ConnectError

    async def send(self: object, request: httpx.Request) -> httpx.Response:
        attempts.append(request.url.host)
        if request.url.host == addresses[0]:
            raise failure("network failure")
        return httpx.Response(200)

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", send)
    async with PinnedMCPTransport("https://mcp.example/mcp", addresses) as transport:
        request = httpx.Request("POST", "https://mcp.example/mcp", json={})
        response = await transport.handle_async_request(request)
        assert response.status_code == 200
        assert attempts == list(addresses)
        attempts.clear()
        failure = httpx.ReadError
        with pytest.raises(httpx.ReadError):
            await transport.handle_async_request(request)
        assert attempts == [addresses[0]]


@pytest.mark.anyio
async def test_mcp_sdk_transport_owns_secure_client_and_preserves_auth_headers(
    monkeypatch: pytest.MonkeyPatch, ) -> None:
    clients: list[httpx.AsyncClient] = []

    @asynccontextmanager
    async def stream_client(url: str, *, http_client: httpx.AsyncClient,
                            terminate_on_close: bool) -> AsyncIterator[str]:
        assert url == "https://mcp.example/mcp"
        assert terminate_on_close
        assert not http_client.follow_redirects
        assert not http_client.trust_env
        assert http_client.headers["Authorization"] == "Bearer scoped-test-token"
        assert isinstance(http_client._transport, PinnedMCPTransport)
        clients.append(http_client)
        yield "streams"

    monkeypatch.setattr("trpc_service.mcp.transport.streamable_http_client", stream_client)
    manager = PinnedMCPSessionManager(StreamableHttpParameters(url="https://mcp.example/mcp"),
                                      ("8.8.8.8", ))
    async with manager._create_streamable_http_client({"Authorization":
                                                       "Bearer scoped-test-token"}) as streams:
        assert streams == "streams"
        assert not clients[0].is_closed
    assert clients[0].is_closed


@pytest.mark.parametrize("prefix", ["//evil.example", "/api/", "/api?redirect=x", "/api<script>"])
def test_api_prefix_cannot_inject_a_browser_origin_or_script(prefix: str) -> None:
    with pytest.raises(ValueError, match="API prefix"):
        Settings(_env_file=None, api_prefix=prefix)
