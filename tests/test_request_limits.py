from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request

from trpc_service.web.body_limit import RequestBodyLimitMiddleware
from trpc_service.web.errors import install_exception_handlers


@pytest.fixture
def bounded_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(RequestBodyLimitMiddleware, api_prefix="/custom/v2", max_bytes=64)
    install_exception_handlers(app)

    @app.post("/custom/v2/auth/login")
    @app.post("/custom/v2/tenants/t/knowledge-bases/k/documents")
    @app.put("/custom/v2/tenants/t/knowledge-bases/k/documents/d")
    async def consume(request: Request) -> dict[str, int]:
        return {"size": len(await request.body())}

    @app.get("/limited")
    async def limited() -> None:
        raise HTTPException(429, "try later", headers={"Retry-After": "60"})

    return app


@pytest.mark.anyio
async def test_limit_counts_chunked_body_even_if_declared_length_is_smaller(
    bounded_app: FastAPI, ) -> None:
    consumed: list[int] = []

    async def body() -> AsyncIterator[bytes]:
        for i in range(10):
            consumed.append(i)
            yield b"x" * 32

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bounded_app),
                                 base_url="http://test") as client:
        for headers in ({}, {"Content-Length": "1"}):
            consumed.clear()
            response = await client.post("/custom/v2/auth/login", content=body(), headers=headers)
            assert response.status_code == 413
            assert response.json()["error"]["code"] == "payload_too_large"
            assert consumed == [0, 1, 2]


@pytest.mark.anyio
async def test_upload_allowance_is_route_scoped_and_normal_input_is_preserved(
    bounded_app: FastAPI, ) -> None:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bounded_app),
                                 base_url="http://test") as client:
        exact = await client.post("/custom/v2/auth/login", content=b"x" * 64)
        assert exact.json() == {"size": 64}
        for method, path in (("POST", ""), ("PUT", "/d")):
            upload = await client.request(method,
                                          "/custom/v2/tenants/t/knowledge-bases/k/documents" + path,
                                          content=b"x" * 1024)
            assert upload.json() == {"size": 1024}
        wrong_route = await client.post("/custom/v2/auth/login",
                                        content=b"x" * 1024,
                                        headers={"Content-Type": "application/pdf"})
        assert wrong_route.status_code == 413
        oversized = await client.post("/custom/v2/tenants/t/knowledge-bases/k/documents",
                                      content=b"x",
                                      headers={"Content-Length": str(11 * 1024**2)})
        assert oversized.status_code == 413


@pytest.mark.anyio
async def test_invalid_lengths_and_retry_after_keep_safe_public_contract(
    bounded_app: FastAPI, ) -> None:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bounded_app),
                                 base_url="http://test") as client:
        for length in ("-1", "invalid"):
            response = await client.post("/custom/v2/auth/login",
                                         content=b"x",
                                         headers={"Content-Length": length})
            assert response.status_code == 400
        response = await client.get("/limited")
        assert response.status_code == 429
        assert response.headers["Retry-After"] == "60"
        assert response.json()["error"]["code"] == "rate_limited"


@pytest.mark.anyio
async def test_actual_login_rejects_body_before_json_parsing(api_client: httpx.AsyncClient) -> None:
    response = await api_client.post("/api/v1/auth/login", content=b"x" * (1024**2 + 1))
    assert response.status_code == 413
    assert response.headers["X-Request-ID"]


@pytest.mark.anyio
async def test_interrupted_input_never_reaches_the_application() -> None:
    from starlette.types import Message, Receive, Scope, Send

    called: list[str] = []
    sent: list[Message] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        called.append(scope["type"])

    async def disconnect() -> Message:
        return {"type": "http.disconnect"}

    async def timeout() -> Message:
        raise TimeoutError

    async def send(message: Message) -> None:
        sent.append(message)

    middleware = RequestBodyLimitMiddleware(app, api_prefix="/api/v1", max_bytes=64)
    scope: Scope = {"type": "http", "method": "POST", "path": "/api/v1/auth/login", "headers": []}
    await middleware(scope, disconnect, send)
    assert not sent and not called
    await middleware(scope, timeout, send)
    assert sent[0]["status"] == 408
    assert not called
    await middleware({"type": "lifespan"}, disconnect, send)
    assert called == ["lifespan"]
