import httpx
import pytest
from pytest import LogCaptureFixture
from fastapi import FastAPI

from trpc_service.web.errors import install_exception_handlers


@pytest.mark.anyio
async def test_not_found_uses_the_public_error_contract(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/api/v1/tenants/00000000-0000-0000-0000-000000000000")

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "not_found",
            "message": "tenant not found",
        }
    }


@pytest.mark.anyio
async def test_validation_errors_do_not_echo_request_values(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/api/v1/tenants/not-a-uuid")

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "validation_error"
    assert body["error"]["message"] == "request validation failed"
    assert "not-a-uuid" not in response.text


@pytest.mark.anyio
async def test_unhandled_exception_uses_safe_public_error_contract(
        caplog: LogCaptureFixture) -> None:
    app = FastAPI()
    install_exception_handlers(app)

    @app.get("/explode")
    async def explode() -> None:
        raise RuntimeError("private database and credential details")

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/explode")

    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "code": "internal_error",
            "message": "internal server error",
        }
    }
    assert "private database" not in response.text
    assert "private database" not in caplog.text
    assert "RuntimeError" in caplog.text
