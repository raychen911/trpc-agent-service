import httpx
import pytest

from trpc_service.config import Settings
from tests.conftest import create_test_app


@pytest.mark.anyio
async def test_health_reports_service_identity() -> None:
    settings = Settings(
        _env_file=None,
        database_url="sqlite+aiosqlite:///:memory:",
        auto_create_schema=True,
    )
    app = create_test_app(settings)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    await app.state.engine.dispose()

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "trpc-agent-service",
        "version": "0.1.0",
    }


@pytest.mark.anyio
async def test_readiness_checks_database_connection() -> None:
    settings = Settings(
        _env_file=None,
        database_url="sqlite+aiosqlite:///:memory:",
        auto_create_schema=True,
    )
    app = create_test_app(settings)
    transport = httpx.ASGITransport(app=app)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/ready")

    await app.state.engine.dispose()

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "checks": {
            "database": "ok",
            "worker_nodes": 1,
        },
    }


@pytest.mark.anyio
async def test_control_plane_stays_ready_without_workers_and_exposes_same_origin_config() -> None:
    settings = Settings(_env_file=None,
                        database_url="sqlite+aiosqlite:///:memory:",
                        auto_create_schema=True,
                        worker_concurrency=0,
                        api_prefix="/custom/v2")
    app = create_test_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://test:8765") as client:
            ready = await client.get("/ready")
            config = await client.get("/console/config.js")
            api = await client.get("/custom/v2/admin/me")
    assert ready.status_code == 200
    assert ready.json()["checks"]["worker_nodes"] == 0
    assert '"apiPrefix": "/custom/v2"' in config.text
    assert config.headers["cache-control"] == "no-store"
    assert api.status_code == 401
