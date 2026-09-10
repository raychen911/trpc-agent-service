"""HTTP service foundation tests."""

from fastapi.testclient import TestClient

from trpc_service.config.settings import ServiceSettings
from trpc_service.storage.database import Database
from trpc_service.web.app import create_app


def test_health_and_readiness() -> None:
    settings = ServiceSettings(
        _env_file=None,
        app_env="test",
        database_url="sqlite+aiosqlite:///:memory:",
    )
    database = Database(settings.database_url)
    application = create_app(settings=settings, database=database)

    with TestClient(application) as client:
        health = client.get("/health")
        ready = client.get("/ready")

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert health.json()["environment"] == "test"
    assert ready.status_code == 200
    assert ready.json() == {
        "status": "ready",
        "database": True,
        "execution_queue": True,
    }
