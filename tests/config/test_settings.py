"""Fail-closed production configuration tests."""

from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from trpc_service.config import Environment, Settings


def _production(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "_env_file": None,
        "env": Environment.PRODUCTION,
        "database_url": "postgresql+asyncpg://runtime:password@db/agent",
        "secret_key": SecretStr("s" * 40),
        "admin_api_key": SecretStr("a" * 40),
        "public_base_url": "https://agent.example.test",
    }
    values.update(overrides)
    return Settings(**values)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("database_url", "sqlite+aiosqlite:///./data/prod.db"),
        ("secret_key", SecretStr("change-me-with-at-least-32-random-characters")),
        ("admin_api_key", SecretStr("short")),
        ("public_base_url", "http://agent.example.test"),
    ],
)
def test_production_rejects_insecure_configuration(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        _production(**{field: value})


def test_secret_values_are_not_exposed_by_repr() -> None:
    settings = _production()
    rendered = repr(settings)

    assert "s" * 40 not in rendered
    assert "a" * 40 not in rendered
