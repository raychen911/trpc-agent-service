from __future__ import annotations

import asyncpg
import pytest

from trpc_service import probe


@pytest.fixture(autouse=True)
def _clean_probe_environment(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("TRPC_SERVICE_RUNTIME_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.delenv("TRPC_SERVICE_ENVIRONMENT", raising=False)
    monkeypatch.delenv("TRPC_SERVICE_TENANT_SECRET_ROOT", raising=False)


def test_windows_native_file_reference_is_normalized(monkeypatch, tmp_path) -> None:
    secret = tmp_path / "secret"
    secret.write_text("value\n", encoding="utf-8")
    monkeypatch.setenv("TRPC_SERVICE_TENANT_SECRET_ROOT", str(tmp_path))
    monkeypatch.setenv("FILE_REF", f"file://{secret}")

    assert probe._resolve_reference("FILE_REF") == "value"


def test_file_traversal_and_symlink_escape_are_rejected(monkeypatch, tmp_path) -> None:
    root = tmp_path / "secrets"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("outside\n", encoding="utf-8")
    monkeypatch.setenv("TRPC_SERVICE_ENVIRONMENT", "production")
    monkeypatch.setenv("TRPC_SERVICE_TENANT_SECRET_ROOT", str(root))

    monkeypatch.setenv("TRAVERSAL_REF", f"file://{root / '..' / 'outside'}")
    assert probe._resolve_reference("TRAVERSAL_REF") is None

    link = root / "link"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("symlinks are unavailable in this test environment")
    monkeypatch.setenv("SYMLINK_REF", f"file://{link}")
    assert probe._resolve_reference("SYMLINK_REF") is None


@pytest.mark.asyncio
async def test_missing_configured_database_password_fails_before_connect(
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRPC_SERVICE_DATABASE_DSN", "postgresql+asyncpg://runtime@db/service")
    monkeypatch.setenv("TRPC_SERVICE_DATABASE_PASSWORD_REF", "env://MISSING_PASSWORD")

    async def unexpected_connect(*_args, **_kwargs):
        raise AssertionError("probe must fail before opening a connection")

    monkeypatch.setattr(asyncpg, "connect", unexpected_connect)
    assert not await probe.check("admin")


@pytest.mark.asyncio
async def test_missing_configured_redis_password_fails_before_connect(
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRPC_SERVICE_DATABASE_DSN", "postgresql+asyncpg://runtime@db/service")
    monkeypatch.setenv("TRPC_SERVICE_REDIS_URL", "redis://cache/0")
    monkeypatch.setenv("TRPC_SERVICE_REDIS_PASSWORD_REF", "env://MISSING_REDIS_PASSWORD")

    async def unexpected_connect(*_args, **_kwargs):
        raise AssertionError("probe must fail before opening a connection")

    monkeypatch.setattr(asyncpg, "connect", unexpected_connect)
    assert not await probe.check("gateway")
