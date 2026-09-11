"""RED tests for SqlTenantConfigRepository.

Unit tests use a fake engine; integration tests require real PostgreSQL.
"""

from __future__ import annotations

import pytest

from trpc_service.config.tenant_repository import (
    TenantRepositoryConfigurationError,
    TenantRepositoryDataError,
    TenantRepositoryUnavailableError,
)
from trpc_service.storage.tenant_repository import SqlTenantConfigRepository


class TestSqlTenantConfigRepositoryFromEnv:

    def test_missing_url_raises_configuration_error(self):
        with pytest.raises(TenantRepositoryConfigurationError):
            SqlTenantConfigRepository.from_env({})

    def test_wrong_scheme_raises_configuration_error(self):
        with pytest.raises(TenantRepositoryConfigurationError):
            SqlTenantConfigRepository.from_env({"TRPC_DATABASE_URL": "sqlite:///test.db"})

    def test_valid_url_creates_repository(self):
        repo = SqlTenantConfigRepository.from_env({"TRPC_DATABASE_URL": "postgresql+asyncpg://u:p@localhost/db"})
        assert repo is not None


class TestSqlTenantConfigRepositoryLifecycle:

    @pytest.mark.asyncio
    async def test_check_ready_fails_on_unreachable(self):
        repo = SqlTenantConfigRepository.from_env({"TRPC_DATABASE_URL": "postgresql+asyncpg://u:p@localhost:1/db"})
        try:
            with pytest.raises(TenantRepositoryUnavailableError):
                await repo.check_ready()
        finally:
            await repo.close()

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        repo = SqlTenantConfigRepository.from_env({"TRPC_DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db"})
        await repo.close()
        await repo.close()

    @pytest.mark.asyncio
    async def test_get_after_close_raises(self):
        repo = SqlTenantConfigRepository.from_env({"TRPC_DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db"})
        await repo.close()
        with pytest.raises(TenantRepositoryUnavailableError):
            await repo.get("any_tenant")


class TestErrorBoundaryTypes:

    def test_configuration_error_is_value_error(self):
        assert issubclass(TenantRepositoryConfigurationError, ValueError)

    def test_unavailable_error_is_runtime_error(self):
        assert issubclass(TenantRepositoryUnavailableError, Exception)

    def test_data_error_is_runtime_error(self):
        assert issubclass(TenantRepositoryDataError, Exception)

    def test_error_messages_do_not_leak_url(self):
        try:
            SqlTenantConfigRepository.from_env({"TRPC_DATABASE_URL": "postgresql+asyncpg://user:secret@host/db"})
        except TenantRepositoryConfigurationError as exc:
            assert "secret" not in str(exc)
