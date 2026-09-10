import pytest

from tests.contract.data.repository_contracts import assert_tenant_isolation
from trpc_service.storage.memory import InMemoryDataRepository


@pytest.mark.asyncio
async def test_inmemory_repository_is_tenant_scoped() -> None:
    await assert_tenant_isolation(InMemoryDataRepository())
