from uuid import UUID

from trpc_service.storage.data_models import DataScope, MemoryRecord


async def assert_tenant_isolation(repository) -> None:
    scope_a = DataScope(tenant_id="tenant-alpha", trace_id=UUID(int=1))
    scope_b = DataScope(tenant_id="tenant-beta", trace_id=UUID(int=2))
    first = MemoryRecord(tenant_id="tenant-alpha", namespace="default", memory_key="same", content={"v": 1})
    await repository.compare_and_set(scope_a, first, expected_version=None)
    assert await repository.read_content(scope_b, "default", "same") is None
    metadata = await repository.get_metadata(scope_a, "default", "same")
    assert metadata.tenant_id == "tenant-alpha"
