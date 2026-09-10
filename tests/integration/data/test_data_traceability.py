from uuid import UUID
from trpc_service.storage.data_models import DataScope

def test_scope_keeps_trace_fields():
    scope=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1),owner_trace_id=UUID(int=2),execution_trace_id=UUID(int=3))
    assert scope.trace_id.int == 1 and scope.owner_trace_id.int == 2 and scope.execution_trace_id.int == 3
