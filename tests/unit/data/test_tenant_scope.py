from uuid import UUID

import pytest

from trpc_service.storage.data_models import DataScope


def test_scope_is_immutable_and_diagnostic_is_pseudonymous() -> None:
    scope = DataScope(tenant_id="tenant-alpha", trace_id=UUID(int=1))
    with pytest.raises(Exception):
        scope.tenant_id = "tenant-beta"
    diagnostic = scope.diagnostic()
    assert diagnostic["tenant_digest"] != scope.tenant_id
    assert "tenant-alpha" not in str(diagnostic)


def test_scope_rejects_missing_or_blank_tenant() -> None:
    with pytest.raises(Exception):
        DataScope(tenant_id="", trace_id=UUID(int=1))
