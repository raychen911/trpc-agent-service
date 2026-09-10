from __future__ import annotations

import pytest

from trpc_service.storage.session_backend import SessionBackendFactory


async def test_backends_are_tenant_agent_scoped_and_close_is_idempotent() -> None:
    factory = SessionBackendFactory()
    alpha = factory.get_backend("tenant-alpha", "agent-alpha")
    assert alpha is factory.get_backend("tenant-alpha", "agent-alpha")
    assert alpha is not factory.get_backend("tenant-beta", "agent-beta")
    assert alpha is not factory.get_backend("tenant-alpha", "agent-other")
    await factory.close()
    await factory.close()
    with pytest.raises(RuntimeError, match="closed"):
        factory.get_backend("tenant-alpha", "agent-alpha")
