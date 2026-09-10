from __future__ import annotations

import inspect
from datetime import datetime, timezone
from uuid import uuid4

from trpc_service.storage.models import IdempotencyKey


def assert_async_port(port: type, *methods: str) -> None:
    for name in methods:
        assert inspect.iscoroutinefunction(getattr(port, name)), (
            f"{port.__name__}.{name} must be async"
        )


async def assert_configuration_contract(repository: object) -> None:
    material = await repository.get_auth_material("binding-alpha", "local_http")
    assert material.binding_id == "binding-alpha"
    assert material.secret_ref


async def assert_idempotency_contract(repository: object) -> None:
    """Vendor-neutral behavior only: no Redis key or Python dict assertions."""
    key = IdempotencyKey(
        tenant_id="tenant-alpha", binding_id="binding-alpha",
        external_message_id=f"contract-{uuid4()}",
    )
    now = datetime.now(timezone.utc)
    first_trace, duplicate_trace = uuid4(), uuid4()
    first = await repository.claim(key, "a" * 64, first_trace, now)
    assert first.disposition.value == "acquired" and first.owner_token
    duplicate = await repository.claim(key, "a" * 64, duplicate_trace, now)
    assert duplicate.disposition.value == "processing"
    assert duplicate.original_trace_id == first_trace
    conflict = await repository.claim(key, "b" * 64, uuid4(), now)
    assert conflict.disposition.value == "conflict"
