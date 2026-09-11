"""Lifecycle contract for binding-owned IM channel services."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest

from trpc_service.channels.binding import ChannelBinding
from trpc_service.config.secret_resolver import EnvSecretResolver


@dataclass
class _Service:
    started: int = 0
    closed: int = 0
    fail_start: bool = False

    async def start(self) -> None:
        self.started += 1
        if self.fail_start:
            raise RuntimeError("upstream detail must not escape")

    async def close(self) -> None:
        self.closed += 1


class _Repository:

    def __init__(self, bindings: tuple[ChannelBinding, ...] = ()) -> None:
        self.bindings = bindings
        self.fail = False

    async def list_enabled(self) -> tuple[ChannelBinding, ...]:
        if self.fail:
            raise RuntimeError("database endpoint must not escape")
        return self.bindings


def _binding(
    *,
    binding_id: UUID | None = None,
    version: int = 1,
    external_account_id: str = "account_demo",
    secret_ref: str = "env:TRPC_IM_SECRET",
) -> ChannelBinding:
    return ChannelBinding(
        binding_id=binding_id or uuid4(),
        tenant_id="tenant_demo",
        app_id="app_demo",
        channel="wecom",
        external_account_id=external_account_id,
        secret_ref=secret_ref,
        enabled=True,
        version=version,
    )


def _manager(repository: _Repository, services: list[_Service], *, secrets: Mapping[str, str] | None = None):
    from trpc_service.channels.service_manager import ChannelServiceManager

    def factory(_binding: ChannelBinding, _secret: str) -> _Service:
        service = _Service()
        services.append(service)
        return service

    return ChannelServiceManager(repository, EnvSecretResolver(secrets or {"TRPC_IM_SECRET": "value"}), factory)


@pytest.mark.asyncio
async def test_start_loads_enabled_binding_and_starts_its_service() -> None:
    repository = _Repository((_binding(), ))
    services: list[_Service] = []
    manager = _manager(repository, services)

    await manager.start()

    assert len(services) == 1
    assert services[0].started == 1
    await manager.close()
    assert services[0].closed == 1


@pytest.mark.asyncio
async def test_refresh_starts_replacement_before_closing_old_service() -> None:
    binding_id = uuid4()
    repository = _Repository((_binding(binding_id=binding_id), ))
    services: list[_Service] = []
    manager = _manager(repository, services)
    await manager.start()

    repository.bindings = (_binding(binding_id=binding_id, version=2), )
    assert await manager.refresh_once() is True

    assert [service.started for service in services] == [1, 1]
    assert services[0].closed == 1
    assert services[1].closed == 0
    await manager.close()
    assert services[1].closed == 1


@pytest.mark.asyncio
async def test_refresh_closes_service_once_when_binding_is_disabled_or_deleted() -> None:
    repository = _Repository((_binding(), ))
    services: list[_Service] = []
    manager = _manager(repository, services)
    await manager.start()

    repository.bindings = ()
    assert await manager.refresh_once() is True
    assert await manager.refresh_once() is True
    await manager.close()

    assert services[0].closed == 1


@pytest.mark.asyncio
async def test_failed_refresh_keeps_authenticated_old_service() -> None:
    repository = _Repository((_binding(), ))
    services: list[_Service] = []
    manager = _manager(repository, services)
    await manager.start()

    repository.fail = True
    assert await manager.refresh_once() is False
    assert services[0].closed == 0
    await manager.close()
    assert services[0].closed == 1


@pytest.mark.asyncio
async def test_failed_replacement_does_not_close_old_service() -> None:
    binding_id = uuid4()
    repository = _Repository((_binding(binding_id=binding_id), ))
    services: list[_Service] = []
    fail_replacement = False

    def factory(_binding: ChannelBinding, _secret: str) -> _Service:
        service = _Service(fail_start=fail_replacement)
        services.append(service)
        return service

    from trpc_service.channels.service_manager import ChannelServiceManager

    manager = ChannelServiceManager(repository, EnvSecretResolver({"TRPC_IM_SECRET": "value"}), factory)
    await manager.start()
    fail_replacement = True
    repository.bindings = (_binding(binding_id=binding_id, version=2), )

    assert await manager.refresh_once() is True
    assert services[0].closed == 0
    assert services[1].closed == 1
    await manager.close()
    assert services[0].closed == 1


@pytest.mark.asyncio
async def test_initial_binding_failure_does_not_block_other_binding() -> None:
    repository = _Repository((_binding(external_account_id="bad"), _binding(external_account_id="good")))
    services: list[_Service] = []

    def factory(binding: ChannelBinding, _secret: str) -> _Service:
        service = _Service(fail_start=binding.external_account_id == "bad")
        services.append(service)
        return service

    from trpc_service.channels.service_manager import ChannelServiceManager
    manager = ChannelServiceManager(repository, EnvSecretResolver({"TRPC_IM_SECRET": "value"}), factory)

    await manager.start()

    assert [service.started for service in services] == [1, 1]
    assert [service.closed for service in services] == [1, 0]
    await manager.close()
    assert [service.closed for service in services] == [1, 1]


@pytest.mark.asyncio
async def test_failed_binding_is_retried_on_next_refresh() -> None:
    repository = _Repository((_binding(external_account_id="bad"), _binding(external_account_id="good")))
    services: list[_Service] = []
    fail_bad = True

    def factory(binding: ChannelBinding, _secret: str) -> _Service:
        service = _Service(fail_start=fail_bad and binding.external_account_id == "bad")
        services.append(service)
        return service

    from trpc_service.channels.service_manager import ChannelServiceManager
    manager = ChannelServiceManager(repository, EnvSecretResolver({"TRPC_IM_SECRET": "value"}), factory)

    await manager.start()
    fail_bad = False

    assert await manager.refresh_once() is True

    assert [service.started for service in services] == [1, 1, 1]
    assert [service.closed for service in services[:2]] == [1, 0]
    await manager.close()
    assert [service.closed for service in services[1:]] == [1, 1]


@pytest.mark.asyncio
async def test_initial_secret_failure_does_not_block_startup_and_retries() -> None:
    from trpc_service.channels.service_manager import ChannelServiceManager

    repository = _Repository((_binding(), ))
    environ: dict[str, str] = {}
    services: list[_Service] = []

    def factory(_binding: ChannelBinding, _secret: str) -> _Service:
        service = _Service()
        services.append(service)
        return service

    manager = ChannelServiceManager(repository, EnvSecretResolver(environ), factory)

    await manager.start()

    assert services == []
    environ["TRPC_IM_SECRET"] = "value"
    assert await manager.refresh_once() is True
    assert [service.started for service in services] == [1]
    await manager.close()
    assert [service.closed for service in services] == [1]


@pytest.mark.asyncio
async def test_initial_repository_failure_raises_fixed_safe_error() -> None:
    from trpc_service.channels.service_manager import ChannelServiceManager, ChannelServiceManagerStartupError

    repository = _Repository((_binding(), ))
    repository.fail = True
    manager = ChannelServiceManager(repository, EnvSecretResolver({"TRPC_IM_SECRET": "value"}),
                                    lambda _binding, _secret: _Service())

    with pytest.raises(ChannelServiceManagerStartupError, match="^channel services could not be started$"):
        await manager.start()
