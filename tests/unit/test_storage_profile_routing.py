from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest

import trpc_service._cli as cli
from trpc_service.storage.services import (
    CompositeTenantServiceFactory,
    RegisteredTenantServiceBundle,
    TenantDataServices,
    TenantStorageProfileRegistry,
)
from trpc_service.tenant.models import (
    AuditPolicy,
    BudgetPolicy,
    ModelPolicy,
    StorageSelection,
    TenantConfig,
    TenantContext,
    ToolPolicy,
)


def config(tenant_id: str = "tenant-a") -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant_id,
        app_id="app",
        version=1,
        model=ModelPolicy(provider="fake", model="fake"),
        tools=ToolPolicy(),
        budget=BudgetPolicy(),
        audit=AuditPolicy(),
        storage=StorageSelection(profile_id="profile-a"),
    )


def context(tenant_id: str = "tenant-a") -> TenantContext:
    return TenantContext(
        tenant_id=tenant_id,
        app_id="app",
        config_version=1,
        channel_binding_id="binding",
        principal_id="user",
        session_id="session",
        request_id="request",
        trace_id="trace",
    )


def _bundle() -> TenantDataServices:
    sentinel = cast(Any, object())
    return TenantDataServices(
        sentinel,
        sentinel,
        sentinel,
        sentinel,
        sentinel,
        sentinel,
    )


@dataclass
class _DefaultFactory:
    services: TenantDataServices
    calls: int = 0

    async def for_context(self, _context: object, _config: object) -> TenantDataServices:
        self.calls += 1
        return self.services


@pytest.mark.asyncio
async def test_composite_routes_only_an_exact_tenant_profile_registration() -> None:
    registered = _bundle()
    fallback_bundle = _bundle()
    fallback = _DefaultFactory(fallback_bundle)
    selection = StorageSelection(
        profile_id="profile-a",
        session_backend="redis",
        memory_backend="postgresql",
        artifact_backend="s3",
        knowledge_backend="external_memory",
    )
    registry = TenantStorageProfileRegistry(
        {
            ("tenant-a", "profile-a"): RegisteredTenantServiceBundle(
                selection=selection,
                services=registered,
            )
        }
    )
    factory = CompositeTenantServiceFactory(registry, fallback)

    selected = config().model_copy(update={"storage": selection})
    assert await factory.for_context(context(), selected) is registered
    assert fallback.calls == 0

    other_tenant = config("tenant-b").model_copy(update={"storage": selection})
    assert await factory.for_context(context("tenant-b"), other_tenant) is fallback_bundle
    assert fallback.calls == 1


@pytest.mark.asyncio
async def test_registered_profile_selection_drift_fails_closed_without_secret_leak() -> None:
    secret = "postgresql://worker:secret@example.invalid/runtime"
    bundle = _bundle()
    registration = RegisteredTenantServiceBundle(
        selection=StorageSelection(profile_id="profile-a"),
        services=bundle,
    )
    factory = CompositeTenantServiceFactory(
        TenantStorageProfileRegistry({("tenant-a", "profile-a"): registration}),
        _DefaultFactory(_bundle()),
    )
    changed = config().model_copy(
        update={
            "storage": StorageSelection(
                profile_id="profile-a",
                session_backend="redis",
            )
        }
    )

    with pytest.raises(LookupError, match="pinned configuration") as captured:
        await factory.for_context(context(), changed)
    assert secret not in str(captured.value)


def test_registry_rejects_unscoped_or_mismatched_keys_without_echoing_values() -> None:
    bundle = _bundle()
    registration = RegisteredTenantServiceBundle(
        selection=StorageSelection(profile_id="profile-a"),
        services=bundle,
    )
    with pytest.raises(ValueError, match="registry key"):
        TenantStorageProfileRegistry({("", "profile-a"): registration})
    with pytest.raises(ValueError, match="profile id"):
        TenantStorageProfileRegistry({("tenant-a", "different"): registration})


@pytest.mark.asyncio
async def test_formal_worker_factory_uses_composite_and_rejects_unimplemented_backends() -> None:
    repository = type("Repository", (), {"pool": object()})()
    factory = cli._worker_tenant_service_factory(repository, object())
    assert isinstance(factory, CompositeTenantServiceFactory)

    unsupported = config().model_copy(
        update={
            "storage": StorageSelection(
                profile_id="profile-a",
                session_backend="redis",
            )
        }
    )
    with pytest.raises(ValueError, match="session_backend"):
        await factory.for_context(context(), unsupported)


@pytest.mark.asyncio
async def test_formal_worker_factory_routes_distinct_prebuilt_physical_bundles() -> None:
    repository = type("Repository", (), {"pool": object()})()
    tenant_a_bundle = _bundle()
    tenant_b_bundle = _bundle()
    selection = StorageSelection(
        profile_id="profile-a",
        knowledge_backend="external_memory",
    )
    factory = cli._worker_tenant_service_factory(
        repository,
        object(),
        registered_profiles={
            ("tenant-a", "profile-a"): RegisteredTenantServiceBundle(
                selection=selection,
                services=tenant_a_bundle,
            ),
            ("tenant-b", "profile-a"): RegisteredTenantServiceBundle(
                selection=selection,
                services=tenant_b_bundle,
            ),
        },
    )

    tenant_a = config().model_copy(update={"storage": selection})
    tenant_b = config("tenant-b").model_copy(update={"storage": selection})
    assert await factory.for_context(context(), tenant_a) is tenant_a_bundle
    assert await factory.for_context(context("tenant-b"), tenant_b) is tenant_b_bundle


@pytest.mark.asyncio
async def test_formal_worker_loads_strict_secret_free_profile_registry_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_file = tmp_path / "storage-profiles.json"
    registry_file.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "profiles": [
                    {
                        "tenant_id": "tenant-a",
                        "profile_id": "profile-a",
                        "bundle": "default_postgresql_s3_pgvector",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TRPC_SERVICE_STORAGE_PROFILE_REGISTRY_FILE", str(registry_file))
    repository = type("Repository", (), {"pool": object()})()

    factory = cast(
        CompositeTenantServiceFactory,
        cli._worker_tenant_service_factory(repository, object()),
    )
    assert factory._registry.resolve(context(), config()) is not None
    assert await factory.for_context(context(), config()) is not None


def test_profile_registry_file_rejects_unknown_bundle_and_secret_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_file = tmp_path / "storage-profiles.json"
    secret = "postgresql://worker:secret@example.invalid/runtime"
    registry_file.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "profiles": [
                    {
                        "tenant_id": "tenant-a",
                        "profile_id": "profile-a",
                        "bundle": "redis",
                        "dsn": secret,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TRPC_SERVICE_STORAGE_PROFILE_REGISTRY_FILE", str(registry_file))
    repository = type("Repository", (), {"pool": object()})()

    with pytest.raises(ValueError, match="schema") as captured:
        cli._worker_tenant_service_factory(repository, object())
    assert secret not in str(captured.value)
