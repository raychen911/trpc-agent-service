"""Construct supported tRPC services from validated backend profiles."""

from __future__ import annotations

from collections.abc import Callable

from trpc_agent_sdk.memory import InMemoryMemoryService
from trpc_agent_sdk.memory import RedisMemoryService
from trpc_agent_sdk.memory import SqlMemoryService
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.sessions import RedisSessionService
from trpc_agent_sdk.sessions import SqlSessionService

from trpc_service.tenant import BackendKind

from .models import BackendProfile
from .models import ResourceType


SecretResolver = Callable[[str], str]


class TrpcServiceFactory:
    """Build only the backend combinations supported by tRPC public services."""

    def __init__(self, secret_resolver: SecretResolver) -> None:
        self._secret_resolver = secret_resolver

    def __call__(self, profile: BackendProfile, resource: ResourceType):
        if resource in {ResourceType.SESSION, ResourceType.SUMMARY}:
            if profile.kind is BackendKind.IN_MEMORY:
                return InMemorySessionService()
            db_url = self._secret_resolver(profile.secret_ref.uri)
            if profile.kind is BackendKind.REDIS:
                return RedisSessionService(db_url=db_url, is_async=True, **profile.options)
            if profile.kind is BackendKind.SQL:
                return SqlSessionService(db_url=db_url, is_async=True, **profile.options)
        if resource is ResourceType.MEMORY:
            if profile.kind is BackendKind.IN_MEMORY:
                return InMemoryMemoryService(enabled=True)
            db_url = self._secret_resolver(profile.secret_ref.uri)
            if profile.kind is BackendKind.REDIS:
                return RedisMemoryService(db_url=db_url, is_async=True, enabled=True, **profile.options)
            if profile.kind is BackendKind.SQL:
                return SqlMemoryService(db_url=db_url, is_async=True, enabled=True, **profile.options)
        raise ValueError(f"no tRPC service factory for {resource.value} on {profile.kind.value}")
