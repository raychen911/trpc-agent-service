# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Secret reference resolution."""

from __future__ import annotations

import os
from trpc_service.log.audit import register_secret
from typing import Protocol


class SecretResolutionError(RuntimeError):
    """Raised when a configured secret reference cannot be resolved."""


class SecretResolver(Protocol):
    """Resolve an opaque reference into a secret value."""

    async def resolve(self, reference: str) -> str:
        """Return the secret value for ``reference``."""


class EnvironmentSecretResolver:
    """Resolve only ``env://NAME`` references from the process environment."""

    async def resolve(self, reference: str) -> str:
        if not reference.startswith("env://"):
            raise SecretResolutionError(f"unsupported secret reference scheme: {reference!r}")
        name = reference.removeprefix("env://")
        if not name:
            raise SecretResolutionError("environment secret reference is empty")
        value = os.environ.get(name)
        if not value:
            raise SecretResolutionError(f"environment secret is not set: {name}")
        register_secret(value)
        return value


class SecretProviderRegistry:
    """Dispatch opaque references by URI scheme without exposing values in config."""

    def __init__(self, providers: dict[str, SecretResolver] | None = None) -> None:
        self._providers = {"env": EnvironmentSecretResolver(), **(providers or {})}

    def register(self, scheme: str, provider: SecretResolver) -> None:
        if not scheme or ":" in scheme:
            raise ValueError("secret provider scheme must be a simple non-empty name")
        self._providers[scheme] = provider

    async def resolve(self, reference: str) -> str:
        scheme, separator, _ = reference.partition("://")
        provider = self._providers.get(scheme) if separator else None
        if provider is None:
            raise SecretResolutionError(f"unsupported secret reference scheme: {reference!r}")
        value = await provider.resolve(reference)
        register_secret(value)
        return value
