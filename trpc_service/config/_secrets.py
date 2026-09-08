"""Runtime secret references with bounded, pluggable resolution.

Configuration objects keep references such as ``env://MODEL_API_KEY`` or
``file://qq/app-secret``.  Plaintext is resolved only at the adapter boundary,
which keeps domain configuration serialisable and prevents secret-manager
details from leaking into tenants, gateways, or workers.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from pathlib import Path
from typing import Mapping
from typing import Optional

from pydantic import SecretStr
from pydantic import BaseModel

from trpc_service.log import SecretMasker

SecretBackend = Callable[[str, Optional[str]], str]

_REFERENCE = re.compile(r"^(?P<scheme>[a-z][a-z0-9+.-]*)://(?P<target>.+)$")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED_SECRET_SCHEMES = {"env", "file", "vault", "aws-kms", "gcp-sm", "azure-kv"}


def is_secret_ref(value: str) -> bool:
    """Return whether ``value`` is a supported reference-shaped string."""
    match = _REFERENCE.fullmatch(value)
    return match is not None and match.group("scheme").lower() in _RESERVED_SECRET_SCHEMES


class SecretResolver:
    """Resolve secret references through small scheme-specific backends.

    ``env://`` and ``file://`` are built in. Vault, KMS, or a cloud secret
    manager can be added through :meth:`register` without coupling tenant
    models to a vendor SDK.
    """

    def __init__(
        self,
        *,
        environ: Optional[Mapping[str, str]] = None,
        file_root: Optional[str | Path] = None,
        backends: Optional[dict[str, SecretBackend]] = None,
    ) -> None:
        self._environ = environ if environ is not None else os.environ
        self._file_root = Path(file_root or "/run/secrets").resolve()
        self._backends: dict[str, SecretBackend] = {
            "env": self._resolve_env,
            "file": self._resolve_file,
        }
        self._backends.update(backends or {})

    def register(self, scheme: str, backend: SecretBackend) -> None:
        """Register or replace one secret-manager backend."""
        normalized = scheme.lower().strip()
        if not re.fullmatch(r"[a-z][a-z0-9+.-]*", normalized):
            raise ValueError("secret backend scheme is invalid")
        self._backends[normalized] = backend

    def resolve(self, value: str | SecretStr, *, tenant_id: Optional[str] = None) -> str:
        """Resolve one reference, or return a legacy literal unchanged."""
        raw = value.get_secret_value() if isinstance(value, SecretStr) else value
        match = _REFERENCE.fullmatch(raw)
        if match is None:
            SecretMasker.register_secret(raw)
            return raw
        scheme = match.group("scheme").lower()
        backend = self._backends.get(scheme)
        if backend is None:
            if scheme in _RESERVED_SECRET_SCHEMES:
                raise ValueError(f"unsupported secret reference scheme: {scheme}")
            SecretMasker.register_secret(raw)
            return raw
        resolved = backend(match.group("target"), tenant_id)
        if not resolved:
            raise ValueError(f"secret reference resolved to an empty value: {scheme}://***")
        SecretMasker.register_secret(resolved)
        return resolved

    def _resolve_env(self, target: str, _tenant_id: Optional[str]) -> str:
        if not _ENV_NAME.fullmatch(target):
            raise ValueError("env secret reference must contain one environment variable name")
        try:
            return self._environ[target]
        except KeyError as exc:
            raise ValueError(f"secret environment variable is not set: {target}") from exc

    def _resolve_file(self, target: str, tenant_id: Optional[str]) -> str:
        relative = Path(target)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("file secret reference must stay below the configured secret root")
        root = self._file_root
        if tenant_id:
            if not re.fullmatch(r"[A-Za-z0-9_.-]+", tenant_id):
                raise ValueError("tenant id is unsafe for file secret resolution")
            root = (root / tenant_id).resolve()
        try:
            candidate = (root / relative).resolve(strict=True)
        except OSError as exc:
            raise ValueError("file secret reference does not exist") from exc
        if candidate == root or root not in candidate.parents or not candidate.is_file():
            raise ValueError("file secret reference escapes its tenant secret root")
        return candidate.read_text(encoding="utf-8").rstrip("\r\n")


DEFAULT_SECRET_RESOLVER = SecretResolver()


def resolve_secret(
    value: Optional[str | SecretStr],
    *,
    tenant_id: Optional[str] = None,
    resolver: Optional[SecretResolver] = None,
) -> str:
    """Resolve an optional value through the process default resolver."""
    if value is None:
        return ""
    return (resolver or DEFAULT_SECRET_RESOLVER).resolve(value, tenant_id=tenant_id)


def resolve_model_secrets(model: BaseModel, *, tenant_id: str, resolver: SecretResolver) -> BaseModel:
    """Return a deep copy with every ``SecretStr`` resolved at an I/O boundary."""

    def resolve_value(value):
        if isinstance(value, SecretStr):
            return SecretStr(resolver.resolve(value, tenant_id=tenant_id))
        if isinstance(value, BaseModel):
            updates = {name: resolve_value(getattr(value, name)) for name in value.__class__.model_fields}
            return value.model_copy(deep=True, update=updates)
        if isinstance(value, dict):
            return {key: resolve_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [resolve_value(item) for item in value]
        return value

    return resolve_value(model)
