"""Secret references and providers.

Configuration stores references only. Resolved values are intentionally never
represented in model dumps or exception messages.
"""

from __future__ import annotations

import os
import re
from collections.abc import Collection
from pathlib import Path
from stat import S_ISLNK
from typing import Protocol
from urllib.parse import unquote, urlparse

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class SecretResolutionError(RuntimeError):
    """Raised without embedding a secret value or sensitive path contents."""


class SecretRef(BaseModel):
    """Immutable URI reference to a secret.

    Supported schemes are ``env://NAME`` and ``file:///absolute/path``.
    ``literal://`` is available only through an explicitly enabled provider in
    local tests and must never be used in production configuration.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    uri: str

    @model_validator(mode="before")
    @classmethod
    def accept_uri_string(cls, value: object) -> object:
        if isinstance(value, str):
            return {"uri": value}
        return value

    @field_validator("uri")
    @classmethod
    def validate_uri(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme not in {"env", "file", "literal"}:
            raise ValueError("secret reference scheme must be env, file, or literal")
        if parsed.query or parsed.fragment or parsed.params:
            raise ValueError("secret reference query and fragment are forbidden")
        if parsed.scheme in {"env", "file"} and not (parsed.netloc or parsed.path):
            raise ValueError("secret reference target cannot be empty")
        if parsed.scheme == "env":
            name = parsed.netloc or parsed.path.lstrip("/")
            if not _ENV_NAME.fullmatch(name):
                raise ValueError("environment secret name is invalid")
        return value

    def __str__(self) -> str:
        return f"SecretRef({self.uri.split(':', 1)[0]}://***)"


_ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]{0,127}$")
_TENANT_ENV_PREFIXES = ("TRPC_TENANT_", "TRPC_FEISHU_", "TRPC_WECOM_")


def _secret_path(value: str) -> Path:
    """Convert a file URI to a native path without accepting URI metadata."""

    parsed = urlparse(value)
    if parsed.scheme != "file" or parsed.query or parsed.fragment or parsed.params:
        raise SecretResolutionError("mounted secret reference is invalid")
    if parsed.netloc and os.name != "nt":
        raise SecretResolutionError("mounted secret host is invalid")
    raw_path = unquote(parsed.path)
    if not raw_path:
        raise SecretResolutionError("mounted secret reference is empty")
    if os.name == "nt":
        if parsed.netloc:
            raw_path = f"//{parsed.netloc}{raw_path}"
        elif len(raw_path) >= 3 and raw_path[0] == "/" and raw_path[2] == ":":
            raw_path = raw_path[1:]
    return Path(raw_path)


def validate_tenant_secret_ref(
    secret_ref: SecretRef,
    *,
    allowed_env_names: Collection[str] | None = None,
    secret_root: Path | None = None,
) -> None:
    """Validate a tenant-controlled reference before it reaches a resolver.

    Process settings may still use ordinary ``resolve`` calls.  Tenant config
    must use this narrower API: literals are never accepted, environment names
    must be pre-registered, and files must live below the explicitly supplied
    secret root.
    """

    parsed = urlparse(secret_ref.uri)
    if parsed.scheme == "literal":
        raise SecretResolutionError("literal tenant secrets are disabled")
    if parsed.scheme == "env":
        name = parsed.netloc or parsed.path.lstrip("/")
        allowed = set(allowed_env_names or ())
        if not _ENV_NAME.fullmatch(name) or name not in allowed:
            raise SecretResolutionError("tenant environment secret is not registered")
        return
    if parsed.scheme != "file" or secret_root is None:
        raise SecretResolutionError("tenant file secret root is not configured")
    root = secret_root.resolve()
    path = _secret_path(secret_ref.uri)
    if not path.is_absolute():
        raise SecretResolutionError("tenant secret path must be absolute")
    try:
        lexical_relative = path.relative_to(root)
    except ValueError as exc:
        raise SecretResolutionError("tenant secret path is outside the configured root") from exc
    if ".." in lexical_relative.parts:
        raise SecretResolutionError("tenant secret path traversal is forbidden")
    # Inspect the path supplied by the tenant before resolving it.  Resolving
    # first would erase an in-root symlink and make it indistinguishable from
    # an ordinary file path.
    _reject_symlink_components(root, path)
    resolved = path.resolve(strict=False)
    if resolved != root and root not in resolved.parents:
        raise SecretResolutionError("tenant secret path is outside the configured root")


def _reject_symlink_components(root: Path, path: Path) -> None:
    """Reject symlinks even when their resolved target stays under the root."""

    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise SecretResolutionError("tenant secret path is outside the configured root") from exc
    current = root
    for part in relative.parts:
        current /= part
        try:
            if S_ISLNK(current.lstat().st_mode):
                raise SecretResolutionError("tenant secret symlinks are disabled")
        except FileNotFoundError:
            # The resolver will report a missing secret later; validation
            # should still permit a not-yet-mounted secret file.
            continue
        except OSError as exc:
            raise SecretResolutionError("tenant secret path cannot be inspected") from exc


class SecretProvider(Protocol):
    """Resolve a reference at the last responsible moment."""

    def resolve(self, secret_ref: SecretRef) -> str: ...


class LocalSecretProvider:
    """Resolve environment and mounted-file secrets.

    This provider deliberately refuses literal secrets unless constructed for
    isolated development or tests.
    """

    def __init__(
        self,
        *,
        allow_literal: bool = False,
        secret_root: Path | None = None,
        allowed_env_names: Collection[str] | None = None,
    ) -> None:
        self._allow_literal = allow_literal
        self._secret_root = secret_root.resolve() if secret_root is not None else None
        self._allowed_env_names = frozenset(allowed_env_names or ())

    def resolve(self, secret_ref: SecretRef) -> str:
        parsed = urlparse(secret_ref.uri)
        if parsed.scheme == "env":
            name = parsed.netloc or parsed.path.lstrip("/")
            value = os.getenv(name)
            if value is None:
                raise SecretResolutionError(f"required environment secret {name!r} is not set")
            return value
        if parsed.scheme == "file":
            path = _secret_path(secret_ref.uri)
            if self._secret_root is not None:
                validate_tenant_secret_ref(
                    secret_ref,
                    allowed_env_names=self._allowed_env_names,
                    secret_root=self._secret_root,
                )
            try:
                value = path.read_text(encoding="utf-8").rstrip("\r\n")
            except OSError as exc:
                raise SecretResolutionError("mounted secret could not be read") from exc
            if not value:
                raise SecretResolutionError("mounted secret is empty")
            return value
        if parsed.scheme == "literal" and self._allow_literal:
            return unquote(parsed.netloc + parsed.path)
        raise SecretResolutionError("literal secrets are disabled")

    def resolve_tenant(self, secret_ref: SecretRef) -> str:
        """Resolve only a pre-registered tenant reference."""

        validate_tenant_secret_ref(
            secret_ref,
            allowed_env_names=self._allowed_env_names,
            secret_root=self._secret_root,
        )
        parsed = urlparse(secret_ref.uri)
        if parsed.scheme == "env":
            name = parsed.netloc or parsed.path.lstrip("/")
            value = os.getenv(name)
            if value is None:
                raise SecretResolutionError("registered tenant environment secret is not set")
            return value
        path = _secret_path(secret_ref.uri)
        try:
            value = path.read_text(encoding="utf-8").rstrip("\r\n")
        except OSError as exc:
            raise SecretResolutionError("mounted tenant secret could not be read") from exc
        if not value:
            raise SecretResolutionError("mounted tenant secret is empty")
        return value


__all__ = [
    "LocalSecretProvider",
    "SecretProvider",
    "SecretRef",
    "SecretResolutionError",
    "validate_tenant_secret_ref",
]
