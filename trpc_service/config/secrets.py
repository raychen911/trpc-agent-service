"""Resolve secret references without persisting secret values."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import unquote, urlsplit

from dotenv import dotenv_values


class SecretResolutionError(RuntimeError):
    """Raised when a configured secret reference cannot be resolved safely."""


class SecretResolver:
    """Resolve ``env://`` and ``file://`` references at runtime."""

    def __init__(self, dotenv_path: str | Path | None = ".env") -> None:
        self._dotenv_path = Path(dotenv_path) if dotenv_path is not None else None

    def resolve(self, reference: str) -> str:
        parsed = urlsplit(reference)
        if parsed.scheme == "env":
            name = parsed.netloc or parsed.path.lstrip("/")
            value = os.environ.get(name)
            if value is None and self._dotenv_path is not None and self._dotenv_path.is_file():
                value = dotenv_values(self._dotenv_path).get(name)
            if value is None:
                raise SecretResolutionError(f"environment secret is not set: {name}")
            return self._require_value(value, reference)

        if parsed.scheme == "file":
            raw_path = unquote(f"{parsed.netloc}{parsed.path}")
            if os.name == "nt" and raw_path.startswith("/") and len(raw_path) > 2:
                raw_path = raw_path.lstrip("/")
            try:
                value = Path(raw_path).read_text(encoding="utf-8")
            except OSError as exc:
                raise SecretResolutionError(f"file secret cannot be read: {reference}") from exc
            return self._require_value(value.strip(), reference)

        if parsed.scheme == "vault":
            raise SecretResolutionError("vault:// requires a configured Vault client")
        raise SecretResolutionError(f"unsupported secret reference: {reference}")

    @staticmethod
    def _require_value(value: str, reference: str) -> str:
        if not value:
            raise SecretResolutionError(f"secret resolved to an empty value: {reference}")
        return value


__all__ = ["SecretResolutionError", "SecretResolver"]
