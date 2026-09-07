"""Secret resolution, credential-safe logging, and data redaction."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import re
import stat
import time
from pathlib import Path
from typing import Any, ClassVar, Protocol
from urllib.parse import unquote, urlparse

import httpx

from tenant_agent.models import RedactionPolicy, SecretRef


class SecretResolutionError(RuntimeError):
    pass


class SecretResolver(Protocol):
    async def resolve(self, reference: SecretRef) -> str: ...


class SecretRegistry:
    """Tracks resolved values only so exact values can be scrubbed from telemetry."""

    def __init__(self) -> None:
        self._fingerprints: dict[str, str] = {}

    def register(self, value: str) -> None:
        if value:
            fingerprint = hashlib.sha256(value.encode()).hexdigest()
            self._fingerprints[fingerprint] = value

    def redact_exact_values(self, text: str, replacement: str = "[SECRET]") -> str:
        output = text
        for value in tuple(self._fingerprints.values()):
            if len(value) >= 4:
                output = output.replace(value, replacement)
        return output


class CompositeSecretResolver:
    """Resolve environment, mounted-file, or HashiCorp Vault references."""

    def __init__(
        self,
        *,
        file_root: Path,
        registry: SecretRegistry | None = None,
        vault_address: str | None = None,
        vault_token_env: str = "VAULT_TOKEN",  # noqa: S107 - environment variable name, not a secret
        vault_token_file: Path | None = None,
        cache_ttl_seconds: float = 0.0,
    ) -> None:
        self._file_root = file_root.resolve()
        self._registry = registry or SecretRegistry()
        configured_vault_address = (
            vault_address if vault_address is not None else os.getenv("VAULT_ADDR") or ""
        )
        self._vault_address = configured_vault_address.rstrip("/")
        self._vault_token_env = vault_token_env
        configured_token_file = vault_token_file or (
            Path(raw_token_file) if (raw_token_file := os.getenv("VAULT_TOKEN_FILE")) else None
        )
        self._vault_token_file = configured_token_file
        self._cache_ttl_seconds = max(0.0, cache_ttl_seconds)
        self._cache: dict[str, tuple[float, str]] = {}
        self._inflight: dict[str, asyncio.Task[str]] = {}
        self._cache_guard = asyncio.Lock()

    @property
    def registry(self) -> SecretRegistry:
        return self._registry

    @staticmethod
    def _is_reparse_point(path: Path) -> bool:
        if path.is_symlink():
            return True
        is_junction = getattr(path, "is_junction", None)
        if callable(is_junction) and is_junction():
            return True
        try:
            attributes = int(getattr(os.lstat(path), "st_file_attributes", 0))
        except OSError:
            return False
        reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
        return bool(attributes & reparse_flag)

    async def resolve(self, reference: SecretRef) -> str:
        now = time.monotonic()
        cached = self._cache.get(reference.uri)
        if cached and cached[0] > now:
            return cached[1]
        async with self._cache_guard:
            cached = self._cache.get(reference.uri)
            if cached and cached[0] > time.monotonic():
                return cached[1]
            task = self._inflight.get(reference.uri)
            if task is None:
                task = asyncio.create_task(
                    self._resolve_uncached(reference),
                    name="resolve-secret",
                )
                self._inflight[reference.uri] = task
        try:
            value = await asyncio.shield(task)
        finally:
            if task.done():
                async with self._cache_guard:
                    if self._inflight.get(reference.uri) is task:
                        self._inflight.pop(reference.uri, None)
        if self._cache_ttl_seconds:
            self._cache[reference.uri] = (
                time.monotonic() + self._cache_ttl_seconds,
                value,
            )
        return value

    async def _resolve_uncached(self, reference: SecretRef) -> str:
        parsed = urlparse(reference.uri)
        scheme = parsed.scheme.lower()
        if scheme == "env":
            name = (parsed.netloc + parsed.path).lstrip("/")
            value = os.getenv(name)
            if value is None:
                raise SecretResolutionError(f"environment secret {name!r} is not set")
        elif scheme == "file":
            raw_path = unquote(parsed.netloc + parsed.path)
            segments = raw_path.split("/")
            if (
                raw_path.startswith(("/", "\\"))
                or "\\" in raw_path
                or any(segment in {"", ".", ".."} for segment in segments)
            ):
                raise SecretResolutionError("secret file reference must be a canonical relative path")
            requested = self._file_root / Path(*segments)
            resolved = requested.resolve()
            try:
                resolved.relative_to(self._file_root)
            except ValueError as exc:
                raise SecretResolutionError("secret file escapes the configured root") from exc
            if len(segments) > 1:
                tenant_root_path = self._file_root / segments[0]
                tenant_root = tenant_root_path.resolve()
                if self._is_reparse_point(tenant_root_path):
                    if not tenant_root_path.is_symlink():
                        raise SecretResolutionError("tenant secret root reparse point is not allowed")
                    visible_target = os.readlink(tenant_root_path).replace("\\", "/")
                    if visible_target != f"..data/{segments[0]}":
                        raise SecretResolutionError(
                            "tenant secret root is not a valid AtomicWriter projection"
                        )
                    data_link = self._file_root / "..data"
                    if not data_link.is_symlink():
                        raise SecretResolutionError("AtomicWriter data link is missing")
                    data_target = os.readlink(data_link).replace("\\", "/")
                    if "/" in data_target or not data_target.startswith(".."):
                        raise SecretResolutionError("AtomicWriter data target is invalid")
                    try:
                        relative_root = tenant_root.relative_to(self._file_root)
                    except ValueError as exc:
                        raise SecretResolutionError("tenant secret root escapes the mount") from exc
                    atomic_parts = relative_root.parts
                    if not (
                        len(atomic_parts) >= 2
                        and atomic_parts[0].startswith("..")
                        and atomic_parts[1] == segments[0]
                    ):
                        raise SecretResolutionError(
                            "tenant secret root is not a valid AtomicWriter projection"
                        )
                try:
                    resolved.relative_to(tenant_root)
                except ValueError as exc:
                    raise SecretResolutionError("secret file escapes its tenant directory") from exc
            try:
                value = resolved.read_text(encoding="utf-8").strip()
            except OSError as exc:
                raise SecretResolutionError("secret file could not be read") from exc
        elif scheme == "vault":
            value = await self._resolve_vault(parsed)
        else:
            raise SecretResolutionError(
                f"secret provider {scheme!r} requires a deployment-specific resolver plugin"
            )
        if not value:
            raise SecretResolutionError("resolved secret is empty")
        self._registry.register(value)
        return value

    async def _resolve_vault(self, parsed: Any) -> str:
        if not self._vault_address:
            raise SecretResolutionError("VAULT_ADDR is not configured")
        token: str | None
        if self._vault_token_file is not None:
            try:
                token = (
                    await asyncio.to_thread(
                        self._vault_token_file.read_text,
                        encoding="utf-8",
                    )
                ).strip()
            except OSError as exc:
                raise SecretResolutionError("Vault token file could not be read") from exc
        else:
            token = os.getenv(self._vault_token_env)
        if not token:
            raise SecretResolutionError("Vault authentication token is not configured")
        self._registry.register(token)
        path = (parsed.netloc + parsed.path).lstrip("/")
        field = parsed.fragment or "value"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(
                    f"{self._vault_address}/v1/{path}",
                    headers={"X-Vault-Token": token},
                )
                response.raise_for_status()
                payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise SecretResolutionError("Vault secret lookup failed") from exc
        data = payload.get("data", {})
        if isinstance(data.get("data"), dict):
            data = data["data"]
        value = data.get(field)
        if not isinstance(value, str):
            raise SecretResolutionError("Vault response did not contain the requested string field")
        return value


class Redactor:
    EMAIL = re.compile(r"(?<![\w.+-])[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}(?![\w.-])")
    PHONE = re.compile(r"(?<![A-Za-z0-9_])(?:\+?\d[\d .()-]{7,}\d)(?![A-Za-z0-9_])")
    DATE_LIKE = re.compile(r"^(?:\d{4}-\d{2}-\d{2}(?:[ T]\d{1,2})?|\d{8}[ T]\d{6})$")
    BEARER = re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]{8,}")
    API_KEY = re.compile(
        r"(?i)(api[_-]?key|access[_-]?token|client[_-]?secret|password)"
        r"(\s*[=:]\s*)[\"']?[A-Za-z0-9._~+/=-]{6,}[\"']?"
    )
    TELEGRAM_TOKEN = re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b")
    PRIVATE_KEY = re.compile(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
        re.DOTALL,
    )
    SENSITIVE_KEYS: ClassVar[frozenset[str]] = frozenset(
        {
            "authorization",
            "api_key",
            "apikey",
            "access_token",
            "refresh_token",
            "client_secret",
            "password",
            "secret",
            "token",
            "database_url",
            "dsn",
        }
    )

    def __init__(
        self,
        policy: RedactionPolicy | None = None,
        registry: SecretRegistry | None = None,
    ) -> None:
        self.policy = policy or RedactionPolicy()
        self.registry = registry or SecretRegistry()

    def text(self, value: str) -> str:
        replacement = self.policy.replacement
        output = self.registry.redact_exact_values(value, replacement)
        if self.policy.redact_credentials:
            output = self.PRIVATE_KEY.sub(replacement, output)
            output = self.TELEGRAM_TOKEN.sub(replacement, output)
            output = self.BEARER.sub(r"\1" + replacement, output)
            output = self.API_KEY.sub(r"\1\2" + replacement, output)
        if self.policy.redact_email:
            output = self.EMAIL.sub(replacement, output)
        if self.policy.redact_phone:

            def replace_phone(match: re.Match[str]) -> str:
                candidate = match.group(0)
                digit_count = sum(character.isdigit() for character in candidate)
                if self.DATE_LIKE.fullmatch(candidate) or not 8 <= digit_count <= 15:
                    return candidate
                return replacement

            output = self.PHONE.sub(replace_phone, output)
        return output

    def value(self, value: Any, *, key: str | None = None) -> Any:
        if key and key.lower() in self.SENSITIVE_KEYS:
            return self.policy.replacement
        if isinstance(value, str):
            return self.text(value)
        if isinstance(value, dict):
            return {str(k): self.value(v, key=str(k)) for k, v in value.items()}
        if isinstance(value, (list, tuple, set, frozenset)):
            return [self.value(item) for item in value]
        return value


class RedactingLogFilter(logging.Filter):
    def __init__(self, redactor: Redactor):
        super().__init__()
        self._redactor = redactor

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = self._redactor.text(record.msg)
        if record.args:
            if isinstance(record.args, tuple):
                record.args = tuple(self._redactor.value(item) for item in record.args)
            else:
                record.args = self._redactor.value(record.args)
        for name in ("exc_text", "stack_info"):
            current = getattr(record, name, None)
            if isinstance(current, str):
                setattr(record, name, self._redactor.text(current))
        return True


class RedactingFormatter(logging.Formatter):
    """Redact the final rendered message, including formatted tracebacks."""

    def __init__(self, redactor: Redactor, fmt: str) -> None:
        super().__init__(fmt=fmt)
        self._redactor = redactor

    def format(self, record: logging.LogRecord) -> str:
        return self._redactor.text(super().format(record))


def configure_safe_logging(level: str, redactor: Redactor) -> None:
    log_format = "%(asctime)s %(levelname)s %(name)s %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=log_format,
    )
    safe_filter = RedactingLogFilter(redactor)
    safe_formatter = RedactingFormatter(redactor, log_format)
    root = logging.getLogger()
    loggers = [root]
    loggers.extend(
        logger for logger in logging.Logger.manager.loggerDict.values() if isinstance(logger, logging.Logger)
    )
    seen_handlers: set[int] = set()
    for logger in loggers:
        for handler in logger.handlers:
            if id(handler) in seen_handlers:
                continue
            seen_handlers.add(id(handler))
            for installed in tuple(handler.filters):
                if isinstance(installed, RedactingLogFilter):
                    handler.removeFilter(installed)
            handler.addFilter(safe_filter)
            handler.setFormatter(safe_formatter)


def verify_token(presented: str | None, expected: str) -> bool:
    if not presented:
        return False
    scheme, separator, value = presented.strip().partition(" ")
    candidate = value if separator and scheme.casefold() == "bearer" else presented.strip()
    return hmac.compare_digest(candidate.encode(), expected.encode())


def content_fingerprint(value: str, key: bytes) -> str:
    return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()
