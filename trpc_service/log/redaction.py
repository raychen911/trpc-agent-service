"""Central redaction used before logs, traces, audit details, and model I/O."""

from collections.abc import Mapping, Sequence
import logging
import re
from typing import Any, Literal

from uvicorn.logging import AccessFormatter, DefaultFormatter

_SECRET_KEYS = frozenset({
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "credential",
    "database_password",
    "password",
    "proxy_authorization",
    "secret",
    "secret_key",
    "set_cookie",
    "token",
})
_BEARER = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+\-/=]{8,}")
_API_KEY = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")
_URL_PASSWORD = re.compile(r"(?P<prefix>://[^:/\s]+:)[^@/\s]+(?=@)")
_QUERY_SECRET = re.compile(r"(?i)(?P<prefix>[?&](?:api[_-]?key|access[_-]?token|auth(?:orization)?|"
                           r"password|secret|token)=)[^&#\s]+")
_EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_CN_PHONE = re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")


class SensitiveDataRedactor:
    """Recursively remove known credentials and optionally common PII."""

    @staticmethod
    def _normalized_key(key: object) -> str:
        return str(key).strip().lower().replace("-", "_")

    def redact_text(self, value: str, *, redact_pii: bool) -> str:
        """Redact secrets in every context and PII when tenant policy enables it."""

        redacted = _BEARER.sub("Bearer [REDACTED]", value)
        redacted = _API_KEY.sub("[REDACTED_SECRET]", redacted)
        redacted = _URL_PASSWORD.sub(r"\g<prefix>[REDACTED]", redacted)
        # Access logs include the request target, so query credentials must be
        # removed even when their value does not match a known key format.
        redacted = _QUERY_SECRET.sub(r"\g<prefix>[REDACTED]", redacted)
        if redact_pii:
            redacted = _EMAIL.sub("[REDACTED_EMAIL]", redacted)
            redacted = _CN_PHONE.sub("[REDACTED_PHONE]", redacted)
        return redacted

    def redact_value(self, value: object, *, redact_pii: bool) -> object:
        """Return a JSON-compatible redacted copy without mutating caller data."""

        if isinstance(value, str):
            return self.redact_text(value, redact_pii=redact_pii)
        if isinstance(value, Mapping):
            return self.redact_mapping(value, redact_pii=redact_pii)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return [self.redact_value(item, redact_pii=redact_pii) for item in value]
        return value

    def redact_mapping(
        self,
        value: Mapping[Any, object],
        *,
        redact_pii: bool,
    ) -> dict[str, object]:
        """Redact credential-bearing fields before a payload crosses a sink."""

        result: dict[str, object] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            normalized = self._normalized_key(raw_key)
            if normalized in _SECRET_KEYS or normalized.endswith(("_token", "_secret", "_key")):
                result[key] = "[REDACTED]"
            else:
                result[key] = self.redact_value(item, redact_pii=redact_pii)
        return result


class SafeLogFormatter(logging.Formatter):
    """Redact a fully rendered log line, including dependency arguments."""

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Literal["%", "{", "$"] = "%",
        validate: bool = True,
        *,
        defaults: Mapping[str, object] | None = None,
        use_colors: bool | None = None,
    ) -> None:
        # Uvicorn passes ``use_colors`` to its own formatters. Accept it so this
        # safety formatter can replace them through standard logging.config.
        del use_colors
        super().__init__(
            fmt=fmt,
            datefmt=datefmt,
            style=style,
            validate=validate,
            defaults=defaults,
        )
        self._redactor = SensitiveDataRedactor()

    def format(self, record: logging.LogRecord) -> str:
        """Remove credentials and common PII after normal interpolation."""

        rendered = super().format(record)
        return self._redactor.redact_text(rendered, redact_pii=True)


class SafeUvicornDefaultFormatter(DefaultFormatter):
    """Preserve Uvicorn level fields and redact the rendered server line."""

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        return SensitiveDataRedactor().redact_text(rendered, redact_pii=True)


class SafeUvicornAccessFormatter(AccessFormatter):
    """Preserve Uvicorn access fields and redact the rendered request line."""

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        return SensitiveDataRedactor().redact_text(rendered, redact_pii=True)
