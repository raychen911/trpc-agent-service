"""
Structured Logging with automatic sensitive data masking and TenantContext injection.
"""

import logging
import re
from typing import Any, Dict
from trpc_service.tenant.manager import TenantContext

# Regular expressions for sensitive tokens and personal data
SENSITIVE_PATTERNS = [
    (re.compile(r"(sk-[a-zA-Z0-9_\-]{16,})", re.IGNORECASE), r"sk-***REDACTED***"),
    (re.compile(r"(Bearer\s+)([a-zA-Z0-9_\-\.]{16,})", re.IGNORECASE), r"\1***REDACTED***"),
    (re.compile(r"(\"password\"\s*:\s*\")[^\"]+(\")", re.IGNORECASE), r'\1***REDACTED***\2'),
    (re.compile(r"(\"secret\"\s*:\s*\")[^\"]+(\")", re.IGNORECASE), r'\1***REDACTED***\2'),
    (re.compile(r"(1[3-9]\d{9})"), r"\g<1>"),  # Phone number masking below
]


def mask_sensitive_text(text: str) -> str:
    """Mask credentials, tokens, and PII from log output."""
    if not isinstance(text, str):
        return text
    # Mask API keys and secrets
    for pattern, repl in SENSITIVE_PATTERNS[:4]:
        text = pattern.sub(repl, text)
    # Mask ID cards first: 18 digits -> 6********4
    text = re.sub(r"\b([1-9]\d{5})\d{8}(\d{3}[0-9Xx])\b", r"\1********\2", text)
    # Mask phone numbers: 13812345678 -> 138****5678
    text = re.sub(r"\b(1[3-9]\d)\d{4}(\d{4})\b", r"\1****\2", text)
    return text


class MaskingLogFormatter(logging.Formatter):
    """Log formatter that injects tenant_id and trace_id, then masks secrets."""

    def format(self, record: logging.LogRecord) -> str:
        tenant_id = TenantContext.get_tenant_id() or "system"
        trace_id = TenantContext.get_trace_id() or "-"
        user_id = TenantContext.get_user_id() or "-"

        # Inject context tags into record message
        record.tenant_id = tenant_id
        record.trace_id = trace_id
        record.user_id = user_id

        formatted = super().format(record)
        return mask_sensitive_text(formatted)


def setup_logger(name: str = "trpc_agent", level: int = logging.INFO) -> logging.Logger:
    """Configure structured logger with security masking."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = "[%(asctime)s] [%(levelname)s] [tid:%(tenant_id)s] [trace:%(trace_id)s] %(name)s: %(message)s"
        handler.setFormatter(MaskingLogFormatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(handler)

    return logger


app_logger = setup_logger()
