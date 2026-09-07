# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Sensitive-data redaction helpers for tenant governance."""

from __future__ import annotations

import re
from typing import Any
from typing import Optional

from trpc_service.tenant import DesensitizeRule

# Default rules applied in addition to tenant-supplied rules. ``replace`` is a
# regex replacement template (may reference capture groups via ``\\1``).
DEFAULT_REDACTION_RULES: list[dict[str, str]] = [
    {
        "pattern": r"1[3-9]\d{9}",
        "replace": "1**********"
    },  # CN mobile number
    {
        "pattern": r"\d{17}[\dXx]",
        "replace": "******************"
    },  # CN ID card
    {
        "pattern": r"sk-[A-Za-z0-9_-]{8,}",
        "replace": "sk-***"
    },  # OpenAI-style key
    {
        "pattern": r"Bearer\s+[A-Za-z0-9._~+/=-]+",
        "replace": "Bearer ***"
    },  # Bearer token
    {
        "pattern": r"(?i)\b(api[_-]?key|secret|token|password)\b\s*[=:]\s*[\"']?[A-Za-z0-9._-]+",
        "replace": r"\1=***"
    },  # key=value style
]


class SensitiveDataRedactor:
    """Applies regex redaction rules to text and nested containers."""

    def __init__(self, default_rules: Optional[list[dict[str, str]]] = None) -> None:
        self._default_rules = [dict(r) for r in (default_rules or DEFAULT_REDACTION_RULES)]
        self._default_compiled = self._compile(self._default_rules)

    @staticmethod
    def _compile(rules: list[dict[str, str]]) -> list[tuple[re.Pattern, str]]:
        compiled: list[tuple[re.Pattern, str]] = []
        for rule in rules:
            try:
                compiled.append((re.compile(rule["pattern"]), rule.get("replace", "***")))
            except re.error:  # skip invalid tenant-supplied patterns rather than crash
                continue
        return compiled

    def redact(self, text: Optional[str], extra_rules: Optional[list[DesensitizeRule]] = None) -> Optional[str]:
        """Redact a string using default + tenant-supplied rules."""
        if text is None:
            return None
        if not isinstance(text, str):
            return text
        compiled = self._default_compiled
        if extra_rules:
            compiled = compiled + self._compile([{"pattern": r.pattern, "replace": r.replace} for r in extra_rules])
        for pattern, replace in compiled:
            text = pattern.sub(replace, text)
        return text

    def redact_any(self, value: Any, extra_rules: Optional[list[DesensitizeRule]] = None) -> Any:
        """Recursively redact strings inside dicts / lists / scalars."""
        if value is None:
            return None
        if isinstance(value, str):
            return self.redact(value, extra_rules)
        if isinstance(value, dict):
            return {key: self.redact_any(item, extra_rules) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self.redact_any(item, extra_rules) for item in value]
        return value
