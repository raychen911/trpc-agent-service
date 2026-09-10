"""Tenant-scoped content inspection and redaction."""

from __future__ import annotations

import re
from enum import StrEnum
from pydantic import BaseModel, ConfigDict


class InspectionAction(StrEnum):
    ALLOW = "allow"
    REDACT = "redact"
    REJECT = "reject"


class ContentInspection(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    action: InspectionAction
    safe_text: str
    findings: tuple[str, ...] = ()


def inspect(content: str, *, rules: dict[str, str] | None = None) -> ContentInspection:
    rules = rules or {}
    findings: list[str] = []
    safe = content
    patterns = {
        "email": (r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "[REDACTED_EMAIL]"),
        "phone": (r"(?<!\d)(?:\+?\d[\d -]{8,}\d)(?!\d)", "[REDACTED_PHONE]"),
        "token": (r"(?i)\b(?:token|secret|api[_-]?key)\s*[:=]\s*[^\s,;]+", "[REDACTED_SECRET]"),
    }
    for name, (pattern, replacement) in patterns.items():
        if name not in rules:
            continue
        if re.search(pattern, safe):
            findings.append(name)
            if rules[name] == "reject":
                return ContentInspection(action=InspectionAction.REJECT, safe_text="", findings=tuple(findings))
            safe = re.sub(pattern, replacement, safe)
    return ContentInspection(action=InspectionAction.REDACT if findings else InspectionAction.ALLOW, safe_text=safe, findings=tuple(findings))
