"""Deterministic fixed-category sensitive content policy (Stage 6B2 Task 1).

Only platform-defined categories exist — no tenant regexes (ReDoS and
unauditable behavior are explicitly out of scope, and fuzzy PII detection
must not be sold as a reliable capability):

- ``credential``     Bearer tokens and well-known API key shapes
- ``private_key``    PEM/OpenSSH private-key blocks
- ``credential_dsn`` URI with an embedded password (``scheme://user:pass@``)

Detection is deterministic, bounded, and pure: patterns are precompiled at
import, text longer than :attr:`ContentPolicy.max_inspect_chars` is NOT
regex-scanned (it fails closed instead), and no scanned source text is ever
retained on the policy, the decision, or any raised value.  Unexpected
internal errors also fail closed with an uncategorized block.

The fixed public texts below are the only strings a content hit may ever
produce toward users; they name no category and echo no source text.  The
enforcement direction (input vs output) is applied by the Worker boundary
via ``input_action``/``output_action``; :meth:`ContentPolicy.inspect`
reports detection only.
"""

from __future__ import annotations

import re
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, StrictBool

ContentCategory = Literal["none", "credential", "private_key", "credential_dsn"]

CONTENT_INPUT_BLOCKED_TEXT: Final[str] = \
    "Your message was not processed because it did not pass the content policy."
CONTENT_OUTPUT_BLOCKED_TEXT: Final[str] = \
    "The reply was replaced because it did not pass the content policy."

# --- Precompiled fixed patterns --------------------------------------------
# Checked in this order: private_key, credential, credential_dsn.  Every
# pattern is a bounded character class with no nested quantifiers, so no
# input can drive super-linear backtracking.

_PRIVATE_KEY_PATTERNS: tuple[re.Pattern[str], ...] = (re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----"), )

_CREDENTIAL_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9\-._~+/]{16,}=*"),
    re.compile(r"\bsk-[A-Za-z0-9_\-]{16,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bghp_[A-Za-z0-9]{36}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bglpat-[A-Za-z0-9_\-]{16,}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_\-]{30,}"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9\-]{10,}"),
)

_DSN_PATTERNS: tuple[re.Pattern[str], ...] = (
    # scheme://[user]:[password]@  — the colon inside the authority before
    # the '@' is what makes this a credential-bearing DSN; bare hosts,
    # host:port and user-only SSH/Git style references do not match.
    re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://[^:/\s@]*:[^/\s@]+@"), )


def _detect_category(text: str) -> ContentCategory | None:
    """Return the first fixed category matching ``text``, else None."""
    for patterns, category in (
        (_PRIVATE_KEY_PATTERNS, "private_key"),
        (_CREDENTIAL_PATTERNS, "credential"),
        (_DSN_PATTERNS, "credential_dsn"),
    ):
        for pattern in patterns:
            if pattern.search(text) is not None:
                return category
    return None


class ContentPolicyConfig(BaseModel):
    """Versioned per-tenant content governance switches (Stage 6B2).

    Booleans are strict (never int-substitutable) and actions are limited to
    the two fixed literals; anything else fails validation at the write
    boundary before it can ever reach history.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    enabled: StrictBool = True
    input_action: Literal["allow", "block"] = "block"
    output_action: Literal["allow", "block"] = "block"


class ContentPolicyDecision(BaseModel):
    """Immutable detection verdict — a fixed enum, never source text."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    allowed: StrictBool
    category: ContentCategory


_ALLOW: Final[ContentPolicyDecision] = ContentPolicyDecision(allowed=True, category="none")
# Fail-closed verdict for over-long or internally errored inspections: block
# without claiming a category we could not confirm.
_FAIL_CLOSED: Final[ContentPolicyDecision] = ContentPolicyDecision(allowed=False, category="none")


class ContentPolicy:
    """Pure detector over a frozen :class:`ContentPolicyConfig`.

    Holds only the immutable config; nothing from scanned text is stored.
    """

    def __init__(self, config: ContentPolicyConfig, *, max_inspect_chars: int = 262_144) -> None:
        self._config = config
        self.max_inspect_chars = max_inspect_chars

    @property
    def config(self) -> ContentPolicyConfig:
        return self._config

    def inspect(self, text: str) -> ContentPolicyDecision:
        """Detect sensitive content.  Never raises, never retains ``text``."""
        if not isinstance(text, str):
            return _FAIL_CLOSED
        if not self._config.enabled:
            return _ALLOW
        if len(text) > self.max_inspect_chars:
            return _FAIL_CLOSED
        try:
            category = _detect_category(text)
        except Exception:
            return _FAIL_CLOSED
        if category is None:
            return _ALLOW
        return ContentPolicyDecision(allowed=False, category=category)


__all__ = [
    "CONTENT_INPUT_BLOCKED_TEXT",
    "CONTENT_OUTPUT_BLOCKED_TEXT",
    "ContentCategory",
    "ContentPolicy",
    "ContentPolicyConfig",
    "ContentPolicyDecision",
]
