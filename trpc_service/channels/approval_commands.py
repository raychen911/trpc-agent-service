"""Shared strict approval-command parser (Stage 6A2).

One implementation for Console, WeCom and Feishu — adapters only convert
frames; command recognition lives here so the three entries cannot drift.
Anything that is not an exact whole-message command is ordinary chat text.
"""

from __future__ import annotations

import re
import uuid

from trpc_service.governance.approval import ApprovalDecision

_COMMAND_PATTERN = re.compile(r"^/(approve|reject)\s+"
                              r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$")


def parse_approval_command(text: str) -> tuple[ApprovalDecision, uuid.UUID] | None:
    """Return (decision, approval_id) for an exact command, else ``None``."""
    if not isinstance(text, str):
        return None
    match = _COMMAND_PATTERN.fullmatch(text.strip())
    if match is None:
        return None
    try:
        return (match.group(1), uuid.UUID(match.group(2)))
    except ValueError:  # pragma: no cover - pattern already guarantees shape
        return None


__all__ = ["parse_approval_command"]
