"""Safe structured operational logging helpers.

Only already-classified identifiers and outcomes are accepted here. Request bodies,
signatures, credentials and external user identifiers deliberately have no parameters.
Raw trace ids are never logged: every line references the trace through the
pseudonymous ``trace_digest`` (FR-006/FR-007, DEC-001).
"""

from __future__ import annotations

import json
import logging

from trpc_service.observability.context import trace_digest as _trace_digest_of

_LOGGER = logging.getLogger("trpc_service")


def log_operational(*, component: str, operation: str, error_type: str, retryable: bool, trace_digest: str) -> None:
    """Emit only the allow-listed, low-cardinality operational envelope."""
    from trpc_service.observability.operational import OperationalEvent
    event = OperationalEvent(component=component, operation=operation, error_type=error_type, retryable=retryable, trace_digest=trace_digest)
    _LOGGER.warning(json.dumps(event.to_dict(), separators=(",", ":")))


def log_delivery(*, trace_id, outcome: str, status_code: int) -> None:
    """Emit the local delivery outcome keyed by trace DIGEST, never raw ids."""
    _LOGGER.info(
        json.dumps(
            {
                "event": "local_message_delivery",
                "trace_digest": _trace_digest_of(trace_id),
                "outcome": outcome,
                "status_code": status_code,
            },
            separators=(",", ":"),
        )
    )


__all__ = ["log_delivery", "log_operational"]
