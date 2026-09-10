"""Local observable overlay harness (FR-026, DEC-002, DEC-003).

Simulates the ``deploy/local-observable`` topology in-process when Docker is
unavailable: gateway + two workers + collector, with health endpoints,
telemetry outage handling and collector debug output. The real Compose
artifacts live in ``deploy/local-observable/``; this harness is the offline
mirror used by the e2e contract tests.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any


def _digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()[:16]


class LocalObservableOverlay:
    """In-process mirror of the minimal observable deployment."""

    NODES = ("gateway", "worker-a", "worker-b", "collector")
    BUFFER_CAPACITY = 1_000

    def __init__(self) -> None:
        self._up = False
        self._collector_up = False
        self._buffered = 0
        self._dropped = 0
        self._sessions: dict[tuple[str, str], int] = {}
        self._stages: dict[str, list[str]] = {}
        self._audit_records = 0
        self._collector_stopped_at: float | None = None

    @classmethod
    async def up(cls) -> "LocalObservableOverlay":
        overlay = cls()
        overlay._up = True
        overlay._collector_up = True
        return overlay

    async def down(self) -> None:
        self._up = False

    def nodes(self) -> list[str]:
        return list(self.NODES)

    async def health_live(self, service: str) -> int:
        return 200 if self._up and service in self.NODES else 503

    async def health_ready(self, service: str) -> int:
        return 200 if self._up and service in self.NODES else 503

    async def stop_collector(self) -> None:
        self._collector_up = False
        self._collector_stopped_at = time.monotonic()

    async def start_collector(self) -> None:
        self._collector_up = True
        self._collector_stopped_at = None
        # Recovery flush: bounded buffer drains on exporter restore.
        self._buffered = 0

    async def send_message(
        self, worker: str, tenant_id: str, session_id: str, text: str
    ) -> dict[str, Any]:
        """Business always succeeds; telemetry outage only degrades."""

        if not self._up:
            raise RuntimeError("overlay is down")
        key = (tenant_id, session_id)
        turn = self._sessions.get(key, 0) + 1
        self._sessions[key] = turn
        trace_reference = _digest(f"{tenant_id}|{session_id}|{turn}")
        self._stages[trace_reference] = [
            "adapter", "gateway", "worker", "runner", "data", "delivery",
        ]
        self._audit_records += 1  # formal audit never changes
        if self._collector_up:
            pass
        elif self._buffered < self.BUFFER_CAPACITY:
            self._buffered += 1
        else:
            self._dropped += 1
        return {
            "status": "succeeded",
            "tenant_id": tenant_id,
            "session_id": session_id,
            "turn": turn,
            "trace_reference": trace_reference,
            "worker": worker,
        }

    async def debug_stages(self, trace_reference: str) -> list[str]:
        """Collector debug output: stage names only, no payloads."""

        return list(self._stages.get(trace_reference, []))

    async def platform_health(self) -> dict[str, Any]:
        if not self._up:
            return {"state": "unready"}
        if not self._collector_up:
            return {"state": "degraded", "reason_codes": ["telemetry_unavailable"]}
        return {"state": "ready", "reason_codes": []}

    async def telemetry_state(self) -> dict[str, int]:
        return {
            "buffered": self._buffered,
            "capacity": self.BUFFER_CAPACITY,
            "dropped": self._dropped,
        }

    async def wait_until_healthy(self, timeout_seconds: float = 35) -> bool:
        if not self._up or not self._collector_up:
            return False
        return True

    async def audit_record_count(self) -> int:
        return self._audit_records
