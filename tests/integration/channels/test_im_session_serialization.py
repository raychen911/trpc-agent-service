from __future__ import annotations

import asyncio
from collections import defaultdict

import pytest

from tests.integration.channels.multinode_support import TwoNodeIMHarness
from trpc_service.channels.contracts import Channel
from trpc_service.worker.service import AgentExecution


class ConcurrencyTracker:
    def __init__(self) -> None:
        self.active_by_session: dict[str, int] = defaultdict(int)
        self.max_by_session: dict[str, int] = defaultdict(int)
        self.active_total = 0
        self.max_total = 0
        self.calls = 0


class TrackingPreparedRun:
    def __init__(self, tracker: ConcurrencyTracker, session_id: str) -> None:
        self.tracker = tracker
        self.session_id = session_id

    async def execute(self, timeout_seconds: float = 30) -> AgentExecution:
        del timeout_seconds
        tracker = self.tracker
        tracker.calls += 1
        tracker.active_by_session[self.session_id] += 1
        tracker.active_total += 1
        tracker.max_by_session[self.session_id] = max(
            tracker.max_by_session[self.session_id],
            tracker.active_by_session[self.session_id],
        )
        tracker.max_total = max(tracker.max_total, tracker.active_total)
        await asyncio.sleep(0.04)
        tracker.active_by_session[self.session_id] -= 1
        tracker.active_total -= 1
        return AgentExecution("tracked", 1, 1)


class TrackingWorker:
    def __init__(self, tracker: ConcurrencyTracker) -> None:
        self.tracker = tracker

    @property
    def call_count(self) -> int:
        return self.tracker.calls

    async def prepare(self, context, identity, text):
        del context, text
        return TrackingPreparedRun(self.tracker, identity.platform_session_id)

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_same_session_serializes_while_distinct_sessions_run_in_parallel() -> None:
    tracker = ConcurrencyTracker()
    harness = await TwoNodeIMHarness.create(
        Channel.FEISHU,
        workers=[TrackingWorker(tracker), TrackingWorker(tracker)],
    )
    try:
        same_session_events = [
            harness.event(
                message_id=f"same-session-{index}",
                conversation_id="shared-conversation",
                sender_id="shared-sender",
                text=f"same {index}",
            )
            for index in range(2)
        ]
        await asyncio.gather(
            harness.adapters[0].handle_provider_event(same_session_events[0]),
            harness.adapters[1].handle_provider_event(same_session_events[1]),
        )
        assert max(tracker.max_by_session.values()) == 1

        tracker.max_total = 0
        distinct_events = [
            harness.event(
                message_id=f"distinct-session-{index}",
                conversation_id=f"conversation-{index}",
                sender_id=f"sender-{index}",
                text=f"distinct {index}",
            )
            for index in range(2)
        ]
        await asyncio.gather(
            harness.adapters[0].handle_provider_event(distinct_events[0]),
            harness.adapters[1].handle_provider_event(distinct_events[1]),
        )
        assert tracker.max_total >= 2
    finally:
        await harness.close()
