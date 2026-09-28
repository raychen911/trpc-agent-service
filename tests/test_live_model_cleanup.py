"""Acceptance failures must not hide leaked resources or the original error."""

from contextlib import asynccontextmanager
from uuid import uuid4

import pytest

from live_model_check import cleanup_steps, wait_for_test_tasks


@pytest.mark.anyio
async def test_cleanup_continues_after_document_failure_and_preserves_original(capsys) -> None:
    completed = []
    original = AssertionError("original model failure")

    async def document() -> None:
        raise RuntimeError("provider body contains a secret")

    async def disable() -> None:
        completed.append("agent and tenant disabled")

    async def close() -> None:
        completed.append("closed")

    await cleanup_steps([("document", document), ("disable", disable), ("close", close)], original)
    assert completed == ["agent and tenant disabled", "closed"]
    assert str(original) == "original model failure"
    assert "document: RuntimeError" in original.__notes__[0]
    output = capsys.readouterr().out
    assert '"cleanup": "incomplete"' in output
    assert "secret" not in output


@pytest.mark.anyio
async def test_cleanup_failure_changes_success_verdict_only_after_all_steps() -> None:
    completed = []

    async def failed() -> None:
        raise TimeoutError()

    async def close() -> None:
        completed.append(True)

    with pytest.raises(RuntimeError, match="cleanup incomplete"):
        await cleanup_steps([("task drain", failed), ("close", close)], None)
    assert completed == [True]


@pytest.mark.anyio
async def test_cleanup_waits_for_worker_before_cancelling_late_reply() -> None:
    polls = 0
    replies = []

    class Database:

        async def scalar(self, query):
            nonlocal polls
            polls += 1
            if polls == 1:
                return 1  # Worker still owns the task after the caller timed out.
            replies.append("late reply")
            return 0  # Final commit and queue completion now visible.

    @asynccontextmanager
    async def sessions():
        yield Database()

    async def drain() -> None:
        await wait_for_test_tasks(sessions, uuid4(), timeout=2)

    async def cancel() -> None:
        assert replies == ["late reply"]
        replies.clear()

    await cleanup_steps([("drain", drain), ("outbox", cancel)], None)
    assert polls == 2 and replies == []


@pytest.mark.anyio
async def test_unfinished_worker_is_reported_and_cleanup_still_closes(capsys) -> None:

    class Database:

        async def scalar(self, query):
            return 1

    @asynccontextmanager
    async def sessions():
        yield Database()

    closed = []

    async def drain() -> None:
        await wait_for_test_tasks(sessions, uuid4(), timeout=.01)

    async def close() -> None:
        closed.append(True)

    error = TimeoutError("original acceptance wait")
    await cleanup_steps([("task drain", drain), ("close", close)], error)
    assert closed == [True]
    assert "task drain: TimeoutError" in error.__notes__[0]
    assert '"cleanup": "incomplete"' in capsys.readouterr().out
