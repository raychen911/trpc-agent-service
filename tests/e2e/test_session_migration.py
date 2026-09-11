"""Migration under seeded load, resumed writes and fresh SQL connections."""

import asyncio
import json
import os
import subprocess
import sys

import pytest

from trpc_service.migration import copy_and_verify_trpc_session
from trpc_service.migration import snapshot_trpc_session
from trpc_service.storage import ConflictError

from .seed import append_turn
from .seed import canonical_session
from .seed import seed_sessions


@pytest.mark.asyncio
async def test_bulk_copy_incremental_catchup_and_fresh_reader(session_backends):
    source, target = session_backends.source, session_backends.target
    keys = await seed_sessions(source, session_backends.suffix)
    for key in keys[:3]:
        await copy_and_verify_trpc_session(source, target, target_writes_paused=True, **key.kwargs())
    # Writes continue after the initial snapshot; rerun catches up using preserved IDs.
    await append_turn(source, keys[0], 5)
    for _ in range(2):
        for key in keys[:3]:
            await copy_and_verify_trpc_session(source, target, target_writes_paused=True, **key.kwargs())
    reader = session_backends.reopen_target()
    for key in keys[:3]:
        expected = canonical_session(await source.get_session(**key.kwargs()))
        actual = canonical_session(await reader.get_session(**key.kwargs()))
        assert actual == expected
        ids = [event["id"] for event in actual["events"]]
        assert len(ids) == len(set(ids))
    for key in keys[3:]:
        assert await target.get_session(**key.kwargs()) is None, "unselected tenant was migrated"
        assert len((await source.get_session(**key.kwargs())).events) == 8


class FailAfterAppend:
    """Inject a lost acknowledgement after the real destination committed an event."""

    def __init__(self, service, fail_at):
        self.service, self.fail_at, self.calls = service, fail_at, 0

    def __getattr__(self, name):
        return getattr(self.service, name)

    async def append_event(self, session, event):
        result = await self.service.append_event(session, event)
        self.calls += 1
        if self.calls == self.fail_at:
            raise ConnectionError("injected destination acknowledgement loss")
        return result


@pytest.mark.asyncio
async def test_copy_resumes_after_destination_commit_acknowledgement_loss(session_backends):
    source, target = session_backends.source, session_backends.target
    key = (await seed_sessions(source, session_backends.suffix, sessions_per_tenant=1))[0]
    faulty = FailAfterAppend(target, 3)
    with pytest.raises(ConnectionError, match="acknowledgement loss"):
        await copy_and_verify_trpc_session(source, faulty, target_writes_paused=True, **key.kwargs())
    assert len((await target.get_session(**key.kwargs())).events) == 3
    reader = session_backends.reopen_target()
    await copy_and_verify_trpc_session(source, reader, target_writes_paused=True, **key.kwargs())
    assert canonical_session(await reader.get_session(**key.kwargs())) == canonical_session(
        await source.get_session(**key.kwargs()))


@pytest.mark.asyncio
async def test_explicit_reverse_sync_preserves_post_switch_writes(session_backends):
    source, target = session_backends.source, session_backends.target
    key = (await seed_sessions(source, session_backends.suffix, sessions_per_tenant=1))[0]
    await copy_and_verify_trpc_session(source, target, target_writes_paused=True, **key.kwargs())
    await append_turn(target, key, 5)
    # A route-only rollback would lose turn 5. Explicit reverse catchup is required here.
    await copy_and_verify_trpc_session(target, source, target_writes_paused=True, **key.kwargs())
    await append_turn(source, key, 6)
    await copy_and_verify_trpc_session(source, target, target_writes_paused=True, **key.kwargs())
    assert canonical_session(await target.get_session(**key.kwargs())) == canonical_session(
        await source.get_session(**key.kwargs()))


@pytest.mark.asyncio
async def test_stale_backfill_must_not_mutate_newer_destination(session_backends):
    source, target = session_backends.source, session_backends.target
    key = (await seed_sessions(source, session_backends.suffix, sessions_per_tenant=1))[0]
    await copy_and_verify_trpc_session(source, target, target_writes_paused=True, **key.kwargs())
    await append_turn(target, key, 5)
    before = canonical_session(await target.get_session(**key.kwargs()))
    with pytest.raises(ConflictError):
        await copy_and_verify_trpc_session(source, target, target_writes_paused=True, **key.kwargs())
    assert canonical_session(await target.get_session(**key.kwargs())) == before


@pytest.mark.asyncio
async def test_migrated_history_is_visible_in_an_independent_process(session_backends):
    source, target = session_backends.source, session_backends.target
    key = (await seed_sessions(source, session_backends.suffix, sessions_per_tenant=1))[0]
    await copy_and_verify_trpc_session(source, target, target_writes_paused=True, **key.kwargs())
    result = await asyncio.to_thread(
        subprocess.run, [sys.executable, "-m", "tests.e2e.session_reader"],
        input=json.dumps(key.kwargs()), text=True, capture_output=True, timeout=60, check=True,
        env={**os.environ, "E2E_READER_SQL_URL": session_backends.sql_url},
    )
    snapshots = [line.removeprefix("E2E_SNAPSHOT=") for line in result.stdout.splitlines()
                 if line.startswith("E2E_SNAPSHOT=")]
    assert len(snapshots) == 1
    assert json.loads(snapshots[0]) == canonical_session(await source.get_session(**key.kwargs()))


@pytest.mark.asyncio
async def test_state_update_after_last_event_survives_migration(session_backends):
    source, target = session_backends.source, session_backends.target
    key = (await seed_sessions(source, session_backends.suffix, sessions_per_tenant=1))[0]
    session = await source.get_session(**key.kwargs())
    session.state = {**session.state, "turn": 99}
    await source.update_session(session)
    assert (await source.get_session(**key.kwargs())).state["turn"] == 99
    try:
        await copy_and_verify_trpc_session(source, target, target_writes_paused=True, **key.kwargs())
    except ConflictError:
        pass
    assert canonical_session(await target.get_session(**key.kwargs())) == canonical_session(
        await source.get_session(**key.kwargs()))


@pytest.mark.asyncio
async def test_migration_requires_offline_target_before_any_write(session_backends):
    source, target = session_backends.source, session_backends.target
    key = (await seed_sessions(source, session_backends.suffix, sessions_per_tenant=1))[0]
    with pytest.raises(ConflictError, match="paused"):
        await copy_and_verify_trpc_session(source, target, **key.kwargs())
    assert await target.get_session(**key.kwargs()) is None


@pytest.mark.asyncio
async def test_state_only_conflict_requires_exact_approved_snapshot(session_backends):
    source, target = session_backends.source, session_backends.target
    key = (await seed_sessions(source, session_backends.suffix, sessions_per_tenant=1))[0]
    await copy_and_verify_trpc_session(source, target, target_writes_paused=True, **key.kwargs())
    before = await snapshot_trpc_session(target, **key.kwargs())
    session = await target.get_session(**key.kwargs())
    session.state = {**session.state, "turn": 99}
    await target.update_session(session)
    changed = await snapshot_trpc_session(target, **key.kwargs())
    for approved in (None, before.digest):
        with pytest.raises(ConflictError):
            await copy_and_verify_trpc_session(source, target, target_writes_paused=True,
                                               expected_target_digest=approved, **key.kwargs())
        assert (await snapshot_trpc_session(target, **key.kwargs())).digest == changed.digest
    # Explicit offline approval can replace state without pretending event count is a revision.
    await copy_and_verify_trpc_session(source, target, target_writes_paused=True,
                                       expected_target_digest=changed.digest, **key.kwargs())
    assert (await snapshot_trpc_session(target, **key.kwargs())).digest == before.digest


@pytest.mark.asyncio
async def test_same_event_id_with_changed_content_is_rejected_without_writes(session_backends):
    source, target = session_backends.source, session_backends.target
    key = (await seed_sessions(source, session_backends.suffix, sessions_per_tenant=1))[0]
    await copy_and_verify_trpc_session(source, target, target_writes_paused=True, **key.kwargs())
    session = await target.get_session(**key.kwargs())
    session.events[0].author = "different-author"
    await target.update_session(session)
    before = await snapshot_trpc_session(target, **key.kwargs())
    with pytest.raises(ConflictError, match="divergent"):
        await copy_and_verify_trpc_session(source, target, target_writes_paused=True,
                                           expected_target_digest=before.digest, **key.kwargs())
    assert (await snapshot_trpc_session(target, **key.kwargs())).digest == before.digest


@pytest.mark.asyncio
async def test_state_only_source_update_and_interrupted_final_state_restore(session_backends):
    source, target = session_backends.source, session_backends.target
    key = (await seed_sessions(source, session_backends.suffix, sessions_per_tenant=1))[0]
    session = await source.get_session(**key.kwargs())
    session.state = {**session.state, "turn": 99}
    await source.update_session(session)
    # Failure after the last append, before the final state snapshot was applied.
    with pytest.raises(ConnectionError):
        await copy_and_verify_trpc_session(source, FailAfterAppend(target, len(session.events)),
                                           target_writes_paused=True, **key.kwargs())
    checkpoint = await snapshot_trpc_session(target, **key.kwargs())
    assert checkpoint.payload["state"]["turn"] != 99
    await copy_and_verify_trpc_session(source, target, target_writes_paused=True,
                                       expected_target_digest=checkpoint.digest, **key.kwargs())
    approved = await snapshot_trpc_session(target, **key.kwargs())
    assert approved.payload["state"]["turn"] == 99
    session = await source.get_session(**key.kwargs())
    session.state = {**session.state, "turn": 100}
    await source.update_session(session)
    await copy_and_verify_trpc_session(source, target, target_writes_paused=True,
                                       expected_target_digest=approved.digest, **key.kwargs())
    assert (await target.get_session(**key.kwargs())).state["turn"] == 100
