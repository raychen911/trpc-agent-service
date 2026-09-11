"""R1B unit tests: offline, bounded tenant state migration (Redis<->SQL).

The migration copies the current config-version app namespace to the
predicted NEXT-version namespace on the target backend using ONLY public
SDK Session/Memory methods, validates counts + a canonical digest, and only
then performs the single ``expected_version`` CAS on
``backend_profile.state_backend``.  Source data is never modified; any
failure leaves the configuration untouched; re-runs are deterministic.

All scenarios run against the SDK InMemory services (dev/test tier — never a
production fallback), with the tenant/receipt repositories and the backend
resolver faked around the same public protocols used in production.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest

from trpc_agent_sdk.memory import InMemoryMemoryService, MemoryServiceConfig
from trpc_agent_sdk.sessions import (
    InMemorySessionService,
    SessionServiceConfig,
)
from trpc_agent_sdk.types import Content, EventActions, Part, Ttl
from trpc_agent_sdk.events import Event

from trpc_service.config.tenant import TenantBackendProfile, TenantConfig
from trpc_service.config.tenant_repository import TenantConfigVersionConflictError
from trpc_service.storage.state_migration import (
    StateMigrationError,
    StateMigrationPreconditionError,
    StateMigrationResult,
    StateMigrationUnavailableError,
    migrate_tenant_state,
)
from tests.tenant_helpers import make_app_config, make_audit_policy, make_backend_profile, make_governance

TENANT = "t-migrate"
APP_ID = "app_demo"
USER = "usr_v1_" + "a" * 48


def _config(version: int = 1, state_backend: str = "redis") -> TenantConfig:
    return TenantConfig(
        tenant_id=TENANT,
        enabled=True,
        version=version,
        app=make_app_config(app_id=APP_ID),
        governance=make_governance(),
        backend_profile=make_backend_profile(state_backend),
        audit_policy=make_audit_policy(),
    )


def _ns(version: int) -> str:
    return f"{TENANT}:{APP_ID}:v{version}"


def _memory() -> InMemoryMemoryService:
    return InMemoryMemoryService(memory_service_config=MemoryServiceConfig(enabled=True, ttl=Ttl(enable=False)))


def _session_service() -> InMemorySessionService:
    # Mirrors production backends: both Redis and SQL persist historical
    # events (R1B durable-summary configuration).
    return InMemorySessionService(session_config=SessionServiceConfig(
        ttl=Ttl(enable=False),
        store_historical_events=True,
    ))


class RecordingSessionService(InMemorySessionService):
    """InMemory service that records every public call name it receives."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.calls: list[str] = []
        self.fail_on: set[str] = set()

    def _gate(self, name: str) -> None:
        self.calls.append(name)
        if name in self.fail_on:
            raise RuntimeError("boom postgresql+asyncpg://secret:pw@hidden/db")

    async def create_session(self, **kwargs):
        self._gate("create_session")
        return await super().create_session(**kwargs)

    async def get_session(self, **kwargs):
        self._gate("get_session")
        return await super().get_session(**kwargs)

    async def list_sessions(self, **kwargs):
        self._gate("list_sessions")
        return await super().list_sessions(**kwargs)

    async def delete_session(self, **kwargs):
        self._gate("delete_session")
        return await super().delete_session(**kwargs)

    async def append_event(self, session, event):
        self._gate("append_event")
        return await super().append_event(session, event)

    async def update_session(self, session):
        self._gate("update_session")
        return await super().update_session(session)


class RecordingMemoryService(InMemoryMemoryService):

    def __init__(self) -> None:
        super().__init__(memory_service_config=MemoryServiceConfig(enabled=True, ttl=Ttl(enable=False)))
        self.stored: list[str] = []
        self.fail_on: set[str] = set()

    async def store_session(self, session, agent_context=None):
        self.stored.append(f"{session.app_name}|{session.user_id}|{session.id}")
        if "store_session" in self.fail_on:
            raise RuntimeError("boom secret memory backend down")
        return await super().store_session(session, agent_context=agent_context)


class LyingUpdateService(RecordingSessionService):
    """Target that silently drops update_session writes (validation must catch)."""

    async def update_session(self, session):
        self.calls.append("update_session")
        return None


class FakeBackend:

    def __init__(self, session_service, memory_service) -> None:
        self.session_service = session_service
        self.memory_service = memory_service
        self.ready_raises = False

    def check_ready(self) -> None:
        if self.ready_raises:
            from trpc_service.storage.state_backend import StateBackendConfigurationError
            raise StateBackendConfigurationError("Redis is not ready")

    async def close(self) -> None:
        return None


class FakeResolver:
    """Stands in for TenantStateBackendResolver: selection only via profile."""

    def __init__(self, redis: FakeBackend, sql: FakeBackend) -> None:
        self._backends = {"redis": redis, "sql": sql}
        self.resolve_count = 0

    def resolve(self, profile: TenantBackendProfile) -> FakeBackend:
        self.resolve_count += 1
        return self._backends[profile.state_backend]


class FakeTenantRepository:

    def __init__(self, config: TenantConfig | None) -> None:
        self.config = config
        self.update_calls: list[tuple[str, int, Any]] = []
        self.get_calls: list[str] = []
        self.update_error: Exception | None = None

    async def get(self, tenant_id: str) -> TenantConfig | None:
        self.get_calls.append(tenant_id)
        if self.config is not None and self.config.tenant_id == tenant_id:
            return self.config
        return None

    async def update(self, tenant_id: str, expected_version: int, desired) -> TenantConfig:
        self.update_calls.append((tenant_id, expected_version, desired))
        if self.update_error is not None:
            raise self.update_error
        if self.config is None or self.config.version != expected_version:
            raise TenantConfigVersionConflictError("version conflict")
        updated = self.config.model_copy(update={
            "version": expected_version + 1,
            "backend_profile": desired.backend_profile,
            "enabled": desired.enabled,
        })
        self.config = updated
        return updated

    async def check_ready(self) -> None:
        return None

    async def close(self) -> None:
        return None


class ReadOnlyTenantRepository(FakeTenantRepository):
    """Repository without the versioned write path (JSON snapshot style)."""

    update = None  # type: ignore[assignment]


class FakeReceiptRepository:

    def __init__(self, processing: int = 0, error: Exception | None = None) -> None:
        self.processing = processing
        self.error = error
        self.calls = 0

    async def count_processing_by_tenant(self, tenant_id: str) -> int:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.processing


async def _seed_sessions(src: InMemorySessionService, app_name: str) -> dict[str, Any]:
    """Two users, deterministic event graph incl. state deltas + summary flags."""
    marker = f"seed-{uuid.uuid4().hex[:8]}"
    for user, session_id, extra in (
        (USER, "sess-1", True),
        ("usr_v1_" + "b" * 48, "sess-2", False),
    ):
        session = await src.create_session(app_name=app_name,
                                           user_id=user,
                                           session_id=session_id,
                                           state={"created_by_seed": user[-1]})
        await src.append_event(
            session,
            Event(
                invocation_id=f"inv-{session_id}-u",
                author="user",
                content=Content(role="user", parts=[Part.from_text(text=f"{marker} hello {user[-1]}")]),
            ))
        await src.append_event(
            session,
            Event(
                invocation_id=f"inv-{session_id}-m",
                author=APP_ID,
                content=Content(role="model", parts=[Part.from_text(text=f"{marker} model answer {user[-1]}")]),
                actions=EventActions(state_delta={
                    "mood": "calm",
                    "app:theme": "dark",
                    "user:locale": "zh",
                }),
            ))
        if extra:
            # A retained summary Event + a compressed historical Event, exactly
            # as the SDK durable-summary pipeline leaves them.
            summary_event = Event(
                invocation_id="summary",
                author="system",
                content=Content(role="user", parts=[Part.from_text(text=f"Previous conversation summary: {marker}")]),
            )
            summary_event.set_summary_event(True)
            await src.append_event(session, summary_event)
            old_event = Event(
                invocation_id="inv-old",
                author="user",
                content=Content(role="user", parts=[Part.from_text(text=f"{marker} ancient question?")]),
            )
            session.historical_events.append(old_event)
            await src.update_session(session)
    return {"marker": marker}


class TestMigrationHappyPath:

    def test_redis_to_sql_copies_validates_then_single_cas(self):
        src_mem = _memory()
        tgt_mem = RecordingMemoryService()
        src = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        backend_src = FakeBackend(src, src_mem)
        backend_tgt = FakeBackend(tgt, tgt_mem)
        resolver = FakeResolver(redis=backend_src, sql=backend_tgt)
        repo = FakeTenantRepository(_config(version=1, state_backend="redis"))
        receipts = FakeReceiptRepository(processing=0)

        async def _scenario():
            seeded = await _seed_sessions(src, _ns(1))
            src.calls.clear()  # the "source is read-only" assertion covers
            tgt.calls.clear()  # the migration itself, not the test seeding
            result = await migrate_tenant_state(
                tenant_id=TENANT,
                target_backend="sql",
                expected_version=1,
                offline=True,
                tenant_repository=repo,
                receipt_repository=receipts,
                resolver=resolver,
            )
            return seeded, result

        seeded, result = asyncio.run(_scenario())

        assert isinstance(result, StateMigrationResult)
        assert (result.tenant_id, result.source_backend, result.target_backend) == (TENANT, "redis", "sql")
        assert (result.source_version, result.target_version) == (1, 2)
        assert result.session_count == 2
        # active events (3/2) + the one historical event on sess-1.
        assert result.event_count == 6

        # ONE CAS update with expected_version=1 and ONLY state_backend flipped.
        assert len(repo.update_calls) == 1
        tenant_id, expected, desired = repo.update_calls[0]
        assert (tenant_id, expected) == (TENANT, 1)
        assert desired.backend_profile.state_backend == "sql"
        assert desired.backend_profile.artifact_backend == "s3"
        assert desired.app == _config().app
        assert desired.governance == _config().governance
        assert repo.config is not None and repo.config.version == 2
        assert repo.config.backend_profile.state_backend == "sql"

        # Target holds the NEXT-version namespace...
        target_list = asyncio.run(tgt.list_sessions(app_name=_ns(2)))
        assert sorted((s.user_id, s.id) for s in target_list.sessions) == sorted([
            (USER, "sess-1"),
            ("usr_v1_" + "b" * 48, "sess-2"),
        ])

        # ...with identity, event ids, timestamps, state, summary flags,
        # historical events and conversation state all preserved.
        async def _assert_target():
            s1 = await tgt.get_session(app_name=_ns(2), user_id=USER, session_id="sess-1")
            s1_src = await src.get_session(app_name=_ns(1), user_id=USER, session_id="sess-1")
            assert s1 is not None and s1_src is not None
            assert [e.id for e in s1.events] == [e.id for e in s1_src.events]
            assert [e.timestamp for e in s1.events] == [e.timestamp for e in s1_src.events]
            assert s1.events[-1].is_summary_event() is True
            assert len(s1.historical_events) == 1
            assert s1.historical_events[0].id == s1_src.historical_events[0].id
            assert s1.state["mood"] == "calm"
            # app/user scoped state carried through the public create_session
            # state API (prefixes land in the right buckets on read-back).
            assert s1.state["app:theme"] == "dark"
            assert s1.state["user:locale"] == "zh"
            assert s1.state["created_by_seed"] == USER[-1]
            assert s1.conversation_count == s1_src.conversation_count
            # Source namespace data is untouched and still on the source.
            s1_again = await src.get_session(app_name=_ns(1), user_id=USER, session_id="sess-1")
            assert s1_again is not None
            assert [e.id for e in s1_again.events] == [e.id for e in s1_src.events]

        asyncio.run(_assert_target())

        # Source backend saw ONLY read operations — no delete/create/write.
        assert set(src.calls) <= {"list_sessions", "get_session"}
        # Memory was re-materialised on the TARGET namespace only.
        assert all(key.startswith(_ns(2)) for key in tgt_mem.stored), tgt_mem.stored
        assert len(tgt_mem.stored) == 2

        # The copied Memory is searchable under the target key.
        async def _assert_memory():
            s1 = await tgt.get_session(app_name=_ns(2), user_id=USER, session_id="sess-1")
            resp = await tgt_mem.search_memory(key=s1.save_key, query=seeded["marker"], limit=10)
            assert resp.memories

        asyncio.run(_assert_memory())

    def test_sql_to_redis_uses_the_same_path_for_rollback(self):
        src = _session_service()
        tgt = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt_mem = RecordingMemoryService()
        resolver = FakeResolver(
            redis=FakeBackend(tgt, tgt_mem),
            sql=FakeBackend(src, _memory()),
        )
        repo = FakeTenantRepository(_config(version=4, state_backend="sql"))
        receipts = FakeReceiptRepository()

        async def _scenario():
            await _seed_sessions(src, _ns(4))
            return await migrate_tenant_state(
                tenant_id=TENANT,
                target_backend="redis",
                expected_version=4,
                offline=True,
                tenant_repository=repo,
                receipt_repository=receipts,
                resolver=resolver,
            )

        result = asyncio.run(_scenario())
        assert (result.source_backend, result.target_backend) == ("sql", "redis")
        assert (result.source_version, result.target_version) == (4, 5)
        assert result.session_count == 2
        assert repo.config.backend_profile.state_backend == "redis"
        assert repo.config.version == 5
        listed = asyncio.run(tgt.list_sessions(app_name=_ns(5)))
        assert len(listed.sessions) == 2


class TestMigrationPreconditions:

    def _harness(self, config: TenantConfig | None, *, processing: int = 0, repo=None):
        src = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        resolver = FakeResolver(redis=FakeBackend(src, _memory()), sql=FakeBackend(tgt, _memory()))
        repo = repo or FakeTenantRepository(config)
        receipts = FakeReceiptRepository(processing=processing)
        return src, tgt, resolver, repo, receipts

    def _migrate(self, *, offline=True, target="sql", expected=1, resolver, repo, receipts):
        return asyncio.run(
            migrate_tenant_state(
                tenant_id=TENANT,
                target_backend=target,
                expected_version=expected,
                offline=offline,
                tenant_repository=repo,
                receipt_repository=receipts,
                resolver=resolver,
            ))

    def test_offline_flag_is_mandatory_and_checked_before_anything(self):
        src, tgt, resolver, repo, receipts = self._harness(_config())

        async def _seed():
            await _seed_sessions(src, _ns(1))
            src.calls.clear()  # assertion below covers the migration only

        asyncio.run(_seed())
        with pytest.raises(StateMigrationPreconditionError) as exc:
            self._migrate(offline=False, resolver=resolver, repo=repo, receipts=receipts)
        assert str(exc.value) == "migration must be explicitly marked offline"
        assert repo.update_calls == []
        assert set(src.calls) <= {"list_sessions", "get_session"}
        assert tgt.calls == []
        assert resolver.resolve_count == 0

    def test_unknown_tenant_is_refused(self):
        _, _, resolver, repo, receipts = self._harness(None)
        with pytest.raises(StateMigrationPreconditionError) as exc:
            self._migrate(resolver=resolver, repo=repo, receipts=receipts)
        assert str(exc.value) == "tenant not found"

    def test_expected_version_mismatch_is_refused_before_copying(self):
        src, tgt, resolver, repo, receipts = self._harness(_config(version=7))
        with pytest.raises(StateMigrationPreconditionError) as exc:
            self._migrate(expected=3, resolver=resolver, repo=repo, receipts=receipts)
        assert str(exc.value) == "tenant configuration version does not match expected_version"
        assert tgt.calls == []

    def test_same_source_and_target_is_refused(self):
        src, tgt, resolver, repo, receipts = self._harness(_config(version=1, state_backend="sql"))
        with pytest.raises(StateMigrationPreconditionError) as exc:
            self._migrate(target="sql", resolver=resolver, repo=repo, receipts=receipts)
        assert str(exc.value) == "source and target state backend are identical"
        assert tgt.calls == []

    def test_processing_receipts_refuse_migration(self):
        src, tgt, resolver, repo, receipts = self._harness(_config(), processing=2)
        with pytest.raises(StateMigrationPreconditionError) as exc:
            self._migrate(resolver=resolver, repo=repo, receipts=receipts)
        assert str(exc.value) == "tenant has processing message executions; stop traffic first"
        assert tgt.calls == []
        assert repo.update_calls == []

    def test_receipt_query_failure_maps_to_unavailable(self):
        src, tgt, resolver, repo, receipts = self._harness(_config(), repo=None)
        receipts.error = RuntimeError("db down")
        with pytest.raises(StateMigrationUnavailableError) as exc:
            self._migrate(resolver=resolver, repo=repo, receipts=receipts)
        assert str(exc.value) == "state backend is not available"
        assert repo.update_calls == []

    def test_read_only_repository_is_refused(self):
        src, tgt, resolver, _, receipts = self._harness(_config())
        repo = ReadOnlyTenantRepository(_config())
        with pytest.raises(StateMigrationPreconditionError) as exc:
            self._migrate(resolver=resolver, repo=repo, receipts=receipts)
        assert str(exc.value) == "tenant configuration repository cannot perform CAS updates"
        assert tgt.calls == []

    def test_closed_target_backend_is_unavailable(self):
        src, tgt, resolver, repo, receipts = self._harness(_config())
        backend_sql = resolver._backends["sql"]
        backend_sql.ready_raises = True
        with pytest.raises(StateMigrationUnavailableError) as exc:
            self._migrate(resolver=resolver, repo=repo, receipts=receipts)
        assert str(exc.value) == "state backend is not available"
        assert repo.update_calls == []

    def test_invalid_target_backend_string_is_refused(self):
        _, _, resolver, repo, receipts = self._harness(_config())
        with pytest.raises(StateMigrationPreconditionError) as exc:
            self._migrate(target="minio", resolver=resolver, repo=repo, receipts=receipts)
        assert str(exc.value) == "unknown target state backend"
        assert resolver.resolve_count == 0

    @pytest.mark.parametrize("bad_version", [0, -1, True, False])
    def test_bad_expected_version_is_refused_before_any_io(self, bad_version):
        # The argument-validity gate must fire BEFORE any repository read,
        # any resolver selection, and any backend call (distinct from the
        # version-MISMATCH branch, which happens after tenant lookup).
        src, tgt, resolver, repo, receipts = self._harness(_config())
        with pytest.raises(StateMigrationPreconditionError) as exc:
            self._migrate(expected=bad_version, resolver=resolver, repo=repo, receipts=receipts)
        assert str(exc.value) == "expected_version must be a positive integer"
        assert repo.get_calls == []
        assert repo.update_calls == []
        assert resolver.resolve_count == 0
        assert tgt.calls == []
        assert src.calls == []
        assert receipts.calls == 0  # the processing gate is never consulted


class TestMigrationFailureSemantics:

    def _migrate_with(self, tgt, repo, resolver, receipts):
        return asyncio.run(
            migrate_tenant_state(
                tenant_id=TENANT,
                target_backend="sql",
                expected_version=1,
                offline=True,
                tenant_repository=repo,
                receipt_repository=receipts,
                resolver=resolver,
            ))

    def test_target_write_failure_maps_to_unavailable_with_sanitized_message(self):
        src = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt.fail_on = {"update_session"}
        resolver = FakeResolver(redis=FakeBackend(src, _memory()), sql=FakeBackend(tgt, _memory()))
        repo = FakeTenantRepository(_config())
        receipts = FakeReceiptRepository()

        async def _seed():
            await _seed_sessions(src, _ns(1))

        asyncio.run(_seed())
        with pytest.raises(StateMigrationUnavailableError) as exc:
            self._migrate_with(tgt, repo, resolver, receipts)
        # Fixed safe message — the upstream exception text (DSN-like content)
        # must never surface.
        assert str(exc.value) == "state backend is not available"
        assert "boom" not in str(exc.value)
        assert "postgresql" not in str(exc.value)
        assert repo.update_calls == []
        assert repo.config.version == 1
        assert repo.config.backend_profile.state_backend == "redis"

    def test_memory_write_failure_maps_to_unavailable(self):
        src = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt_mem = RecordingMemoryService()
        tgt_mem.fail_on = {"store_session"}
        resolver = FakeResolver(redis=FakeBackend(src, _memory()), sql=FakeBackend(tgt, tgt_mem))
        repo = FakeTenantRepository(_config())
        receipts = FakeReceiptRepository()

        async def _seed():
            await _seed_sessions(src, _ns(1))

        asyncio.run(_seed())
        with pytest.raises(StateMigrationUnavailableError) as exc:
            self._migrate_with(tgt, repo, resolver, receipts)
        assert str(exc.value) == "state backend is not available"
        assert repo.update_calls == []

    def test_validation_before_cas_silent_target_loss_refuses_switch(self):
        src = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt = LyingUpdateService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        resolver = FakeResolver(redis=FakeBackend(src, _memory()), sql=FakeBackend(tgt, _memory()))
        repo = FakeTenantRepository(_config())
        receipts = FakeReceiptRepository()

        async def _seed():
            await _seed_sessions(src, _ns(1))
            src.calls.clear()  # assertion below covers the migration only

        asyncio.run(_seed())
        with pytest.raises(StateMigrationPreconditionError) as exc:
            self._migrate_with(tgt, repo, resolver, receipts)
        assert str(exc.value) == "migrated state failed target validation; configuration not switched"
        assert repo.update_calls == []
        assert repo.config.backend_profile.state_backend == "redis"
        assert repo.config.version == 1
        # Source untouched.
        assert set(src.calls) <= {"list_sessions", "get_session"}

    def test_cas_conflict_leaves_configuration_unchanged(self):
        src = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        resolver = FakeResolver(redis=FakeBackend(src, _memory()), sql=FakeBackend(tgt, _memory()))
        repo = FakeTenantRepository(_config())
        receipts = FakeReceiptRepository()

        # Simulate a concurrent admin write racing the CAS: the repository
        # head moved between precheck and update.
        original_update = repo.update

        async def _racing_update(tenant_id, expected_version, desired):
            repo.config = repo.config.model_copy(update={"version": repo.config.version + 1})
            return await original_update(tenant_id, expected_version, desired)

        repo.update = _racing_update  # type: ignore[method-assign]

        async def _seed():
            await _seed_sessions(src, _ns(1))

        asyncio.run(_seed())
        with pytest.raises(StateMigrationPreconditionError) as exc:
            self._migrate_with(tgt, repo, resolver, receipts)
        assert str(exc.value) == "tenant configuration changed during migration; configuration not switched"
        assert repo.config.backend_profile.state_backend == "redis"

    def test_reentry_after_failed_attempt_is_deterministic(self):
        src = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt_mem = RecordingMemoryService()
        resolver = FakeResolver(redis=FakeBackend(src, _memory()), sql=FakeBackend(tgt, tgt_mem))
        repo = FakeTenantRepository(_config())
        receipts = FakeReceiptRepository()

        async def _seed():
            await _seed_sessions(src, _ns(1))

        asyncio.run(_seed())

        # First attempt fails mid-copy.
        tgt.fail_on = {"update_session"}
        with pytest.raises(StateMigrationUnavailableError):
            self._migrate_with(tgt, repo, resolver, receipts)
        tgt.fail_on = set()

        # Re-running with the same arguments succeeds...
        first = self._migrate_with(tgt, repo, resolver, receipts)
        snapshot_after_first = asyncio.run(self._target_snapshot(tgt))

        # ...and re-running the copy stage again leaves the target namespace
        # byte-identical (deterministic overwrite, no duplication).
        repo.config = _config()  # reset head so the same expected_version applies
        second = self._migrate_with(tgt, repo, resolver, receipts)
        snapshot_after_second = asyncio.run(self._target_snapshot(tgt))
        assert snapshot_after_first == snapshot_after_second
        assert (second.session_count, second.event_count) == (first.session_count, first.event_count)

    @staticmethod
    async def _target_snapshot(tgt: InMemorySessionService):
        resp = await tgt.list_sessions(app_name=_ns(2))
        out = {}
        for s in sorted(resp.sessions, key=lambda item: (item.user_id, item.id)):
            full = await tgt.get_session(app_name=_ns(2), user_id=s.user_id, session_id=s.id)
            out[f"{s.user_id}/{s.id}"] = [
                [(e.id, e.timestamp) for e in full.events],
                [(e.id, e.timestamp) for e in full.historical_events],
                full.state,
                full.conversation_count,
            ]
        return out


class TestMigrationResultShape:

    def test_result_is_frozen(self):
        result = StateMigrationResult(
            tenant_id=TENANT,
            source_backend="redis",
            target_backend="sql",
            source_version=1,
            target_version=2,
            session_count=3,
            event_count=7,
        )
        with pytest.raises(Exception):
            result.session_count = 4  # type: ignore[misc]

    def test_error_hierarchy(self):
        assert issubclass(StateMigrationPreconditionError, StateMigrationError)
        assert issubclass(StateMigrationUnavailableError, StateMigrationError)

    def test_empty_tenant_migrates_cleanly_with_zero_counts(self):
        src = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        tgt = RecordingSessionService(
            session_config=SessionServiceConfig(ttl=Ttl(enable=False), store_historical_events=True))
        resolver = FakeResolver(redis=FakeBackend(src, _memory()), sql=FakeBackend(tgt, _memory()))
        repo = FakeTenantRepository(_config())
        receipts = FakeReceiptRepository()
        result = asyncio.run(
            migrate_tenant_state(
                tenant_id=TENANT,
                target_backend="sql",
                expected_version=1,
                offline=True,
                tenant_repository=repo,
                receipt_repository=receipts,
                resolver=resolver,
            ))
        assert (result.session_count, result.event_count) == (0, 0)
        assert len(repo.update_calls) == 1
        assert repo.config.version == 2


__all__: list[str] = []
