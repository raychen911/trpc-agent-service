from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from trpc_service.config.secrets import SecretRef
from trpc_service.config.settings import Environment, ServiceSettings
from trpc_service.faults import (
    FaultStage,
    FaultStageControlError,
    FaultStageEvent,
    NoopFaultStageController,
    PostgresFaultStageController,
)


class _Transaction:
    async def __aenter__(self) -> _Transaction:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Acquire:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _Connection:
        return self._connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Connection:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def execute(self, query: str, *args: Any) -> str:
        if query.lstrip().startswith("SELECT set_config"):
            return "SELECT 1"
        if "INSERT INTO fault_stage_controls" in query:
            (
                tenant_id,
                control_id,
                run_id,
                stage,
                fingerprint,
                worker_id,
                inbound_id,
                turn_id,
                execution_key,
                stream_id,
                fencing_token,
                token_hash,
                _expires_at,
            ) = args
            if any(
                row["tenant_id"] == tenant_id
                and row["run_id"] == run_id
                and row["stage"] == stage
                and row["target_fingerprint"] == fingerprint
                for row in self.rows.values()
            ):
                raise asyncpg.UniqueViolationError("duplicate control")
            self.rows[str(control_id)] = {
                "tenant_id": tenant_id,
                "control_id": str(control_id),
                "run_id": run_id,
                "stage": stage,
                "target_fingerprint": fingerprint,
                "target_worker_id": worker_id,
                "target_inbound_id": inbound_id,
                "target_turn_id": turn_id,
                "target_execution_key": execution_key,
                "target_stream_id": stream_id,
                "target_fencing_token": fencing_token,
                "token_hash": token_hash,
                "status": "armed",
                "expires_at": _expires_at,
            }
            return "INSERT 0 1"
        if "SET status='expired'" in query:
            control_id = str(args[1])
            row = self.rows.get(control_id)
            if row is not None and row["status"] == "entered":
                row["status"] = "expired"
            return "UPDATE 1"
        raise AssertionError(f"unexpected execute query: {query}")

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "marker_status='entered'" in query:
            (
                tenant_id,
                run_id,
                token_hash,
                stage,
                fingerprint,
                worker_id,
                inbound_id,
                turn_id,
                execution_key,
                stream_id,
                fencing_token,
            ) = args
            now = datetime.now(UTC)
            for row in self.rows.values():
                if (
                    row["tenant_id"] == tenant_id
                    and row["run_id"] == run_id
                    and row["token_hash"] == token_hash
                    and row["stage"] == stage
                    and row["target_fingerprint"] == fingerprint
                    and row["target_worker_id"] == worker_id
                    and row["target_inbound_id"] == inbound_id
                    and row["target_turn_id"] == turn_id
                    and row["target_execution_key"] == execution_key
                    and row["target_stream_id"] == stream_id
                    and row["target_fencing_token"] == fencing_token
                    and row["status"] == "armed"
                    and row["expires_at"] > now
                ):
                    row.update(
                        {
                            "status": "entered",
                            "marker_status": "entered",
                            "marker_worker_id": worker_id,
                            "marker_inbound_id": inbound_id,
                            "marker_turn_id": turn_id,
                            "marker_execution_key": execution_key,
                            "marker_stream_id": stream_id,
                            "marker_fencing_token": fencing_token,
                        }
                    )
                    return {"control_id": row["control_id"], "expires_at": row["expires_at"]}
            return None
        if query.lstrip().startswith("SELECT status, expires_at"):
            tenant_id, control_id, run_id, token_hash, stage, fingerprint = args
            row = self.rows.get(str(control_id))
            if row is None:
                return None
            if all(
                row[key] == value
                for key, value in {
                    "tenant_id": tenant_id,
                    "run_id": run_id,
                    "token_hash": token_hash,
                    "stage": stage,
                    "target_fingerprint": fingerprint,
                }.items()
            ):
                return {"status": row["status"], "expires_at": row["expires_at"]}
            return None
        if "SET status='released'" in query:
            tenant_id, control_id, run_id, token_hash = args
            row = self.rows.get(str(control_id))
            if row is not None and all(
                (
                    row["tenant_id"] == tenant_id,
                    row["run_id"] == run_id,
                    row["token_hash"] == token_hash,
                    row["status"] == "entered",
                    row["expires_at"] > datetime.now(UTC),
                )
            ):
                row["status"] = "released"
                return {"control_id": row["control_id"]}
            return None
        raise AssertionError(f"unexpected fetchrow query: {query}")

    async def fetch(self, query: str, *args: Any) -> list[dict[str, str]]:
        if "DELETE FROM fault_stage_controls" not in query:
            raise AssertionError(f"unexpected fetch query: {query}")
        tenant_id, _limit = args
        now = datetime.now(UTC)
        expired = [
            control_id
            for control_id, row in self.rows.items()
            if row["tenant_id"] == tenant_id and row["expires_at"] <= now
        ]
        for control_id in expired:
            del self.rows[control_id]
        return [{"control_id": control_id} for control_id in expired]


class _Pool:
    def __init__(self) -> None:
        self.connection = _Connection()

    def acquire(self) -> _Acquire:
        return _Acquire(self.connection)


def _event(**updates: Any) -> FaultStageEvent:
    values: dict[str, Any] = {
        "stage": FaultStage.TOOL,
        "tenant_id": "tenant-a",
        "worker_id": "worker-a",
        "inbound_id": "inbound-a",
        "turn_id": "turn-a",
        "execution_key": "execution-a",
        "stream_id": "stream-a",
        "fencing_token": 7,
    }
    values.update(updates)
    return FaultStageEvent(**values)


def _controller(pool: _Pool, *, run_id: str = "run-a") -> PostgresFaultStageController:
    return PostgresFaultStageController(
        pool,
        run_id=run_id,
        run_token="test-only-fault-token-0123456789abcdef",
        poll_interval_seconds=0.001,
    )


@pytest.mark.asyncio
async def test_noop_controller_is_disabled() -> None:
    assert await NoopFaultStageController().checkpoint(_event()) is False


def test_event_is_immutable_and_contains_no_content_fields() -> None:
    event = _event()
    with pytest.raises((TypeError, ValueError)):
        event.worker_id = "other"  # type: ignore[misc]
    assert "payload" not in event.model_dump()
    assert "text" not in event.model_dump()
    assert "arguments" not in event.model_dump()


@pytest.mark.parametrize(
    ("stage", "missing_field"),
    [
        (FaultStage.ENQUEUE, "inbound_id"),
        (FaultStage.ENQUEUE, "stream_id"),
        (FaultStage.TOOL, "turn_id"),
        (FaultStage.TOOL, "execution_key"),
        (FaultStage.COMMIT_TXN_OPEN, "inbound_id"),
        (FaultStage.COMMIT_TXN_OPEN, "turn_id"),
        (FaultStage.COMMIT_TXN_OPEN, "fencing_token"),
    ],
)
def test_event_requires_stage_specific_identity(stage: FaultStage, missing_field: str) -> None:
    with pytest.raises(ValueError, match=missing_field):
        _event(stage=stage, **{missing_field: None})


def test_event_and_controller_identifiers_have_bounded_lengths() -> None:
    with pytest.raises(ValueError):
        _event(tenant_id="t" * 257)
    with pytest.raises(ValueError):
        _event(worker_id="w" * 257)
    with pytest.raises(ValueError):
        _event(execution_key="e" * 257)
    with pytest.raises(FaultStageControlError, match="maximum length"):
        _controller(_Pool(), run_id="r" * 129)


def test_controller_requires_long_run_token_and_bounded_wait_timeout() -> None:
    with pytest.raises(FaultStageControlError, match="32 bytes"):
        PostgresFaultStageController(_Pool(), run_id="run-a", run_token="too-short")
    with pytest.raises(ValueError, match="wait timeout"):
        PostgresFaultStageController(
            _Pool(),
            run_id="run-a",
            run_token="test-only-fault-token-0123456789abcdef",
            wait_timeout_seconds=300.01,
        )


@pytest.mark.asyncio
async def test_postgres_controller_rejects_wrong_tenant_run_stage_and_target() -> None:
    pool = _Pool()
    controller = _controller(pool)
    event = _event()
    await controller.arm(event)

    assert await controller.checkpoint(_event(tenant_id="tenant-b")) is False
    assert await controller.checkpoint(_event(stage=FaultStage.ENQUEUE)) is False
    assert await controller.checkpoint(_event(worker_id="worker-b")) is False
    assert await _controller(pool, run_id="run-b").checkpoint(event) is False


@pytest.mark.asyncio
async def test_control_is_one_shot_and_duplicate_arm_fails() -> None:
    pool = _Pool()
    controller = _controller(pool)
    event = _event()
    await controller.arm(event)
    with pytest.raises(FaultStageControlError):
        await controller.arm(event)


@pytest.mark.asyncio
async def test_checkpoint_enters_then_release_returns_true() -> None:
    pool = _Pool()
    controller = _controller(pool)
    event = _event()
    control_id = await controller.arm(event)
    checkpoint = asyncio.create_task(controller.checkpoint(event))
    await asyncio.sleep(0.01)
    assert pool.connection.rows[control_id]["status"] == "entered"
    assert await controller.release(control_id, tenant_id=event.tenant_id) is True
    assert await checkpoint is True
    assert pool.connection.rows[control_id]["marker_status"] == "entered"


@pytest.mark.asyncio
async def test_checkpoint_expires_and_cannot_be_reused() -> None:
    pool = _Pool()
    controller = _controller(pool)
    event = _event()
    control_id = await controller.arm(event, ttl_seconds=0.01)
    assert await controller.checkpoint(event) is False
    assert pool.connection.rows[control_id]["status"] == "expired"
    assert await controller.release(control_id, tenant_id=event.tenant_id) is False
    assert await controller.checkpoint(event) is False


@pytest.mark.asyncio
async def test_expired_controls_can_be_cleaned() -> None:
    pool = _Pool()
    controller = _controller(pool)
    await controller.arm(_event(), ttl_seconds=0.01)
    await asyncio.sleep(0.02)
    assert await controller.cleanup_expired(tenant_id="tenant-a") == 1
    assert pool.connection.rows == {}


@pytest.mark.asyncio
async def test_token_is_stored_only_as_hash_and_markers_are_content_free() -> None:
    pool = _Pool()
    secret = "test-only-fault-token-0123456789abcdef"
    controller = PostgresFaultStageController(
        pool, run_id="run-a", run_token=secret, poll_interval_seconds=0.001
    )
    event = _event()
    control_id = await controller.arm(event, ttl_seconds=0.01)
    row = pool.connection.rows[control_id]
    assert secret not in repr(row)
    assert len(row["token_hash"]) == 64
    assert secret not in repr(controller.__dict__)
    assert await controller.checkpoint(event) is False
    assert secret not in repr(pool.connection.rows[control_id])


def test_settings_default_disabled_and_enabled_requires_run_id() -> None:
    settings = ServiceSettings(_env_file=None)
    assert settings.fault_injection_enabled is False
    assert settings.fault_injection_run_id is None
    assert settings.fault_injection_run_token_ref.uri == (
        "env://TRPC_SERVICE_FAULT_INJECTION_RUN_TOKEN"
    )
    enabled = ServiceSettings(
        _env_file=None,
        fault_injection_enabled=True,
        fault_injection_run_id="run-a",
    )
    assert enabled.fault_injection_enabled is True
    with pytest.raises(ValueError, match="run_id"):
        ServiceSettings(_env_file=None, fault_injection_enabled=True)


def test_production_forbids_fault_injection_even_when_run_id_is_present() -> None:
    with pytest.raises(ValueError, match="forbidden in production"):
        ServiceSettings(
            _env_file=None,
            environment=Environment.PRODUCTION,
            fault_injection_enabled=True,
            fault_injection_run_id="run-a",
        )


def test_production_rejects_literal_fault_token_reference() -> None:
    with pytest.raises(ValueError, match="literal secret references"):
        ServiceSettings(
            _env_file=None,
            environment=Environment.PRODUCTION,
            allow_development_token=False,
            oidc_issuer="https://issuer.example",
            oidc_audience="trpc-service",
            fault_injection_run_token_ref=SecretRef(uri="literal://do-not-use"),
        )


def test_fault_stage_migration_has_rls_and_runtime_grants() -> None:
    migration = Path("migrations/versions/0006_fault_stage_controls.py").read_text(encoding="utf-8")
    assert "0006_fault_stage_controls" in migration
    assert "0005_add_feishu_channel" in migration
    assert "CREATE TABLE fault_stage_controls" in migration
    assert "ENABLE ROW LEVEL SECURITY" in migration
    assert "tenant_isolation_fault_stage_controls" in migration
    assert "GRANT SELECT, INSERT, UPDATE, DELETE" in migration
    assert "interval '5 minutes'" in migration
    assert "marker_status IS NULL OR marker_status = 'entered'" in migration
    assert "Acceptance reports must read markers before cleanup" in migration
    assert "payload" not in migration.lower()
