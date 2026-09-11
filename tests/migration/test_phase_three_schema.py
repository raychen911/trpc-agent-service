"""Explicit upgrade preserves old data and requires a deliberate identity cutover."""

import pytest
from sqlalchemy import select

from trpc_service.persistence import Database
from trpc_service.persistence.models import Base, TenantRow, TenantConfigVersionRow, ServiceSchemaRow
from trpc_service.persistence.models import InboundMessageRow
from trpc_service.persistence.schema import NEW_TABLES, upgrade_phase_two
from trpc_service.management import ConfigurationStore


def test_version_three_requires_explicit_additive_budget_upgrade(tmp_path):
    from trpc_service.persistence.schema import BUDGET_TABLES, ACTION_TABLES
    db = Database(f"sqlite:///{tmp_path / 'version-three.db'}")
    try:
        Base.metadata.create_all(
            db.engine,
            tables=[table for name, table in Base.metadata.tables.items() if name not in BUDGET_TABLES | ACTION_TABLES])
        with db.sessions.begin() as session:
            session.add(ServiceSchemaRow(component="platform", version=3))
            session.add(TenantRow(tenant_id="tenant_saved", name="Saved", status="active", active_config_version=1))
        with pytest.raises(RuntimeError):
            db.create_schema()
        with pytest.raises(ValueError):
            upgrade_phase_two(db, offline=False, backup_id="verified-backup")
        assert upgrade_phase_two(db, offline=True, backup_id="verified-backup")["version"] == 5
        db.check_schema()
        with db.sessions() as session:
            assert session.get(TenantRow, "tenant_saved").status == "active"
    finally:
        db.close()


def test_version_four_adds_actions_preserving_price_and_refuses_live_session(tmp_path):
    from decimal import Decimal
    from datetime import timedelta
    from trpc_service.persistence.schema import ACTION_TABLES
    from trpc_service.persistence.models import ModelPriceRow, SessionLeaseRow, utcnow
    db = Database(f"sqlite:///{tmp_path / 'version-four.db'}")
    try:
        Base.metadata.create_all(
            db.engine, tables=[table for name, table in Base.metadata.tables.items() if name not in ACTION_TABLES])
        with db.sessions.begin() as session:
            session.add(ServiceSchemaRow(component="platform", version=4))
            session.add(
                ModelPriceRow(tenant_id="tenant_saved",
                              price_id="v1",
                              model_id="model_saved",
                              currency="USD",
                              input_per_million=Decimal("0.1"),
                              output_per_million=Decimal("0.2")))
            session.add(
                SessionLeaseRow(tenant_id="tenant_saved",
                                session_id="session",
                                owner_id="writer",
                                fencing_token=1,
                                expires_at=utcnow() + timedelta(seconds=60)))
        with pytest.raises(RuntimeError, match="stop session writers"):
            upgrade_phase_two(db, offline=True, backup_id="verified-backup")
        assert not (db.table_names() & ACTION_TABLES)
        with db.sessions.begin() as session:
            session.get(SessionLeaseRow, ("tenant_saved", "session")).expires_at = utcnow() - timedelta(seconds=1)
        assert upgrade_phase_two(db, offline=True, backup_id="verified-backup")["version"] == 5
        db.check_schema()
        with db.sessions() as session:
            assert session.get(ModelPriceRow, ("tenant_saved", "v1")).input_per_million == Decimal("0.1")
    finally:
        db.close()


@pytest.fixture
def old_database(tmp_path, tenant):
    db = Database(f"sqlite:///{tmp_path / 'old.db'}")
    Base.metadata.create_all(db.engine,
                             tables=[table for name, table in Base.metadata.tables.items() if name not in NEW_TABLES])
    with db.sessions.begin() as session:
        session.add(TenantRow(tenant_id=tenant.tenant_id, name=tenant.name, status="active", active_config_version=3))
        config = tenant.model_dump(mode="json")
        for binding in config["channel_bindings"]:
            for name in ("identity_version", "conversation_epoch", "group_mode", "transport"):
                binding.pop(name)
        session.add(
            TenantConfigVersionRow(tenant_id=tenant.tenant_id,
                                   config_version=3,
                                   status="published",
                                   config_json=config,
                                   content_hash="original-hash",
                                   created_by="operator"))
    yield db
    db.close()


def test_unversioned_database_is_not_silently_initialized(old_database):
    with pytest.raises(RuntimeError, match="unversioned"):
        old_database.create_schema()
    with pytest.raises(RuntimeError, match="unversioned"):
        old_database.check_schema()
    assert "service_schema" not in old_database.table_names()


def test_upgrade_preserves_snapshot_and_suspends_old_identity(old_database):
    result = upgrade_phase_two(old_database, offline=True, backup_id="verified-test-backup")
    assert result["changed"] and result["suspended_tenants"] == 1
    old_database.check_schema()
    assert ConfigurationStore(old_database).active() == ()
    with old_database.sessions() as session:
        snapshot = session.scalar(select(TenantConfigVersionRow))
        assert snapshot.content_hash == "original-hash"
        assert "identity_version" not in snapshot.config_json["channel_bindings"][0]
        assert session.scalar(select(TenantRow)).status == "suspended"
        assert session.get(ServiceSchemaRow, "platform").version == 5
    assert not upgrade_phase_two(old_database, offline=True, backup_id="verified-test-backup")["changed"]


def test_pending_work_blocks_entire_upgrade(old_database):
    with old_database.sessions.begin() as session:
        session.add(
            InboundMessageRow(inbound_message_id="old-message",
                              tenant_id="tenant_acme",
                              channel_binding_id="binding_primary",
                              external_message_id="old",
                              payload_hash="hash",
                              payload_json={},
                              request_id="request",
                              trace_id="trace",
                              execution_id="execution",
                              partition_key="old-scope",
                              config_version=3,
                              storage_revision=4,
                              status="pending"))
    with pytest.raises(RuntimeError, match="drain or reconcile"):
        upgrade_phase_two(old_database, offline=True, backup_id="verified-test-backup")
    assert "service_schema" not in old_database.table_names()
    with old_database.sessions() as session:
        assert session.scalar(select(TenantRow)).status == "active"
