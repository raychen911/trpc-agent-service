"""Version ledger and migration plan tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy import text

from trpc_service.migrations import Migration
from trpc_service.migrations import SchemaMigrator


def _script(path: Path, text_value: str) -> Path:
    path.write_text(text_value, encoding="utf-8")
    return path


def test_migrator_applies_each_version_once_and_supports_dry_run(tmp_path):
    database = f"sqlite:///{tmp_path / 'migration.db'}"
    one = _script(tmp_path / "one.sql", "-- baseline\nCREATE TABLE widget (id INTEGER PRIMARY KEY);")
    two = _script(tmp_path / "two.sql", "ALTER TABLE widget ADD COLUMN name TEXT;")
    migrator = SchemaMigrator(database, [Migration(1, "base", one), Migration(2, "name", two)])

    assert [item.version for item in migrator.validate()] == [1, 2]
    assert [item.version for item in migrator.migrate(dry_run=True)] == [1, 2]
    assert [item.version for item in migrator.migrate()] == [1, 2]
    assert migrator.pending() == []
    assert migrator.migrate() == []
    with create_engine(database).connect() as connection:
        columns = [row[1] for row in connection.execute(text("PRAGMA table_info(widget)"))]
    assert columns == ["id", "name"]
    migrator.close()


def test_migration_discovery_order_validation_and_ignored_files(tmp_path):
    schema = _script(tmp_path / "schema.sql", "CREATE TABLE base (id INTEGER);")
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    _script(migrations / "0003_third.sql", "CREATE TABLE third (id INTEGER);")
    _script(migrations / "not-a-migration.sql", "BROKEN")
    migrator = SchemaMigrator.discover(
        f"sqlite:///{tmp_path / 'discover.db'}",
        schema_file=schema,
        migrations_dir=migrations,
    )
    assert [(item.version, item.name) for item in migrator.validate()] == [(1, "baseline"), (3, "third")]
    migrator.close()

    empty = _script(tmp_path / "empty.sql", "-- only a comment")
    invalid = SchemaMigrator(f"sqlite:///{tmp_path / 'empty.db'}", [Migration(1, "empty", empty)])
    with pytest.raises(ValueError, match="contains no SQL"):
        invalid.validate()
    invalid.close()

    with pytest.raises(ValueError, match="versions must be unique"):
        SchemaMigrator(
            f"sqlite:///{tmp_path / 'duplicate.db'}",
            [Migration(1, "one", schema), Migration(1, "again", schema)],
        )
