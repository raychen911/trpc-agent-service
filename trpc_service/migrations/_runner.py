"""Small, dependency-free SQL migration ledger for MySQL deployments."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Iterable

from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy.engine import Engine

from trpc_service.tenant._persistence import mysql_sync_url

_MIGRATION_NAME = re.compile(r"^(?P<version>\d+)_(?P<name>[A-Za-z0-9_.-]+)\.sql$")

ledger_metadata = MetaData()
schema_migration_table = Table(
    "schema_migration",
    ledger_metadata,
    Column("version", BigInteger, primary_key=True),
    Column("name", String(255), nullable=False),
    Column("applied_at", DateTime(timezone=True), nullable=False),
)


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path


class SchemaMigrator:
    """Apply ordered SQL scripts once and record each committed version."""

    def __init__(self, db_url: str, migrations: Iterable[Migration]) -> None:
        self._engine: Engine = create_engine(mysql_sync_url(db_url), pool_pre_ping=True)
        self._migrations = sorted(migrations, key=lambda item: item.version)
        versions = [item.version for item in self._migrations]
        if len(versions) != len(set(versions)):
            raise ValueError("migration versions must be unique")

    @classmethod
    def discover(cls, db_url: str, *, schema_file: Path, migrations_dir: Path) -> "SchemaMigrator":
        migrations = [Migration(1, "baseline", schema_file)]
        for path in migrations_dir.glob("*.sql"):
            match = _MIGRATION_NAME.fullmatch(path.name)
            if match is None:
                continue
            migrations.append(Migration(int(match.group("version")), match.group("name"), path))
        return cls(db_url, migrations)

    def pending(self) -> list[Migration]:
        ledger_metadata.create_all(self._engine)
        with self._engine.connect() as connection:
            applied = set(connection.execute(select(schema_migration_table.c.version)).scalars())
        return [item for item in self._migrations if item.version not in applied]

    def validate(self) -> list[Migration]:
        """Validate file readability and statement splitting without a database."""
        for migration in self._migrations:
            if not self._statements(migration.path.read_text(encoding="utf-8")):
                raise ValueError(f"migration {migration.version} contains no SQL statements")
        return list(self._migrations)

    def migrate(self, *, dry_run: bool = False) -> list[Migration]:
        pending = self.pending()
        if dry_run:
            return pending
        for migration in pending:
            statements = self._statements(migration.path.read_text(encoding="utf-8"))
            with self._engine.begin() as connection:
                for statement in statements:
                    connection.exec_driver_sql(statement)
                connection.execute(
                    insert(schema_migration_table).values(
                        version=migration.version,
                        name=migration.name,
                        applied_at=datetime.now(timezone.utc),
                    ))
        return pending

    @staticmethod
    def _statements(script: str) -> list[str]:
        statements = []
        for raw in script.split(";"):
            lines = [line for line in raw.splitlines() if not line.lstrip().startswith("--")]
            statement = "\n".join(lines).strip()
            if statement:
                statements.append(statement)
        return statements

    def close(self) -> None:
        self._engine.dispose()
