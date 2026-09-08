"""CLI for applying or inspecting schema migrations."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from trpc_service.config import SecretResolver
from trpc_service.config import ServiceSettings
from trpc_service.config import resolve_secret
from ._runner import SchemaMigrator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply tRPC-Agent service database migrations")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--schema-file", type=Path, default=Path("data/schema.mysql.sql"))
    parser.add_argument("--migrations-dir", type=Path, default=Path("data/migrations"))
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    settings = ServiceSettings.from_env()
    resolver = SecretResolver(file_root=settings.secret_file_root)
    mysql_url = resolve_secret(settings.mysql_url, resolver=resolver)
    if not mysql_url:
        raise ValueError("migrations require TRPC_SERVICE_MYSQL_URL")
    migrator = SchemaMigrator.discover(
        mysql_url,
        schema_file=args.schema_file,
        migrations_dir=args.migrations_dir,
    )
    try:
        selected = migrator.validate() if args.check else migrator.migrate(dry_run=args.dry_run)
        for migration in selected:
            print(f"{migration.version:04d} {migration.name}")
    finally:
        migrator.close()


if __name__ == "__main__":
    main()
