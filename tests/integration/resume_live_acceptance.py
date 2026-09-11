"""Resume this isolated interrupted acceptance without resetting durable evidence."""

import asyncio
import json
import os
from pathlib import Path
import sys

from sqlalchemy.engine import make_url
from trpc_service.im_setup import load_bundle
from trpc_service.persistence import Database
from trpc_service.telemetry.logging import configure_logging
from trpc_service.telemetry.runtime import configure, shutdown
from trpc_service.channels.protected_acceptance import run


def main():
    report = Path(sys.argv[1]).resolve()
    previous = json.loads(report.read_text(encoding="utf-8"))
    root = Path(previous["trace_directory"]).parent
    stores = json.loads((root / "stores.json").read_text(encoding="utf-8"))
    for line in Path("deploy/protected-test.env.example").read_text().splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            os.environ[key] = value
    load_bundle(".secrets/im.json")
    load_bundle(".secrets/feishu.json")
    for setting, key in (("TRPC_DATABASE_URL", "platform_database"),
                         ("TRPC_SESSION_DATABASE_URL", "runtime_database")):
        os.environ[setting] = make_url(os.environ[setting]).set(
            database=stores[key]).render_as_string(hide_password=False)
    os.environ["TRPC_REDIS_URL"] = make_url(os.environ["TRPC_REDIS_URL"]).set(
        database=str(stores["redis_database"])).render_as_string(hide_password=False)
    os.environ["TRPC_TRACE_DIRECTORY"] = previous["trace_directory"]
    configure_logging("live-recovery")
    database = Database(os.environ["TRPC_DATABASE_URL"])
    configure("live-recovery", database)
    try:
        return asyncio.run(run(database, 600, resume_report=str(report)))
    finally:
        shutdown()
        database.close()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
