"""Windows-safe entry point reusing the prepared real-IM knowledge stores."""

import asyncio
import json
import os
from pathlib import Path
import sys

from sqlalchemy.engine import make_url


def main():
    root = Path(sys.argv[1]).resolve()
    challenge = sys.argv[2]
    stores = json.loads((root / "stores.json").read_text(encoding="utf-8"))
    for line in Path("deploy/protected-test.env.example").read_text().splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            os.environ[key] = value
    from trpc_service.im_setup import load_bundle
    load_bundle(".secrets/im.json")
    load_bundle(".secrets/feishu.json")
    for setting, key in (("TRPC_DATABASE_URL", "platform_database"),
                         ("TRPC_SESSION_DATABASE_URL", "runtime_database")):
        os.environ[setting] = make_url(os.environ[setting]).set(
            database=stores[key]).render_as_string(hide_password=False)
    os.environ["TRPC_REDIS_URL"] = make_url(os.environ["TRPC_REDIS_URL"]).set(
        database=str(stores["redis_database"])).render_as_string(hide_password=False)
    os.environ["TRPC_TRACE_DIRECTORY"] = str(root / "traces")
    os.environ["TRPC_ACCEPTANCE_CHALLENGE_ID"] = challenge
    from trpc_service.persistence import Database
    from trpc_service.telemetry.runtime import configure, shutdown
    from trpc_service.channels.protected_acceptance import run
    database = Database(os.environ["TRPC_DATABASE_URL"])
    configure("live-knowledge-im", database)
    try:
        return asyncio.run(run(database, 3600))
    finally:
        shutdown()
        database.close()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
