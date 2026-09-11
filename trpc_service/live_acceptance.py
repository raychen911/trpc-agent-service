"""Real external IM/model acceptance; each run preserves its own database evidence."""

import asyncio
import json
import os
from pathlib import Path
import traceback
import uuid

import psycopg
from psycopg import sql
from redis import Redis
from sqlalchemy.engine import make_url


def allocate():
    """Create new test stores; never clear or rewrite a previous acceptance run."""
    source = make_url(os.environ["TRPC_DATABASE_URL"])
    if source.get_backend_name() != "postgresql":
        raise ValueError("real acceptance requires PostgreSQL")
    run_id = uuid.uuid4().hex[:12]
    root = Path("reports") / ("live-" + run_id)
    root.mkdir(parents=True)
    with psycopg.connect(source.set(drivername="postgresql").render_as_string(hide_password=False),
                         autocommit=True,
                         connect_timeout=5) as connection:
        for suffix, setting in (("platform", "TRPC_DATABASE_URL"), ("runtime", "TRPC_SESSION_DATABASE_URL")):
            name = "acceptance_" + run_id + "_" + suffix
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            os.environ[setting] = source.set(database=name).render_as_string(hide_password=False)
    redis_url = make_url(os.environ["TRPC_REDIS_URL"])
    for index in range(1, 16):
        url = redis_url.set(database=str(index)).render_as_string(hide_password=False)
        client = Redis.from_url(url)
        try:
            if client.dbsize() == 0 and client.set("acceptance_owner", run_id, nx=True):
                os.environ["TRPC_REDIS_URL"] = url
                break
        finally:
            client.close()
    else:
        raise ValueError("no empty isolated Redis test database; use dedicated fresh test infrastructure")
    os.environ.update(TRPC_RUNTIME_MODE="protected",
                      TRPC_CONFIG_SOURCE="database",
                      TRPC_TRACE_DIRECTORY=str((root / "traces").resolve()),
                      TRPC_ACCEPTANCE_INDEX_VERSION=str(int(run_id[:7], 16) + 1))
    root.joinpath("stores.json").write_text(json.dumps({
        "run_id": run_id,
        "platform_database": "acceptance_" + run_id + "_platform",
        "runtime_database": "acceptance_" + run_id + "_runtime",
        "redis_database": index
    }),
                                            encoding="utf-8")
    return root


def _run(timeout=600):
    from trpc_service.im_setup import load_bundle
    from trpc_service.telemetry.sdk_logging import configure_sdk_logging
    from trpc_service.telemetry.runtime import configure, shutdown
    from trpc_service.persistence import Database
    from trpc_service.protected_demo import prepare
    from trpc_service._cli import init_resources
    from trpc_service.channels.protected_acceptance import run
    load_bundle(os.environ.get("TRPC_IM_SECRETS_FILE", ".secrets/im.json"))
    load_bundle(os.environ.get("TRPC_FEISHU_SECRETS_FILE", ".secrets/feishu.json"))
    configure_sdk_logging()
    root = allocate()
    database = Database(os.environ["TRPC_DATABASE_URL"])
    try:
        database.create_schema()
        init_resources()
        prepare(database, str(root / "config"))
        configure("live-channel", database)
        print("evidence_directory=" + str(root.resolve()), flush=True)
        return asyncio.run(run(database, timeout))
    finally:
        shutdown()
        database.close()


def main(timeout=600):
    try:
        return _run(timeout)
    except Exception as error:
        target = Path("reports") / ("live-preflight-failed-" + uuid.uuid4().hex + ".json")
        target.parent.mkdir(parents=True, exist_ok=True)
        root = Path(__file__).resolve().parent
        locations = []
        for frame in traceback.extract_tb(error.__traceback__):
            path = Path(frame.filename).resolve()
            if path.is_relative_to(root):
                locations.append({
                    "file": path.relative_to(root).as_posix(),
                    "line": frame.lineno,
                    "function": frame.name
                })
        target.write_text(json.dumps({
            "scope": "real_im_model_and_backends",
            "passed": False,
            "error_type": type(error).__name__,
            "locations": locations,
            "note": "Real acceptance did not complete; no simulation fallback."
        }),
                          encoding="utf-8")
        print("report=" + str(target.resolve()), flush=True)
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
