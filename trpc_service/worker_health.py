"""A per-container heartbeat distinguishes event-loop liveness from SQL readiness."""

import asyncio
import json
import os
import tempfile
import time
from pathlib import Path


def health_path():
    return Path(os.environ.get("TRPC_HEALTH_FILE", str(Path(tempfile.gettempdir()) / "trpc-worker-health.json")))


def write_health(ready):
    target = health_path()
    pending = target.with_suffix(".tmp")
    pending.write_text(json.dumps({"pid": os.getpid(), "updated": time.time(), "ready": ready}), encoding="utf-8")
    pending.replace(target)


def check_health():
    value = json.loads(health_path().read_text(encoding="utf-8"))
    if time.time() - value["updated"] > 15 or not value["ready"]:
        raise RuntimeError("worker is not ready")


async def heartbeat(database, stop):
    from sqlalchemy import text
    from trpc_service.worker import wait_or_stop

    def ping():
        with database.sessions() as session:
            session.execute(text("SELECT 1"))

    try:
        while not stop.is_set():
            ready = True
            try:
                await asyncio.wait_for(asyncio.to_thread(ping), timeout=5)
            except Exception:
                ready = False
            write_health(ready)
            await wait_or_stop(stop, 2)
    finally:
        write_health(False)
