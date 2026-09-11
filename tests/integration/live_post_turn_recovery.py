"""Real HTTP administration and provider-backed recovery of an isolated live run."""

import argparse
import asyncio
import json
import os
from pathlib import Path
import secrets
import socket

from fastapi import FastAPI
import httpx
from sqlalchemy import select
from sqlalchemy.engine import make_url
import uvicorn

from trpc_service.im_setup import load_bundle
from trpc_service.management import management_router
from trpc_service.persistence import Database
from trpc_service.persistence.models import ModelAttemptRow, PostTurnTaskRow
from trpc_service.channels.protected_acceptance import snapshot
from trpc_service.reliability.post_turn import DurablePostTurnWorker
from trpc_service.service_runtime import ServiceRuntime
from trpc_service.telemetry.runtime import configure, shutdown
from trpc_service.telemetry.sdk_logging import configure_sdk_logging


async def recover(root):
    stores = json.loads(root.joinpath("stores.json").read_text(encoding="utf-8"))
    run_id = stores["run_id"]
    if len(run_id) != 12 or any(character not in "0123456789abcdef" for character in run_id):
        raise ValueError("invalid isolated acceptance run")
    for setting, suffix in (("TRPC_DATABASE_URL", "platform"), ("TRPC_SESSION_DATABASE_URL", "runtime")):
        expected = "acceptance_" + run_id + "_" + suffix
        if stores[suffix + "_database"] != expected:
            raise ValueError("recovery only supports the original isolated acceptance databases")
        os.environ[setting] = make_url(os.environ[setting]).set(database=expected).render_as_string(hide_password=False)
    os.environ["TRPC_REDIS_URL"] = make_url(
        os.environ["TRPC_REDIS_URL"]).set(database=str(stores["redis_database"])).render_as_string(hide_password=False)
    load_bundle(".secrets/im.json")
    load_bundle(".secrets/feishu.json")
    token = secrets.token_urlsafe(48)
    os.environ.update(TRPC_ADMIN_TOKEN=token,
                      TRPC_ADMIN_ACTOR="live_recovery_operator",
                      TRPC_RUNTIME_MODE="protected",
                      TRPC_CONFIG_SOURCE="database",
                      TRPC_TRACE_DIRECTORY=str(root.joinpath("recovery-traces").resolve()))
    configure_sdk_logging()
    database = Database(os.environ["TRPC_DATABASE_URL"])
    configure("live-recovery", database)
    runtime = ServiceRuntime(database)
    app = FastAPI()
    app.include_router(management_router(database))
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(app, access_log=False, log_level="critical", lifespan="off"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    report = {
        "scope": "real_http_provider_reconciliation_and_post_turn_recovery",
        "passed": False,
        "full_im_acceptance_passed": False,
        "operations": []
    }
    try:
        while not server.started:
            if serving.done():
                await serving
                raise RuntimeError("management server stopped before startup")
            await asyncio.sleep(0.05)
        with database.sessions() as session:
            attempts = [(row.tenant_id, row.attempt_id) for row in session.scalars(
                select(ModelAttemptRow).where(ModelAttemptRow.status == "pending_reconciliation"))]
            tasks = [(row.tenant_id, row.task_id, row.attempt) for row in session.scalars(
                select(PostTurnTaskRow).where(PostTurnTaskRow.task_type == "summary", PostTurnTaskRow.status ==
                                              "failed"))]
        async with httpx.AsyncClient(base_url="http://127.0.0.1:" + str(listener.getsockname()[1]),
                                     headers={"Authorization": "Bearer " + token},
                                     timeout=60,
                                     trust_env=False) as client:
            for tenant, attempt in attempts:
                response = await client.post(f"/admin/tenants/{tenant}/model-attempts/{attempt}/reconcile")
                report["operations"].append({
                    "tenant": tenant,
                    "operation": "reconcile",
                    "http_status": response.status_code
                })
                if response.status_code != 200:
                    raise RuntimeError("provider reconciliation failed")
            for tenant, task, attempt in tasks:
                response = await client.post(f"/admin/tenants/{tenant}/post-turn/{task}/retry",
                                             json={"expected_attempt": attempt})
                report["operations"].append({
                    "tenant": tenant,
                    "operation": "retry_summary",
                    "http_status": response.status_code
                })
                if response.status_code != 200:
                    raise RuntimeError("summary requeue failed")
        await runtime.start()
        worker = DurablePostTurnWorker(runtime.post,
                                       runtime.process_post_turn,
                                       worker_id="live-recovery-" + run_id,
                                       max_attempts=1)
        for _ in tasks:
            if not await worker.run_once():
                raise RuntimeError("requeued summary was not claimed")
        report["tenants"] = snapshot(database)
        report["passed"] = all(
            value["post_turn"].get("summary") == "succeeded" and all(attempt["status"] == "settled"
                                                                     for attempt in value["model_attempts"])
            for value in report["tenants"].values())
    except Exception as error:
        report["error_type"] = type(error).__name__
    finally:
        server.should_exit = True
        await serving
        listener.close()
        await runtime.close()
        shutdown()
        database.close()
        target = root / "post-turn-recovery.json"
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("report=" + str(target.resolve()), flush=True)
        print("recovery_passed=" + str(report["passed"]), flush=True)
    return report["passed"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence_directory", type=Path)
    args = parser.parse_args()
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    raise SystemExit(0 if asyncio.run(recover(args.evidence_directory)) else 1)
