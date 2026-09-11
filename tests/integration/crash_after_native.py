"""A disposable process barrier after a real SDK final write, before projection."""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

from trpc_service.channels import NormalizedInboundMessage
from trpc_service.persistence import Database
from trpc_service.service_runtime import ServiceRuntime


async def main():
    database = Database(os.environ["TRPC_DATABASE_URL"])
    runtime = ServiceRuntime(database)
    await runtime.start()
    run_id = sys.argv[1]
    message = NormalizedInboundMessage(
        channel="wecom", webhook_public_id="callback_acme", external_message_id=f"crash-{run_id}",
        external_user_id=f"crash-{run_id}", conversation_type="direct", text=f"native-survives-{run_id}",
        request_id=f"crash-{run_id}", received_at=datetime.now(timezone.utc))
    route = runtime.router.route_message(message)
    receipt = runtime.pipeline.ingest(message, route, trace_id=run_id)
    service = runtime.sessions[(route.tenant_id, route.agent_app_id, route.config_version)]
    append = service.append_event

    async def append_and_pause(session, event):
        result = await append(session, event)
        if event.author != "user" and event.is_final_response():
            print("CRASH_READY=" + json.dumps({"inbound_message_id": receipt.inbound_message_id}), flush=True)
            await asyncio.Event().wait()
        return result

    service.append_event = append_and_pause
    work = runtime.inbox.claim(worker_id=f"crash-{run_id}", lease_seconds=3)
    assert work.inbound_message_id == receipt.inbound_message_id
    await runtime.pipeline.execute(work, runtime.registry, worker_id=f"crash-{run_id}", lease_seconds=3)


if __name__ == "__main__":
    asyncio.run(main())
