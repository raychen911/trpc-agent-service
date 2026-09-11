"""Child process used to test a real crash between claim and execution."""

import json
import os
import time

from trpc_service.persistence import Database
from trpc_service.reliability import InboxRepository


if __name__ == "__main__":
    database = Database(os.environ["OPS_WORKER_DATABASE_URL"])
    work = InboxRepository(database).claim(worker_id="crashing-worker", lease_seconds=0.3)
    assert work is not None
    print("OPS_CLAIM=" + json.dumps({"execution_id": work.execution_id, "inbound_id": work.inbound_message_id}),
          flush=True)
    while True:
        time.sleep(1)
