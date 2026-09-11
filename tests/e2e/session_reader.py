"""Independent process used to prove migrated data is not coming from a client cache."""

import asyncio
import json
import os
import sys

from trpc_agent_sdk.sessions import SqlSessionService

from .seed import canonical_session


async def main():
    service = SqlSessionService(db_url=os.environ["E2E_READER_SQL_URL"])
    try:
        session = await service.get_session(**json.load(sys.stdin))
        print("E2E_SNAPSHOT=" + json.dumps(canonical_session(session), ensure_ascii=True))
    finally:
        await service.close()


if __name__ == "__main__":
    asyncio.run(main())
