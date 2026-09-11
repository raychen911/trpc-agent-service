"""Independent SDK writer for deterministic stale-owner contract tests."""

import asyncio


def writer(connection, kind, url, identity):
    # Windows spawn creates an independent interpreter and native connection pool.
    from trpc_agent_sdk.sessions import RedisSessionService, SqlSessionService
    from trpc_agent_sdk.events import Event
    from trpc_agent_sdk.types import Content, Part
    from trpc_service.sdk_provenance import verify_official_sdk

    async def run():
        verify_official_sdk()
        service = (SqlSessionService(db_url=url, is_async=False, expire_on_commit=False)
                   if kind == "sql" else RedisSessionService(db_url=url))
        try:
            snapshot = await service.get_session(**identity)
            connection.send(("ready", snapshot is not None))
            command, token = connection.recv()
            event = Event(id="stale-owner-event",
                          author="test_agent",
                          invocation_id="old-execution",
                          content=Content(role="model", parts=[Part.from_text(text="stale owner")]),
                          custom_metadata={"platform_fencing_token": token})
            try:
                if command == "append":
                    await service.append_event(snapshot, event)
                else:
                    snapshot.events.append(event)
                    await service.update_session(snapshot)
            except Exception as error:
                connection.send(("rejected", type(error).__name__))
            else:
                connection.send(("accepted", None))
        finally:
            await service.close()

    try:
        asyncio.run(run())
    except Exception as error:
        connection.send(("actor_error", type(error).__name__))
    finally:
        connection.close()
