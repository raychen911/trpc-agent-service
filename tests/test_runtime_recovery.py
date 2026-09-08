import asyncio

from trpc_service.storage.runtime import run_inbound_loop, run_outbox_loop


class FlakyWorker:
    def __init__(self, stop: asyncio.Event) -> None:
        self.stop = stop
        self.calls = 0

    async def poll_once(self) -> int:
        self.calls += 1
        if self.calls == 1:
            raise ConnectionError("temporary backend outage")
        self.stop.set()
        return 0


def test_inbound_loop_recovers_after_temporary_database_failure() -> None:
    async def scenario() -> None:
        stop = asyncio.Event()
        worker = FlakyWorker(stop)
        await asyncio.wait_for(run_inbound_loop(worker, stop, 0.001), timeout=1)
        assert worker.calls == 2

    asyncio.run(scenario())


def test_outbox_loop_recovers_after_temporary_database_failure() -> None:
    async def scenario() -> None:
        stop = asyncio.Event()
        worker = FlakyWorker(stop)
        await asyncio.wait_for(run_outbox_loop(worker, stop, 0.001), timeout=1)
        assert worker.calls == 2

    asyncio.run(scenario())
