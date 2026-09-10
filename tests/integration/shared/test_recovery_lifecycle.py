import asyncio

from trpc_service.storage.contracts import AuditUnavailable
from trpc_service.web.app import SharedRuntime


async def test_recovery_loop_survives_transient_outage_and_is_stopped_cleanly() -> None:
    class Recovery:
        calls = 0

        async def run_once(self, _tenant: str) -> int:
            self.calls += 1
            if self.calls == 1:
                raise AuditUnavailable()
            return 0

    class Closeable:
        closed = False

        async def close(self) -> None:
            self.closed = True

    recovery, adapters, worker = Recovery(), Closeable(), Closeable()
    runtime = SharedRuntime(
        adapters=adapters, metrics=None, secrets=None, worker=worker,
        gateway=None, now=lambda: None, recovery=recovery,
    )
    await runtime.start_recovery()
    await asyncio.sleep(0.55)
    assert recovery.calls >= 3
    assert runtime.recovery_task is not None and not runtime.recovery_task.done()
    await runtime.close()
    assert adapters.closed and worker.closed and runtime.recovery_task.done()
