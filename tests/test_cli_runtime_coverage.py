"""Protocol-level coverage for CLI worker roles and cleanup paths."""

import sys
from types import SimpleNamespace


class Closeable:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def runtime_pair():
    import trpc_service._cli as cli

    gateway = SimpleNamespace(
        storage=Closeable(),
        telemetry=Closeable(),
        storage_manager=None,
    )
    repository = SimpleNamespace(closed=False)

    def close_repository():
        repository.closed = True

    repository.close = close_repository
    admin = SimpleNamespace(
        tenants=SimpleNamespace(repository=repository),
    )
    return cli, gateway, admin, repository


def test_run_outbound_delivers_parts_and_records_dead_letter(monkeypatch):
    cli, gateway, admin, repository = runtime_pair()
    config = SimpleNamespace(
        tenant_id="tenant-a",
        channel_binding=lambda channel, account: SimpleNamespace(channel=channel, account_id=account),
    )
    admin.tenants.get_tenant = lambda tenant_id: config
    storage = SimpleNamespace(
        idempotency=SimpleNamespace(
            complete=lambda *args: setattr(storage.idempotency, "completed", args),
            release_delivery=lambda *args: setattr(storage.idempotency, "released", args),
        ),
        audit=SimpleNamespace(append=lambda record: setattr(storage.audit, "last", record)),
    )
    manager = Closeable()
    manager.get = lambda _: storage

    class Queue(Closeable):
        def __init__(self, url):
            super().__init__()
            self.url = url

        def update(self, item):
            self.updated = item

        def consume(self, deliver, on_dead_letter):
            deliver(
                {
                    "tenant_id": "tenant-a",
                    "channel": "web",
                    "account_id": "account",
                    "external_user_id": "user",
                    "session_id": "session",
                    "idempotency_key": "idempotency",
                    "response_ref": "response",
                    "trace_id": "trace",
                    "answer": "answer",
                    "messages": [
                        {
                            "channel": "web",
                            "account_id": "account",
                            "session_id": "session",
                            "external_user_id": "user",
                            "text": "part",
                            "attachments": [],
                            "metadata": {"message_type": "text"},
                        }
                    ],
                }
            )
            on_dead_letter(
                {
                    "tenant_id": "tenant-a",
                    "channel": "web",
                    "idempotency_key": "dead-idempotency",
                    "trace_id": "dead-trace",
                },
                "provider failed",
            )

    class Adapter:
        def send(self, *, message, binding):
            return SimpleNamespace(ok=True, metadata={"external_id": "sent"})

    class Limiter:
        def __init__(self, url):
            self.url = url

        def acquire(self, key, qps):
            return None

    monkeypatch.setattr(cli, "_create_runtime", lambda: (gateway, admin))
    monkeypatch.setattr(cli, "OutboundDeliveryQueue", Queue)
    monkeypatch.setattr(cli, "TenantStorageManager", lambda: manager)
    monkeypatch.setattr(cli, "default_channel_adapters", lambda: {"web": Adapter()})
    monkeypatch.setattr(cli, "ChannelRateLimiter", Limiter)
    monkeypatch.setattr(cli, "send_with_retry", lambda sender: sender())

    cli.run_outbound()
    assert storage.idempotency.completed[1] == "idempotency"
    assert storage.idempotency.released[1] == "dead-idempotency"
    assert storage.audit.last.decision == "delivery_failed"
    assert manager.closed and gateway.storage.closed and gateway.telemetry.closed
    assert repository.closed


def test_run_durable_outbox_publishes_records_and_runs_once(monkeypatch, capsys):
    cli, gateway, admin, repository = runtime_pair()
    config = SimpleNamespace(tenant_id="tenant-a")
    admin.tenants.repository.all_active = lambda: [config]
    storage = SimpleNamespace(inbox_outbox=object())
    manager = Closeable()
    manager.get = lambda _: storage
    captured = {}

    class Dispatcher:
        def __init__(self, store, publish, **kwargs):
            captured["publish"] = publish

        def run_once(self, limit):
            captured["publish"](
                SimpleNamespace(
                    event_id="event-1",
                    tenant_id="tenant-a",
                    topic="session.ready.v2",
                    aggregate_id="session",
                    payload={"x": 1},
                )
            )
            return 2

    monkeypatch.delenv("DURABLE_OUTBOX_SINK_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("OUTBOUND_QUEUE_URL", raising=False)
    monkeypatch.setattr(cli, "_create_runtime", lambda: (gateway, admin))
    monkeypatch.setattr(cli, "TenantStorageManager", lambda: manager)
    monkeypatch.setattr(cli, "DurableOutboxDispatcher", Dispatcher)

    cli.run_durable_outbox(once=True, limit=3)
    output = capsys.readouterr().out
    assert '"event_id": "event-1"' in output
    assert '"processed": {"tenant-a": 2}' in output
    assert manager.closed and gateway.storage.closed and gateway.telemetry.closed
    assert repository.closed


def test_main_dispatches_all_roles_and_worker_cleanup(monkeypatch):
    import trpc_service._cli as cli

    called = []
    monkeypatch.setattr(cli, "run_demo", lambda: called.append("demo"))
    monkeypatch.setattr(cli, "run_compensate", lambda *args: called.append("compensate"))
    monkeypatch.setattr(cli, "run_outbound", lambda: called.append("outbound"))
    monkeypatch.setattr(cli, "run_durable_outbox", lambda *args: called.append("durable-outbox"))
    monkeypatch.setattr(cli, "run_mailbox_maintenance", lambda *args: called.append("mailbox-maintenance"))
    monkeypatch.setattr(cli, "run_replay", lambda *args: called.append("replay"))

    for argv, expected in (
        (["trpc-agent", "demo"], "demo"),
        (["trpc-agent", "compensate", "--once"], "compensate"),
        (["trpc-agent", "outbound"], "outbound"),
        (["trpc-agent", "durable-outbox", "--once"], "durable-outbox"),
        (["trpc-agent", "mailbox-maintenance", "--once"], "mailbox-maintenance"),
        (["trpc-agent", "replay", "--tenant-id", "t", "--record-id", "r", "--operator", "o"], "replay"),
    ):
        monkeypatch.setattr(sys, "argv", argv)
        cli.main()
        assert called[-1] == expected

    cli, gateway, admin, repository = runtime_pair()
    class Queue(Closeable):
        def __init__(self, url):
            super().__init__()

        def consume(self, handler):
            self.handler = handler

    manager = Closeable()
    manager.get = lambda config: object()
    monkeypatch.setattr(cli, "_create_runtime", lambda: (gateway, admin))
    monkeypatch.setattr(cli, "WorkerQueue", Queue)
    monkeypatch.setattr(cli, "TenantStorageManager", lambda: manager)
    monkeypatch.setattr(cli, "build_runtime_worker", lambda *args: SimpleNamespace(run=lambda *args: []))
    monkeypatch.setattr(sys, "argv", ["trpc-agent", "worker"])
    cli.main()
    assert manager.closed and gateway.storage.closed and gateway.telemetry.closed
    assert repository.closed
