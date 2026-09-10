from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict

from trpc_service.agent.bridge import build_runtime_worker
from trpc_service.agent.model_client import ModelClientError
from trpc_service.channels import InboundMessage, default_channel_adapters
from trpc_service.channels.base import Attachment, OutboundMessage
from trpc_service.channels.outbound import build_outbound_messages, split_outbound_messages
from trpc_service.channels.outbound_queue import OutboundDeliveryQueue
from trpc_service.channels.reliable import ChannelRateLimiter, send_with_retry
from trpc_service.gateway.redis_streams import RedisStreamsTransport
from trpc_service.gateway.session_ready_consumer import SessionReadyConsumer, publish_session_ready
from trpc_service.gateway.worker_queue import WorkerQueue
from trpc_service.storage.base import AuditRecord
from trpc_service.storage.compensation import replay_compensations
from trpc_service.storage.durable import DurableOutboxDispatcher
from trpc_service.storage.manager import TenantStorageManager
from trpc_service.tenant.models import AgentEvent, RunRequest, TenantContext, UserInput


def _create_runtime():
    # CLI commands create their own runtime. Avoid the module-level FastAPI
    # app initialization in web.app, which would otherwise open a second set
    # of tenant/storage/queue resources in every worker process.
    os.environ.setdefault("TRPC_AGENT_NO_AUTO_APP", "1")
    from trpc_service.web.app import create_runtime

    return create_runtime()


def run_demo() -> None:
    gateway, _ = _create_runtime()
    message = InboundMessage(
        channel="telegram",
        account_id="corp_account_1",
        external_message_id="cli-demo-1",
        external_user_id="cli-user",
        text="你好，介绍一下当前服务",
    )
    try:
        session_id, events, response_ref = gateway.dispatch(message)
    except ModelClientError as exc:
        print(f"model request failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    try:
        print(
            json.dumps(
                {
                    "session_id": session_id,
                    "response": events[-1].content if events else "",
                    "response_ref": response_ref,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    finally:
        gateway.storage_manager.close() if gateway.storage_manager else None
        gateway.storage.close()
        gateway.telemetry.close()


def run_compensate(once: bool = False, limit: int = 100, interval: float = 5.0) -> None:
    gateway, admin = _create_runtime()
    manager = TenantStorageManager()
    try:
        while True:
            counts: dict[str, int] = {}
            for config in getattr(admin.tenants.repository, "all_active", list)():
                storage = manager.get(config)
                counts[config.tenant_id] = replay_compensations(
                    storage,
                    limit=limit,
                    tenant_id=config.tenant_id,
                )
            print(json.dumps({"processed": counts}, ensure_ascii=False), flush=True)
            if once:
                return
            time.sleep(interval)
    finally:
        manager.close()
        gateway.storage_manager.close() if gateway.storage_manager else None
        gateway.storage.close()
        gateway.telemetry.close()
        close = getattr(admin.tenants.repository, "close", None)
        if close:
            close()


def run_outbound() -> None:
    """Consume the shared outbound delivery queue in a stateless process."""

    gateway, admin = _create_runtime()
    queue = OutboundDeliveryQueue(os.getenv("OUTBOUND_QUEUE_URL"))
    adapters = default_channel_adapters()
    limiter = ChannelRateLimiter(os.getenv("REDIS_URL"))
    manager = TenantStorageManager()

    def deliver(item: dict) -> None:
        config = admin.tenants.get_tenant(item["tenant_id"])
        binding = config.channel_binding(item["channel"], item["account_id"])
        storage = manager.get(config)
        messages = [
            OutboundMessage(
                channel=raw["channel"],
                account_id=raw["account_id"],
                session_id=raw["session_id"],
                external_user_id=raw["external_user_id"],
                text=raw["text"],
                group_id=raw.get("group_id"),
                attachments=[Attachment(**attachment) for attachment in raw.get("attachments", [])],
                metadata=dict(raw.get("metadata", {})),
            )
            for raw in item.get("messages", [])
        ]
        completed = {int(index) for index in item.get("completed_parts", [])}
        results = list(item.get("results", []))
        for index, message in enumerate(messages):
            if index in completed:
                continue
            limiter.acquire(
                f"{message.channel}:{message.account_id}",
                int(item.get("send_qps_limit", 20)),
            )
            result = send_with_retry(
                lambda message=message: adapters[message.channel].send(message=message, binding=binding)
            )
            if not result.ok:
                raise RuntimeError(result.error or "channel delivery failed")
            completed.add(index)
            results.append(result.metadata)
            item["completed_parts"] = sorted(completed)
            item["results"] = results
            queue.update(item)
        reply = {
            "parts": len(messages),
            "message_types": [message.metadata.get("message_type", "text") for message in messages],
            "results": results,
        }
        storage.idempotency.complete(
            item["tenant_id"],
            item["idempotency_key"],
            item["response_ref"],
            {"text": item.get("answer", ""), "reply": reply, "delivered": True},
        )
        storage.audit.append(
            AuditRecord(
                audit_id=str(__import__("uuid").uuid4()),
                tenant_id=item["tenant_id"],
                channel=item["channel"],
                user_id=item.get("external_user_id"),
                session_id=item.get("session_id"),
                agent_name=item.get("agent_name"),
                decision="delivered",
                latency_ms=int((time.time() - float(item.get("queued_at", time.time()))) * 1000),
                trace_id=item["trace_id"],
                metadata={"parts": len(messages), "queued": True},
            )
        )

    def dead_letter(item: dict, error: str) -> None:
        config = admin.tenants.get_tenant(item["tenant_id"])
        storage = manager.get(config)
        storage.idempotency.release_delivery(item["tenant_id"], item["idempotency_key"])
        storage.audit.append(
            AuditRecord(
                audit_id=str(__import__("uuid").uuid4()),
                tenant_id=item["tenant_id"],
                channel=item["channel"],
                user_id=item.get("external_user_id"),
                session_id=item.get("session_id"),
                agent_name=item.get("agent_name"),
                decision="delivery_failed",
                error_type="outbound_dead_letter",
                trace_id=item["trace_id"],
                metadata={"error": error, "queued": True},
            )
        )

    try:
        queue.consume(deliver, on_dead_letter=dead_letter)
    finally:
        manager.close()
        queue.close()
        gateway.storage_manager.close() if gateway.storage_manager else None
        gateway.storage.close()
        gateway.telemetry.close()
        close = getattr(admin.tenants.repository, "close", None)
        if close:
            close()


def run_durable_outbox(once: bool = False, limit: int = 100, interval: float = 5.0) -> None:
    """Publish PostgreSQL/SQLite outbox records to a reconstructible stream."""

    gateway, admin = _create_runtime()
    manager = TenantStorageManager()
    publisher = None
    outbound_queue = None
    sink_url = os.getenv("DURABLE_OUTBOX_SINK_URL", os.getenv("REDIS_URL", "")).strip()
    if sink_url:
        import redis

        publisher = redis.Redis.from_url(sink_url, decode_responses=True)
    if os.getenv("OUTBOUND_QUEUE_URL", "").strip():
        try:
            outbound_queue = OutboundDeliveryQueue(os.getenv("OUTBOUND_QUEUE_URL"))
        except Exception:
            outbound_queue = None
    owner = f"durable-outbox:{os.getpid()}"

    def publish(record) -> None:
        payload = dict(record.payload)
        if (
            record.topic == "agent.response"
            and outbound_queue is not None
            and payload.get("channel") != "web"
            and payload.get("idempotency_key")
        ):
            events = [AgentEvent(**item) for item in payload.get("events", [])]
            messages = build_outbound_messages(
                events,
                channel=payload["channel"],
                account_id=payload["account_id"],
                session_id=payload["aggregate_id"],
                external_user_id=payload["external_user_id"],
                group_id=payload.get("group_id"),
            )
            parts = split_outbound_messages(messages, 4096, idempotency_key=payload["idempotency_key"])
            outbound_queue.enqueue(
                {
                    "tenant_id": record.tenant_id,
                    "channel": payload["channel"],
                    "account_id": payload["account_id"],
                    "external_user_id": payload["external_user_id"],
                    "session_id": payload["aggregate_id"],
                    "group_id": payload.get("group_id"),
                    "agent_name": payload.get("agent_name"),
                    "messages": [asdict(message) for message in parts],
                    "answer": payload.get("text", ""),
                    "response_ref": payload["response_ref"],
                    "idempotency_key": payload["idempotency_key"],
                    "trace_id": payload.get("trace_id", record.event_id),
                    "queued_at": time.time(),
                    "send_qps_limit": 20,
                },
                task_id=f"{payload['idempotency_key']}:outbound",
            )
        encoded = json.dumps(
            {
                "event_id": record.event_id,
                "tenant_id": record.tenant_id,
                "topic": record.topic,
                "aggregate_id": record.aggregate_id,
                "payload": record.payload,
            },
            ensure_ascii=False,
            default=str,
        )
        if publisher is not None:
            if record.topic == "session.ready.v2":
                publish_session_ready(record, publisher)
            else:
                publisher.xadd(
                    os.getenv("DURABLE_OUTBOX_STREAM", "trpc-agent:outbox:events"),
                    {"payload": encoded},
                )
        else:
            print(encoded, flush=True)

    try:
        while True:
            processed: dict[str, int] = {}
            for config in getattr(admin.tenants.repository, "all_active", list)():
                storage = manager.get(config)
                store = getattr(storage, "inbox_outbox", None)
                if store is None:
                    continue
                dispatcher = DurableOutboxDispatcher(
                    store,
                    publish,
                    owner=owner,
                    tenant_id=config.tenant_id,
                )
                processed[config.tenant_id] = dispatcher.run_once(limit=limit)
            if once:
                print(json.dumps({"processed": processed}, ensure_ascii=False), flush=True)
                return
            time.sleep(interval)
    finally:
        if publisher is not None:
            close = getattr(publisher, "close", None)
            if close:
                close()
        if outbound_queue is not None:
            outbound_queue.close()
        manager.close()
        gateway.storage_manager.close() if gateway.storage_manager else None
        gateway.storage.close()
        gateway.telemetry.close()
        close = getattr(admin.tenants.repository, "close", None)
        if close:
            close()


def run_session_ready_worker() -> None:
    """Consume session.ready.v2 notices with durable mailbox claims."""

    gateway, admin = _create_runtime()
    manager = TenantStorageManager()
    redis_url = os.getenv("SESSION_READY_REDIS_URL", os.getenv("REDIS_URL", "")).strip()
    if not redis_url:
        raise RuntimeError("SESSION_READY_REDIS_URL or REDIS_URL is required")
    transport = RedisStreamsTransport(
        redis_url,
        prefix=os.getenv("SESSION_READY_STREAM_PREFIX", "trpc-agent"),
        stream="session-ready",
        stream_key=os.getenv("SESSION_READY_STREAM", "trpc-agent:session-ready:stream"),
        group=os.getenv("SESSION_READY_GROUP", "session-workers"),
        consumer=os.getenv("SESSION_READY_CONSUMER") or None,
        max_attempts=int(os.getenv("SESSION_READY_MAX_ATTEMPTS", "5")),
    )
    consumers_by_tenant: dict[str, SessionReadyConsumer] = {}
    owner = os.getenv("SESSION_READY_WORKER_ID", f"session-worker:{os.getpid()}")

    def make_executor(config, storage):
        worker = build_runtime_worker(storage, gateway.telemetry)

        def execute(claim):
            record = claim.inbox
            payload = dict(record.payload)
            channel = str(payload.get("channel", ""))
            account_id = str(payload.get("account_id", ""))
            binding = config.channel_binding(channel, account_id)
            context = TenantContext(
                tenant_id=config.tenant_id,
                agent_app_id=binding.agent_app_id,
                config_version=config.config_version,
                trace_id=str(payload.get("trace_id") or record.message_id),
                session_id=record.session_id,
                channel=channel,
                user_id=str(payload.get("internal_user_id") or payload.get("external_user_id") or "anonymous"),
            )
            request = RunRequest(
                tenant_context=context,
                user_input=UserInput(
                    text=str(payload.get("text") or ""),
                    metadata={
                        "external_message_id": payload.get("external_message_id"),
                        "group_id": payload.get("group_id"),
                        "attachments": payload.get("attachments", []),
                    },
                ),
                idempotency_key=record.dedupe_key,
            )
            events = worker.run(request, config, storage)
            text_value = next((event.content for event in reversed(events) if event.event_type == "message_end"), "")
            result = {
                "text": text_value,
                "response_ref": f"{channel}:{payload.get('external_message_id', record.message_id)}:response",
                "events": [asdict(event) for event in events],
                "trace_id": context.trace_id,
                "channel": channel,
                "account_id": account_id,
                "external_user_id": payload.get("external_user_id"),
                "idempotency_key": record.dedupe_key,
            }
            storage.idempotency.complete(config.tenant_id, record.dedupe_key, result["response_ref"], result)
            return result

        return execute

    try:
        for config in getattr(admin.tenants.repository, "all_active", list)():
            storage = manager.get(config)
            mailbox = getattr(storage, "session_mailbox_v2", None)
            inbox = getattr(storage, "inbox_outbox", None)
            if mailbox is None or inbox is None:
                continue
            consumers_by_tenant[config.tenant_id] = SessionReadyConsumer(
                    mailbox,
                    inbox,
                    transport,
                    make_executor(config, storage),
                    owner=owner,
                    lease_seconds=int(os.getenv("MAILBOX_LEASE_SECONDS", "180")),
            )
        # A single stream consumer is shared across tenants; dispatch is based
        # on the tenant_id embedded in each durable notice.
        from trpc_service.gateway.session_ready_consumer import SessionReadyMultiplexer
        SessionReadyMultiplexer(transport, consumers_by_tenant).consume_forever(
            block_ms=int(float(os.getenv("SESSION_READY_POLL_SECONDS", "1")) * 1000)
        )
    finally:
        manager.close()
        transport.close()
        gateway.storage.close()
        gateway.telemetry.close()
        close = getattr(admin.tenants.repository, "close", None)
        if close:
            close()


def run_mailbox_maintenance(
    once: bool = False,
    limit: int = 100,
    interval: float = 5.0,
) -> None:
    """Recover expired leases, release due retries, and rebuild ready notices."""

    gateway, admin = _create_runtime()
    manager = TenantStorageManager()
    try:
        while True:
            counts: dict[str, dict[str, int]] = {}
            for config in getattr(admin.tenants.repository, "all_active", list)():
                mailbox = getattr(manager.get(config), "session_mailbox_v2", None)
                if mailbox is None:
                    continue
                counts[config.tenant_id] = {
                    "expired_leases": mailbox.sweep_expired_leases(
                        limit=limit, tenant_id=config.tenant_id
                    ),
                    "due_retries": mailbox.schedule_retries(
                        limit=limit, tenant_id=config.tenant_id
                    ),
                    "ready_reconciled": mailbox.reconcile_sessions(
                        limit=limit, tenant_id=config.tenant_id
                    ),
                }
            print(json.dumps({"processed": counts}, ensure_ascii=False), flush=True)
            if once:
                return
            time.sleep(interval)
    finally:
        manager.close()
        gateway.storage_manager.close() if gateway.storage_manager else None
        gateway.storage.close()
        gateway.telemetry.close()
        close = getattr(admin.tenants.repository, "close", None)
        if close:
            close()


def run_replay(kind: str, tenant_id: str, record_id: str, operator: str) -> None:
    """Replay one dead durable record with a persistent operator audit entry."""

    if not tenant_id or not record_id or not operator:
        raise SystemExit("replay requires --tenant-id, --record-id, and --operator")
    gateway, admin = _create_runtime()
    manager = TenantStorageManager()
    try:
        config = admin.tenants.get_tenant(tenant_id)
        storage = manager.get(config)
        if kind == "outbox":
            store = getattr(storage, "inbox_outbox", None)
            if store is None:
                raise RuntimeError("storage backend does not support a durable outbox")
            replayed = store.replay_outbox(record_id, tenant_id)
        elif kind == "compensation":
            replayed = storage.compensation.replay(record_id, tenant_id=tenant_id)
        else:
            raise ValueError(f"unsupported replay kind: {kind}")
        storage.audit.append(
            AuditRecord(
                audit_id=str(__import__("uuid").uuid4()),
                tenant_id=tenant_id,
                decision="operator_replay",
                trace_id=f"operator-replay:{record_id}",
                metadata={
                    "kind": kind,
                    "record_id": record_id,
                    "operator": operator,
                },
            )
        )
        print(
            json.dumps(
                {"status": "requeued", "kind": kind, "record": asdict(replayed)},
                ensure_ascii=False,
                default=str,
            ),
            flush=True,
        )
    finally:
        manager.close()
        gateway.storage_manager.close() if gateway.storage_manager else None
        gateway.storage.close()
        gateway.telemetry.close()
        close = getattr(admin.tenants.repository, "close", None)
        if close:
            close()


def main() -> None:
    parser = argparse.ArgumentParser(description="tRPC-Agent multi-tenant service")
    parser.add_argument(
        "command",
        nargs="?",
        default="demo",
        choices=[
            "demo",
            "worker",
            "compensate",
            "outbound",
            "durable-outbox",
            "session-ready-worker",
            "mailbox-maintenance",
            "replay",
        ],
    )
    parser.add_argument("--once", action="store_true", help="run the selected maintenance role once and exit")
    parser.add_argument("--limit", type=int, default=100, help="max records per tenant per pass")
    parser.add_argument("--interval", type=float, default=5.0, help="seconds between compensation passes")
    parser.add_argument("--kind", choices=["outbox", "compensation"], default="outbox")
    parser.add_argument("--tenant-id", default="")
    parser.add_argument("--record-id", default="")
    parser.add_argument("--operator", default=os.getenv("TRPC_REPLAY_OPERATOR", ""))
    args = parser.parse_args()
    if args.command == "demo":
        run_demo()
    elif args.command == "compensate":
        run_compensate(args.once, args.limit, args.interval)
    elif args.command == "outbound":
        run_outbound()
    elif args.command == "durable-outbox":
        run_durable_outbox(args.once, args.limit, args.interval)
    elif args.command == "session-ready-worker":
        run_session_ready_worker()
    elif args.command == "mailbox-maintenance":
        run_mailbox_maintenance(args.once, args.limit, args.interval)
    elif args.command == "replay":
        run_replay(args.kind, args.tenant_id, args.record_id, args.operator)
    else:
        gateway, admin = _create_runtime()
        queue = WorkerQueue(os.getenv("WORKER_QUEUE_URL"))
        manager = TenantStorageManager()
        worker = build_runtime_worker(gateway.storage, gateway.telemetry)
        try:
            queue.consume(lambda request, config: worker.run(request, config, manager.get(config)))
        finally:
            manager.close()
            gateway.storage.close()
            gateway.telemetry.close()
            close = getattr(admin.tenants.repository, "close", None)
            if close:
                close()
            queue.close()


if __name__ == "__main__":
    main()
