"""Visible no-network demonstrations for customer service and turn recovery."""
import asyncio
import time

from trpc_service.agent import AgentWorker, TenantRuntimeManager
from trpc_service.channels.customer_service import CustomerServiceAdapter, FakeCustomerServiceClient
from trpc_service.channels.customer_store import InMemoryCustomerStore
from trpc_service.config import AgentAppConfig, ChannelBindingConfig, ServiceSettings, TenantConfig
from trpc_service.gateway import InMemoryIdempotencyStore
from trpc_service.gateway.dispatcher import AgentTaskProcessor
from trpc_service.gateway.models import NormalizedInboundMessage, RequestRecord
from trpc_service.gateway.requests import InMemoryRequestStore
from trpc_service.gateway.models import AgentRequest
from trpc_service.gateway.queue import InMemoryAgentTaskQueue, AgentTaskEnvelope
from trpc_service.gateway.repair import RequestRepairService
from trpc_service.gateway.service import GatewayService
from trpc_service.offline import OfflineRuntimeFactory
from trpc_service.storage import InMemorySessionExecutionGuard
from trpc_service.tenant import InMemoryTenantRegistry
from trpc_service.web import build_container


async def run_reliability_demo(name):
    if name == "session-lock":
        guard = InMemorySessionExecutionGuard()
        active = maximum = 0
        epochs = []

        async def task():
            nonlocal active, maximum
            async with guard.hold("same") as lease:
                active += 1
                maximum = max(active, maximum)
                epochs.append(lease.epoch)
                await asyncio.sleep(.001)
                active -= 1

        await asyncio.gather(*(task() for _ in range(20)))
        assert maximum == 1 and len(set(epochs)) == 20
        return {
            "backend": "InMemory",
            "tasks": 20,
            "max_same_session_concurrency": maximum,
            "execution_epochs": epochs,
            "native_fencing": "run integration tests for Redis/PostgreSQL"
        }
    if name == "idempotency-recovery":
        store = InMemoryRequestStore()

        async def reserve(index):
            request = AgentRequest(request_id=str(index),
                                   tenant_id="demo",
                                   app_id="assistant",
                                   config_version=1,
                                   user_id="student",
                                   session_id="lesson",
                                   text="hello",
                                   metadata={"admission_complete": True})
            return await store.reserve_and_create_request(
                RequestRecord(tenant_id="demo",
                              request_id=str(index),
                              state="reserved",
                              idempotency_key="same",
                              payload_hash="same",
                              request=request))

        results = await asyncio.gather(*(reserve(index) for index in range(20)))
        created = sum(item[1] for item in results)
        assert created == 1

        class FailOnceQueue(InMemoryAgentTaskQueue):
            failures = 0

            async def enqueue(self, envelope):
                if self.failures == 0:
                    self.failures += 1
                    raise ConnectionError("injected queue outage")
                return await super().enqueue(envelope)

        queue = FailOnceQueue()
        record = results[0][0]
        try:
            await queue.enqueue(AgentTaskEnvelope(request=record.request, idempotency_key="same"))
        except ConnectionError:
            pass
        repaired = await RequestRepairService(store, queue).repair_stale(older_than_seconds=0)
        delivery = await queue.receive("demo", timeout_seconds=.1)
        assert repaired == 1 and delivery.envelope.request.request_id == record.request_id
        await queue.ack(delivery)
        return {
            "backend": "InMemory",
            "concurrent_submissions": 20,
            "created_requests": created,
            "injected_enqueue_failures": queue.failures,
            "repaired_requests": repaired,
            "all_return_same_request": len({item[0].request_id
                                            for item in results}) == 1,
            "fault_test": "tests/test_session_recovery.py"
        }

    tenant = TenantConfig(tenant_id="demo",
                          apps={
                              "assistant":
                              AgentAppConfig(app_id="assistant",
                                             model={"model_name": "offline"},
                                             runtime={
                                                 "summary_event_threshold": 4,
                                                 "summary_keep_recent": 2
                                             })
                          })
    if name == "customer-service":
        binding = ChannelBindingConfig(binding_id="kf-demo",
                                       app_id="assistant",
                                       channel="wecom_kf",
                                       corp_id="corp-demo",
                                       open_kfid="kf-demo")
        tenant.channels = [binding]
        store = InMemoryCustomerStore()
        fake = FakeCustomerServiceClient({
            "": {
                "msg_list": [{
                    "msgid": "m1",
                    "origin": 3,
                    "open_kfid": "kf-demo",
                    "external_userid": "student",
                    "msgtype": "text",
                    "text": {
                        "content": "hello customer service"
                    },
                    "send_time": int(time.time())
                }],
                "next_cursor":
                "cursor-1",
                "has_more":
                0
            }
        })
        adapter = CustomerServiceAdapter(binding, fake, store)
        container = build_container(ServiceSettings(), [tenant], channel_adapters={binding.binding_id: adapter})
        container.customer_store = store
        factory = OfflineRuntimeFactory()
        container.runtimes = TenantRuntimeManager(container.registry, factory)
        container.gateway = GatewayService(container.registry, AgentWorker(container.runtimes, container.guard),
                                           container.idempotency, container.gateway._requests)
        container.task_processor = AgentTaskProcessor(container.queue,
                                                      container.gateway,
                                                      container.outbox,
                                                      consumer="demo")
        try:
            await container.setup_customer_channels()
            await store.notify(binding.binding_id, "fake-token", "fake-notification")
            await store.notify(binding.binding_id, "fake-token", "fake-notification")
            await container.customer_runtimes[binding.binding_id].step()
            assert await container.task_processor.process_one(.1)
            assert await container.delivery_worker.deliver_due() == 1
            assert len(fake.sent) == 1
            return {
                "client": "FakeCustomerServiceClient",
                "duplicate_notifications": 2,
                "replies": len(fake.sent),
                "reply_text": fake.sent[0]["text"]["content"],
                "cursor": (await store.snapshot(binding.binding_id))["cursor"]
            }
        finally:
            await container.close()

    if name == "group-session":
        tenant.apps["assistant"].runtime.group_session_mode = "shared"
        tenant.channels = [ChannelBindingConfig(binding_id="group-demo", app_id="assistant", channel="wecom")]
    registry = InMemoryTenantRegistry([tenant])
    factory = OfflineRuntimeFactory()
    runtimes = TenantRuntimeManager(registry, factory)
    gateway = GatewayService(registry, AgentWorker(runtimes, InMemorySessionExecutionGuard()),
                             InMemoryIdempotencyStore())
    try:
        replies = []
        for index in range(2 if name == "group-session" else 3):
            if name == "group-session":
                req, key = await gateway.inbound_request(
                    NormalizedInboundMessage(binding_id="group-demo",
                                             channel="wecom",
                                             message_id=str(index),
                                             external_user_id=f"member-{index}",
                                             external_conversation_id="class-group",
                                             is_group=True,
                                             text=f"member-{index}"))
            else:
                req, key = await gateway.web_request(tenant_id="demo",
                                                     app_id="assistant",
                                                     external_user_id="student",
                                                     session_id="lesson",
                                                     text=f"fact-{index}")
            replies.append((await gateway.chat(req, key)).text)
        runtime = await runtimes.get("demo", "assistant", 1)
        session = await runtime.runner.session_service.get_session(app_name=runtime.runner.app_name,
                                                                   user_id=req.user_id,
                                                                   session_id=req.session_id)
        if name == "group-session":
            assert replies[-1].endswith("user_turns=2")
            return {"mode": "shared", "member_replies": replies, "shared_history": True}
        summaries = sum(event.is_summary_event() for event in session.events)
        assert summaries == 1
        return {
            "backend": "InMemory",
            "turns": 3,
            "summary_events": summaries,
            "active_events": len(session.events),
            "stages": [item["stage"] for item in session.state["_platform_turns"].values()],
            "real_model_cost": False
        }
    finally:
        await runtimes.close()
