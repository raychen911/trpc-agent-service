"""Bounded real IM acceptance against the published, protected deployment.

Only this isolated harness enrolls the sender of an exact random private challenge.
Normal channel processes continue to require operator-managed membership.
"""

import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import uuid
import unicodedata

from sqlalchemy import select
from trpc_service.channels.runtime import ChannelRuntime
from trpc_service.persistence.models import (IMMembershipRow, AuditLogRow, InboundMessageRow, OutboxMessageRow,
                                             ModelAttemptRow, PostTurnTaskRow, PendingActionRow, ActionNoticeRow,
                                             ToolInvocationRow, ExecutionAttemptRow)


def challenge_key(text):
    """Ignore copy/paste typography, while retaining every letter and digit.

    Only the isolated acceptance gate uses this; action authorization is unchanged.
    """
    # WeCom includes the literal @bot display name in group message content;
    # Feishu strips it during normalization.  Remove mentions only for this
    # bounded acceptance matcher, never from persisted user content.
    text = re.sub(r"@[\w\-\u3400-\u9fff]+", "", text)
    return "".join(char for char in unicodedata.normalize("NFKC", text) if char.isalnum())


def identity_digest(value):
    return hashlib.sha256((value or "").encode()).hexdigest()[:10]


class ChallengeChannels(ChannelRuntime):

    def __init__(self, *args, challenges, isolation_challenges=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.challenges, self.accepted = challenges, {}
        self.isolation_challenges = isolation_challenges or {}
        self.actors = {}
        self.diagnostics = {}

    def connection_ready(self, binding):
        super().connection_ready(binding)
        diagnostics = self.diagnostics.setdefault(binding.tenant_id, {})
        diagnostics["connections"] = diagnostics.get("connections", 0) + 1
        print(json.dumps({"im_connected": binding.tenant_id}), flush=True)

    def connection_failed(self, binding, error):
        super().connection_failed(binding, error)
        diagnostics = self.diagnostics.setdefault(binding.tenant_id, {})
        diagnostics["connection_errors"] = diagnostics.get("connection_errors", 0) + 1
        diagnostics["last_error_type"] = type(error).__name__
        print(json.dumps({"im_connection_error": {
            "tenant": binding.tenant_id,
            "error_type": type(error).__name__
        }}),
              flush=True)

    async def record(self, event, binding, lease, router, *, next_offset=None):
        diagnostics = self.diagnostics.setdefault(binding.tenant_id, {})
        diagnostics["received"] = diagnostics.get("received", 0) + 1
        message = event.message
        canonical = next((text for text in self.challenges[binding.tenant_id]
                          if message and challenge_key(message.text) == challenge_key(text)), None)
        isolation = next((text for text in self.isolation_challenges.get(binding.tenant_id, [])
                          if message and challenge_key(message.text) == challenge_key(text)), None)
        # Once the exact challenge enrolled an actor, permit its normal private
        # follow-up (e.g. a model-requested confirmation). This is not approval
        # of a side effect: ActionService still requires the signed /action.
        if (canonical is None and message and message.conversation_type.value == "direct"
                and binding.tenant_id in self.actors and len(message.text) <= 1000):
            followup_route = self.store.route_message(message)
            if followup_route.actor_id == self.actors[binding.tenant_id]:
                canonical = message.text.strip()
        if canonical is None:
            canonical = isolation
        matched = bool(message and canonical is not None and (
            message.conversation_type.value == "direct" or isolation is not None))
        diagnostics["challenge_matches"] = diagnostics.get("challenge_matches", 0) + int(matched)
        print(json.dumps({"im_event": {
            "tenant": binding.tenant_id,
            "kind": event.kind,
            "ignore_reason": event.reason if event.kind == "ignored" else None,
            "challenge_matched": matched,
            "conversation_type": message.conversation_type.value if message else None,
            "text_length": len(message.text) if message else 0,
            "canonical_text_match": canonical is not None,
            "event_id": event.event_id,
            "chat_digest": identity_digest(message.external_chat_id if message else None),
            "user_digest": identity_digest(message.external_user_id if message else None)
        }},
                         ensure_ascii=False),
              flush=True)
        if event.kind == "action":
            return await super().record(event, binding, lease, router, next_offset=next_offset)
        if event.kind != "chat" or event.message is None:
            return None
        if not matched:
            return None
        key = (binding.tenant_id, canonical,
               message.conversation_type.value, message.external_chat_id,
               message.external_user_id, message.thread_id)
        previous = self.accepted.get(key)
        if previous and previous != event.event_id:
            # A repeated challenge is normally a duplicate delivery.  During
            # a resumed real acceptance, however, the previous attempt may
            # have failed before any side effect; permit a fresh delivery so
            # the operator can retry without allocating a new challenge.
            with self.database.sessions() as session:
                prior = session.scalar(select(InboundMessageRow).where(
                    InboundMessageRow.tenant_id == binding.tenant_id,
                    InboundMessageRow.external_message_id == previous).limit(1))
            if prior is None or prior.status not in {"failed_final", "dead_letter"}:
                return None
        route = self.store.route_message(message)
        if (message.conversation_type.value == "direct"
                and binding.tenant_id in self.actors and self.actors[binding.tenant_id] != route.actor_id):
            return None
        if message.conversation_type.value == "direct":
            self.actors[binding.tenant_id] = route.actor_id
        if previous is None:
            with self.database.sessions.begin() as session:
                session.merge(
                    IMMembershipRow(tenant_id=route.tenant_id,
                                    binding_id=route.channel_binding_id,
                                    actor_id=route.actor_id,
                                    active=True,
                                    grants_json=["chat", "approve"] if message.conversation_type.value == "direct"
                                    else ["chat"]))
                session.add(
                    AuditLogRow(audit_id=uuid.uuid4().hex,
                                tenant_id=route.tenant_id,
                                user_id=route.actor_id,
                                decision="protected_challenge_enrolled",
                                redacted=True,
                                metadata_json={}))
            self.accepted[key] = event.event_id
        receipt = await super().record(event, binding, lease, router)
        duplicate = await super().record(event, binding, lease, router)
        if duplicate.inbound_id != receipt.inbound_id or not duplicate.duplicate:
            raise RuntimeError("durable duplicate verification failed")
        return receipt


def snapshot(database):
    result = {}
    with database.sessions() as session:
        for tenant in ("demo_wecom", "demo_feishu"):
            excluded = os.environ.get("TRPC_ACCEPTANCE_PRIOR_FAILED_IDS", "").split(",")
            inputs = list(session.scalars(select(InboundMessageRow).where(
                InboundMessageRow.tenant_id == tenant, InboundMessageRow.inbound_message_id.not_in(excluded))))
            outputs = list(session.scalars(select(OutboxMessageRow).where(
                OutboxMessageRow.tenant_id == tenant, OutboxMessageRow.inbound_message_id.not_in(excluded))))
            attempts = list(session.scalars(select(ModelAttemptRow).where(ModelAttemptRow.tenant_id == tenant)))
            tasks = list(session.scalars(select(PostTurnTaskRow).where(PostTurnTaskRow.tenant_id == tenant)))
            actions = list(session.scalars(select(PendingActionRow).where(PendingActionRow.tenant_id == tenant)))
            notices = list(session.scalars(select(ActionNoticeRow).where(ActionNoticeRow.tenant_id == tenant)))
            tools = list(session.scalars(select(ToolInvocationRow).where(ToolInvocationRow.tenant_id == tenant)))
            result[tenant] = {
                "input_statuses": [row.status for row in inputs],
                "reply_statuses": [row.status for row in outputs],
                "trace_ids": [row.trace_id for row in inputs],
                "session_ids": [row.payload_json["route"]["session_id"] for row in inputs],
                "model_attempts": [{
                    "status": row.status,
                    "input_tokens": row.input_tokens,
                    "output_tokens": row.output_tokens,
                    "cost": str(row.actual_amount),
                    "currency": row.currency
                } for row in attempts],
                "post_turn": {
                    row.task_type: row.status
                    for row in tasks
                },
                "post_turn_tasks": [{
                    "kind": row.task_type,
                    "status": row.status
                } for row in tasks],
                "actions": [{
                    "tool": row.tool_name,
                    "status": row.status
                } for row in actions],
                "action_notices": [{
                    "kind": row.kind,
                    "status": row.status
                } for row in notices],
                "tools": [{
                    "name": row.tool_name,
                    "status": row.status
                } for row in tools],
                "worker_ids":
                list(
                    session.scalars(
                        select(ExecutionAttemptRow.worker_id).where(
                            ExecutionAttemptRow.execution_id.in_([row.execution_id for row in inputs])).distinct())),
                "passed":
                len(inputs) >= 2 and all(row.status == "succeeded" for row in inputs) and bool(outputs)
                and all(row.status == "delivered" for row in outputs) and bool(attempts) and all(row.status == "settled"
                                                                                                 for row in attempts)
                and len(tasks) == 2 * len(inputs) and all(row.status == "succeeded" for row in tasks)
                and len(actions) == 1
                and actions[0].status == "succeeded" and bool(notices) and all(row.status == "delivered"
                                                                               for row in notices)
                and {row.tool_name
                     for row in tools
                     if row.status == "succeeded"} >= {"write_artifact", "read_artifact", "search_knowledge"}
            }
    return result


async def native_evidence(database, store, result, filename=None, marker=None):
    from trpc_service.storage.protected_session import ProtectedSessionService
    from trpc_service.agent import tenant_app_name
    for tenant_id, evidence in result.items():
        with database.sessions() as session:
            row = session.scalar(select(InboundMessageRow).where(InboundMessageRow.tenant_id == tenant_id))
            if row is None:
                evidence["native_verified"] = False
                continue
            route = dict(row.payload_json["route"])
            execution_ids = set(
                session.scalars(select(InboundMessageRow.execution_id).where(InboundMessageRow.tenant_id == tenant_id)))
        profile = store.resource_profile(tenant_id, route["config_version"], "session")
        app = tenant_app_name(tenant_id, route["agent_app_id"])
        reader = getattr(ProtectedSessionService,
                         profile.kind.value)(store.connection(tenant_id, route["config_version"], "session"), app)
        try:
            native = await reader.get_session(app_name=app,
                                              user_id=route["internal_user_id"],
                                              session_id=route["session_id"])
            originals = [] if native is None else [
                item for item in [*native.historical_events, *native.events] if not item.is_summary_event()
            ]
            verified = (len(originals) >= 4 and all(
                (item.custom_metadata or {}).get("platform_execution_id") in execution_ids for item in originals))
            evidence.update(native_verified=verified,
                            session_backend=profile.kind.value,
                            native_revision=native.native_revision if native else None,
                            original_event_count=len(originals),
                            native_format="protected_v1")
            evidence["passed"] = evidence["passed"] and verified
        finally:
            await reader.close()
        if filename is not None:
            from trpc_agent_sdk.abc import ArtifactId
            from trpc_agent_sdk.memory import SqlMemoryService
            from trpc_service.storage.runtime_resources import build_resources
            config = store.published(tenant_id, route["config_version"])
            resources = await build_resources(store, config, database)
            memory = SqlMemoryService(db_url=store.connection(tenant_id, route["config_version"], "memory"),
                                      enabled=True,
                                      is_async=False,
                                      expire_on_commit=False)
            try:
                entry = await resources.artifact.load_artifact(artifact_id=ArtifactId(
                    app_name=app, user_id=route["internal_user_id"], session_id=route["session_id"], filename=filename))
                matches = await memory.search_memory(app + "/" + route["internal_user_id"], marker)
                evidence["artifact_verified"] = bool(entry and entry.data.text == marker)
                evidence["memory_verified"] = bool(matches.memories)
                evidence[
                    "passed"] = evidence["passed"] and evidence["artifact_verified"] and evidence["memory_verified"]
            finally:
                await resources.close()
                await memory.close()


def trace_evidence(result):
    directory = os.environ.get("TRPC_TRACE_DIRECTORY")
    spans = []
    if directory:
        for path in Path(directory).glob("*.jsonl"):
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    spans.append(json.loads(line))
                except json.JSONDecodeError:
                    continue  # A concurrently written final line is retried at completion.
    required = {
        "im.callback", "worker.execute", "model.provider_response", "tool.invoke", "object.read", "vector.search",
        "memory.store", "session.append", "im.reply", "action.execute"
    }
    for evidence in result.values():
        names = {span["name"] for span in spans if span["trace_id"] in evidence["trace_ids"]}
        evidence["trace_operations"] = sorted(names)
        evidence["missing_trace_operations"] = sorted(required - names)
        evidence["passed"] = evidence["passed"] and required <= names


async def run(database, timeout=600, *, resume_report=None):
    from trpc_service.management import ConfigurationStore
    from trpc_service.service_runtime import ServiceRuntime
    store = ConfigurationStore(database)
    prior_failed = [value for value in os.environ.get("TRPC_ACCEPTANCE_PRIOR_FAILED_IDS", "").split(",") if value]
    if prior_failed:
        with database.sessions() as session:
            for identity in prior_failed:
                row = session.get(InboundMessageRow, identity)
                if row is None or row.status != "failed_final" or row.error_type != "agent_error":
                    raise ValueError("only verified pre-model failures can be superseded")
                for model in (ModelAttemptRow, ToolInvocationRow, PendingActionRow):
                    if session.scalar(select(model).where(model.execution_id == row.execution_id).limit(1)):
                        raise ValueError("prior execution has external side effects; cannot supersede")
    tenants = store.active()
    if {tenant.tenant_id for tenant in tenants} != {"demo_wecom", "demo_feishu"}:
        raise ValueError("prepare a dedicated protected demo database first")
    previous = json.loads(Path(resume_report).read_text(encoding="utf-8")) if resume_report else None
    initial = snapshot(database)
    if previous:
        if (previous.get("passed") or previous.get("error_type") != "WorkerExited"
                or previous.get("trace_directory") != os.environ.get("TRPC_TRACE_DIRECTORY")
                or any(initial[key]["trace_ids"] != previous["tenants"][key]["trace_ids"] for key in initial)):
            raise ValueError("resume requires the exact interrupted acceptance evidence")
    elif any(value["input_statuses"] for value in initial.values()) and not os.environ.get("TRPC_ACCEPTANCE_CONTINUE"):
        raise ValueError("this bounded test requires a fresh demo database; previous evidence is preserved")
    os.environ.update(TRPC_RUNTIME_MODE="protected", TRPC_CONFIG_SOURCE="database")
    # Validate all configured resources before prompting the user to send anything.
    preflight = ServiceRuntime(database)
    try:
        await preflight.start()
    finally:
        await preflight.close()
    run_id = (previous["challenge_id"] if previous else os.environ.get(
        "TRPC_ACCEPTANCE_CHALLENGE_ID", uuid.uuid4().hex[:12]))
    if len(run_id) != 12 or any(character not in "0123456789abcdef" for character in run_id):
        raise ValueError("acceptance challenge id must contain twelve lowercase hexadecimal characters")
    filename = "acceptance-" + run_id + ".txt"
    challenges = {
        tenant.tenant_id: [
            f"真实验收 {run_id}：请调用 write_artifact 保存 {filename}，内容为 lighthouse-{run_id}，等待我确认。",
            f"真实验收 {run_id}：请调用 read_artifact 读取 {filename}，再调用 "
            "search_knowledge，knowledge_base_id=acceptance，使用验收预计算向量 "
            "[1,0]；简短回复文件内容和检索内容。"
        ]
        for tenant in tenants
    }
    isolation_challenges = {
        tenant.tenant_id: [
            f"隔离验收 {run_id}：群A用户1，请仅回复 scope-a-u1-{tenant.tenant_id}-{run_id}",
            f"隔离验收 {run_id}：群A用户2，请仅回复 scope-a-u2-{tenant.tenant_id}-{run_id}",
            f"隔离验收 {run_id}：群B用户1，请仅回复 scope-b-u1-{tenant.tenant_id}-{run_id}",
            f"隔离验收 {run_id}：飞书线程2，请仅回复 scope-thread2-{tenant.tenant_id}-{run_id}"
        ]
        for tenant in tenants
    }
    from trpc_service.storage.runtime_resources import build_resources
    for tenant in tenants:
        if tenant.embedding:
            challenges[tenant.tenant_id][1] = (
                f"真实验收 {run_id}：请调用 read_artifact 读取 {filename}，再调用 search_knowledge，"
                "knowledge_base_id=acceptance，query=灯塔项目的知识库验收标识是什么？简短回复文件内容和检索内容。")
        resources = await build_resources(store, tenant, database)
        try:
            if tenant.embedding:
                from trpc_service.storage.knowledge_ingestion import KnowledgeIngestionService, configured_embedder
                ingestion = KnowledgeIngestionService(
                    resources.knowledge, tenant_id=tenant.tenant_id, knowledge_base_id="acceptance",
                    index_version=resources.index_version, embedder=configured_embedder(tenant))
                content = "灯塔项目的知识库验收标识是 knowledge-" + tenant.tenant_id + "-" + run_id
                existing = await asyncio.to_thread(
                    resources.knowledge.iter_index, tenant.tenant_id, "acceptance", resources.index_version)
                if not any(not item.deleted and item.document_id == "live-" + run_id and item.text == content
                           for item in existing):
                    await ingestion.ingest_document("live-" + run_id, content)
            else:
                await asyncio.to_thread(resources.knowledge.upsert, tenant.tenant_id, "acceptance",
                                        resources.index_version,
                                        "live-" + run_id, "live-" + run_id, [1.0, 0.0],
                                        "knowledge-" + tenant.tenant_id + "-" + run_id)
        finally:
            await resources.close()
    runtime = ChallengeChannels(
        database,
        store,
        identity_key=os.environ["TRPC_IDENTITY_KEY"].encode(),
        encryption_keys=[value.encode() for value in os.environ["TRPC_IM_CONTEXT_KEYS"].split(",")],
        challenges=challenges, isolation_challenges=isolation_challenges)
    if previous or os.environ.get("TRPC_ACCEPTANCE_CONTINUE"):
        with database.sessions() as session:
            for row in session.scalars(select(InboundMessageRow)):
                if row.inbound_message_id in prior_failed:
                    continue
                route = row.payload_json["route"]
                if row.payload_json["message"]["conversation_type"] == "direct":
                    runtime.actors[row.tenant_id] = route["actor_id"]
                text = row.payload_json["message"]["text"]
                all_challenges = [*challenges[row.tenant_id],
                                  *runtime.isolation_challenges.get(row.tenant_id, [])]
                canonical = next((value for value in all_challenges
                                  if challenge_key(value) == challenge_key(text)), text.strip())
                message = row.payload_json["message"]
                runtime.accepted[(row.tenant_id, canonical, message["conversation_type"],
                                  message.get("external_chat_id"), message.get("external_user_id"),
                                  message.get("thread_id"))] = row.external_message_id
    stop = asyncio.Event()
    channel_task = asyncio.create_task(runtime.run(stop))
    processes, passed = [], False
    report = {
        "scope": "two_tenant_two_agent_processes_real_im_protected_session",
        "production_ready": False,
        "prior_failed_inbound_ids": prior_failed,
        "human_read_confirmation": False,
        "challenge_id": run_id,
        "trace_directory": os.environ.get("TRPC_TRACE_DIRECTORY")
    }
    if previous:
        report["resumed_from"] = str(Path(resume_report).resolve())
    process_logs = []
    try:
        for command in ("worker", "worker", "post-turn", "action-worker"):
            log_path = Path(os.environ["TRPC_TRACE_DIRECTORY"]).parent / (command + "-" + uuid.uuid4().hex[:8] + ".log")
            log_file = log_path.open("wb")
            process_logs.append(log_file)
            processes.append(await asyncio.create_subprocess_exec(sys.executable,
                                                                  "-m",
                                                                  "trpc_service._cli",
                                                                  command,
                                                                  stdout=log_file,
                                                                  stderr=log_file,
                                                                  **({
                                                                      "creationflags": 0x08000000
                                                                  } if os.name == "nt" else {})))
        print(json.dumps(
            {
                "send_private_challenges": {
                    key: value[0]
                    for key, value in challenges.items()
                },
                "next": "请在各 IM 内按机器人发回的 /action 指令批准；文件保存成功后会给出读取口令。"
            },
            ensure_ascii=False),
              flush=True)
        deadline = asyncio.get_running_loop().time() + timeout
        prompted = set()
        restarted = False
        while asyncio.get_running_loop().time() < deadline:
            if channel_task.done():
                await channel_task
                break
            if any(process.returncode is not None for process in processes):
                report["error_type"] = "WorkerExited"
                report["exited_processes"] = [
                    {"index": index, "exit_code": process.returncode,
                     "log": process_logs[index].name}
                    for index, process in enumerate(processes)
                    if process.returncode is not None
                ]
                break
            report["tenants"] = snapshot(database)
            if not restarted and all(
                    any(action["tool"] == "write_artifact" and action["status"] == "succeeded"
                        for action in evidence["actions"])
                    and any(status == "succeeded" for status in evidence["input_statuses"])
                    for evidence in report["tenants"].values()):
                for process in processes[:2]:
                    process.terminate()
                    await process.wait()
                for index in range(2):
                    processes[index] = await asyncio.create_subprocess_exec(sys.executable,
                                                                            "-m",
                                                                            "trpc_service._cli",
                                                                            "worker",
                                                                            stdout=asyncio.subprocess.DEVNULL,
                                                                            stderr=asyncio.subprocess.DEVNULL,
                                                                            **({
                                                                                "creationflags": 0x08000000
                                                                            } if os.name == "nt" else {}))
                restarted = True
                report["agent_processes_restarted_between_turns"] = True
            for tenant_id, evidence in report["tenants"].items():
                if restarted and tenant_id not in prompted:
                    prompted.add(tenant_id)
                    print(json.dumps({"send_read_challenge": {
                        tenant_id: challenges[tenant_id][1]
                    }}, ensure_ascii=False),
                          flush=True)
                    print(json.dumps({"send_isolation_challenges": {
                        tenant_id: isolation_challenges[tenant_id]
                    }}, ensure_ascii=False), flush=True)
            if all(value["passed"] for value in report["tenants"].values()):
                await native_evidence(database, store, report["tenants"], filename, "lighthouse-" + run_id)
                trace_evidence(report["tenants"])
                passed = all(value["passed"] for value in report["tenants"].values())
                if passed:
                    break
            await asyncio.sleep(1)
    except Exception as error:
        report["error_type"] = type(error).__name__
        raise
    finally:
        stop.set()
        channel_task.cancel()
        await asyncio.gather(channel_task, return_exceptions=True)
        for process in processes:
            if process.returncode is None:
                process.terminate()
            await process.wait()
        for log_file in process_logs:
            log_file.close()
        from trpc_service.telemetry.runtime import shutdown
        shutdown()
        report["passed"] = passed
        if not passed and "error_type" not in report:
            report["error_type"] = "AcceptanceIncomplete"
        report["ingress_diagnostics"] = runtime.diagnostics
        if "tenants" not in report:
            report["tenants"] = snapshot(database)
        target = Path("reports") / ("protected-dual-im-" + uuid.uuid4().hex + ".json")
        target.parent.mkdir(exist_ok=True)
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("report=" + str(target.resolve()), flush=True)
    return passed
