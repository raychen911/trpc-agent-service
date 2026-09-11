"""Synthetic business data written through the production tRPC public interfaces."""

from dataclasses import dataclass

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import EventActions
from trpc_agent_sdk.types import Part


@dataclass(frozen=True)
class SessionKey:
    app_name: str
    user_id: str
    session_id: str

    def kwargs(self):
        return vars(self)


async def append_turn(service, key, turn):
    session = await service.get_session(**key.kwargs())
    assert session is not None
    for author, text in (("user", f"订单第 {turn} 轮：退货规则？🙂"), ("support", f"第 {turn} 轮答复")):
        await service.append_event(session, Event(
            id=f"{key.session_id}-{turn}-{author}", author=author,
            timestamp=1720000000.0 + turn * 2 + (author != "user"),
            invocation_id=f"inv-{key.session_id}-{turn}",
            custom_metadata={"seed": "migration-e2e", "turn": turn},
            content=Content(role="user" if author == "user" else "model", parts=[Part.from_text(text=text)]),
            actions=EventActions(state_delta={"turn": turn, "order": {"status": "open", "items": [1, 2]}}),
        ))


async def seed_sessions(service, suffix, sessions_per_tenant=3, turns=4):
    keys = []
    for tenant in (f"tenant_a_{suffix}", f"tenant_b_{suffix}"):
        for index in range(sessions_per_tenant):
            # Same literal user/session identifiers across tenants deliberately test namespace isolation.
            key = SessionKey(f"{tenant}:support", f"user-{index % 2}", f"session-{suffix}-{index}")
            await service.create_session(**key.kwargs(), state={"owner": tenant, "initial": "初始值"})
            keys.append(key)
            for turn in range(1, turns + 1):
                await append_turn(service, key, turn)
    return keys


def canonical_session(session):
    """Independent oracle: do not use migration's hash or its revision=len(events) shortcut."""
    assert session is not None
    return {
        "app": session.app_name, "user": session.user_id, "id": session.id,
        "state": dict(session.state),
        "events": [
            {"id": event.id, "author": event.author, "invocation_id": event.invocation_id,
             "content": event.content.model_dump(mode="json") if event.content else None,
             "actions": event.actions.model_dump(mode="json"),
             "metadata": event.custom_metadata, "partial": event.partial}
            for event in session.events
        ],
    }
