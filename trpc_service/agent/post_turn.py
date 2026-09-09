"""Strict, resumable post-turn processing above the unmodified SDK.

The SDK's default post-turn hook logs failures and returns. The service instead
disables that hook and commits explicit stages under the execution lease.
"""

from copy import deepcopy

from trpc_agent_sdk.sessions import SessionSummarizer, SummarizerSessionManager
from trpc_agent_sdk.sessions._summarizer_checker import set_summarizer_events_count_threshold


class PostTurnError(RuntimeError):
    pass


class IncompleteRunError(RuntimeError):
    """A partial turn cannot be safely replayed without side-effect review."""


def configure_summary(runner, policy, model=None):
    if not policy.summary_enabled:
        return
    summarizer = SessionSummarizer(
        model=model or runner.agent.model,
        check_summarizer_functions=[set_summarizer_events_count_threshold(policy.summary_event_threshold)],
        keep_recent_count=policy.summary_keep_recent,
    )
    manager = SummarizerSessionManager(model=model or runner.agent.model, summarizer=summarizer)
    runner.session_service.set_summarizer_manager(manager, force=True)
    # The SDK normally binds the manager to the underlying service. Never allow
    # its update_session to bypass platform metadata/lease enforcement.
    manager.set_session_service(runner.session_service, force=True)


class TurnFinalizer:

    def __init__(self, runner, policy):
        self.runner = runner
        self.policy = policy

    @staticmethod
    def result_from_events(session, request_id):
        events = [event for event in session.events if event.request_id == request_id]
        final = next((event for event in reversed(events) if event.author != "user" and not event.is_summary_event()
                      and event.is_final_response() and not event.is_error()), None)
        if final is None:
            return None
        usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        for event in events:
            if event.usage_metadata:
                usage["input_tokens"] += int(event.usage_metadata.prompt_token_count or 0)
                usage["output_tokens"] += int(event.usage_metadata.candidates_token_count or 0)
                usage["total_tokens"] += int(event.usage_metadata.total_token_count or 0)
        return {"text": final.get_text(), "usage": usage, "stage": "agent_finished"}

    @staticmethod
    def _is_plain_user_event(event) -> bool:
        """Return whether discarding this event cannot repeat model/tool side effects."""
        actions = event.actions
        return bool(event.author == "user" and not event.partial and not event.is_error()
                    and not event.get_function_calls() and not event.get_function_responses()
                    and not event.long_running_tool_ids and not actions.state_delta and not actions.artifact_delta
                    and not actions.transfer_to_agent and not actions.escalate)

    async def discard_abandoned_user_turn(self, session, request_id) -> bool:
        """Remove a crash residue only when it consists solely of plain user input.

        Runner persists the user Event before calling the model. If the process
        dies in that window, a retry must remove the old Event or the same input
        would appear twice. Any model/tool/action evidence remains fail-closed.
        The caller holds both the Session lease and the storage-native fence.
        """
        active = [event for event in session.events if event.request_id == request_id]
        historical = [event for event in session.historical_events if event.request_id == request_id]
        turns = session.state.get("_platform_turns", {})
        if not active or historical or request_id in turns:
            return False
        if not all(self._is_plain_user_event(event) for event in active):
            return False
        session.events = [event for event in session.events if event.request_id != request_id]
        # Runner increments once before appending the user Event. Undo that
        # abandoned invocation before the replacement invocation starts.
        session.conversation_count = max(0, session.conversation_count - 1)
        await self.runner.session_service.update_session(session)
        return True

    async def finish(self, session, request_id, agent_context=None):
        turns = deepcopy(session.state.get("_platform_turns", {}))
        turn = turns.get(request_id)
        if turn is None:
            turn = self.result_from_events(session, request_id)
            if turn is None:
                return None
            turns[request_id] = turn
            session.state["_platform_turns"] = turns
            await self.runner.session_service.update_session(session)
        if turn["stage"] == "agent_finished":
            manager = self.runner.session_service.summarizer_manager
            required = bool(self.policy.summary_enabled and manager and await manager.should_summarize_session(session))
            if self.policy.summary_enabled and manager is None:
                raise PostTurnError("summary_manager_not_configured")
            if required:
                old_ids = {e.id for e in session.events if e.is_summary_event()}
                await self.runner.session_service.create_session_summary(session)
                if not any(e.is_summary_event() and e.id not in old_ids for e in session.events):
                    raise PostTurnError("summary_generation_failed")
            turn["stage"] = "summary_done" if required else "summary_skipped"
            session.state["_platform_turns"] = turns
            await self.runner.session_service.update_session(session)
        if turn["stage"] in {"summary_done", "summary_skipped"}:
            if self.runner.memory_service and self.runner.memory_service.enabled:
                # Failure intentionally propagates, so neither completed SSE nor
                # durable success/Outbox can be emitted after a failed store.
                await self.runner.memory_service.store_session(session, agent_context=agent_context)
            turn["stage"] = "memory_done"
            session.state["_platform_turns"] = turns
            await self.runner.session_service.update_session(session)
        return turn

    async def recover_pending(self, session, agent_context=None):
        # A process may exit after persisting the final Event but before writing
        # its stage marker. Repair that gap even when a *new* request arrives.
        identifiers = dict.fromkeys(e.request_id for e in session.events if e.request_id and not e.is_summary_event())
        for request_id in identifiers:
            if request_id not in session.state.get("_platform_turns", {}):
                if self.result_from_events(session, request_id) is None:
                    if await self.discard_abandoned_user_turn(session, request_id):
                        continue
                    raise IncompleteRunError("partial_session_turn_requires_review")
                await self.finish(session, request_id, agent_context)
        for request_id, turn in list(session.state.get("_platform_turns", {}).items()):
            if turn["stage"] != "memory_done":
                await self.finish(session, request_id, agent_context)
