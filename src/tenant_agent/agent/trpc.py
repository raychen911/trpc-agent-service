"""Production tRPC-Agent-Python Runner adapter."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from sqlalchemy.engine import make_url

from tenant_agent.agent.tools import build_tools
from tenant_agent.governance.policies import ConfirmationManager, GovernanceService, confirmation_tokens
from tenant_agent.models import (
    AgentEvent,
    AgentEventType,
    BackendKind,
    RoutedEnvelope,
    TenantConfig,
)
from tenant_agent.security import CompositeSecretResolver
from tenant_agent.services.native_session import (
    ProvisionedSqlSessionService,
    assert_native_sql_runtime_role_unprivileged,
    validate_native_sql_schema,
)
from tenant_agent.settings import Settings
from tenant_agent.storage.base import TenantDataPlane

logger = logging.getLogger(__name__)
RunnerCacheKey = tuple[str, str, int, int]


@dataclass(slots=True)
class _RunnerBundle:
    cache_key: RunnerCacheKey
    runner: Any
    model_name: str
    last_used_monotonic: float
    active_runs: int = 0


class TrpcAgentEngine:
    """Pools immutable Runner graphs by tenant/app/config revision."""

    def __init__(
        self,
        *,
        settings: Settings,
        secrets: CompositeSecretResolver,
        governance: GovernanceService,
        confirmations: ConfirmationManager,
    ) -> None:
        self.settings = settings
        self.secrets = secrets
        self.governance = governance
        self.confirmations = confirmations
        self._runners: dict[RunnerCacheKey, _RunnerBundle] = {}
        self._guard = asyncio.Lock()

    async def close(self) -> None:
        async with self._guard:
            runners = tuple(self._runners.values())
            self._runners.clear()
        for bundle in runners:
            await bundle.runner.close()

    @staticmethod
    async def _close_preflight_candidate(candidate: Any) -> None:
        close = getattr(candidate, "close", None) or getattr(candidate, "aclose", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result

    async def preflight_tenant(self, tenant: TenantConfig) -> None:
        """Construct runtime model/session objects without invoking a provider."""

        checked_profiles: set[str] = set()
        for app_id, app in tenant.apps.items():
            if app.model_profile in checked_profiles:
                continue
            profile = tenant.models[app.model_profile]
            if profile.provider != "deterministic":
                model = await self._model(tenant, app_id)
                await self._close_preflight_candidate(model)
            checked_profiles.add(app.model_profile)
        service = await self._session_service(tenant)
        await self._close_preflight_candidate(service)

    async def _release_bundle(self, key: RunnerCacheKey) -> None:
        async with self._guard:
            bundle = self._runners.get(key)
            if bundle is not None:
                bundle.active_runs = max(0, bundle.active_runs - 1)
                bundle.last_used_monotonic = time.monotonic()
            await self._evict_inactive_locked()

    async def _evict_inactive_locked(
        self,
        *,
        exclude: RunnerCacheKey | None = None,
    ) -> None:
        now = time.monotonic()
        expired = [
            key
            for key, bundle in self._runners.items()
            if (
                key != exclude
                and bundle.active_runs == 0
                and now - bundle.last_used_monotonic >= self.settings.runner_cache_ttl_seconds
            )
            or (
                key != exclude
                and bundle.active_runs == 0
                and key[3] != int(now // max(1.0, self.settings.runner_cache_ttl_seconds))
            )
        ]
        for key in expired:
            bundle = self._runners.pop(key)
            await bundle.runner.close()

        while len(self._runners) > self.settings.runner_cache_max_entries:
            candidates = [
                (key, bundle)
                for key, bundle in self._runners.items()
                if key != exclude and bundle.active_runs == 0
            ]
            if not candidates:
                break
            key, bundle = min(
                candidates,
                key=lambda item: item[1].last_used_monotonic,
            )
            self._runners.pop(key)
            await bundle.runner.close()

    async def _resolve_reference(self, reference: Any) -> str:
        if reference.uri == "env://TAP_CONTROL_DATABASE_URL":
            return self.settings.control_database_url.get_secret_value()
        return await self.secrets.resolve(reference)

    async def _resolve_native_dsn(self, tenant: TenantConfig) -> str | None:
        backend = tenant.data_backends.session
        if backend.kind is BackendKind.INMEMORY:
            return None
        native_reference = backend.native_dsn_ref or backend.dsn_ref
        if native_reference is None:
            raise ValueError("persistent session backend requires a DSN reference")
        native_dsn = await self._resolve_reference(native_reference)
        if backend.kind is BackendKind.SQL:
            if backend.dsn_ref is None or backend.native_dsn_ref is None:
                raise ValueError("SQL Session requires separate platform and native DSNs")
            platform_dsn = await self._resolve_reference(backend.dsn_ref)
            if make_url(platform_dsn) == make_url(native_dsn):
                raise ValueError("platform and native SQL Session DSNs must be different")
            await validate_native_sql_schema(native_dsn)
            if self.settings.environment == "production":
                await assert_native_sql_runtime_role_unprivileged(native_dsn)
        return native_dsn

    async def _session_service(self, tenant: TenantConfig) -> Any:
        from trpc_agent_sdk.sessions import (
            InMemorySessionService,
            RedisSessionService,
            SessionServiceConfig,
        )

        backend = tenant.data_backends.session
        config = SessionServiceConfig(max_events=200, store_historical_events=True)
        dsn = await self._resolve_native_dsn(tenant)
        if backend.kind is BackendKind.INMEMORY:
            return InMemorySessionService(session_config=config)
        if backend.kind is BackendKind.REDIS:
            if backend.options.get("cluster"):
                try:
                    from trpc_agent_sdk.sessions import RedisClusterSessionService
                except ImportError as exc:
                    raise RuntimeError("this tRPC-Agent release lacks Redis Cluster support") from exc
                return RedisClusterSessionService(db_url=dsn, is_async=True, session_config=config)
            return RedisSessionService(db_url=dsn, is_async=True, session_config=config)
        if backend.kind is BackendKind.SQL:
            return ProvisionedSqlSessionService(
                db_url=dsn,
                is_async=True,
                session_config=config,
                pool_pre_ping=True,
                expire_on_commit=False,
            )
        raise ValueError(f"unsupported tRPC session backend {backend.kind.value}")

    async def _model(self, tenant: TenantConfig, app_id: str) -> Any:
        from trpc_agent_sdk.configs import ExponentialBackoffConfig, ModelRetryConfig
        from trpc_agent_sdk.models import AnthropicModel, LiteLLMModel, OpenAIModel
        from trpc_agent_sdk.types import GenerateContentConfig

        app = tenant.apps[app_id]
        profile = tenant.models[app.model_profile]
        if profile.provider == "deterministic":
            raise ValueError("deterministic model profiles use DeterministicEngine")
        assert profile.api_key_ref is not None
        api_key = await self.secrets.resolve(profile.api_key_ref)
        kwargs = {
            "model_name": profile.model_name,
            "api_key": api_key,
            "base_url": profile.base_url or "",
            "generate_content_config": GenerateContentConfig(max_output_tokens=profile.max_output_tokens),
            "model_retry_config": ModelRetryConfig(
                num_retries=profile.retry_count,
                backoff=ExponentialBackoffConfig(
                    initial_backoff=profile.retry_initial_seconds,
                    max_backoff=profile.retry_max_seconds,
                    jitter=True,
                ),
            ),
        }
        if profile.provider == "openai-compatible":
            return OpenAIModel(**kwargs)
        if profile.provider == "anthropic":
            return AnthropicModel(**kwargs)
        if profile.provider == "litellm":
            return LiteLLMModel(**kwargs)
        raise ValueError(f"unsupported model provider {profile.provider}")

    async def _bundle(
        self,
        tenant: TenantConfig,
        app_id: str,
        plane: TenantDataPlane,
    ) -> _RunnerBundle:
        cache_epoch = int(time.monotonic() // max(1.0, self.settings.runner_cache_ttl_seconds))
        key: RunnerCacheKey = (
            tenant.tenant_id,
            app_id,
            tenant.revision,
            cache_epoch,
        )
        async with self._guard:
            cached = self._runners.get(key)
            if cached:
                if (
                    cached.active_runs == 0
                    and time.monotonic() - cached.last_used_monotonic
                    >= self.settings.runner_cache_ttl_seconds
                ):
                    self._runners.pop(key)
                    await cached.runner.close()
                else:
                    cached.last_used_monotonic = time.monotonic()
                    cached.active_runs += 1
                    return cached
            from trpc_agent_sdk.agents import LlmAgent
            from trpc_agent_sdk.events import Event
            from trpc_agent_sdk.runners import Runner

            class RecoverableRunner(Runner):  # type: ignore[misc]
                async def _append_new_message_to_session(
                    runner_self: Any,
                    session: Any,
                    new_message: Any,
                    invocation_context: Any,
                ) -> None:
                    if not new_message.parts:
                        raise ValueError("No parts in the new_message.")
                    request_id = invocation_context.agent_context.get_metadata("message_id")
                    if request_id and any(
                        event.author == "user" and event.request_id == str(request_id)
                        for event in (*session.historical_events, *session.events)
                    ):
                        return
                    event = Event(
                        invocation_id=invocation_context.invocation_id,
                        author="user",
                        content=new_message,
                        request_id=str(request_id) if request_id else None,
                    )
                    await runner_self.session_service.append_event(
                        session=session,
                        event=event,
                    )

            app = tenant.apps[app_id]
            profile = tenant.models[app.model_profile]
            model = await self._model(tenant, app_id)
            tools = build_tools(
                tenant=tenant,
                app_id=app_id,
                plane=plane,
                governance=self.governance,
                confirmations=self.confirmations,
            )
            agent = LlmAgent(
                name=app.agent_name,
                description=app.description,
                model=model,
                instruction=app.instruction,
                tools=tools,
            )
            service = await self._session_service(tenant)
            tenant_scope = hashlib.sha256(tenant.tenant_id.encode()).hexdigest()[:16]
            runner = RecoverableRunner(
                app_name=f"tap:{tenant_scope}:{app_id}",
                agent=agent,
                session_service=service,
                enable_post_turn_processing=False,
            )
            bundle = _RunnerBundle(
                cache_key=key,
                runner=runner,
                model_name=profile.model_name,
                last_used_monotonic=time.monotonic(),
                active_runs=1,
            )
            self._runners[key] = bundle
            await self._evict_inactive_locked(exclude=key)
            return bundle

    @staticmethod
    def _usage_tokens(event: Any) -> tuple[int, int]:
        usage = getattr(event, "usage_metadata", None)
        if not usage:
            return 0, 0
        return (
            int(getattr(usage, "prompt_token_count", 0) or getattr(usage, "input_tokens", 0) or 0),
            int(getattr(usage, "candidates_token_count", 0) or getattr(usage, "output_tokens", 0) or 0),
        )

    async def _recover_native_result(
        self,
        bundle: _RunnerBundle,
        routed: RoutedEnvelope,
        agent_context: Any,
    ) -> AgentEvent | None:
        session_service = getattr(bundle.runner, "session_service", None)
        app_name = getattr(bundle.runner, "app_name", None)
        if session_service is None or app_name is None:
            return None
        session = await session_service.get_session(
            app_name=app_name,
            user_id=routed.internal_user_id,
            session_id=routed.session_id,
            agent_context=agent_context,
        )
        if session is None:
            return None
        events = tuple(session.historical_events) + tuple(session.events)
        for user_index in range(len(events) - 1, -1, -1):
            user_event = events[user_index]
            if user_event.author != "user" or user_event.request_id != routed.inbound.message_id:
                continue
            for candidate in reversed(events[user_index + 1 :]):
                if (
                    candidate.invocation_id != user_event.invocation_id
                    or candidate.partial
                    or not candidate.is_final_response()
                ):
                    continue
                text = candidate.get_text()
                if not text:
                    continue
                invocation_events = [
                    event
                    for event in events[user_index + 1 :]
                    if event.invocation_id == user_event.invocation_id
                ]
                candidate_index = invocation_events.index(candidate)
                usage_rows = [self._usage_tokens(event) for event in invocation_events[: candidate_index + 1]]
                input_tokens = sum(row[0] for row in usage_rows)
                output_tokens = sum(row[1] for row in usage_rows)
                return AgentEvent(
                    event_id=candidate.id or uuid.uuid4().hex,
                    event_type=AgentEventType.TEXT_FINAL,
                    text=text,
                    token_input=input_tokens,
                    token_output=output_tokens,
                    payload={"recovered_from_native_session": True},
                )
        return None

    async def _stream_impl(
        self,
        *,
        bundle: _RunnerBundle,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        effective_text: str,
        plane: TenantDataPlane,
    ) -> AsyncIterator[AgentEvent]:
        from trpc_agent_sdk.configs import RunConfig
        from trpc_agent_sdk.context import new_agent_context
        from trpc_agent_sdk.types import Content, Part

        app = tenant.apps[routed.inbound.app_id]
        profile = tenant.models[app.model_profile]
        timeout_seconds = min(
            self.settings.model_timeout_seconds,
            profile.timeout_seconds,
        )
        metadata = {
            "tenant_id": tenant.tenant_id,
            "config_revision": tenant.revision,
            "channel": routed.inbound.channel.value,
            "binding_id": routed.inbound.binding_id,
            "user_id": routed.internal_user_id,
            "session_id": routed.session_id,
            "message_id": routed.inbound.message_id,
            "confirmation_tokens": confirmation_tokens(routed.inbound.text),
        }
        agent_context = new_agent_context(timeout=int(timeout_seconds * 1_000), metadata=metadata)
        message = Content(role="user", parts=[Part.from_text(text=effective_text)])
        accumulated = ""
        saw_final = False
        total_input = 0
        total_output = 0
        budget = tenant.governance.budget
        run_config = RunConfig(
            streaming=True,
            max_llm_calls=budget.max_llm_calls_per_request,
            max_iterations=budget.max_llm_calls_per_request,
            max_tool_calls=budget.max_tool_calls_per_request,
        )

        recovered = await self._recover_native_result(
            bundle,
            routed,
            agent_context,
        )
        if recovered is not None:
            yield recovered
            return

        try:
            async with asyncio.timeout(timeout_seconds):
                async for event in bundle.runner.run_async(
                    user_id=routed.internal_user_id,
                    session_id=routed.session_id,
                    new_message=message,
                    run_config=run_config,
                    agent_context=agent_context,
                ):
                    usage = getattr(event, "usage_metadata", None)
                    if usage:
                        total_input += int(
                            getattr(usage, "prompt_token_count", 0) or getattr(usage, "input_tokens", 0) or 0
                        )
                        total_output += int(
                            getattr(usage, "candidates_token_count", 0)
                            or getattr(usage, "output_tokens", 0)
                            or 0
                        )
                    if getattr(event, "error_code", None):
                        yield AgentEvent(
                            event_id=event.id or uuid.uuid4().hex,
                            event_type=AgentEventType.ERROR,
                            payload={"error_type": "model_error"},
                            token_input=total_input,
                            token_output=total_output,
                        )
                        return
                    if not event.content or not event.content.parts:
                        continue
                    for part in event.content.parts:
                        if part.function_call:
                            yield AgentEvent(
                                event_id=event.id or uuid.uuid4().hex,
                                event_type=AgentEventType.TOOL_START,
                                tool_name=part.function_call.name,
                                payload={"argument_keys": sorted((part.function_call.args or {}).keys())},
                            )
                        elif part.function_response:
                            yield AgentEvent(
                                event_id=event.id or uuid.uuid4().hex,
                                event_type=AgentEventType.TOOL_RESULT,
                                tool_name=part.function_response.name,
                                payload={"completed": True},
                            )
                        elif part.text:
                            if event.partial:
                                accumulated += part.text
                                yield AgentEvent(
                                    event_id=event.id or uuid.uuid4().hex,
                                    event_type=AgentEventType.TEXT_DELTA,
                                    text=part.text,
                                    partial=True,
                                )
                            else:
                                saw_final = True
                                accumulated = part.text
                                yield AgentEvent(
                                    event_id=event.id or uuid.uuid4().hex,
                                    event_type=AgentEventType.TEXT_FINAL,
                                    text=part.text,
                                    token_input=total_input,
                                    token_output=total_output,
                                )
        except TimeoutError:
            yield AgentEvent(
                event_id=uuid.uuid4().hex,
                event_type=AgentEventType.ERROR,
                payload={"error_type": "model_timeout"},
                token_input=total_input,
                token_output=total_output,
            )
            return
        except Exception as exc:
            logger.warning("tRPC runner failed with %s", exc.__class__.__name__)
            yield AgentEvent(
                event_id=uuid.uuid4().hex,
                event_type=AgentEventType.ERROR,
                payload={"error_type": "model_error"},
                token_input=total_input,
                token_output=total_output,
            )
            return
        if accumulated and not saw_final:
            yield AgentEvent(
                event_id=uuid.uuid4().hex,
                event_type=AgentEventType.TEXT_FINAL,
                text=accumulated,
                token_input=total_input,
                token_output=total_output,
            )

    async def stream(
        self,
        *,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        effective_text: str,
        plane: TenantDataPlane,
    ) -> AsyncIterator[AgentEvent]:
        bundle = await self._bundle(tenant, routed.inbound.app_id, plane)
        try:
            async for event in self._stream_impl(
                bundle=bundle,
                tenant=tenant,
                routed=routed,
                effective_text=effective_text,
                plane=plane,
            ):
                yield event
        finally:
            await self._release_bundle(bundle.cache_key)


class EngineSelector:
    def __init__(self, deterministic: Any, trpc: TrpcAgentEngine) -> None:
        self.deterministic = deterministic
        self.trpc = trpc

    def for_tenant(self, tenant: TenantConfig, app_id: str) -> Any:
        app = tenant.apps[app_id]
        profile = tenant.models[app.model_profile]
        return self.deterministic if profile.provider == "deterministic" else self.trpc

    async def close(self) -> None:
        await self.deterministic.close()
        await self.trpc.close()
