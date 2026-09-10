"""Agent 执行韧性层：超时重试 + 租户级熔断。"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Awaitable, Callable, Dict

from trpc_service.agent.runner import RunResult
from trpc_service.log import get_logger

# error_type 命中任一子串 → 可重试（SDK error_code 或异常类名）
RETRYABLE_MARKERS = (
    "timeout",
    "connection",
    "rate",
    "unavailable",
    "overloaded",
    "server_error",
    "internal",
)
# 异常类直接命中 → 可重试
RETRYABLE_EXCEPTIONS = (TimeoutError, ConnectionError, OSError)


def _is_retryable(error_type: str) -> bool:
    lowered = (error_type or "").lower()
    return any(marker in lowered for marker in RETRYABLE_MARKERS)


class CircuitBreaker:
    """单租户熔断器（closed → open → half-open → closed）。"""

    def __init__(self, failure_threshold: int, cooldown_seconds: float):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._failures = 0
        self._state = "closed"
        self._opened_at = 0.0

    def allow(self) -> bool:
        """是否放行本次请求；open 且冷却期满 → 转 half-open 放行试探。"""
        if self._state == "closed":
            return True
        if self._state == "open" and time.monotonic() - self._opened_at >= self.cooldown_seconds:
            self._state = "half_open"
            return True
        return self._state == "half_open"

    def record_success(self) -> None:
        self._failures = 0
        self._state = "closed"

    def record_failure(self) -> None:
        self._failures += 1
        if self._state == "half_open" or self._failures >= self.failure_threshold:
            self._state = "open"
            self._opened_at = time.monotonic()

    @property
    def state(self) -> str:
        return self._state


class ResiliencePolicy:
    """租户级重试 + 熔断编排（进程内视角，熔断状态不跨节点共享）。"""

    def __init__(
        self,
        max_retries: int = 2,
        retry_backoff_seconds: float = 0.5,
        failure_threshold: int = 5,
        cooldown_seconds: float = 30.0,
    ):
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._failure_threshold = failure_threshold
        self._cooldown_seconds = cooldown_seconds

    def _breaker(self, tenant_id: str) -> CircuitBreaker:
        breaker = self._breakers.get(tenant_id)
        if breaker is None:
            breaker = CircuitBreaker(self._failure_threshold, self._cooldown_seconds)
            self._breakers[tenant_id] = breaker
        return breaker

    def breaker_state(self, tenant_id: str) -> str:
        return self._breaker(tenant_id).state

    async def execute(
        self,
        tenant_id: str,
        run_fn: Callable[..., Awaitable[RunResult]],
        *args,
        **kwargs,
    ) -> RunResult:
        """熔断检查 → 重试执行 → 成功/最终失败回写熔断器。"""
        breaker = self._breaker(tenant_id)
        if not breaker.allow():
            get_logger("agent.resilience").warning(
                "circuit open, reject fast tenant=%s", tenant_id
            )
            return RunResult(text="", error_type="circuit_open")

        attempts = self.max_retries + 1
        delay = self.retry_backoff_seconds
        last: RunResult = RunResult(text="", error_type="unknown")
        for attempt in range(attempts):
            try:
                result = await run_fn(*args, **kwargs)
            except RETRYABLE_EXCEPTIONS as exc:
                last = RunResult(text="", error_type=type(exc).__name__)
                retryable = True
            except Exception as exc:  # noqa: BLE001  非可重试异常直接失败
                breaker.record_failure()
                return RunResult(text="", error_type=type(exc).__name__, error_message=str(exc))
            else:
                last = result
                retryable = bool(result.error_type) and _is_retryable(result.error_type)

            if not last.error_type:
                breaker.record_success()
                return last
            if not retryable or attempt == attempts - 1:
                breaker.record_failure()
                return last
            await asyncio.sleep(delay)
            delay *= 2
        breaker.record_failure()
        return last

    def reset(self, tenant_id: str = "") -> None:
        if tenant_id:
            self._breakers.pop(tenant_id, None)
        else:
            self._breakers.clear()


def _policy_from_env() -> ResiliencePolicy:
    return ResiliencePolicy(
        max_retries=int(os.getenv("LLM_RETRY_MAX", "2")),
        retry_backoff_seconds=float(os.getenv("LLM_RETRY_BACKOFF", "0.5")),
        failure_threshold=int(os.getenv("CIRCUIT_FAILURE_THRESHOLD", "5")),
        cooldown_seconds=float(os.getenv("CIRCUIT_COOLDOWN", "30")),
    )


resilience_policy = _policy_from_env()
