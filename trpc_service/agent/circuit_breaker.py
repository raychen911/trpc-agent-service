"""
Circuit Breaker and Automated Fallback Strategy Manager.
Protects against LLM timeout/429, Tool execution failures, and database unavailability.
"""

import time
from enum import Enum
from typing import Any, Callable, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Tripped, calls fast-fail or divert to fallback
    HALF_OPEN = "HALF_OPEN"  # Testing if dependency has recovered


class CircuitBreaker:
    """
    Standard Circuit Breaker implementing the CLOSED -> OPEN -> HALF_OPEN state machine.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 30.0,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout_seconds
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_state_change = time.time()

    def record_success(self):
        """Record successful execution."""
        if self.state == CircuitState.HALF_OPEN:
            logger.info("[CircuitBreaker] '%s' recovered! Resetting to CLOSED.", self.name)
            self.state = CircuitState.CLOSED
        self.failure_count = 0

    def record_failure(self):
        """Record failed execution and trip circuit if threshold exceeded."""
        self.failure_count += 1
        logger.warning("[CircuitBreaker] '%s' recorded failure (%d/%d).",
                       self.name, self.failure_count, self.failure_threshold)
        if self.failure_count >= self.failure_threshold and self.state == CircuitState.CLOSED:
            self.state = CircuitState.OPEN
            self.last_state_change = time.time()
            logger.error("[CircuitBreaker] '%s' tripped! State is now OPEN for %.1fs.",
                         self.name, self.recovery_timeout)

    def allow_execution(self) -> bool:
        """Check if request is allowed through."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            if time.time() - self.last_state_change > self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                logger.info("[CircuitBreaker] '%s' entering HALF_OPEN trial.", self.name)
                return True
            return False

        # HALF_OPEN allows single test trial
        return True


class FallbackManager:
    """
    Provides fallback handlers when components fail or time out.
    """

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}

    def get_breaker(self, resource_name: str) -> CircuitBreaker:
        if resource_name not in self._breakers:
            self._breakers[resource_name] = CircuitBreaker(resource_name)
        return self._breakers[resource_name]

    def resolve_model_fallback(self, primary_provider: str) -> str:
        """Fallback routing: OpenAI -> Qwen -> Mock."""
        fallbacks = {
            "openai": "qwen",
            "deepseek": "qwen",
            "qwen": "mock",
        }
        return fallbacks.get(primary_provider.lower(), "mock")

    def tool_failure_graceful_message(self, tool_name: str, error_msg: str) -> str:
        """Generate graceful degradation prompt hint for LLM when a tool fails."""
        return (
            f"系统提示：调用外部工具【{tool_name}】遇到网络或执行异常（{error_msg}）。"
            f"请告知用户该功能暂时受限，并基于已有知识尽量提供帮助。"
        )


fallback_manager = FallbackManager()
