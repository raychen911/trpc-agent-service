"""
Rate Limiter, Sliding Window Pacer, and Retry Queue for IM Channels.
Handles platform frequency limits (e.g. 20 msgs/sec for WeCom, 30 msgs/sec for TG)
and manages Dead Letter Queues (DLQ).
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, List, Optional
import logging
from trpc_service.config.models import OutboundMessage

logger = logging.getLogger(__name__)


@dataclass
class RetryItem:
    message: OutboundMessage
    binding_id: str
    attempt_count: int = 0
    max_attempts: int = 3
    next_retry_time: float = 0.0
    last_error: Optional[str] = None


class TokenBucketLimiter:
    """
    Token Bucket rate limiter for per-channel outbound traffic pacing.
    """

    def __init__(self, rate_per_second: float = 20.0, capacity: float = 40.0):
        self.rate = rate_per_second
        self.capacity = capacity
        self.tokens = capacity
        self.last_timestamp = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_timestamp
            self.last_timestamp = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False

    async def wait_for_token(self, timeout: float = 5.0) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            if await self.acquire():
                return True
            await asyncio.sleep(0.05)
        return False


class ChannelRetryQueue:
    """
    Retry Queue with Exponential Backoff and Dead Letter Queue (DLQ).
    """

    def __init__(self):
        self.retry_items: Deque[RetryItem] = deque()
        self.dead_letter_queue: List[RetryItem] = []
        self._lock = asyncio.Lock()

    async def enqueue(self, message: OutboundMessage, binding_id: str, error: str = ""):
        async with self._lock:
            item = RetryItem(
                message=message,
                binding_id=binding_id,
                attempt_count=1,
                next_retry_time=time.time() + 1.0,  # 1s initial delay
                last_error=error,
            )
            self.retry_items.append(item)
            logger.warning("[RetryQueue] Enqueued message %s for retry. Error: %s", message.trace_id, error)

    async def process_retries(self, send_fn: Callable[[OutboundMessage, str], bool]):
        """Process pending items ready for retry."""
        now = time.time()
        async with self._lock:
            pending_count = len(self.retry_items)
            for _ in range(pending_count):
                item = self.retry_items.popleft()
                if now < item.next_retry_time:
                    self.retry_items.append(item)
                    continue

                # Attempt send
                try:
                    success = send_fn(item.message, item.binding_id)
                    if success:
                        logger.info("[RetryQueue] Retry succeeded for message %s", item.message.trace_id)
                        continue
                except Exception as e:
                    item.last_error = str(e)

                item.attempt_count += 1
                if item.attempt_count > item.max_attempts:
                    # Move to Dead Letter Queue
                    self.dead_letter_queue.append(item)
                    logger.error("[RetryQueue] Message %s moved to DLQ after %d attempts.",
                                 item.message.trace_id, item.max_attempts)
                else:
                    # Exponential backoff (1s, 2s, 4s...)
                    item.next_retry_time = time.time() + (2.0 ** (item.attempt_count - 1))
                    self.retry_items.append(item)


channel_retry_queue = ChannelRetryQueue()
