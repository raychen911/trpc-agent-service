"""Retry wrapper for outbound IM delivery with a dead-letter hook."""

from __future__ import annotations

from time import sleep
from typing import Callable
import json
from pathlib import Path
from collections import defaultdict, deque
from threading import RLock
from time import monotonic
import time
import os

from trpc_service.channels.base import SendResult
from trpc_service.security.identifiers import filesystem_component
from trpc_service.security.secrets import redact_secret_data, redact_secret_text


def split_text(text: str, max_length: int) -> list[str]:
    """Split long provider-bound text without dropping content."""
    if max_length <= 0 or len(text) <= max_length:
        return [text]
    return [text[index : index + max_length] for index in range(0, len(text), max_length)]


class ChannelRateLimiter:
    """Channel/account limiter backed by Redis when the service is clustered."""

    def __init__(self, redis_url: str | None = None, prefix: str = "trpc-agent") -> None:
        self._windows: dict[str, deque[float]] = defaultdict(deque)
        self._lock = RLock()
        self._redis = None
        self._prefix = prefix
        url = redis_url or os.getenv("REDIS_URL", "").strip()
        self._coordination_required = bool(url and os.getenv("REQUIRE_SHARED_COORDINATION", "0") == "1")
        if url:
            try:
                import redis

                self._redis = redis.Redis.from_url(url, decode_responses=True)
                self._redis.ping()
            except Exception as exc:
                self._redis = None
                if self._coordination_required:
                    raise RuntimeError("shared channel rate coordination backend is unavailable") from exc

    def acquire(self, key: str, limit: int) -> None:
        if limit <= 0:
            return
        if self._redis is not None:
            # Redis keys are shared by processes and hosts. A monotonic clock
            # has a different origin in every process, so it cannot identify
            # the same wall-clock window across a cluster.
            second = int(time.time())
            redis_key = f"{self._prefix}:channel-rate:{key}:{second}"
            pipe = self._redis.pipeline(transaction=True)
            pipe.incr(redis_key)
            pipe.expire(redis_key, 2)
            count, _ = pipe.execute()
            if int(count) > limit:
                raise RuntimeError("channel rate limit exceeded")
            return
        now = monotonic()
        with self._lock:
            window = self._windows[key]
            while window and now - window[0] >= 1.0:
                window.popleft()
            if len(window) >= limit:
                raise RuntimeError("channel rate limit exceeded")
            window.append(now)


def send_with_retry(
    sender: Callable[[], SendResult], attempts: int = 3, dead_letter: Callable[[SendResult], None] | None = None
) -> SendResult:
    last = SendResult(False, "", "delivery not attempted")
    for attempt in range(attempts):
        try:
            last = sender()
        except Exception as exc:
            last = SendResult(False, "", redact_secret_text(f"{type(exc).__name__}: {exc}"))
        if last.ok:
            return last
        if attempt + 1 < attempts:
            sleep(0.25 * (2**attempt))
    if dead_letter:
        dead_letter(last)
    return last


def persist_dead_letter(
    channel: str, account_id: str, message, result: SendResult, root: str = "data/dead-letter"
) -> None:
    target = Path(root) / filesystem_component(channel, "channel") / filesystem_component(account_id, "account_id")
    target.mkdir(parents=True, exist_ok=True)
    name = (
        f"{filesystem_component(message.session_id, 'session_id')}-"
        f"{filesystem_component(message.external_user_id, 'external_user_id')}.json"
    )
    (target / name).write_text(
        json.dumps(
            redact_secret_data(
                {
                    "message": (
                        message.__dict__
                        if hasattr(message, "__dict__")
                        else {"text": message.text, "session_id": message.session_id}
                    ),
                    "result": {"ok": result.ok, "error": result.error, "response_ref": result.response_ref},
                }
            ),
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )
