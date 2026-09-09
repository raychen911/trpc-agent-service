# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Dependency-free Prometheus text registry for platform-level metrics."""

from __future__ import annotations

import threading
from collections import defaultdict


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


class MetricsRegistry:
    """Small registry with an intentionally bounded label vocabulary."""

    def __init__(self) -> None:
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
        self._lock = threading.Lock()

    def increment(self, name: str, value: float = 1, **labels: str) -> None:
        forbidden = {"user", "user_id", "session", "session_id", "request_id", "trace_id"}
        invalid = forbidden & set(labels)
        if invalid:
            raise ValueError(f"high-cardinality metric labels are forbidden: {sorted(invalid)}")
        key = (name, tuple(sorted((str(k), str(v)) for k, v in labels.items())))
        with self._lock:
            self._counters[key] += value

    def observe(self, name: str, value: float, **labels: str) -> None:
        """Expose a dependency-free summary as ``_count`` and ``_sum``."""
        self.increment(f"{name}_count", 1, **labels)
        self.increment(f"{name}_sum", value, **labels)

    def render(self) -> str:
        lines: list[str] = []
        with self._lock:
            items = sorted(self._counters.items())
        for (name, labels), value in items:
            suffix = ""
            if labels:
                suffix = "{" + ",".join(f'{key}="{_escape(label)}"' for key, label in labels) + "}"
            lines.append(f"{name}{suffix} {value:g}")
        return "\n".join(lines) + ("\n" if lines else "")
