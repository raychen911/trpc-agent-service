# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Small ownership boundary for sync and async runtime resources."""

from __future__ import annotations

import inspect
import logging
from typing import Any

logger = logging.getLogger(__name__)


class RuntimeResources:
    """Close owned resources once, in reverse construction order."""

    def __init__(self, *resources: Any) -> None:
        self._resources: list[Any] = []
        self._closed = False
        for resource in resources:
            self.add(resource)

    def add(self, resource: Any) -> Any:
        if resource is not None and not any(resource is item for item in self._resources):
            self._resources.append(resource)
        return resource

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for resource in reversed(self._resources):
            close = getattr(resource, "close", None)
            if not callable(close):
                continue
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except Exception:  # noqa: BLE001 - one failed close must not leak the rest
                logger.exception("failed to close runtime resource %s", type(resource).__name__)
