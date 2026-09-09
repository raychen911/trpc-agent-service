# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Compatibility helpers for every supported Python version."""

from enum import Enum


class StrEnum(str, Enum):
    """Python 3.10-compatible subset of enum.StrEnum."""

    def __str__(self) -> str:
        return self.value
