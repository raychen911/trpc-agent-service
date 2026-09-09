# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Exceptions shared by gateway admission and persistence layers."""


class MigrationTransitionError(RuntimeError):
    """Tenant admission is briefly paused at a storage route boundary."""
