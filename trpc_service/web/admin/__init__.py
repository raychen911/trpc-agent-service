# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Administrative HTTP API for tenant configuration and audit queries."""

from ._app import create_admin_router

__all__ = ["create_admin_router"]
