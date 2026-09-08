# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Assemble the 3-tenant SaaS demo gateway app."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from trpc_service.web.app import build_app

from agent import create_agent

_TENANTS_CONFIG = os.path.join(os.path.dirname(__file__), "tenants.yaml")


def build_demo_app():
    """Build the demo app using the example's (mock-capable) agent factory."""
    return build_app(tenants_path=_TENANTS_CONFIG, agent_factory=create_agent)


app = build_demo_app()
