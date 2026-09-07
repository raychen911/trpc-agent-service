"""Tests for the service package boundary and command-line entry point."""

from __future__ import annotations

import importlib
from pathlib import Path

import uvicorn

from trpc_service import _cli


def test_cli_runs_gateway_with_selected_options(monkeypatch, tmp_path):
    calls = []
    tenants_path = tmp_path / "tenants.yaml"
    monkeypatch.setenv("TENANTS_CONFIG", "previous.yaml")
    monkeypatch.setattr(uvicorn, "run", lambda *args, **kwargs: calls.append((args, kwargs)))

    _cli.main([
        "--host",
        "127.0.0.1",
        "--port",
        "9090",
        "--tenants-config",
        str(tenants_path),
        "--reload",
    ])

    assert calls == [(('trpc_service.web.app:app', ), {
        "host": "127.0.0.1",
        "port": 9090,
        "reload": True,
    })]
    assert _cli.os.environ["TENANTS_CONFIG"] == str(tenants_path)


def test_service_packages_are_importable_without_vendored_sdk():
    project_root = Path(__file__).resolve().parents[2]
    assert not (project_root / "trpc_agent_sdk").exists()

    for module_name in (
            "trpc_service",
            "trpc_service.agent",
            "trpc_service.channels",
            "trpc_service.config",
            "trpc_service.log",
            "trpc_service.metrics",
            "trpc_service.skill",
            "trpc_service.tenant",
            "trpc_service.tool",
            "trpc_service.web",
            "trpc_service.workspace",
    ):
        assert importlib.import_module(module_name)
