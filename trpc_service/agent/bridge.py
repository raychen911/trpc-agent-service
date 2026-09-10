"""Bridge between the platform layer and the Agent runtime."""

from __future__ import annotations

import importlib
import inspect
import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from trpc_service.storage.factory import StorageBundle
from trpc_service.telemetry.tracing import TraceRecorder


@dataclass(slots=True)
class RuntimeBridgeSpec:
    """Describe how to load a runtime provider.

    ``trpc`` is the production default and uses the installed
    tRPC-Agent-Python runtime. ``local`` is an explicit demo/test mode.
    ``external`` is an explicit application-owned runtime factory.
    """

    mode: str = "trpc"
    factory_path: str = ""
    settings: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> RuntimeBridgeSpec:
        settings_raw = os.getenv("TRPC_AGENT_RUNTIME_SETTINGS_JSON", "").strip()
        settings: dict[str, Any] = {}
        if settings_raw:
            settings = json.loads(settings_raw)
            if not isinstance(settings, dict):
                raise ValueError("TRPC_AGENT_RUNTIME_SETTINGS_JSON must be a JSON object")
        return cls(
            mode=os.getenv("TRPC_AGENT_RUNTIME_MODE", "trpc").strip().lower() or "trpc",
            factory_path=os.getenv("TRPC_AGENT_RUNTIME_FACTORY", "").strip(),
            settings={str(key): value for key, value in settings.items()},
        )


def build_runtime_workers(
    storage: StorageBundle,
    telemetry: TraceRecorder | None = None,
    model_client: Any | None = None,
    spec: RuntimeBridgeSpec | None = None,
) -> list[Any]:
    """Build one or more worker-like runtimes.

    An external runtime factory can return a single worker or an iterable of
    workers. Each object only needs a ``run(request, config, storage)`` method.
    """

    spec = spec or RuntimeBridgeSpec.from_env()
    if spec.mode not in {"trpc", "external", "local", "auto"}:
        raise RuntimeError(f"unsupported TRPC_AGENT_RUNTIME_MODE: {spec.mode}")
    if spec.mode == "local":
        from trpc_service.gateway.router import AgentWorker

        return [AgentWorker(storage, telemetry, model_client)]
    if spec.mode == "trpc":
        from trpc_service.agent.trpc_runtime import TrpcAgentWorker

        return [TrpcAgentWorker(storage, telemetry, settings=spec.settings)]
    if spec.mode == "auto" and not spec.factory_path:
        # Backward-compatible opt-in alias for old local demos. The default
        # mode is trpc, so production never silently falls back.
        from trpc_service.gateway.router import AgentWorker

        return [AgentWorker(storage, telemetry, model_client)]
    external = _load_external_runtime(storage, telemetry, model_client, spec)
    if external is not None:
        if isinstance(external, (list, tuple)):
            workers = list(external)
        else:
            workers = [external]
        if not workers:
            raise RuntimeError("external runtime factory returned no workers")
        for worker in workers:
            if not hasattr(worker, "run"):
                raise RuntimeError("external runtime workers must expose a run() method")
        return workers
    raise RuntimeError("external runtime was not loaded")


def build_runtime_worker(
    storage: StorageBundle,
    telemetry: TraceRecorder | None = None,
    model_client: Any | None = None,
    spec: RuntimeBridgeSpec | None = None,
) -> Any:
    """Return the first runtime worker for single-process entry points."""

    return build_runtime_workers(storage, telemetry, model_client, spec)[0]


def _load_external_runtime(
    storage: StorageBundle,
    telemetry: TraceRecorder | None,
    model_client: Any | None,
    spec: RuntimeBridgeSpec,
) -> Any | None:
    if not spec.factory_path:
        if spec.mode == "external":
            raise RuntimeError("TRPC_AGENT_RUNTIME_FACTORY is required in external mode")
        return None

    module_path, _, attr_name = spec.factory_path.rpartition(":")
    if not module_path or not attr_name:
        raise RuntimeError("TRPC_AGENT_RUNTIME_FACTORY must look like 'package.module:factory'")
    try:
        module = importlib.import_module(module_path)
        factory = getattr(module, attr_name)
    except Exception as exc:
        if spec.mode in {"external", "auto"}:
            raise RuntimeError(f"failed to load external runtime factory: {spec.factory_path}") from exc
        return None

    try:
        return _invoke_factory(factory, storage, telemetry, model_client, spec.settings)
    except Exception:
        if spec.mode in {"external", "auto"}:
            raise
        return None


def _invoke_factory(
    factory: Callable[..., Any],
    storage: StorageBundle,
    telemetry: TraceRecorder | None,
    model_client: Any | None,
    settings: dict[str, Any],
) -> Any:
    kwargs = {
        "storage": storage,
        "telemetry": telemetry,
        "model_client": model_client,
        "settings": settings,
    }
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        signature = None
    if signature is not None:
        accepted = {}
        has_var_kw = False
        for parameter in signature.parameters.values():
            if parameter.kind == inspect.Parameter.VAR_KEYWORD:
                has_var_kw = True
                break
            if parameter.name in kwargs:
                accepted[parameter.name] = kwargs[parameter.name]
        if has_var_kw:
            accepted = kwargs
        return factory(**accepted)
    return factory(**kwargs)
