from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import yaml

from scripts.feishu_http_performance import (
    FeishuHTTPPerformanceOptions,
    _callback_url,
    validate_options,
)
from scripts.performance_fixture import _ids, _synthetic_binding
from trpc_service.tenant.models import Channel

ROOT = Path(__file__).resolve().parents[2]
OVERRIDE = ROOT / "deploy" / "performance-runtime.override.yml"


class _ComposeLoader(yaml.SafeLoader):
    pass


_ComposeLoader.add_constructor(
    "!override",
    lambda loader, node: loader.construct_sequence(node, deep=True),
)


def _override() -> dict[str, object]:
    parsed = yaml.load(OVERRIDE.read_text(encoding="utf-8"), Loader=_ComposeLoader)  # noqa: S506
    assert isinstance(parsed, dict)
    return parsed


def test_fixture_secret_refs_match_gateway_synthetic_environment_exactly() -> None:
    ids = _ids("0123456789abcdef0123456789abcdef")
    binding = _synthetic_binding(ids)
    services = _override()["services"]
    assert isinstance(services, dict)
    gateway = services["gateway"]
    assert isinstance(gateway, dict)
    environment = gateway["environment"]
    assert isinstance(environment, dict)

    expected_refs = {
        "app_secret": "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",
        "verification_token": "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",
        "encrypt_key": "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY",
    }
    assert set(binding.secret_refs) == set(expected_refs)
    assert {
        name: ref.uri.removeprefix("env://") for name, ref in binding.secret_refs.items()
    } == expected_refs

    expected_environment_names = set(expected_refs.values())
    actual_environment_names = {
        str(name) for name in environment if str(name).startswith("TRPC_PERF_FIXTURE_UNUSED_")
    }
    assert actual_environment_names == expected_environment_names
    for name in expected_environment_names:
        value = environment[name]
        assert isinstance(value, str)
        assert value.startswith("${" + name + ":?")
        # The override carries unresolved references, never secret material.
        assert "literal://" not in value


def test_fixture_route_fields_fit_feishu_gateway_and_http_helper() -> None:
    ids = _ids("0123456789abcdef0123456789abcdef")
    binding = _synthetic_binding(ids)

    assert binding.channel == Channel.FEISHU
    assert binding.binding_id == ids["binding_id"]
    assert binding.app_id == ids["app_id"]
    assert binding.account_id == ids["account_id"]
    assert binding.binding_id and binding.app_id and binding.account_id

    # Feishu's callback header identifies the provider account.  The agent
    # app_id remains the tenant configuration identity; the HTTP helper must
    # receive the binding account_id for adapter validation.
    options = FeishuHTTPPerformanceOptions(
        base_url="http://127.0.0.1:18080",
        binding_id=binding.binding_id,
        app_id=binding.account_id,
        verification_token="offline-contract-verification-token",
        encrypt_key="offline-contract-encrypt-key",
        total_requests=1,
        rate_per_second=1.0,
        concurrency=1,
        timeout_seconds=1.0,
        run_id="contract",
    )
    validate_options(options)
    assert _callback_url(options.base_url, options.binding_id) == (
        f"http://127.0.0.1:18080/v1/channels/feishu/{quote(binding.binding_id, safe='')}/callback"
    )


def test_performance_override_is_loopback_bounded_and_has_exact_runtime_shape() -> None:
    services = _override()["services"]
    assert isinstance(services, dict)
    gateway = services["gateway"]
    worker = services["worker"]
    outbox = services["outbox-dispatcher"]
    assert isinstance(gateway, dict)
    assert isinstance(worker, dict)
    assert isinstance(outbox, dict)

    ports = gateway["ports"]
    assert ports == [
        "127.0.0.1:${TRPC_PERF_GATEWAY_PORT:?set to an unused local performance gateway port}:8080"
    ]
    assert all(str(port).startswith("127.0.0.1:") for port in ports)
    # Compose scaling is deliberately supplied by the real gate invocation;
    # the override must not hard-code a replica count that can be bypassed or
    # conflict with the caller's staged resource limits.
    assert "replicas" not in worker["deploy"]
    assert "replicas" not in outbox["deploy"]

    disabled = {
        "admin",
        "channel-dispatcher",
        "jaeger",
        "otel-collector",
        "post-turn-projector",
        "prometheus",
        "toxiproxy",
        "wecom-connector",
    }
    assert {
        name
        for name, service in services.items()
        if isinstance(service, dict) and service.get("profiles") == ["performance-disabled"]
    } == disabled
    assert gateway.get("profiles") != ["performance-disabled"]


def test_performance_override_contains_no_resolved_secret_values() -> None:
    source = OVERRIDE.read_text(encoding="utf-8")
    assert "literal://" not in source
    services = _override()["services"]
    assert isinstance(services, dict)
    gateway = services["gateway"]
    assert isinstance(gateway, dict)
    environment = gateway["environment"]
    assert isinstance(environment, dict)
    for name in (
        "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",
        "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",
        "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY",
    ):
        value = environment[name]
        assert isinstance(value, str)
        assert value == value.strip()
        assert value.startswith("${" + name + ":?")
        assert value.endswith("}")
