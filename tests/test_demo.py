"""Local demo data is useful, deterministic, and inert by default."""

from trpc_service.demo import build_demo_tenant_spec


def test_demo_tenant_contains_no_live_credentials_or_routes() -> None:
    spec = build_demo_tenant_spec()

    assert spec.tenant_id == "tenant-demo"
    assert spec.apps[0].model.provider == "mock"
    assert spec.apps[0].tools.allowed == frozenset({"preload_memory"})
    assert spec.apps[0].governance.redact_sensitive_data
    assert spec.channels
    assert all(not channel.enabled for channel in spec.channels)
    assert all(
        value.startswith("secret://env/")
        for channel in spec.channels
        for value in channel.secret_refs.values()
    )
    assert all(channel.identity_policy.default_action == "deny" for channel in spec.channels)
