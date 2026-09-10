from trpc_service.config.settings import load_runtime_settings
from trpc_service.storage.contracts import ConfigurationUnavailable, StateBackendUnavailable
from trpc_service.web.errors import map_platform_error


def test_settings_errors_and_representations_do_not_expose_credentials_or_vendor_details() -> None:
    secret = "never-print-" + "this-password"
    settings = load_runtime_settings({"TRPC_RUNTIME_PROFILE": "shared", "TRPC_NODE_ID": "node",
                                      "TRPC_SHARED_REDIS_URL": f"redis://:{secret}@127.0.0.1/0",
                                      "TRPC_SHARED_DATABASE_URL": f"postgresql+asyncpg://u:{secret}@127.0.0.1/db"})
    rendered = repr(settings)
    assert secret not in rendered and "**********" in rendered
    for error in (ConfigurationUnavailable("private postgres host"), StateBackendUnavailable("private redis host")):
        mapped = map_platform_error(error)
        assert "private" not in mapped.message and "redis" not in mapped.message.lower() and "postgres" not in mapped.message.lower()
