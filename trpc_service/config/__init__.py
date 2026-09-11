"""Configuration and secret-resolution primitives.

Configuration deliberately contains references to secrets, never secret material.  The
provider is injected at process startup so the same release snapshot can be used by a
mock, environment variables, or a managed secret service.
"""

from .settings import (
    AppSettings,
    EnvironmentSecretProvider,
    MockSecretProvider,
    SecretNotFoundError,
    SecretProvider,
    Settings,
    get_settings,
    parse_secret_json,
)

__all__ = [
    "AppSettings",
    "EnvironmentSecretProvider",
    "MockSecretProvider",
    "SecretNotFoundError",
    "SecretProvider",
    "Settings",
    "get_settings",
    "parse_secret_json",
]
