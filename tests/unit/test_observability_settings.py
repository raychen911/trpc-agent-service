"""T090: bounded observability/release settings reject illegal values.

Ceiling above 25%, zero/negative buffer capacity, resolve window below the
probe TTL or an overhead ceiling above 10% must fail configuration loudly
instead of being silently corrected (FR-008, FR-009, FR-018, NFR-007,
DEC-001, DEC-002).
"""

from __future__ import annotations

import pytest

from trpc_service.config.settings import (
    ConfigurationError,
    load_observability_settings,
)


def test_defaults_are_bounded_and_sane() -> None:
    settings = load_observability_settings({})
    assert settings.sampling_default_rate == 0.10
    assert settings.sampling_tenant_ceiling == 0.25
    assert settings.buffer_capacity == 10_000
    assert settings.alert_fire_window_observations == 3
    assert settings.alert_resolve_window_seconds == 60
    assert settings.capacity_overhead_ceiling_pct == 10.0
    assert settings.otlp_endpoint is None


def test_legal_overrides_load() -> None:
    settings = load_observability_settings(
        {
            "TRPC_OBS_SAMPLING_DEFAULT": "0.2",
            "TRPC_OBS_SAMPLING_CEILING": "0.2",
            "TRPC_OBS_BUFFER_CAPACITY": "5000",
            "TRPC_OTLP_ENDPOINT": "http://collector:4318",
        }
    )
    assert settings.sampling_default_rate == 0.2
    assert settings.sampling_tenant_ceiling == 0.2
    assert settings.buffer_capacity == 5000
    assert settings.otlp_endpoint == "http://collector:4318"


@pytest.mark.parametrize(
    "environ",
    [
        {"TRPC_OBS_SAMPLING_CEILING": "0.30"},  # above the 25% hard ceiling
        {"TRPC_OBS_SAMPLING_DEFAULT": "0"},  # non-positive rate
        {"TRPC_OBS_BUFFER_CAPACITY": "0"},  # zero capacity
        {"TRPC_OBS_BUFFER_CAPACITY": "-10"},
        {"TRPC_OBS_ALERT_RESOLVE_S": "10"},  # below the 30s probe TTL
        {"TRPC_OBS_CAPACITY_OVERHEAD": "15"},  # above the 10% DEC-005 ceiling
    ],
)
def test_illegal_values_are_rejected_not_corrected(environ: dict[str, str]) -> None:
    with pytest.raises(ConfigurationError):
        load_observability_settings(environ)
