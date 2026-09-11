from __future__ import annotations

from trpc_service.config.rollout import select_config_version


def test_rollout_selector_is_deterministic_and_honours_boundaries():
    key = ("tenant_one", "wecom", "message_one")
    first = select_config_version(*key, active_version=4, candidate_version=5, candidate_percent=50)
    assert first == select_config_version(*key, active_version=4, candidate_version=5, candidate_percent=50)
    assert select_config_version(*key, active_version=4, candidate_version=5, candidate_percent=1) in {4, 5}
    assert select_config_version(*key, active_version=4, candidate_version=5, candidate_percent=99) in {4, 5}


def test_rollout_selector_separates_channel_and_tenant():
    values = {
        select_config_version("tenant_one",
                              "wecom",
                              f"m{i}",
                              active_version=1,
                              candidate_version=2,
                              candidate_percent=50)
        for i in range(40)
    }
    assert values == {1, 2}
