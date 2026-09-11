"""Validate dimensions and avoid multiplying total message tokens by model calls twice."""

from tests.benchmarks.capacity import project_requirements


def test_capacity_projection_uses_total_message_tokens_once():
    result = project_requirements(10, 2, 4, 1500, 3, 20, .5)
    assert result == {"mean_inflight": 20, "tail_headroom_inflight": 40, "planning_workers": 4,
                      "required_model_tpm": 900000, "required_model_rpm": 1800}
