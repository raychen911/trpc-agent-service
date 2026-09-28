from uuid import uuid4

from trpc_service.agent.configuration import select_agent_config_version
from trpc_service.agent.models import AgentApp


def test_canary_selection_is_sticky_and_keeps_stable_fallback() -> None:
    """Every node derives the same bounded cohort from the principal identity."""

    agent = AgentApp(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        name="Canary Agent",
        stable_config_version=3,
        canary_config_version=4,
        canary_percent=50,
    )
    selected = {select_agent_config_version(agent, f"principal-{index}") for index in range(100)}

    assert selected == {3, 4}
    assert select_agent_config_version(agent, "principal-1") == select_agent_config_version(
        agent, "principal-1")
    assert select_agent_config_version(agent, None) == 3
