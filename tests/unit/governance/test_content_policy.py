from __future__ import annotations

from trpc_service.governance import content


def test_content_policy_redacts_marker_without_storing_original() -> None:
    result = content.inspect("contact test@example.com", rules={"email": "redact"})
    assert result.action.value == "redact"
    assert "test@example.com" not in result.safe_text
    assert all("test@example.com" not in str(item) for item in result.findings)
