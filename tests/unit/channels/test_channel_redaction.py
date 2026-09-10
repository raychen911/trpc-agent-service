from __future__ import annotations

import re
from pathlib import Path

from trpc_service.channels.base import ProviderOutcomeUnknown, classify_provider_error
from trpc_service.config.settings import ChannelCredentialSettings
from trpc_service.channels.contracts import Channel
from trpc_service.storage.contracts import SecretBytes


def test_unknown_vendor_exception_is_redacted_and_credentials_are_not_printable() -> None:
    marker = "credential-marker-that-must-not-appear"
    error = classify_provider_error(RuntimeError(marker))
    assert isinstance(error, ProviderOutcomeUnknown)
    assert marker not in str(error)
    credentials = ChannelCredentialSettings(
        channel=Channel.FEISHU,
        app_or_bot_id=SecretBytes(marker.encode()),
        secret=SecretBytes(marker.encode()),
    )
    assert marker not in repr(credentials)


def test_changed_source_has_no_literal_secret_token_ticket_or_access_key_value() -> None:
    roots = [Path("trpc_service"), Path("tests")]
    assignment = re.compile(
        r"""(?ix)
        (?:secret|token|ticket|access_key)
        \s*[:=]\s*
        [\"'][A-Za-z0-9_\-]{16,}[\"']
        """
    )
    violations = []
    for root in roots:
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if assignment.search(text):
                violations.append(str(path))
    assert violations == []
