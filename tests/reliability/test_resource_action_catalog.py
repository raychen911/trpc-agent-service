"""Validate the deployed file action rather than a refund placeholder."""

import pytest

from trpc_service.governance.resource_actions import prepare_write


def test_file_proposal_describes_an_immutable_session_scoped_write():
    args = {"filename": "note.txt", "text": "private document contents"}
    normalized, preconditions, summary = prepare_write(args)
    assert normalized == args
    assert preconditions == {"operation": "append_immutable_version"}
    assert "private document contents" not in str(summary)


@pytest.mark.parametrize("args", [
    {
        "filename": "../other.txt",
        "text": "x"
    },
    {
        "filename": "note.txt",
        "text": "x",
        "tenant_id": "other"
    },
    {
        "filename": "note.txt",
        "text": "x" * 100_001
    },
])
def test_file_proposal_rejects_path_scope_and_unbounded_payload(args):
    with pytest.raises(ValueError):
        prepare_write(args)
