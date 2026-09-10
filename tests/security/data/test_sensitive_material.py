from trpc_service.storage.canonical import content_digest

def test_sensitive_value_is_represented_by_digest_only():
    marker="sensitive-test-marker"
    assert marker not in content_digest(marker)
