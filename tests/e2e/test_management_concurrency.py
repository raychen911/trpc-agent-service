"""The database, rather than a process lock, arbitrates publication."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from sqlalchemy import select

from tests.conftest import build_tenant
from trpc_service.management import ConfigurationStore, ConfigDraft, ConfigConflict, ProfileRegistration
from trpc_service.persistence.models import AuditLogRow


def test_platform_completion_atomicity(platform_database):
    from tests.reliability.test_phase_two_completion import (
        test_finalization_rolls_back_all_platform_completion_writes as verify,
    )
    verify(platform_database)


def test_only_one_concurrent_publication_commits(platform_database, monkeypatch):
    monkeypatch.delenv("TRPC_RUNTIME_MODE", raising=False)
    store = ConfigurationStore(platform_database)
    config = build_tenant()
    profiles = {}
    for resource, ref in config.data_backends.model_dump(mode="json").items():
        profile = profiles.setdefault(ref["profile_id"], dict(
            profile_id=ref["profile_id"], profile_revision=1, kind=ref["kind"], provider="test",
            endpoint="localhost", secret_ref={"uri": "env://BACKEND_URL"}, capabilities=[]))
        profile["capabilities"].append(resource)
    for profile in profiles.values():
        store.register_profile(ProfileRegistration.model_validate(profile), "operator")
    for version in (3, 4):
        draft = ConfigDraft(config=config.model_copy(update={"config_version": version}),
                            profile_revisions={role: 1 for role in type(config.data_backends).model_fields})
        store.save_draft(draft, "operator")
    barrier = Barrier(2)

    def publish(version):
        barrier.wait(timeout=10)
        try:
            store.publish(config.tenant_id, version, 0, "operator")
            return version
        except ConfigConflict:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(publish, (3, 4)))
    winner, = [version for version in results if version is not None]
    assert store.active()[0].config_version == winner
    with platform_database.sessions() as session:
        audits = list(session.scalars(select(AuditLogRow).where(AuditLogRow.decision == "config_published")))
        assert len(audits) == 1 and audits[0].config_version == winner
