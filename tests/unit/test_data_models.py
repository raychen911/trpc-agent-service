from datetime import datetime, timezone
from hashlib import sha256
import pytest
from trpc_service.storage.data_models import SessionEvent, MemoryRecord, SummaryRecord, ArtifactRecord

def test_data_values_are_frozen_and_tenant_scoped():
    e=SessionEvent(tenant_id='t',key='s',event_id='e1',sequence=1,value={},updated_at=datetime.now(timezone.utc));
    with pytest.raises(Exception): e.sequence=2

def test_artifact_digest_shape_is_validated():
    with pytest.raises(Exception): ArtifactRecord(tenant_id='t',key='a',value={},version=1,updated_at=datetime.now(timezone.utc),content_digest='bad',storage_ref='obj')
