from datetime import datetime, timedelta, timezone
import pytest
from trpc_service.storage.data_models import ArtifactUpload, UploadStatus

def test_artifact_upload_is_immutable_and_has_ttl_state() -> None:
    upload = ArtifactUpload(tenant_id="tenant-alpha", upload_id="u", artifact_id="a", temp_ref_digest="a"*64, expected_digest="b"*64, status=UploadStatus.STAGED, expires_at=datetime.now(timezone.utc)+timedelta(hours=1))
    assert upload.status is UploadStatus.STAGED
    with pytest.raises(Exception): upload.status = UploadStatus.VERIFIED
