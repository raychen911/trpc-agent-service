from trpc_service.storage.data_models import KnowledgeDocument, KnowledgeStatus

def test_knowledge_defaults_to_pending_index() -> None:
    document = KnowledgeDocument(tenant_id="tenant-alpha", document_id="d", metadata={}, content_digest="a"*64)
    assert document.index_status is KnowledgeStatus.PENDING_INDEX
