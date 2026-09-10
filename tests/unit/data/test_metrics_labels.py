import pytest
from trpc_service.metrics.models import DataMetricEvent

def test_data_metrics_are_low_cardinality():
    event=DataMetricEvent(resource_type="memory",backend_type="postgres",operation="put",outcome="committed",duration_ms=1)
    assert set(event.model_dump()) == {"resource_type","backend_type","operation","outcome","duration_ms"}
    with pytest.raises(Exception): DataMetricEvent(resource_type="tenant-alpha",backend_type="postgres",operation="put",outcome="committed",duration_ms=1)
