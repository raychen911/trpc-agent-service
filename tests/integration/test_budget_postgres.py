"""PostgreSQL row locks arbitrate separate Worker connections against one budget."""

from tests.integration.test_im_postgres import postgres as postgres_fixture
from tests.reliability.test_budget import seed, contend

postgres = postgres_fixture


def test_postgres_two_workers_cannot_exceed_shared_budget(postgres):
    ledger = seed(postgres)
    contend(ledger)
