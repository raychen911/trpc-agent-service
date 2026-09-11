"""Budget controls inherit operator auth and return exact, tenant-scoped amounts."""

from tests.reliability.test_management import managed as managed_fixture
from tests.reliability.test_budget import seed, reserve

managed = managed_fixture


def test_budget_api_auth_precision_scope_and_revision(managed):
    client, db = managed
    ledger = seed(db)
    path = "/admin/tenants/tenant_acme"
    assert client.get(path + "/budgets", headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert client.get("/admin/tenants/tenant_other/budgets").json() == []
    assert client.get(path + "/budgets").json()[0]["daily_limit"] == "1.0000000000"
    update = {"daily_limit": "2", "monthly_limit": "10", "expected_revision": 1, "timezone": "Asia/Shanghai"}
    assert client.put(path + "/budgets/CNY", json=update).json() == {"revision": 2}
    assert client.put(path + "/budgets/CNY", json=update).status_code == 409
    update["daily_limit"] = 0.1
    assert client.put(path + "/budgets/CNY", json=update).status_code == 422
    reserve(ledger)
    ledger.mark_sent("tenant_acme", "attempt-1")
    ledger.pending("tenant_acme", "attempt-1")
    attempts = client.get(path + "/model-attempts?status=pending_reconciliation").json()
    assert len(attempts) == 1 and attempts[0]["actual_amount"] is None
    assert "request_hash" not in attempts[0] and "evidence_hash" not in attempts[0]
    assert client.get("/admin/tenants/tenant_other/model-attempts").json() == []
    assert client.get(path + "/budget-accounts").json()[0]["reserved"] == "0.6000000000"
    assert client.post(path + "/model-attempts/attempt-1/settle", json={"cost": "0"}).status_code == 404


def test_model_prices_are_immutable_and_validation_does_not_echo_input(managed):
    client, db = managed
    seed(db)
    path = "/admin/tenants/tenant_acme/model-prices"
    price = {
        "price_id": "price-v1",
        "model_id": "model-test",
        "currency": "CNY",
        "input_per_million": "1",
        "output_per_million": "2"
    }
    assert client.post(path, json=price).status_code == 201
    price["input_per_million"] = "3"
    assert client.post(path, json=price).status_code == 409
    price["input_per_million"] = "synthetic-pasted-secret"
    response = client.post(path, json=price)
    assert response.status_code == 422 and "synthetic-pasted-secret" not in response.text
