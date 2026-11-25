# API layer tests (endpoints, validation, error handling)
from datetime import date, timedelta


def make_account(client, balance=0):
    r = client.post("/api/accounts", json={"owner_name": "Emir", "currency": "try",
                                           "opening_balance": balance})
    assert r.status_code == 201
    return r.json()


def test_create_and_get_account(client):
    acc = make_account(client, 100)
    assert acc["currency"] == "TRY"
    r = client.get(f"/api/accounts/{acc['id']}")
    assert r.status_code == 200 and r.json()["balance"] == "100.00"
    assert len(client.get("/api/accounts").json()) == 1


def test_account_validation(client):
    r = client.post("/api/accounts", json={"owner_name": " ", "currency": "GBP", "opening_balance": -5})
    assert r.status_code == 422
    fields = {d["field"] for d in r.json()["details"]}
    assert {"owner_name", "currency", "opening_balance"} <= fields


def test_unknown_account_gives_404(client):
    r = client.get("/api/accounts/42")
    assert r.status_code == 404
    assert r.json() == {"error": "account 42 not found"}


def test_post_transaction_returns_202_and_is_processed(client, broker):
    acc = make_account(client)
    r = client.post("/api/transactions", json={"account_id": acc["id"], "type": "DEPOSIT",
                                               "amount": "1500.00", "category": "salary",
                                               "description": "november salary"})
    assert r.status_code == 202
    assert r.json()["status"] == "PENDING"   # answer is sent before the consumer works
    tx = client.get(f"/api/transactions/{r.json()['id']}").json()
    assert tx["status"] == "COMPLETED"


def test_transaction_validation_errors(client, broker):
    acc = make_account(client)
    bad = [
        {"account_id": acc["id"], "type": "DEPOSIT", "amount": -10},
        {"account_id": acc["id"], "type": "DEPOSIT", "amount": 0},
        {"account_id": acc["id"], "type": "DEPOSIT", "amount": "12.345"},
        {"account_id": acc["id"], "type": "STEAL", "amount": 10},
        {"account_id": acc["id"], "type": "DEPOSIT", "amount": 10, "category": "casino"},
        {"type": "DEPOSIT", "amount": 10},
    ]
    for body in bad:
        r = client.post("/api/transactions", json=body)
        assert r.status_code == 422, body
        assert r.json()["error"] == "invalid input"
    assert broker.messages == []  # nothing invalid reached Kafka


def test_transaction_for_missing_account(client, broker):
    r = client.post("/api/transactions", json={"account_id": 99, "type": "DEPOSIT", "amount": 5})
    assert r.status_code == 404


def test_broker_down_gives_503_and_failed_status(client, broker):
    acc = make_account(client)
    broker.down = True
    r = client.post("/api/transactions", json={"account_id": acc["id"], "type": "DEPOSIT", "amount": 5})
    assert r.status_code == 503
    page = client.get("/api/transactions", params={"status": "FAILED"}).json()
    assert page["total"] == 1
    logs = client.get(f"/api/transactions/{page['items'][0]['id']}/logs").json()
    assert logs[-1]["event"] == "FAILED"


def test_list_with_filters(client, broker):
    acc = make_account(client)
    for t, a in [("DEPOSIT", 500), ("PAYMENT", 40), ("PAYMENT", 900), ("WITHDRAWAL", 100)]:
        client.post("/api/transactions", json={"account_id": acc["id"], "type": t, "amount": a})

    def total(**p):
        return client.get("/api/transactions", params={"account_id": acc["id"], **p}).json()["total"]

    assert total() == 4
    assert total(type="PAYMENT") == 2
    assert total(status="REJECTED") == 1       # 900 payment, balance was not enough
    assert total(min_amount=100, max_amount=500) == 2
    assert total(date_from=str(date.today())) == 4
    assert total(date_to=str(date.today() - timedelta(days=1))) == 0
    page = client.get("/api/transactions", params={"limit": 2, "offset": 0}).json()
    assert len(page["items"]) == 2 and page["total"] == 4


def test_bad_filters(client):
    assert client.get("/api/transactions", params={"min_amount": 50, "max_amount": 10}).status_code == 400
    assert client.get("/api/transactions", params={"date_from": "2026-12-01",
                                                   "date_to": "2026-11-01"}).status_code == 400
    assert client.get("/api/transactions", params={"date_from": "yesterday"}).status_code == 422
    assert client.get("/api/transactions", params={"type": "LOAN"}).status_code == 422
    assert client.get("/api/transactions", params={"limit": 1000}).status_code == 422


def test_summary_endpoint(client, broker):
    acc = make_account(client)
    client.post("/api/transactions", json={"account_id": acc["id"], "type": "DEPOSIT", "amount": 300})
    client.post("/api/transactions", json={"account_id": acc["id"], "type": "PAYMENT", "amount": 75,
                                           "category": "bills"})
    s = client.get("/api/reports/summary", params={"account_id": acc["id"]}).json()
    assert s["total_deposit"] == "300.00"
    assert s["net"] == "225.00"
    assert s["spending_by_category"] == {"bills": "75.00"}
    assert client.get("/api/reports/summary", params={"account_id": 555}).status_code == 404


def test_health_and_ui(client):
    h = client.get("/health").json()
    assert h["database"] == "ok" and h["kafka"] == "down"  # no broker in tests
    assert "FinLog" in client.get("/").text
