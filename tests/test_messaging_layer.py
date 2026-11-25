# Messaging layer tests (event format + both consumers)
from decimal import Decimal

from app.data import repository
from app.data.database import SessionLocal
from app.messaging import consumer


def _pending_tx(type_="DEPOSIT", amount="100", start="0"):
    with SessionLocal() as db:
        acc = repository.create_account(db, "A", "TRY", Decimal(start))
        tx = repository.create_transaction(db, acc.id, type_, Decimal(amount), "other", "")
        return acc.id, tx.id


def test_processor_completes_event_and_returns_log_message():
    acc_id, tx_id = _pending_tx("DEPOSIT", "100")
    log_msg = consumer.handle_transaction_event({"transaction_id": tx_id})
    assert log_msg["event"] == "COMPLETED"
    assert log_msg["transaction_id"] == tx_id
    assert "new balance 100" in log_msg["message"]


def test_processor_skips_duplicate_and_unknown_events():
    _, tx_id = _pending_tx()
    assert consumer.handle_transaction_event({"transaction_id": tx_id}) is not None
    assert consumer.handle_transaction_event({"transaction_id": tx_id}) is None   # duplicate
    assert consumer.handle_transaction_event({"transaction_id": 9999}) is None    # unknown


def test_log_writer_saves_log_row():
    _, tx_id = _pending_tx()
    ok = consumer.handle_log_event({"transaction_id": tx_id, "event": "COMPLETED",
                                    "message": "done", "source": "processor"})
    assert ok
    with SessionLocal() as db:
        events = [l.event for l in repository.get_logs(db, tx_id)]
    assert events == ["CREATED", "COMPLETED"]


def test_log_writer_ignores_log_for_missing_transaction():
    assert consumer.handle_log_event({"transaction_id": 777, "event": "X", "message": "m"}) is False


def test_full_event_flow_through_both_topics(client, broker):
    acc = client.post("/api/accounts", json={"owner_name": "Nouria", "opening_balance": 50}).json()
    r = client.post("/api/transactions", json={"account_id": acc["id"], "type": "PAYMENT",
                                               "amount": 20, "category": "food"})
    tx_id = r.json()["id"]

    # 1 event on the transactions topic with the expected fields
    events = broker.topic("transaction-events")
    assert len(events) == 1
    topic, key, value = events[0]
    assert key == acc["id"]
    assert value["transaction_id"] == tx_id and value["amount"] == "20.00"

    # the processor sent 1 message to the log topic
    assert len(broker.topic("transaction-logs")) == 1

    # and the log writer stored it
    logs = client.get(f"/api/transactions/{tx_id}/logs").json()
    assert [(l["event"], l["source"]) for l in logs] == [("CREATED", "api"), ("COMPLETED", "processor")]
    assert client.get(f"/api/accounts/{acc['id']}").json()["balance"] == "30.00"
