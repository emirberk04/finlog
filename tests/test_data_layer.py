# Data layer tests (ORM + repository functions)
from datetime import date, timedelta
from decimal import Decimal

from app.data import repository
from app.data.database import SessionLocal
from app.data.models import Transaction


def test_account_is_saved_and_read_back():
    with SessionLocal() as db:
        acc = repository.create_account(db, "Ela Soydan", "TRY", Decimal("100.00"))
        acc_id = acc.id
    with SessionLocal() as db:  # new session -> really comes from the database
        again = repository.get_account(db, acc_id)
        assert again.owner_name == "Ela Soydan"
        assert again.balance == Decimal("100.00")


def test_new_transaction_is_pending_and_has_created_log():
    with SessionLocal() as db:
        acc = repository.create_account(db, "A", "TRY", Decimal("0"))
        tx = repository.create_transaction(db, acc.id, "DEPOSIT", Decimal("50"), "salary", "")
        assert tx.status == "PENDING"
        logs = repository.get_logs(db, tx.id)
        assert [l.event for l in logs] == ["CREATED"]
        # relationship works both ways
        assert db.get(Transaction, tx.id).account.id == acc.id
        assert len(acc.transactions) == 1


def test_process_deposit_and_payment_change_balance():
    with SessionLocal() as db:
        acc = repository.create_account(db, "A", "TRY", Decimal("0"))
        d = repository.create_transaction(db, acc.id, "DEPOSIT", Decimal("300"), "salary", "")
        p = repository.create_transaction(db, acc.id, "PAYMENT", Decimal("120.50"), "food", "")
        assert repository.process_transaction(db, d.id).status == "COMPLETED"
        assert repository.process_transaction(db, p.id).status == "COMPLETED"
        db.refresh(acc)
        assert acc.balance == Decimal("179.50")


def test_withdrawal_bigger_than_balance_is_rejected():
    with SessionLocal() as db:
        acc = repository.create_account(db, "A", "TRY", Decimal("10"))
        w = repository.create_transaction(db, acc.id, "WITHDRAWAL", Decimal("50"), "other", "")
        tx = repository.process_transaction(db, w.id)
        assert tx.status == "REJECTED"
        assert "insufficient balance" in tx.reject_reason
        db.refresh(acc)
        assert acc.balance == Decimal("10")  # not changed


def test_same_transaction_is_not_processed_twice():
    with SessionLocal() as db:
        acc = repository.create_account(db, "A", "TRY", Decimal("0"))
        d = repository.create_transaction(db, acc.id, "DEPOSIT", Decimal("100"), "salary", "")
        repository.process_transaction(db, d.id)
        assert repository.process_transaction(db, d.id) is None  # second time skipped
        db.refresh(acc)
        assert acc.balance == Decimal("100")


def test_filters_and_summary():
    with SessionLocal() as db:
        acc = repository.create_account(db, "A", "TRY", Decimal("0"))
        for t, amt, cat in [("DEPOSIT", "1000", "salary"), ("PAYMENT", "200", "rent"),
                            ("PAYMENT", "30", "food"), ("WITHDRAWAL", "5000", "other")]:
            tx = repository.create_transaction(db, acc.id, t, Decimal(amt), cat, "")
            repository.process_transaction(db, tx.id)

        items, total = repository.list_transactions(db, account_id=acc.id, type_="PAYMENT")
        assert total == 2
        items, total = repository.list_transactions(db, account_id=acc.id, min_amount=Decimal("100"),
                                                    max_amount=Decimal("1000"))
        assert sorted(i.amount for i in items) == [Decimal("200"), Decimal("1000")]
        items, total = repository.list_transactions(db, status="REJECTED")
        assert total == 1
        tomorrow = date.today() + timedelta(days=1)
        assert repository.list_transactions(db, date_from=tomorrow)[1] == 0
        assert repository.list_transactions(db, date_to=date.today())[1] == 4

        s = repository.summary(db, account_id=acc.id)
        assert s["total_deposit"] == Decimal("1000")
        assert s["total_payment"] == Decimal("230")
        assert s["total_withdrawal"] == Decimal("0")  # the big one was rejected
        assert s["net"] == Decimal("770")
        assert s["count_by_status"] == {"COMPLETED": 3, "REJECTED": 1}
        assert s["spending_by_category"]["rent"] == Decimal("200")
