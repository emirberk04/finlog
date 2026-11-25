# Data layer - all database reads/writes go through these functions,
# so the API and the Kafka consumer never write SQL themselves.
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.data.models import Account, Transaction, TransactionLog, now_utc


# ---------- accounts ----------

def create_account(db: Session, owner_name: str, currency: str, opening_balance: Decimal) -> Account:
    acc = Account(owner_name=owner_name.strip(), currency=currency, balance=opening_balance)
    db.add(acc)
    db.commit()
    db.refresh(acc)
    return acc


def get_account(db: Session, account_id: int) -> Account | None:
    return db.get(Account, account_id)


def list_accounts(db: Session) -> list[Account]:
    return list(db.scalars(select(Account).order_by(Account.id)))


# ---------- transactions ----------

def create_transaction(db: Session, account_id: int, type_: str, amount: Decimal,
                       category: str, description: str) -> Transaction:
    tx = Transaction(account_id=account_id, type=type_, amount=amount,
                     category=category, description=description, status="PENDING")
    db.add(tx)
    db.flush()  # we need tx.id for the log row
    db.add(TransactionLog(transaction_id=tx.id, event="CREATED", source="api",
                          message=f"{type_} of {amount:.2f} received, waiting for processing"))
    db.commit()
    db.refresh(tx)
    return tx


def get_transaction(db: Session, tx_id: int) -> Transaction | None:
    return db.get(Transaction, tx_id)


def _day_start(d: date) -> datetime:
    return datetime.combine(d, time.min, tzinfo=timezone.utc)


def list_transactions(db: Session, account_id=None, type_=None, status=None, category=None,
                      min_amount=None, max_amount=None, date_from=None, date_to=None,
                      limit=50, offset=0) -> tuple[list[Transaction], int]:
    q = select(Transaction)
    if account_id is not None:
        q = q.where(Transaction.account_id == account_id)
    if type_:
        q = q.where(Transaction.type == type_)
    if status:
        q = q.where(Transaction.status == status)
    if category:
        q = q.where(Transaction.category == category)
    if min_amount is not None:
        q = q.where(Transaction.amount >= min_amount)
    if max_amount is not None:
        q = q.where(Transaction.amount <= max_amount)
    if date_from:
        q = q.where(Transaction.created_at >= _day_start(date_from))
    if date_to:
        # date_to is inclusive, so go until the start of the next day
        q = q.where(Transaction.created_at < _day_start(date_to + timedelta(days=1)))

    total = db.scalar(select(func.count()).select_from(q.subquery()))
    rows = db.scalars(q.order_by(Transaction.id.desc()).limit(limit).offset(offset))
    return list(rows), total


def mark_failed(db: Session, tx: Transaction, reason: str):
    # used when the event could not be sent to Kafka
    tx.status = "FAILED"
    tx.reject_reason = reason
    tx.processed_at = now_utc()
    db.add(TransactionLog(transaction_id=tx.id, event="FAILED", source="api", message=reason))
    db.commit()


def process_transaction(db: Session, tx_id: int) -> Transaction | None:
    """Called by the Kafka consumer. Applies the transaction to the balance.
    Returns None if there is nothing to do (unknown id or already processed)."""
    tx = db.get(Transaction, tx_id)
    if tx is None or tx.status != "PENDING":
        # Kafka can deliver the same message again, so we skip finished ones
        return None

    acc = db.get(Account, tx.account_id)
    if tx.type == "DEPOSIT":
        acc.balance = acc.balance + tx.amount
        tx.status = "COMPLETED"
    elif acc.balance >= tx.amount:  # WITHDRAWAL or PAYMENT
        acc.balance = acc.balance - tx.amount
        tx.status = "COMPLETED"
    else:
        tx.status = "REJECTED"
        tx.reject_reason = f"insufficient balance ({acc.balance} {acc.currency})"

    tx.processed_at = now_utc()
    db.commit()
    db.refresh(tx)
    return tx


def add_log(db: Session, tx_id: int, event: str, message: str, source: str) -> TransactionLog | None:
    if db.get(Transaction, tx_id) is None:
        return None
    log = TransactionLog(transaction_id=tx_id, event=event, message=message, source=source)
    db.add(log)
    db.commit()
    return log


def get_logs(db: Session, tx_id: int) -> list[TransactionLog]:
    q = select(TransactionLog).where(TransactionLog.transaction_id == tx_id).order_by(TransactionLog.id)
    return list(db.scalars(q))


# ---------- report ----------

def summary(db: Session, account_id=None, date_from=None, date_to=None) -> dict:
    base = select(Transaction)
    if account_id is not None:
        base = base.where(Transaction.account_id == account_id)
    if date_from:
        base = base.where(Transaction.created_at >= _day_start(date_from))
    if date_to:
        base = base.where(Transaction.created_at < _day_start(date_to + timedelta(days=1)))
    sub = base.subquery()

    by_status = {s: c for s, c in db.execute(
        select(sub.c.status, func.count()).group_by(sub.c.status))}

    totals = {"DEPOSIT": Decimal("0"), "WITHDRAWAL": Decimal("0"), "PAYMENT": Decimal("0")}
    for t, s in db.execute(select(sub.c.type, func.sum(sub.c.amount))
                           .where(sub.c.status == "COMPLETED").group_by(sub.c.type)):
        totals[t] = Decimal(s or 0)

    spending = {c: Decimal(s or 0) for c, s in db.execute(
        select(sub.c.category, func.sum(sub.c.amount))
        .where(sub.c.status == "COMPLETED", sub.c.type != "DEPOSIT")
        .group_by(sub.c.category))}

    return {
        "count_by_status": by_status,
        "total_deposit": totals["DEPOSIT"],
        "total_withdrawal": totals["WITHDRAWAL"],
        "total_payment": totals["PAYMENT"],
        "net": totals["DEPOSIT"] - totals["WITHDRAWAL"] - totals["PAYMENT"],
        "spending_by_category": spending,
    }
