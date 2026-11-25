# API layer - REST endpoints
from datetime import date
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.schemas import (AccountCreate, AccountOut, LogOut, TransactionCreate,
                             SummaryOut, TransactionOut, TransactionPage, TxStatus, TxType)
from app.config import settings
from app.data import repository
from app.data.database import get_db
from app.messaging import kafka_client

router = APIRouter(prefix="/api")


# ---------- accounts ----------

@router.post("/accounts", response_model=AccountOut, status_code=201)
def create_account(body: AccountCreate, db: Session = Depends(get_db)):
    return repository.create_account(db, body.owner_name, body.currency, body.opening_balance)


@router.get("/accounts", response_model=list[AccountOut])
def list_accounts(db: Session = Depends(get_db)):
    return repository.list_accounts(db)


@router.get("/accounts/{account_id}", response_model=AccountOut)
def get_account(account_id: int, db: Session = Depends(get_db)):
    acc = repository.get_account(db, account_id)
    if acc is None:
        raise HTTPException(404, f"account {account_id} not found")
    return acc


# ---------- transactions ----------

@router.post("/transactions", response_model=TransactionOut, status_code=status.HTTP_202_ACCEPTED)
async def create_transaction(body: TransactionCreate, db: Session = Depends(get_db)):
    """Saves the transaction as PENDING and sends an event to Kafka.
    The balance is changed later by the consumer, so we answer 202 Accepted."""
    if repository.get_account(db, body.account_id) is None:
        raise HTTPException(404, f"account {body.account_id} not found")

    tx = repository.create_transaction(db, body.account_id, body.type.value, body.amount,
                                       body.category, body.description)
    event = {
        "transaction_id": tx.id,
        "account_id": tx.account_id,
        "type": tx.type,
        "amount": str(tx.amount),  # Decimal is not JSON, send as text
        "created_at": tx.created_at.isoformat(),
    }
    try:
        await kafka_client.publish(settings.topic_transactions, event, key=tx.account_id)
    except kafka_client.KafkaUnavailable:
        repository.mark_failed(db, tx, "message broker is not reachable, transaction was not processed")
        raise HTTPException(503, "message broker is not reachable, please try again later")
    return tx


@router.get("/transactions", response_model=TransactionPage)
def list_transactions(
    account_id: int | None = None,
    type: TxType | None = None,
    status: TxStatus | None = None,
    category: str | None = None,
    min_amount: Decimal | None = Query(None, ge=0),
    max_amount: Decimal | None = Query(None, ge=0),
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    if min_amount is not None and max_amount is not None and min_amount > max_amount:
        raise HTTPException(400, "min_amount can not be bigger than max_amount")
    if date_from and date_to and date_from > date_to:
        raise HTTPException(400, "date_from can not be after date_to")

    items, total = repository.list_transactions(
        db, account_id, type.value if type else None, status.value if status else None,
        category.lower() if category else None, min_amount, max_amount,
        date_from, date_to, limit, offset)
    return {"total": total, "limit": limit, "offset": offset, "items": items}


@router.get("/transactions/{tx_id}", response_model=TransactionOut)
def get_transaction(tx_id: int, db: Session = Depends(get_db)):
    tx = repository.get_transaction(db, tx_id)
    if tx is None:
        raise HTTPException(404, f"transaction {tx_id} not found")
    return tx


@router.get("/transactions/{tx_id}/logs", response_model=list[LogOut])
def get_transaction_logs(tx_id: int, db: Session = Depends(get_db)):
    if repository.get_transaction(db, tx_id) is None:
        raise HTTPException(404, f"transaction {tx_id} not found")
    return repository.get_logs(db, tx_id)


# ---------- report ----------

@router.get("/reports/summary", response_model=SummaryOut)
def report_summary(account_id: int | None = None, date_from: date | None = None,
                   date_to: date | None = None, db: Session = Depends(get_db)):
    if date_from and date_to and date_from > date_to:
        raise HTTPException(400, "date_from can not be after date_to")
    if account_id is not None and repository.get_account(db, account_id) is None:
        raise HTTPException(404, f"account {account_id} not found")
    return repository.summary(db, account_id, date_from, date_to)


# ---------- for the demo ----------

@router.get("/messaging/activity")
def messaging_activity():
    """Last messages sent to / received from Kafka (kept in memory)."""
    return {"connected": kafka_client.is_connected(), "messages": list(kafka_client.recent_activity)}
