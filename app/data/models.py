# Data layer - ORM entities
# Account 1 --- * Transaction 1 --- * TransactionLog
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import DateTime, ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.data.database import Base


def now_utc():
    return datetime.now(timezone.utc)


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(primary_key=True)
    owner_name: Mapped[str] = mapped_column(String(80))
    currency: Mapped[str] = mapped_column(String(3), default="TRY")
    balance: Mapped[Decimal] = mapped_column(Numeric(14, 2), default=Decimal("0.00"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    transactions: Mapped[list["Transaction"]] = relationship(back_populates="account")


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), index=True)
    type: Mapped[str] = mapped_column(String(20))          # DEPOSIT / WITHDRAWAL / PAYMENT
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    category: Mapped[str] = mapped_column(String(30), default="other")
    description: Mapped[str] = mapped_column(String(200), default="")
    status: Mapped[str] = mapped_column(String(20), default="PENDING", index=True)
    reject_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc, index=True)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    account: Mapped[Account] = relationship(back_populates="transactions")
    logs: Mapped[list["TransactionLog"]] = relationship(
        back_populates="transaction", order_by="TransactionLog.id"
    )


class TransactionLog(Base):
    __tablename__ = "transaction_logs"

    id: Mapped[int] = mapped_column(primary_key=True)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.id"), index=True)
    event: Mapped[str] = mapped_column(String(30))   # CREATED / COMPLETED / REJECTED / FAILED
    message: Mapped[str] = mapped_column(String(300))
    source: Mapped[str] = mapped_column(String(30), default="api")  # who wrote it
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    transaction: Mapped[Transaction] = relationship(back_populates="logs")
