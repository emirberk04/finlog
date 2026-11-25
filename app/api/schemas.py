# API layer - request / response models (Pydantic does the input validation)
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator


def _as_utc(v: datetime | None):
    # SQLite gives dates back without time zone, we always store UTC
    if v is not None and v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v


UtcDatetime = Annotated[datetime, AfterValidator(_as_utc)]


class TxType(str, Enum):
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    PAYMENT = "PAYMENT"


class TxStatus(str, Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


CATEGORIES = ["salary", "food", "rent", "bills", "transport", "shopping", "education", "other"]
CURRENCIES = ["TRY", "USD", "EUR"]


class AccountCreate(BaseModel):
    owner_name: str = Field(min_length=2, max_length=80, examples=["Ela Soydan"])
    currency: str = Field(default="TRY", examples=["TRY"])
    opening_balance: Decimal = Field(default=Decimal("0"), ge=0, le=10_000_000, decimal_places=2)

    @field_validator("owner_name")
    @classmethod
    def name_not_blank(cls, v: str):
        if not v.strip():
            raise ValueError("owner_name can not be empty")
        return v

    @field_validator("currency")
    @classmethod
    def known_currency(cls, v: str):
        v = v.upper()
        if v not in CURRENCIES:
            raise ValueError(f"currency must be one of {CURRENCIES}")
        return v


class AccountOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    owner_name: str
    currency: str
    balance: Decimal
    created_at: UtcDatetime


class TransactionCreate(BaseModel):
    account_id: int = Field(gt=0, examples=[1])
    type: TxType
    amount: Decimal = Field(gt=0, le=1_000_000, decimal_places=2, examples=[250.50])
    category: str = Field(default="other", examples=["food"])
    description: str = Field(default="", max_length=200, examples=["lunch at campus"])

    @field_validator("category")
    @classmethod
    def known_category(cls, v: str):
        v = v.lower()
        if v not in CATEGORIES:
            raise ValueError(f"category must be one of {CATEGORIES}")
        return v


class TransactionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    account_id: int
    type: str
    amount: Decimal
    category: str
    description: str
    status: str
    reject_reason: str | None
    created_at: UtcDatetime
    processed_at: UtcDatetime | None


class TransactionPage(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[TransactionOut]


class SummaryOut(BaseModel):
    count_by_status: dict[str, int]
    total_deposit: Decimal
    total_withdrawal: Decimal
    total_payment: Decimal
    net: Decimal
    spending_by_category: dict[str, Decimal]


class LogOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    event: str
    message: str
    source: str
    created_at: UtcDatetime
