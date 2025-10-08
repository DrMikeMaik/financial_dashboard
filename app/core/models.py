"""Data models for the application."""
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal
from enum import Enum


class AssetType(str, Enum):
    CRYPTO = "crypto"
    STOCK = "stock"
    ETF = "etf"
    BOND = "bond"
    CASH = "cash"


class TransactionAction(str, Enum):
    BUY = "buy"
    SELL = "sell"
    TRANSFER = "transfer"
    DIVIDEND = "dividend"
    COUPON = "coupon"
    ADJUSTMENT = "adjustment"


@dataclass
class Account:
    id: int | None
    name: str
    type: str
    currency: str
    balance: Decimal
    active: bool = True
    created_at: datetime | None = None


@dataclass
class Holding:
    id: int | None
    asset_type: AssetType
    symbol: str
    name: str | None
    currency: str
    created_at: datetime | None = None


@dataclass
class Transaction:
    id: int | None
    holding_id: int
    account_id: int | None
    ts: datetime
    action: TransactionAction
    qty: Decimal | None
    price: Decimal | None
    fee: Decimal
    note: str | None = None
    created_at: datetime | None = None


@dataclass
class Price:
    id: int | None
    holding_id: int
    ts: datetime
    price: Decimal
    price_ccy: str
    source: str
    created_at: datetime | None = None


@dataclass
class FXRate:
    id: int | None
    ts: datetime
    base_ccy: str
    quote_ccy: str
    rate: Decimal
    source: str
    created_at: datetime | None = None


@dataclass
class BondMeta:
    id: int | None
    holding_id: int
    face: Decimal
    coupon_rate: Decimal
    coupon_freq: int
    maturity_date: date
    issuer: str | None = None
    created_at: datetime | None = None


@dataclass
class Position:
    """Computed position for a holding."""
    holding: Holding
    qty: Decimal
    avg_cost: Decimal  # in holding currency
    current_price: Decimal  # in holding currency
    value_native: Decimal  # in holding currency
    value_pln: Decimal
    unrealized_pl: Decimal  # in PLN
