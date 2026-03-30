"""Stock/ETF order ledger read/write helpers."""
from datetime import datetime
from decimal import Decimal

import pandas as pd

from app.adapters import stocks_yfinance
from app.core.db import get_connection
from app.core.portfolio import get_fx_rate_info, get_historical_fx_rate_info, get_latest_price_info
from app.services.transaction_service import _parse_timestamp, _to_decimal, _validate_no_oversell


ORDER_COLUMNS = [
    "Time",
    "Paper",
    "B/S",
    "Quantity",
    "Remaining Qty",
    "Price",
    "Commission",
    "Trade Value",
    "FX to PLN",
    "Value Today",
]


def _format_decimal(value: Decimal, places: int) -> str:
    return f"{value:,.{places}f}"


def _format_quantity(value: Decimal | None) -> str:
    if value is None:
        return ""
    if value == value.to_integral():
        return f"{value.to_integral():,}"
    return _format_decimal(value, 4)


def _format_money(value: Decimal | None, currency: str) -> str:
    if value is None:
        return ""
    return f"{_format_decimal(value, 2)} {currency}"


def _format_price(value: Decimal | None, currency: str) -> str:
    if value is None:
        return ""
    return f"{_format_decimal(value, 4)} {currency}"


def _format_time(ts: datetime) -> str:
    return f"{ts:%d.%m.%Y}\n{ts:%H:%M:%S}"


def _format_paper(name: str | None, symbol: str, exchange_label: str | None) -> str:
    title = name or symbol
    subtitle = exchange_label or symbol
    return f"**{title}**  \n{subtitle}"


def _parse_stock_action(action: str | None) -> str:
    normalized = (action or "").strip().lower()
    if normalized not in {"buy", "sell"}:
        raise ValueError("Action must be buy or sell.")
    return normalized


def _resolve_stock_holding(conn, symbol: str, asset_type: str) -> tuple[int | None, str | None, str | None, str | None]:
    row = conn.execute("""
        SELECT id, name, currency, exchange_label
        FROM holdings
        WHERE symbol = ? AND asset_type IN ('stock', 'etf')
        ORDER BY CASE WHEN asset_type = ? THEN 0 ELSE 1 END, id ASC
        LIMIT 1
    """, [symbol, asset_type]).fetchone()
    return row if row else (None, None, None, None)


def _get_or_create_stock_holding(conn, symbol: str, exchange_label_override: str | None) -> tuple[int, str]:
    info = stocks_yfinance.get_info(symbol)
    asset_type = "etf" if info.get("type") == "ETF" else "stock"
    name = info.get("name") or symbol
    currency = (info.get("currency") or "USD").upper()
    exchange_label = (exchange_label_override or info.get("exchange_label") or "").strip() or None

    holding_id, existing_name, existing_currency, existing_exchange_label = _resolve_stock_holding(conn, symbol, asset_type)

    if holding_id is None:
        holding_id = conn.execute("SELECT nextval('seq_holdings_id')").fetchone()[0]
        conn.execute("""
            INSERT INTO holdings (id, asset_type, symbol, name, currency, exchange_label)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [holding_id, asset_type, symbol, name, currency, exchange_label])
        return holding_id, currency

    conn.execute("""
        UPDATE holdings
        SET name = ?, currency = ?, exchange_label = ?
        WHERE id = ?
    """, [
        name or existing_name,
        currency or existing_currency,
        exchange_label if exchange_label is not None else existing_exchange_label,
        holding_id,
    ])
    return holding_id, currency or existing_currency or "USD"


def save_stock_order(
    timestamp_text: str | None,
    symbol: str,
    action: str,
    quantity,
    price,
    commission_pln=0,
    exchange_label_override: str = "",
    note: str = "",
) -> str:
    """Create a stock/ETF buy or sell transaction and auto-create the holding if needed."""
    normalized_symbol = (symbol or "").strip().upper()
    if not normalized_symbol:
        return "✗ Stock/ETF symbol is required."

    try:
        normalized_action = _parse_stock_action(action)
    except ValueError as exc:
        return f"✗ {exc}"

    try:
        timestamp = _parse_timestamp(timestamp_text)
    except ValueError:
        return "✗ Invalid timestamp. Use ISO format like 2026-03-25 14:30:00."

    qty_dec = _to_decimal(quantity)
    price_dec = _to_decimal(price)
    fee_dec = _to_decimal(commission_pln, Decimal("0")) or Decimal("0")

    if qty_dec is None or qty_dec <= 0:
        return "✗ Quantity must be greater than zero."
    if price_dec is None or price_dec < 0:
        return "✗ Price must be zero or greater."
    if fee_dec < 0:
        return "✗ Commission must be zero or greater."

    conn = get_connection()
    try:
        holding_id, _ = _get_or_create_stock_holding(conn, normalized_symbol, exchange_label_override)
        candidate = {
            "id": 10**12,
            "ts": timestamp,
            "action": normalized_action,
            "qty": qty_dec,
        }
        oversell_error = _validate_no_oversell(conn, holding_id, candidate=candidate, skip_txn_id=None)
        if oversell_error:
            return oversell_error

        conn.execute("""
            INSERT INTO transactions (id, holding_id, account_id, ts, action, qty, price, fee, fee_currency, note)
            VALUES (nextval('seq_transactions_id'), ?, NULL, ?, ?, ?, ?, ?, 'PLN', ?)
        """, [holding_id, timestamp, normalized_action, qty_dec, price_dec, fee_dec, note.strip() or None])
        conn.commit()
        return f"✓ Added {normalized_action} order for {normalized_symbol}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def get_stock_orders_df() -> pd.DataFrame:
    """Build the bank-style stock/ETF order ledger with current valuations."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                t.id,
                t.ts,
                h.id AS holding_id,
                h.symbol,
                h.name,
                h.currency,
                h.exchange_label,
                t.action,
                t.qty,
                t.price,
                t.fee,
                COALESCE(t.fee_currency, h.currency) AS fee_currency
            FROM transactions t
            JOIN holdings h ON h.id = t.holding_id
            WHERE h.asset_type IN ('stock', 'etf')
              AND t.action IN ('buy', 'sell')
            ORDER BY t.ts ASC, t.id ASC
        """).fetchall()

        if not rows:
            return pd.DataFrame(columns=ORDER_COLUMNS)

        latest_market = {}
        ledger_rows = []
        open_buy_rows: dict[int, list[int]] = {}

        for row in rows:
            txn_id, ts, holding_id, symbol, name, currency, exchange_label, action, qty, price, fee, fee_currency = row
            qty_dec = Decimal(str(qty or 0))
            price_dec = Decimal(str(price or 0))
            fee_dec = Decimal(str(fee or 0))

            historical_fx, fx_found, _, _ = get_historical_fx_rate_info(
                conn, currency, "PLN", ts.date(), fetch_missing=True
            )
            trade_value_pln = qty_dec * price_dec * historical_fx if fx_found else None

            latest = latest_market.get(holding_id)
            if latest is None:
                latest_price, latest_price_ccy, _, _ = get_latest_price_info(conn, holding_id)
                latest_fx, latest_fx_found, _, _ = get_fx_rate_info(conn, latest_price_ccy or currency, "PLN")
                latest = {
                    "price": latest_price,
                    "price_ccy": latest_price_ccy or currency,
                    "fx_rate": latest_fx,
                    "fx_found": latest_fx_found or (latest_price_ccy or currency) == "PLN",
                }
                latest_market[holding_id] = latest

            row_state = {
                "_id": txn_id,
                "_ts": ts,
                "Time": _format_time(ts),
                "Paper": _format_paper(name, symbol, exchange_label),
                "B/S": "B" if action == "buy" else "S",
                "Quantity": _format_quantity(qty_dec),
                "Remaining Qty": _format_quantity(qty_dec) if action == "buy" else "",
                "Price": _format_price(price_dec, currency),
                "Commission": _format_money(fee_dec, (fee_currency or "PLN").upper()),
                "Trade Value": _format_money(trade_value_pln, "PLN"),
                "FX to PLN": _format_decimal(historical_fx, 4) if fx_found else "",
                "Value Today": "",
                "_holding_id": holding_id,
                "_action": action,
                "_remaining_qty": qty_dec if action == "buy" else None,
            }
            ledger_rows.append(row_state)

            if action == "buy":
                open_buy_rows.setdefault(holding_id, []).append(len(ledger_rows) - 1)
                continue

            remaining_to_sell = qty_dec
            for buy_index in open_buy_rows.get(holding_id, []):
                buy_row = ledger_rows[buy_index]
                open_qty = buy_row["_remaining_qty"] or Decimal("0")
                if open_qty <= 0:
                    continue
                consumed = min(open_qty, remaining_to_sell)
                buy_row["_remaining_qty"] = open_qty - consumed
                remaining_to_sell -= consumed
                if remaining_to_sell <= 0:
                    break

        for row_state in ledger_rows:
            if row_state["_action"] != "buy":
                continue

            remaining_qty = row_state["_remaining_qty"] or Decimal("0")
            row_state["Remaining Qty"] = _format_quantity(remaining_qty)

            latest = latest_market[row_state["_holding_id"]]
            if remaining_qty <= 0 or latest["price"] is None or not latest["fx_found"]:
                row_state["Value Today"] = ""
                continue

            current_value_pln = remaining_qty * latest["price"] * latest["fx_rate"]
            row_state["Value Today"] = _format_money(current_value_pln, "PLN")

        display_rows = sorted(ledger_rows, key=lambda row: (row["_ts"], row["_id"]), reverse=True)
        return pd.DataFrame([{column: row[column] for column in ORDER_COLUMNS} for row in display_rows], columns=ORDER_COLUMNS)
    finally:
        conn.close()
