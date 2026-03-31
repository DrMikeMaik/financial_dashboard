"""Stock/ETF order ledger read/write helpers."""
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

import pandas as pd

from app.adapters import stocks_yfinance
from app.core.db import get_connection
from app.core.portfolio import get_fx_rate_info, get_historical_fx_rate_info, get_latest_price_info
from app.services.transaction_service import _parse_timestamp, _to_decimal, _validate_no_oversell


ORDER_COLUMNS = [
    "Date",
    "Symbol",
    "B/S",
    "Qty",
    "Price",
    "CCY",
    "Comm.",
    "Trade Value",
    "FX PLN",
    "Current Value",
    "Change %",
    "Delete",
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


def _format_percent(value: Decimal | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}%"


def _format_price(value: Decimal | None) -> str:
    if value is None:
        return ""
    return _format_decimal(value, 2)


def _format_date(ts: datetime) -> str:
    return f"{ts:%Y-%m-%d}"


def _format_symbol(symbol: str, exchange_label: str | None) -> str:
    title = symbol or ""
    subtitle = exchange_label or ""
    return f"**{title}**  \n{subtitle}".strip()


def _parse_stock_action(action: str | None) -> str:
    normalized = (action or "").strip().lower()
    if normalized not in {"buy", "sell"}:
        raise ValueError("Action must be buy or sell.")
    return normalized


def _parse_stock_order_choice(order_choice: str | None) -> int | None:
    if not order_choice or not str(order_choice).strip():
        return None
    try:
        return int(str(order_choice).split("|", 1)[0].strip())
    except ValueError:
        return None


def _normalize_result(result: dict[str, Any]) -> dict[str, Any]:
    symbol = (result.get("symbol") or "").strip().upper()
    currency = (result.get("currency") or "").strip().upper() or None
    quote_type = (result.get("type") or "").strip().upper() or None
    exchange_display = result.get("exchange_display") or result.get("exchange")
    exchange_label = result.get("exchange_label") or stocks_yfinance.build_exchange_label({
        "exchDisp": exchange_display,
        "exchange": result.get("exchange"),
        "currency": currency,
    })
    return {
        "symbol": symbol,
        "name": (result.get("name") or symbol).strip(),
        "currency": currency,
        "exchange": result.get("exchange"),
        "exchange_display": exchange_display,
        "exchange_label": exchange_label,
        "type": quote_type,
        "label": _format_search_choice({
            "symbol": symbol,
            "name": result.get("name") or symbol,
            "exchange_display": exchange_display,
            "currency": currency,
        }),
    }


def _format_search_choice(result: dict[str, Any]) -> str:
    name = result.get("name") or result.get("symbol") or ""
    exchange_display = result.get("exchange_display") or result.get("exchange") or ""
    currency = result.get("currency") or ""
    return f"{result.get('symbol', '')} | {name} | {exchange_display} | {currency}"


def search_stock_candidates(query: str) -> tuple[list[dict[str, Any]], str]:
    """Search Yahoo Finance and return normalized stock/ETF candidates."""
    results = [_normalize_result(result) for result in stocks_yfinance.search_instruments(query)]
    if results:
        return results, f"✓ Found {len(results)} Yahoo stock/ETF matches."

    fallback = stocks_yfinance.resolve_exact_symbol(query)
    if fallback is not None:
        result = _normalize_result(fallback)
        return [result], f"✓ Resolved exact Yahoo ticker {result['symbol']}."

    return [], "✗ No Yahoo stock/ETF matches found."


def resolve_search_choice(selected_choice: str | None, results_state: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    """Resolve the selected search label back to its normalized metadata."""
    if not selected_choice:
        return None

    for result in results_state or []:
        if result.get("label") == selected_choice:
            return result
    return None


def list_stock_order_choices(limit: int = 200) -> list[str]:
    """List existing stock/ETF orders for the stock-tab editor."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                t.id,
                t.ts,
                h.symbol,
                t.action,
                t.qty,
                h.currency
            FROM transactions t
            JOIN holdings h ON h.id = t.holding_id
            WHERE h.asset_type IN ('stock', 'etf')
              AND t.action IN ('buy', 'sell')
            ORDER BY t.ts DESC, t.id DESC
            LIMIT ?
        """, [limit]).fetchall()
        return [
            f"{row[0]} | {row[1].date()} | {row[2]} | {row[3]} | {_format_quantity(Decimal(str(row[4] or 0)))} | {row[5] or ''}"
            for row in rows
        ]
    finally:
        conn.close()


def delete_stock_order_by_id(transaction_id: int) -> str:
    """Delete a stock/ETF order by transaction id."""
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT h.id, h.asset_type
            FROM transactions t
            JOIN holdings h ON h.id = t.holding_id
            WHERE t.id = ?
        """, [transaction_id]).fetchone()
        if not row:
            return "✗ Stock/ETF order not found."
        holding_id, asset_type = row
        if asset_type not in {"stock", "etf"}:
            return "✗ Selected transaction is not a stock/ETF order."

        oversell_error = _validate_no_oversell(conn, holding_id, candidate=None, skip_txn_id=transaction_id)
        if oversell_error:
            return "✗ Deleting this transaction would make a later sell invalid."

        conn.execute("DELETE FROM transactions WHERE id = ?", [transaction_id])

        remaining_count = conn.execute("""
            SELECT COUNT(*)
            FROM transactions
            WHERE holding_id = ?
        """, [holding_id]).fetchone()[0]

        if remaining_count == 0:
            conn.execute("DELETE FROM prices WHERE holding_id = ?", [holding_id])
            conn.execute("DELETE FROM holdings WHERE id = ?", [holding_id])

        conn.commit()
        return "✓ Deleted."
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def load_stock_order(order_choice: str | None) -> dict[str, Any]:
    """Load an existing stock/ETF order into the stock-tab form."""
    if _parse_stock_order_choice(order_choice) is None:
        return {
            "search_query": "",
            "results_state": [],
            "selected_choice": None,
            "resolved_symbol": "",
            "trade_currency": "",
            "security_name": "",
            "exchange_label": "",
            "timestamp_text": datetime.combine(datetime.now().date(), time.min),
            "action": "buy",
            "quantity": None,
            "price": None,
            "commission": 0.0,
            "note": "",
            "message": "",
        }

    transaction_id = _parse_stock_order_choice(order_choice)
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT
                t.ts,
                t.action,
                t.qty,
                t.price,
                t.fee,
                t.note,
                h.symbol,
                h.name,
                h.currency,
                h.exchange_label,
                h.asset_type
            FROM transactions t
            JOIN holdings h ON h.id = t.holding_id
            WHERE t.id = ?
              AND h.asset_type IN ('stock', 'etf')
        """, [transaction_id]).fetchone()

        if not row:
            return {
                "search_query": "",
                "results_state": [],
                "selected_choice": None,
                "resolved_symbol": "",
                "trade_currency": "",
                "security_name": "",
                "exchange_label": "",
                "timestamp_text": datetime.combine(datetime.now().date(), time.min),
                "action": "buy",
                "quantity": None,
                "price": None,
                "commission": 0.0,
                "note": "",
                "message": "✗ Stock/ETF order not found.",
            }

        result = _normalize_result({
            "symbol": row[6],
            "name": row[7],
            "currency": row[8],
            "exchange_label": row[9],
            "exchange_display": row[9].replace(f" {row[8]}", "") if row[9] and row[8] and row[9].endswith(f" {row[8]}") else row[9],
            "type": row[10],
        })
        return {
            "search_query": row[6],
            "results_state": [result],
            "selected_choice": result["label"],
            "resolved_symbol": row[6],
            "trade_currency": row[8] or "",
            "security_name": row[7] or "",
            "exchange_label": row[9] or "",
            "timestamp_text": datetime.combine(row[0].date(), time.min),
            "action": row[1],
            "quantity": float(row[2]) if row[2] is not None else None,
            "price": float(row[3]) if row[3] is not None else None,
            "commission": float(row[4] or 0),
            "note": row[5] or "",
            "message": f"Loaded stock/ETF order #{transaction_id}.",
        }
    finally:
        conn.close()


def _resolve_stock_holding(conn, symbol: str, asset_type: str) -> tuple[int | None, str | None, str | None, str | None]:
    row = conn.execute("""
        SELECT id, name, currency, exchange_label
        FROM holdings
        WHERE symbol = ? AND asset_type IN ('stock', 'etf')
        ORDER BY CASE WHEN asset_type = ? THEN 0 ELSE 1 END, id ASC
        LIMIT 1
    """, [symbol, asset_type]).fetchone()
    return row if row else (None, None, None, None)


def _get_or_create_stock_holding_from_result(conn, result: dict[str, Any]) -> int:
    asset_type = "etf" if result.get("type") == "ETF" else "stock"
    holding_id, _, _, existing_exchange_label = _resolve_stock_holding(conn, result["symbol"], asset_type)

    if holding_id is None:
        holding_id = conn.execute("SELECT nextval('seq_holdings_id')").fetchone()[0]
        conn.execute("""
            INSERT INTO holdings (id, asset_type, symbol, name, currency, exchange_label)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [holding_id, asset_type, result["symbol"], result["name"], result["currency"], result["exchange_label"]])
        return holding_id

    conn.execute("""
        UPDATE holdings
        SET name = ?, currency = ?, exchange_label = ?
        WHERE id = ?
    """, [
        result["name"],
        result["currency"],
        result["exchange_label"] if result.get("exchange_label") is not None else existing_exchange_label,
        holding_id,
    ])
    return holding_id


def save_stock_order(
    order_choice: str | None,
    selected_search_choice: str | None,
    results_state: list[dict[str, Any]] | None,
    timestamp_text: str | None,
    action: str,
    quantity,
    price,
    commission_pln=0,
    note: str = "",
) -> str:
    """Create or update a stock/ETF buy or sell transaction from a resolved Yahoo result."""
    selected_result = resolve_search_choice(selected_search_choice, results_state)
    if selected_result is None:
        return "✗ Search and select a Yahoo stock/ETF result before saving."

    try:
        normalized_action = _parse_stock_action(action)
    except ValueError as exc:
        return f"✗ {exc}"

    try:
        timestamp = _normalize_stock_order_timestamp(timestamp_text)
    except ValueError:
        return "✗ Invalid date. Use ISO format like 2026-03-25."

    qty_dec = _to_decimal(quantity)
    price_dec = _to_decimal(price)
    fee_dec = _to_decimal(commission_pln, Decimal("0")) or Decimal("0")

    if qty_dec is None or qty_dec <= 0:
        return "✗ Quantity must be greater than zero."
    if price_dec is None or price_dec < 0:
        return "✗ Price must be zero or greater."
    if fee_dec < 0:
        return "✗ Commission must be zero or greater."

    transaction_id = _parse_stock_order_choice(order_choice)

    conn = get_connection()
    try:
        holding_id = _get_or_create_stock_holding_from_result(conn, selected_result)
        candidate = {
            "id": transaction_id if transaction_id is not None else 10**12,
            "ts": timestamp,
            "action": normalized_action,
            "qty": qty_dec,
        }
        oversell_error = _validate_no_oversell(conn, holding_id, candidate=candidate, skip_txn_id=transaction_id)
        if oversell_error:
            return oversell_error

        if transaction_id is None:
            conn.execute("""
                INSERT INTO transactions (id, holding_id, account_id, ts, action, qty, price, fee, fee_currency, note)
                VALUES (nextval('seq_transactions_id'), ?, NULL, ?, ?, ?, ?, ?, 'PLN', ?)
            """, [holding_id, timestamp, normalized_action, qty_dec, price_dec, fee_dec, note.strip() or None])
            conn.commit()
            return f"✓ Added {normalized_action} order for {selected_result['symbol']}"

        conn.execute("""
            UPDATE transactions
            SET holding_id = ?, ts = ?, action = ?, qty = ?, price = ?, fee = ?, fee_currency = 'PLN', note = ?
            WHERE id = ?
        """, [holding_id, timestamp, normalized_action, qty_dec, price_dec, fee_dec, note.strip() or None, transaction_id])
        conn.commit()
        return f"✓ Updated stock/ETF order #{transaction_id}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def _normalize_stock_order_timestamp(timestamp_value) -> datetime:
    """Normalize stock order input to end-of-day for the chosen calendar date."""
    if isinstance(timestamp_value, datetime):
        target_date = timestamp_value.date()
    elif isinstance(timestamp_value, date):
        target_date = timestamp_value
    else:
        parsed = _parse_timestamp(timestamp_value)
        target_date = parsed.date()

    return datetime.combine(target_date, time(23, 59, 59))


def get_stock_orders_df() -> tuple[pd.DataFrame, list[int]]:
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
            return pd.DataFrame(columns=ORDER_COLUMNS), []

        latest_market = {}
        ledger_rows = []
        open_buy_rows: dict[int, list[int]] = {}
        total_commission_pln = Decimal("0")
        total_trade_value_pln = Decimal("0")
        total_current_value_pln = Decimal("0")
        total_open_cost_pln = Decimal("0")

        for row in rows:
            txn_id, ts, holding_id, symbol, name, currency, exchange_label, action, qty, price, fee, fee_currency = row
            qty_dec = Decimal(str(qty or 0))
            price_dec = Decimal(str(price or 0))
            fee_dec = Decimal(str(fee or 0))

            historical_fx, fx_found, _, _ = get_historical_fx_rate_info(
                conn, currency, "PLN", ts.date(), fetch_missing=True
            )
            trade_value_pln = qty_dec * price_dec * historical_fx if fx_found else None
            commission_pln = fee_dec if (fee_currency or "PLN").upper() == "PLN" else None

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
                "Date": _format_date(ts),
                "Symbol": _format_symbol(symbol, exchange_label),
                "B/S": "B" if action == "buy" else "S",
                "Qty": _format_quantity(qty_dec),
                "Price": _format_price(price_dec),
                "CCY": currency,
                "Comm.": _format_money(fee_dec, (fee_currency or "PLN").upper()),
                "Trade Value": _format_money(trade_value_pln, "PLN"),
                "FX PLN": _format_decimal(historical_fx, 4) if fx_found else "",
                "Current Value": "",
                "Change %": "",
                "Delete": "🗑️",
                "_holding_id": holding_id,
                "_action": action,
                "_remaining_qty": qty_dec if action == "buy" else None,
                "_qty": qty_dec,
                "_trade_value_pln": trade_value_pln,
                "_commission_pln": commission_pln,
            }
            ledger_rows.append(row_state)
            if commission_pln is not None:
                total_commission_pln += commission_pln
            if trade_value_pln is not None:
                total_trade_value_pln += trade_value_pln

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
            latest = latest_market[row_state["_holding_id"]]
            if remaining_qty <= 0 or latest["price"] is None or not latest["fx_found"]:
                row_state["Current Value"] = ""
                continue

            current_value_pln = remaining_qty * latest["price"] * latest["fx_rate"]
            open_trade_value_pln = None
            if row_state["_trade_value_pln"] is not None and row_state["_qty"] > 0:
                open_trade_value_pln = row_state["_trade_value_pln"] * remaining_qty / row_state["_qty"]
            open_commission_pln = None
            if row_state["_commission_pln"] is not None and row_state["_qty"] > 0:
                open_commission_pln = row_state["_commission_pln"] * remaining_qty / row_state["_qty"]
            open_cost_pln = None
            if open_trade_value_pln is not None:
                open_cost_pln = open_trade_value_pln + (open_commission_pln or Decimal("0"))

            row_state["Current Value"] = _format_money(current_value_pln, "PLN")
            if open_cost_pln and open_cost_pln > 0:
                pct_change = ((current_value_pln - open_cost_pln) / open_cost_pln) * Decimal("100")
                row_state["Change %"] = _format_percent(pct_change)
                total_open_cost_pln += open_cost_pln
            total_current_value_pln += current_value_pln

        total_pct_change = None
        if total_open_cost_pln > 0:
            total_pct_change = ((total_current_value_pln - total_open_cost_pln) / total_open_cost_pln) * Decimal("100")

        display_rows = sorted(ledger_rows, key=lambda row: (row["_ts"], row["_id"]), reverse=True)
        display_ids = [row["_id"] for row in display_rows]
        display_rows.append({
            "Date": "Total",
            "Symbol": "",
            "B/S": "",
            "Qty": "",
            "Price": "",
            "CCY": "",
            "Comm.": _format_money(total_commission_pln, "PLN"),
            "Trade Value": _format_money(total_trade_value_pln, "PLN"),
            "FX PLN": "",
            "Current Value": _format_money(total_current_value_pln, "PLN"),
            "Change %": _format_percent(total_pct_change),
            "Delete": "",
        })
        return pd.DataFrame([{column: row[column] for column in ORDER_COLUMNS} for row in display_rows], columns=ORDER_COLUMNS), display_ids
    finally:
        conn.close()
