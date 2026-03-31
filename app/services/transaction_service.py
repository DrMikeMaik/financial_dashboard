"""Transaction CRUD and validation helpers."""
from datetime import datetime
from decimal import Decimal

import pandas as pd

from app.core.db import get_connection


def _parse_transaction_choice(transaction_choice: str | None) -> int | None:
    if not transaction_choice:
        return None
    return int(str(transaction_choice).split("|", 1)[0].strip())


def _parse_timestamp(timestamp_text: str | None) -> datetime:
    if not timestamp_text or not str(timestamp_text).strip():
        return datetime.now()
    return datetime.fromisoformat(str(timestamp_text).strip())


def _to_decimal(value, default: Decimal | None = None) -> Decimal | None:
    if value in (None, ""):
        return default
    return Decimal(str(value))


def _resolve_holding_id(conn, symbol: str | None) -> int | None:
    if not symbol or not str(symbol).strip():
        return None
    row = conn.execute("""
        SELECT id
        FROM holdings
        WHERE symbol = ?
    """, [str(symbol).strip().upper()]).fetchone()
    return row[0] if row else None


def _get_holding_currency(conn, holding_id: int) -> str | None:
    row = conn.execute("""
        SELECT currency
        FROM holdings
        WHERE id = ?
    """, [holding_id]).fetchone()
    return row[0] if row else None


def _resolve_account_id(conn, account_name: str | None) -> int | None:
    if not account_name or not str(account_name).strip():
        return None
    row = conn.execute("""
        SELECT id
        FROM accounts
        WHERE name = ?
    """, [str(account_name).strip()]).fetchone()
    return row[0] if row else None


def _validate_action_fields(action: str, qty: Decimal | None, price: Decimal | None) -> str | None:
    if action in {"buy", "sell"}:
        if qty is None or qty <= 0:
            return f"✗ {action.title()} quantity must be greater than zero."
        if price is None or price < 0:
            return f"✗ {action.title()} price must be zero or greater."
    elif action == "transfer":
        if qty is None or qty <= 0:
            return "✗ Transfer quantity must be greater than zero."
    elif action == "dividend":
        if price is None or price <= 0:
            return "✗ Dividend amount goes in the Price field and must be greater than zero."
    else:
        return f"✗ Unsupported action: {action}"

    return None


def _validate_no_oversell(conn, holding_id: int, candidate: dict | None = None, skip_txn_id: int | None = None) -> str | None:
    txns = conn.execute("""
        SELECT id, ts, action, qty
        FROM transactions
        WHERE holding_id = ?
        ORDER BY ts ASC, id ASC
    """, [holding_id]).fetchall()

    records = []
    for txn_id, ts, action, qty in txns:
        if skip_txn_id is not None and txn_id == skip_txn_id:
            continue
        records.append({
            "id": txn_id,
            "ts": ts,
            "action": action,
            "qty": Decimal(str(qty or 0)),
        })

    if candidate is not None:
        records.append(candidate)

    records.sort(key=lambda row: (row["ts"], row["id"]))

    running_qty = Decimal("0")
    for row in records:
        if row["action"] == "buy":
            running_qty += row["qty"]
        elif row["action"] == "sell":
            running_qty -= row["qty"]
            if running_qty < 0:
                return "✗ Sell would exceed available quantity based on FIFO transaction history."

    return None


def get_transactions_df(limit: int = 50) -> pd.DataFrame:
    """Get recent transactions."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                t.id,
                t.ts,
                h.symbol,
                h.asset_type,
                t.action,
                t.qty,
                t.price,
                t.fee,
                a.name AS account,
                t.note
            FROM transactions t
            JOIN holdings h ON t.holding_id = h.id
            LEFT JOIN accounts a ON t.account_id = a.id
            ORDER BY t.ts DESC, t.id DESC
            LIMIT ?
        """, [limit]).fetchall()

        if not rows:
            return pd.DataFrame(columns=["ID", "Date", "Symbol", "Type", "Action", "Quantity", "Price", "Fee", "Account", "Note"])

        return pd.DataFrame([
            {
                "ID": row[0],
                "Date": str(row[1]),
                "Symbol": row[2],
                "Type": row[3],
                "Action": row[4],
                "Quantity": f"{Decimal(str(row[5])):.8f}" if row[5] is not None else "",
                "Price": f"{Decimal(str(row[6])):.2f}" if row[6] is not None else "",
                "Fee": f"{Decimal(str(row[7] or 0)):.2f}",
                "Account": row[8] or "",
                "Note": row[9] or "",
            }
            for row in rows
        ])
    finally:
        conn.close()


def load_transaction(transaction_choice: str | None) -> tuple[str, str, str | None, str | None, float | None, float | None, float, str, str]:
    """Load a transaction into the edit form."""
    transaction_id = _parse_transaction_choice(transaction_choice)
    if transaction_id is None:
        return datetime.now().replace(microsecond=0).isoformat(sep=" "), "", None, "buy", None, None, 0.0, "", ""

    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT
                t.ts,
                h.symbol,
                a.name,
                t.action,
                t.qty,
                t.price,
                t.fee,
                t.note
            FROM transactions t
            JOIN holdings h ON h.id = t.holding_id
            LEFT JOIN accounts a ON a.id = t.account_id
            WHERE t.id = ?
        """, [transaction_id]).fetchone()

        if not row:
            return datetime.now().replace(microsecond=0).isoformat(sep=" "), "", None, "buy", None, None, 0.0, "", "✗ Transaction not found."

        return (
            row[0].replace(microsecond=0).isoformat(sep=" "),
            row[1],
            row[2],
            row[3],
            float(row[4]) if row[4] is not None else None,
            float(row[5]) if row[5] is not None else None,
            float(row[6] or 0),
            row[7] or "",
            f"Loaded transaction #{transaction_id}.",
        )
    finally:
        conn.close()


def save_transaction(
    transaction_choice: str | None,
    timestamp_text: str,
    symbol: str,
    account_name: str | None,
    action: str,
    quantity,
    price,
    fee=0,
    note: str = "",
) -> str:
    """Create or update a transaction."""
    conn = get_connection()
    try:
        holding_id = _resolve_holding_id(conn, symbol)
        if holding_id is None:
            return f"✗ Holding not found: {symbol}"

        account_id = _resolve_account_id(conn, account_name)
        if account_name and account_id is None:
            return f"✗ Account not found: {account_name}"
        holding_currency = _get_holding_currency(conn, holding_id)

        timestamp = _parse_timestamp(timestamp_text)
        qty_dec = _to_decimal(quantity)
        price_dec = _to_decimal(price)
        fee_dec = _to_decimal(fee, Decimal("0")) or Decimal("0")

        field_error = _validate_action_fields(action, qty_dec, price_dec)
        if field_error:
            return field_error

        transaction_id = _parse_transaction_choice(transaction_choice)
        candidate = {
            "id": transaction_id if transaction_id is not None else 10**12,
            "ts": timestamp,
            "action": action,
            "qty": qty_dec or Decimal("0"),
        }

        oversell_error = _validate_no_oversell(conn, holding_id, candidate=candidate, skip_txn_id=transaction_id)
        if oversell_error:
            return oversell_error

        if transaction_id is None:
            conn.execute("""
                INSERT INTO transactions (id, holding_id, account_id, ts, action, qty, price, fee, fee_currency, note)
                VALUES (nextval('seq_transactions_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [holding_id, account_id, timestamp, action, qty_dec, price_dec, fee_dec, holding_currency, note or None])
            conn.commit()
            return f"✓ Added {action} transaction for {symbol.strip().upper()}"

        conn.execute("""
            UPDATE transactions
            SET holding_id = ?, account_id = ?, ts = ?, action = ?, qty = ?, price = ?, fee = ?, note = ?
            WHERE id = ?
        """, [holding_id, account_id, timestamp, action, qty_dec, price_dec, fee_dec, note or None, transaction_id])
        conn.commit()
        return f"✓ Updated transaction #{transaction_id}"
    except ValueError:
        return "✗ Invalid timestamp. Use ISO format like 2026-03-25 14:30:00."
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def delete_transaction(transaction_choice: str | None) -> str:
    """Delete a transaction if the remaining ledger stays valid."""
    transaction_id = _parse_transaction_choice(transaction_choice)
    if transaction_id is None:
        return "✗ Select a transaction to delete."

    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT holding_id
            FROM transactions
            WHERE id = ?
        """, [transaction_id]).fetchone()
        if not row:
            return "✗ Transaction not found."

        holding_id = row[0]
        oversell_error = _validate_no_oversell(conn, holding_id, candidate=None, skip_txn_id=transaction_id)
        if oversell_error:
            return "✗ Deleting this transaction would make a later sell invalid."

        conn.execute("DELETE FROM transactions WHERE id = ?", [transaction_id])
        conn.commit()
        return f"✓ Deleted transaction #{transaction_id}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()
