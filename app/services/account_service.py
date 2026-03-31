"""CRUD helpers for manual cash accounts."""
from decimal import Decimal

import pandas as pd

from app.core.db import get_connection
from app.core.portfolio import get_fx_rate_info


def _parse_account_choice(account_choice: str | None) -> int | None:
    if not account_choice:
        return None
    return int(str(account_choice).split("|", 1)[0].strip())


def get_accounts_df() -> tuple[pd.DataFrame, list[int]]:
    """Get all accounts as a PLN-first snapshot table."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, name, type, currency, balance, active
            FROM accounts
            ORDER BY CASE WHEN currency = 'PLN' THEN 0 ELSE 1 END, name
        """).fetchall()

        if not rows:
            return pd.DataFrame(columns=["Account", "CCY", "Balance", "Balance (PLN)", "Delete"]), []

        data = []
        account_ids = []
        total_pln = Decimal("0")
        for account_id, name, _, currency, balance, active in rows:
            if not active:
                continue
            balance_dec = Decimal(str(balance or 0))
            fx_rate, found, _, _ = get_fx_rate_info(conn, currency, "PLN")
            balance_pln = balance_dec * fx_rate if found or currency == "PLN" else None
            if balance_pln is not None:
                total_pln += balance_pln

            data.append({
                "Account": name,
                "CCY": currency,
                "Balance": f"{balance_dec:,.2f}",
                "Balance (PLN)": f"{balance_pln:,.2f}" if balance_pln is not None else "",
                "Delete": "🗑️",
            })
            account_ids.append(account_id)

        if not data:
            return pd.DataFrame(columns=["Account", "CCY", "Balance", "Balance (PLN)", "Delete"]), []

        data.append({
            "Account": "Total",
            "CCY": "",
            "Balance": "",
            "Balance (PLN)": f"{total_pln:,.2f}",
            "Delete": "",
        })
        return pd.DataFrame(data), account_ids
    finally:
        conn.close()


def get_account_overview_rows() -> list[dict[str, str]]:
    """Build PLN-only cash rows for the overview positions table."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT currency, balance, active
            FROM accounts
            ORDER BY CASE WHEN currency = 'PLN' THEN 0 ELSE 1 END, currency
        """).fetchall()

        balances_by_currency: dict[str, Decimal] = {}
        for currency, balance, active in rows:
            if not active:
                continue
            balances_by_currency.setdefault(currency, Decimal("0"))
            balances_by_currency[currency] += Decimal(str(balance or 0))

        overview_rows = []
        for currency in sorted(balances_by_currency, key=lambda value: (value != "PLN", value)):
            balance_dec = balances_by_currency[currency]
            fx_rate, found, _, _ = get_fx_rate_info(conn, currency, "PLN")
            if not found and currency != "PLN":
                continue

            balance_pln = balance_dec * fx_rate
            overview_rows.append({
                "Asset Type": "CASH",
                "Symbol": f"{currency} Cash",
                "Quantity": "",
                "Avg Cost (PLN)": "",
                "Current Price (PLN)": "",
                "Value (PLN)": f"{balance_pln:,.2f}",
                "UPL": "0.00",
                "Price Source": "account_balance",
            })

        return overview_rows
    finally:
        conn.close()


def load_account(account_choice: str | None) -> tuple[str, str | None, str, float, str]:
    """Load an account into the edit form."""
    account_id = _parse_account_choice(account_choice)
    if account_id is None:
        return "", None, "PLN", 0.0, ""

    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT name, type, currency, balance, active
            FROM accounts
            WHERE id = ?
        """, [account_id]).fetchone()

        if not row:
            return "", None, "PLN", 0.0, "✗ Account not found."

        return row[0], row[1], row[2], float(row[3]), f"Loaded account #{account_id}."
    finally:
        conn.close()


def save_account(
    account_choice: str | None,
    name: str,
    acc_type: str,
    currency: str,
    balance: float | int | None = 0,
) -> str:
    """Create or update an account."""
    if not name or not name.strip():
        return "✗ Account name is required."
    if not acc_type:
        return "✗ Account type is required."
    if not currency or not currency.strip():
        return "✗ Account currency is required."

    balance_dec = Decimal(str(balance or 0))
    account_id = _parse_account_choice(account_choice)

    conn = get_connection()
    try:
        if account_id is None:
            conn.execute("""
                INSERT INTO accounts (id, name, type, currency, balance, active)
                VALUES (nextval('seq_accounts_id'), ?, ?, ?, ?, ?)
            """, [name.strip(), acc_type, currency.strip().upper(), balance_dec, True])
            conn.commit()
            return f"✓ Added account: {name.strip()}"

        conn.execute("""
            UPDATE accounts
            SET name = ?, type = ?, currency = ?, balance = ?, active = TRUE
            WHERE id = ?
        """, [name.strip(), acc_type, currency.strip().upper(), balance_dec, account_id])
        conn.commit()
        return f"✓ Updated account: {name.strip()}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def delete_account(account_choice: str | None) -> str:
    """Delete an account."""
    account_id = _parse_account_choice(account_choice)
    if account_id is None:
        return "✗ Select an account to delete."

    conn = get_connection()
    try:
        conn.execute("DELETE FROM accounts WHERE id = ?", [account_id])
        conn.commit()
        return f"✓ Deleted account #{account_id}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def delete_account_by_id(account_id: int) -> str:
    """Delete an account by numeric id."""
    return delete_account(str(account_id))
