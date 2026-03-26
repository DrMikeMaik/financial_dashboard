"""CRUD helpers for manual cash accounts."""
from decimal import Decimal

import pandas as pd

from app.core.db import get_connection


def _parse_account_choice(account_choice: str | None) -> int | None:
    if not account_choice:
        return None
    return int(str(account_choice).split("|", 1)[0].strip())


def get_accounts_df() -> pd.DataFrame:
    """Get all accounts."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, name, type, currency, balance, active
            FROM accounts
            ORDER BY name
        """).fetchall()

        if not rows:
            return pd.DataFrame(columns=["ID", "Name", "Type", "Currency", "Balance", "Active"])

        return pd.DataFrame([
            {
                "ID": row[0],
                "Name": row[1],
                "Type": row[2],
                "Currency": row[3],
                "Balance": f"{Decimal(str(row[4])):,.2f}",
                "Active": "✓" if row[5] else "✗",
            }
            for row in rows
        ])
    finally:
        conn.close()


def load_account(account_choice: str | None) -> tuple[str, str | None, str, float, bool, str]:
    """Load an account into the edit form."""
    account_id = _parse_account_choice(account_choice)
    if account_id is None:
        return "", None, "PLN", 0.0, True, ""

    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT name, type, currency, balance, active
            FROM accounts
            WHERE id = ?
        """, [account_id]).fetchone()

        if not row:
            return "", None, "PLN", 0.0, True, "✗ Account not found."

        return row[0], row[1], row[2], float(row[3]), bool(row[4]), f"Loaded account #{account_id}."
    finally:
        conn.close()


def save_account(
    account_choice: str | None,
    name: str,
    acc_type: str,
    currency: str,
    balance: float | int | None = 0,
    active: bool = True,
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
            """, [name.strip(), acc_type, currency.strip().upper(), balance_dec, active])
            conn.commit()
            return f"✓ Added account: {name.strip()}"

        conn.execute("""
            UPDATE accounts
            SET name = ?, type = ?, currency = ?, balance = ?, active = ?
            WHERE id = ?
        """, [name.strip(), acc_type, currency.strip().upper(), balance_dec, active, account_id])
        conn.commit()
        return f"✓ Updated account: {name.strip()}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def delete_account(account_choice: str | None) -> str:
    """Delete an account if it is unused."""
    account_id = _parse_account_choice(account_choice)
    if account_id is None:
        return "✗ Select an account to delete."

    conn = get_connection()
    try:
        txn_count = conn.execute("""
            SELECT COUNT(*)
            FROM transactions
            WHERE account_id = ?
        """, [account_id]).fetchone()[0]

        if txn_count:
            return "✗ Cannot delete an account referenced by transactions."

        conn.execute("DELETE FROM accounts WHERE id = ?", [account_id])
        conn.commit()
        return f"✓ Deleted account #{account_id}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()
