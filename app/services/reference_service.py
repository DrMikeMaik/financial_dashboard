"""Reference-data helpers for Gradio forms."""
from app.core.db import get_connection


def list_holding_symbols() -> list[str]:
    """List all holding symbols."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT symbol
            FROM holdings
            ORDER BY asset_type, symbol
        """).fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


def list_account_names() -> list[str]:
    """List all account names."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT name
            FROM accounts
            ORDER BY active DESC, name
        """).fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


def list_transaction_choices(limit: int = 200) -> list[str]:
    """List recent transactions as dropdown labels."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                t.id,
                t.ts,
                h.symbol,
                t.action
            FROM transactions t
            JOIN holdings h ON h.id = t.holding_id
            ORDER BY t.ts DESC, t.id DESC
            LIMIT ?
        """, [limit]).fetchall()
        return [f"{row[0]} | {row[1]} | {row[2]} | {row[3]}" for row in rows]
    finally:
        conn.close()


def list_account_choices() -> list[str]:
    """List accounts as dropdown labels."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, name, currency
            FROM accounts
            ORDER BY name
        """).fetchall()
        return [f"{row[0]} | {row[1]} | {row[2]}" for row in rows]
    finally:
        conn.close()


def list_bond_choices() -> list[str]:
    """List bond lots as dropdown labels."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, series, purchase_date, qty
            FROM bonds
            ORDER BY series, purchase_date, id
        """).fetchall()
        return [f"{row[0]} | {row[1]} | {row[2]} | qty {row[3]}" for row in rows]
    finally:
        conn.close()


def list_fund_choices() -> list[str]:
    """List active funds as dropdown labels."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, name, currency
            FROM funds
            WHERE active = TRUE
            ORDER BY name, id
        """).fetchall()
        return [f"{row[0]} | {row[1]} | {row[2]}" for row in rows]
    finally:
        conn.close()
