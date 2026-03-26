"""Bond management — simple ledger on the standalone bonds table."""
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from app.core.bonds import parse_series_code
from app.core.db import get_connection


FACE_VALUE = Decimal("100")


def get_bonds_df() -> tuple[pd.DataFrame, list[int]]:
    """Returns (dataframe, list_of_bond_ids) — IDs correspond to data rows (not the total row)."""
    cols = ["Series", "Qty", "Nominal (PLN)", "Date", "Rate (%)", "Maturity", "Delete"]
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, series, qty, purchase_date, rate, maturity
            FROM bonds ORDER BY series, purchase_date
        """).fetchall()

        if not rows:
            return pd.DataFrame(columns=cols), []

        total_qty = 0
        total_nominal = Decimal("0")
        data = []
        bond_ids = []

        for bond_id, series, qty, purchase_date, rate, maturity in rows:
            nominal = qty * FACE_VALUE
            total_qty += qty
            total_nominal += nominal
            bond_ids.append(bond_id)

            data.append({
                "Series": series,
                "Qty": qty,
                "Nominal (PLN)": f"{nominal:,.2f}",
                "Date": str(purchase_date),
                "Rate (%)": f"{Decimal(str(rate)):.2f}" if rate else "",
                "Maturity": str(maturity) if maturity else "",
                "Delete": "🗑️",
            })

        data.append({
            "Series": "Total",
            "Qty": total_qty,
            "Nominal (PLN)": f"{total_nominal:,.2f}",
            "Date": "",
            "Rate (%)": "",
            "Maturity": "",
            "Delete": "",
        })

        return pd.DataFrame(data), bond_ids
    finally:
        conn.close()



def add_bond(series_code: str, qty, purchase_date, rate) -> str:
    if not series_code or not series_code.strip():
        return "✗ Enter a series code."

    try:
        type_code, maturity = parse_series_code(series_code)
    except ValueError as exc:
        return f"✗ {exc}"

    try:
        qty_int = int(qty)
    except (TypeError, ValueError):
        return "✗ Enter a valid quantity."
    if qty_int < 1:
        return "✗ Quantity must be at least 1."

    if isinstance(purchase_date, datetime):
        pdate = purchase_date.date()
    elif purchase_date:
        try:
            pdate = date.fromisoformat(str(purchase_date).strip())
        except ValueError:
            return "✗ Invalid date."
    else:
        return "✗ Select a purchase date."

    if pdate > date.today():
        return "✗ Purchase date cannot be in the future."

    rate_dec = Decimal("0")
    if rate is not None and str(rate).strip():
        try:
            rate_dec = Decimal(str(rate))
            if rate_dec < 0:
                return "✗ Rate cannot be negative."
        except Exception:
            return "✗ Invalid rate."

    series = series_code.strip().upper()
    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO bonds (id, series, qty, purchase_date, rate, maturity)
            VALUES (nextval('seq_bonds_id'), ?, ?, ?, ?, ?)
        """, [series, qty_int, pdate, rate_dec, maturity])
        conn.commit()
        return f"✓ Added {qty_int}x {series} ({pdate})"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def delete_bond_by_id(bond_id: int) -> str:
    conn = get_connection()
    try:
        conn.execute("DELETE FROM bonds WHERE id = ?", [bond_id])
        conn.commit()
        return "✓ Deleted."
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def get_bonds_total() -> Decimal:
    """Sum of all bond nominal values (qty * 100 PLN) for portfolio overview."""
    conn = get_connection()
    try:
        row = conn.execute("SELECT COALESCE(SUM(qty), 0) FROM bonds").fetchone()
        return Decimal(str(row[0])) * FACE_VALUE
    finally:
        conn.close()


