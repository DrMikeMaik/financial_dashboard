"""Bond management — simple ledger on the standalone bonds table."""
import calendar
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from app.core.bonds import parse_series_code
from app.core.db import get_connection


FACE_VALUE = Decimal("100")


def _calc_actual_per_bond(purchase_date: date, rate: Decimal, today: date, maturity: date | None = None) -> Decimal:
    """Compound annually from purchase_date to today (or maturity if sooner), pro-rate partial year."""
    end_date = min(today, maturity) if maturity else today
    if rate == 0 or purchase_date >= end_date:
        return FACE_VALUE
    r = rate / Decimal("100")
    # Count full years and remaining days
    full_years = 0
    anniversary = purchase_date
    while True:
        next_anniversary = anniversary.replace(year=anniversary.year + 1)
        if next_anniversary > end_date:
            break
        full_years += 1
        anniversary = next_anniversary
    # Partial year: days from last anniversary to today
    remaining_days = (end_date - anniversary).days
    year_days = Decimal("365")
    
    value = FACE_VALUE * (1 + r) ** full_years * (1 + r * remaining_days / year_days)
    
    return value


def get_bonds_df() -> tuple[pd.DataFrame, list[int]]:
    """Returns (dataframe, list_of_bond_ids) — IDs correspond to data rows (not the total row)."""
    cols = ["Series", "Qty", "Nominal (PLN)", "Actual (PLN)", "Date Purchased", "Rate (%)", "Maturity", "Delete"]
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, series, qty, purchase_date, rate, maturity
            FROM bonds ORDER BY series, purchase_date
        """).fetchall()

        if not rows:
            return pd.DataFrame(columns=cols), []

        today = date.today()
        total_qty = 0
        total_nominal = Decimal("0")
        total_actual = Decimal("0")
        data = []
        bond_ids = []

        for bond_id, series, qty, purchase_date, rate, maturity in rows:
            nominal = qty * FACE_VALUE
            rate_dec = Decimal(str(rate)) if rate else Decimal("0")
            actual_per_bond = _calc_actual_per_bond(purchase_date, rate_dec, today, maturity)
            actual = qty * actual_per_bond
            total_qty += qty
            total_nominal += nominal
            total_actual += actual
            bond_ids.append(bond_id)

            data.append({
                "Series": series,
                "Qty": qty,
                "Nominal (PLN)": f"{nominal:,.2f}",
                "Actual (PLN)": f"{actual:,.2f}",
                "Date Purchased": str(purchase_date),
                "Rate (%)": f"{rate_dec:.2f}" if rate else "",
                "Maturity": str(maturity) if maturity else "",
                "Delete": "🗑️",
            })

        data.append({
            "Series": "Total",
            "Qty": total_qty,
            "Nominal (PLN)": f"{total_nominal:,.2f}",
            "Actual (PLN)": f"{total_actual:,.2f}",
            "Date Purchased": "",
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
        type_code, maturity_month = parse_series_code(series_code)
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

    # Maturity uses month/year from series code + day from purchase date
    last_day = calendar.monthrange(maturity_month.year, maturity_month.month)[1]
    maturity = date(maturity_month.year, maturity_month.month, min(pdate.day, last_day))

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
    """Sum of all bond actual values for portfolio overview."""
    conn = get_connection()
    try:
        rows = conn.execute("SELECT qty, purchase_date, rate, maturity FROM bonds").fetchall()
        today = date.today()
        total = Decimal("0")
        for qty, purchase_date, rate, maturity in rows:
            rate_dec = Decimal(str(rate)) if rate else Decimal("0")
            total += qty * _calc_actual_per_bond(purchase_date, rate_dec, today, maturity)
        return total
    finally:
        conn.close()


