"""Bond management — simple ledger on the standalone bonds table."""
import calendar
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from app.core.bonds import get_preset, parse_series_code
from app.core.db import get_connection


FACE_VALUE = Decimal("100")
YEAR_DAYS = Decimal("365")


def _add_years_safe(value: date, years: int) -> date:
    """Add years while clamping Feb 29 to the last valid day of month."""
    target_year = value.year + years
    last_day = calendar.monthrange(target_year, value.month)[1]
    return date(target_year, value.month, min(value.day, last_day))


def _calc_actual_per_bond(
    purchase_date: date,
    period_rates: dict[int, Decimal],
    today: date,
    maturity: date | None = None,
) -> tuple[Decimal, str | None]:
    """Compound by yearly schedule and stop at the last known anniversary when a rate is missing."""
    end_date = min(today, maturity) if maturity else today
    if purchase_date >= end_date:
        return FACE_VALUE, None

    value = FACE_VALUE
    period_start = purchase_date
    period_num = 1

    while period_start < end_date:
        next_anniversary = _add_years_safe(period_start, 1)
        period_end = min(next_anniversary, maturity) if maturity else next_anniversary
        if period_end <= period_start:
            break

        rate = period_rates.get(period_num)
        if rate is None:
            return value, "Need rate"

        rate_multiplier = rate / Decimal("100")
        if end_date >= period_end:
            value *= Decimal("1") + rate_multiplier
            period_start = period_end
            period_num += 1
            continue

        elapsed_days = Decimal((end_date - period_start).days)
        value *= Decimal("1") + rate_multiplier * elapsed_days / YEAR_DAYS
        break

    return value, None


def _load_bond_rate_schedules(conn) -> dict[int, dict[int, Decimal]]:
    schedules: dict[int, dict[int, Decimal]] = {}
    rows = conn.execute("""
        SELECT bond_id, period_num, rate
        FROM bond_year_rates
        ORDER BY bond_id, period_num
    """).fetchall()

    for bond_id, period_num, rate in rows:
        schedules.setdefault(bond_id, {})[period_num] = Decimal(str(rate))

    return schedules


def _format_rate_schedule(period_rates: dict[int, Decimal]) -> str:
    if not period_rates:
        return ""
    parts = [f"Y{period_num} {rate:.2f}%" for period_num, rate in sorted(period_rates.items())]
    return "  \n".join(parts)


def _parse_rate(rate) -> Decimal:
    if rate is None or not str(rate).strip():
        raise ValueError("✗ Enter a rate.")
    try:
        rate_dec = Decimal(str(rate))
    except Exception as exc:
        raise ValueError("✗ Invalid rate.") from exc
    if rate_dec < 0:
        raise ValueError("✗ Rate cannot be negative.")
    return rate_dec


def _get_max_periods(series: str) -> int:
    type_code, _ = parse_series_code(series)
    preset = get_preset(type_code)
    return preset.num_periods if preset else 1


def get_bonds_df() -> tuple[pd.DataFrame, list[int]]:
    """Returns (dataframe, list_of_bond_ids) — IDs correspond to data rows (not the total row)."""
    cols = ["Series", "Qty", "Nominal (PLN)", "Actual (PLN)", "Date Purchased", "Rates", "Maturity", "Status", "Delete"]
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, series, qty, purchase_date, maturity
            FROM bonds ORDER BY series, purchase_date
        """).fetchall()

        if not rows:
            return pd.DataFrame(columns=cols), []

        rate_schedules = _load_bond_rate_schedules(conn)
        today = date.today()
        total_qty = 0
        total_nominal = Decimal("0")
        total_actual = Decimal("0")
        data = []
        bond_ids = []

        for bond_id, series, qty, purchase_date, maturity in rows:
            nominal = qty * FACE_VALUE
            period_rates = rate_schedules.get(bond_id, {})
            actual_per_bond, status = _calc_actual_per_bond(purchase_date, period_rates, today, maturity)
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
                "Rates": _format_rate_schedule(period_rates),
                "Maturity": str(maturity) if maturity else "",
                "Status": status or "",
                "Delete": "🗑️",
            })

        data.append({
            "Series": "Total",
            "Qty": total_qty,
            "Nominal (PLN)": f"{total_nominal:,.2f}",
            "Actual (PLN)": f"{total_actual:,.2f}",
            "Date Purchased": "",
            "Rates": "",
            "Maturity": "",
            "Status": "",
            "Delete": "",
        })

        return pd.DataFrame(data), bond_ids
    finally:
        conn.close()



def add_bond(series_code: str, qty, purchase_date, rate) -> str:
    if not series_code or not series_code.strip():
        return "✗ Enter a series code."

    try:
        _, maturity_month = parse_series_code(series_code)
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

    try:
        rate_dec = _parse_rate(rate)
    except ValueError as exc:
        return str(exc).replace("Enter a rate", "Enter an initial rate")

    series = series_code.strip().upper()
    conn = get_connection()
    try:
        bond_id = conn.execute("SELECT nextval('seq_bonds_id')").fetchone()[0]
        conn.execute("""
            INSERT INTO bonds (id, series, qty, purchase_date, maturity)
            VALUES (?, ?, ?, ?, ?)
        """, [bond_id, series, qty_int, pdate, maturity])
        conn.execute("""
            INSERT INTO bond_year_rates (id, bond_id, period_num, rate)
            VALUES (nextval('seq_bond_year_rates_id'), ?, 1, ?)
        """, [bond_id, rate_dec])
        conn.commit()
        return f"✓ Added {qty_int}x {series} ({pdate})"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def append_bond_rate(bond_id: int, rate) -> str:
    try:
        rate_dec = _parse_rate(rate)
    except ValueError as exc:
        return str(exc)

    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT series
            FROM bonds
            WHERE id = ?
        """, [bond_id]).fetchone()
        if not row:
            return "✗ Bond not found."

        series = row[0]
        max_periods = _get_max_periods(series)
        next_period = conn.execute("""
            SELECT COALESCE(MAX(period_num), 0) + 1
            FROM bond_year_rates
            WHERE bond_id = ?
        """, [bond_id]).fetchone()[0]

        if next_period > max_periods:
            return f"✗ {series} already has all {max_periods} yearly rates."

        conn.execute("""
            INSERT INTO bond_year_rates (id, bond_id, period_num, rate)
            VALUES (nextval('seq_bond_year_rates_id'), ?, ?, ?)
        """, [bond_id, next_period, rate_dec])
        conn.commit()
        return f"✓ Added year {next_period} rate for {series}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def delete_bond_by_id(bond_id: int) -> str:
    conn = get_connection()
    try:
        conn.execute("DELETE FROM bond_year_rates WHERE bond_id = ?", [bond_id])
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
        rows = conn.execute("SELECT id, qty, purchase_date, maturity FROM bonds").fetchall()
        rate_schedules = _load_bond_rate_schedules(conn)
        today = date.today()
        total = Decimal("0")
        for bond_id, qty, purchase_date, maturity in rows:
            actual_per_bond, _ = _calc_actual_per_bond(
                purchase_date,
                rate_schedules.get(bond_id, {}),
                today,
                maturity,
            )
            total += qty * actual_per_bond
        return total
    finally:
        conn.close()
