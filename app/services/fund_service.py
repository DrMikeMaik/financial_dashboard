"""Manual funds ledger for account-like recurring investment buckets."""
import calendar
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal

import pandas as pd

from app.core.db import get_connection


@dataclass
class FundSnapshot:
    id: int
    name: str
    start_date: date
    monthly_contribution: Decimal
    starting_amount: Decimal
    current_value: Decimal | None
    current_value_date: date | None
    contributions_count: int
    contributions_total: Decimal
    current_value_pln: Decimal | None
    profit_pln: Decimal | None
    profit_pct: Decimal | None


def _parse_fund_choice(fund_choice: str | None) -> int | None:
    if not fund_choice:
        return None
    return int(str(fund_choice).split("|", 1)[0].strip())


def _parse_date(value, field_label: str, required: bool = True) -> date | None:
    if value in (None, ""):
        if required:
            raise ValueError(f"✗ {field_label} is required.")
        return None

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"✗ Invalid {field_label.lower()}.") from exc


def _parse_amount(value, field_label: str, required: bool = True) -> Decimal | None:
    if value in (None, ""):
        if required:
            raise ValueError(f"✗ {field_label} is required.")
        return None
    try:
        amount = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"✗ Invalid {field_label.lower()}.") from exc
    if amount < 0:
        raise ValueError(f"✗ {field_label} cannot be negative.")
    return amount


def _anniversary_day(year: int, month: int, anchor_day: int) -> int:
    return min(anchor_day, calendar.monthrange(year, month)[1])


def _count_monthly_contributions(start_date: date, as_of_date: date) -> int:
    """Count inclusive monthly anniversaries from start_date through as_of_date."""
    if as_of_date < start_date:
        return 0

    months = (as_of_date.year - start_date.year) * 12 + (as_of_date.month - start_date.month)
    anniversary_day = _anniversary_day(as_of_date.year, as_of_date.month, start_date.day)
    if as_of_date.day < anniversary_day:
        months -= 1

    return max(months + 1, 0)


def _to_datetime(value: date | None) -> datetime | None:
    if value is None:
        return None
    return datetime.combine(value, time.min)


def _build_snapshot(row, conn) -> FundSnapshot:
    fund_id, name, currency, start_date, monthly_contribution, starting_amount, current_value, current_value_date = row

    monthly_dec = Decimal(str(monthly_contribution or 0))
    starting_dec = Decimal(str(starting_amount or 0))
    current_dec = Decimal(str(current_value)) if current_value is not None else None
    value_date = current_value_date or date.today()
    contributions_count = _count_monthly_contributions(start_date, value_date)
    contributions_total = starting_dec + monthly_dec * Decimal(contributions_count)

    current_value_pln = current_dec
    profit_pln = current_dec - contributions_total if current_dec is not None else None
    profit_pct = None
    if profit_pln is not None and contributions_total != 0:
        profit_pct = profit_pln / contributions_total

    return FundSnapshot(
        id=fund_id,
        name=name,
        start_date=start_date,
        monthly_contribution=monthly_dec,
        starting_amount=starting_dec,
        current_value=current_dec,
        current_value_date=current_value_date,
        contributions_count=contributions_count,
        contributions_total=contributions_total,
        current_value_pln=current_value_pln,
        profit_pln=profit_pln,
        profit_pct=profit_pct,
    )


def list_fund_choices() -> list[str]:
    """List active funds as dropdown labels."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, name
            FROM funds
            WHERE active = TRUE
            ORDER BY name, id
        """).fetchall()
        return [f"{row[0]} | {row[1]}" for row in rows]
    finally:
        conn.close()


def get_funds_df() -> tuple[pd.DataFrame, list[int]]:
    """Get all active funds as a display table."""
    cols = ["Fund", "Paid In", "Current Value", "P/L", "Change %", "Updated", "Delete"]
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, name, currency, start_date, monthly_contribution, starting_amount, current_value, current_value_date
            FROM funds
            WHERE active = TRUE
            ORDER BY name, id
        """).fetchall()

        if not rows:
            return pd.DataFrame(columns=cols), []

        data = []
        fund_ids = []
        for row in rows:
            snapshot = _build_snapshot(row, conn)
            fund_ids.append(snapshot.id)
            data.append({
                "Fund": snapshot.name,
                "Paid In": f"{snapshot.contributions_total:,.2f}",
                "Current Value": f"{snapshot.current_value:,.2f}" if snapshot.current_value is not None else "",
                "P/L": f"{snapshot.profit_pln:,.2f}" if snapshot.profit_pln is not None else "",
                "Change %": f"{snapshot.profit_pct * Decimal('100'):,.2f}%" if snapshot.profit_pct is not None else "",
                "Updated": str(snapshot.current_value_date) if snapshot.current_value_date else "",
                "Delete": "🗑️",
            })

        return pd.DataFrame(data), fund_ids
    finally:
        conn.close()


def get_fund_overview_rows() -> list[dict[str, str]]:
    """Build PLN valuation rows for overview tables."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, name, currency, start_date, monthly_contribution, starting_amount, current_value, current_value_date
            FROM funds
            WHERE active = TRUE
            ORDER BY name, id
        """).fetchall()

        overview_rows = []
        for row in rows:
            snapshot = _build_snapshot(row, conn)
            if snapshot.current_value_pln is None:
                continue
            overview_rows.append({
                "Asset Type": "FUND",
                "Symbol": snapshot.name,
                "Quantity": "",
                "Avg Cost (PLN)": "",
                "Current Price (PLN)": "",
                "Value (PLN)": f"{snapshot.current_value_pln:,.2f}",
                "UPL": f"{(snapshot.profit_pln or Decimal('0')):,.2f}",
                "Price Source": "fund_snapshot",
            })

        return overview_rows
    finally:
        conn.close()


def get_funds_total() -> Decimal:
    """Total current fund value in PLN for all active funds with saved snapshots."""
    conn = get_connection()
    try:
        total = Decimal("0")
        rows = conn.execute("""
            SELECT id, name, currency, start_date, monthly_contribution, starting_amount, current_value, current_value_date
            FROM funds
            WHERE active = TRUE
        """).fetchall()
        for row in rows:
            snapshot = _build_snapshot(row, conn)
            if snapshot.current_value_pln is not None:
                total += snapshot.current_value_pln
        return total
    finally:
        conn.close()


def load_fund(fund_choice: str | None) -> tuple[str, datetime | None, float, float, float | None, datetime | None, str]:
    """Load a fund row into the edit form."""
    fund_id = _parse_fund_choice(fund_choice)
    if fund_id is None:
        return "", _to_datetime(date.today()), 0.0, 0.0, None, _to_datetime(date.today()), ""

    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT name, start_date, monthly_contribution, starting_amount, current_value, current_value_date
            FROM funds
            WHERE id = ?
        """, [fund_id]).fetchone()

        if not row:
            return "", _to_datetime(date.today()), 0.0, 0.0, None, _to_datetime(date.today()), "✗ Fund not found."

        return (
            row[0],
            _to_datetime(row[1]),
            float(row[2] or 0),
            float(row[3] or 0),
            float(row[4]) if row[4] is not None else None,
            _to_datetime(row[5]) or _to_datetime(date.today()),
            f"Loaded fund #{fund_id}.",
        )
    finally:
        conn.close()


def save_fund(
    fund_choice: str | None,
    name: str,
    start_date,
    monthly_contribution,
    starting_amount,
    current_value,
    current_value_date,
) -> str:
    """Create or update a manual fund bucket."""
    if not name or not name.strip():
        return "✗ Fund name is required."

    try:
        parsed_start_date = _parse_date(start_date, "Start date")
        monthly_dec = _parse_amount(monthly_contribution, "Monthly contribution")
        starting_dec = _parse_amount(starting_amount, "Starting amount")
        current_dec = _parse_amount(current_value, "Current value", required=False)
        current_value_date_parsed = _parse_date(current_value_date, "Current value date", required=False)
    except ValueError as exc:
        return str(exc)

    if parsed_start_date > date.today():
        return "✗ Start date cannot be in the future."
    if current_value_date_parsed and current_value_date_parsed > date.today():
        return "✗ Current value date cannot be in the future."
    if current_dec is None:
        current_value_date_parsed = None
    if current_dec is not None and current_value_date_parsed is None:
        current_value_date_parsed = date.today()

    fund_id = _parse_fund_choice(fund_choice)
    conn = get_connection()
    try:
        if fund_id is None:
            conn.execute("""
                INSERT INTO funds (
                    id, name, currency, start_date, monthly_contribution, starting_amount, current_value, current_value_date, active
                )
                VALUES (nextval('seq_funds_id'), ?, ?, ?, ?, ?, ?, ?, TRUE)
            """, [
                name.strip(),
                "PLN",
                parsed_start_date,
                monthly_dec,
                starting_dec,
                current_dec,
                current_value_date_parsed,
            ])
            conn.commit()
            return f"✓ Added fund: {name.strip()}"

        conn.execute("""
            UPDATE funds
            SET name = ?, currency = ?, start_date = ?, monthly_contribution = ?, starting_amount = ?,
                current_value = ?, current_value_date = ?, active = TRUE
            WHERE id = ?
        """, [
            name.strip(),
            "PLN",
            parsed_start_date,
            monthly_dec,
            starting_dec,
            current_dec,
            current_value_date_parsed,
            fund_id,
        ])
        conn.commit()
        return f"✓ Updated fund: {name.strip()}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def delete_fund_by_id(fund_id: int) -> str:
    """Delete a fund by numeric id."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM funds WHERE id = ?", [fund_id])
        conn.commit()
        return f"✓ Deleted fund #{fund_id}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()
