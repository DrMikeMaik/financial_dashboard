"""Manual bond metadata and valuation helpers."""
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from app.core.db import get_connection
from app.core.portfolio import calculate_positions, get_latest_price_info


def _parse_bond_choice(bond_choice: str | None) -> int | None:
    if not bond_choice:
        return None
    return int(str(bond_choice).split("|", 1)[0].strip())


def _parse_date(value: str | None) -> date:
    if not value or not str(value).strip():
        raise ValueError("Maturity date is required.")
    return date.fromisoformat(str(value).strip())


def get_bonds_df() -> pd.DataFrame:
    """Get bond metadata plus latest valuation."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                h.id,
                h.symbol,
                h.name,
                h.currency,
                b.face,
                b.coupon_rate,
                b.coupon_freq,
                b.maturity_date,
                b.issuer
            FROM holdings h
            JOIN bond_meta b ON b.holding_id = h.id
            WHERE h.asset_type = 'bond'
            ORDER BY h.symbol
        """).fetchall()

        if not rows:
            return pd.DataFrame(columns=["ID", "Symbol", "Name", "Currency", "Quantity", "Face", "Coupon", "Frequency", "Maturity", "Issuer", "Current Price", "Value (PLN)", "Source"])

        positions = {position.holding.id: position for position in calculate_positions(conn) if position.holding.asset_type == "bond"}

        data = []
        for row in rows:
            holding_id, symbol, name, currency, face, coupon_rate, coupon_freq, maturity_date, issuer = row
            position = positions.get(holding_id)
            latest_price, latest_ccy, latest_source, _ = get_latest_price_info(conn, holding_id)

            data.append({
                "ID": holding_id,
                "Symbol": symbol,
                "Name": name or "",
                "Currency": currency,
                "Quantity": f"{position.qty:.8f}" if position else "",
                "Face": f"{Decimal(str(face)):.2f} {currency}",
                "Coupon": f"{Decimal(str(coupon_rate)):.2f}%",
                "Frequency": f"{coupon_freq}x/year",
                "Maturity": str(maturity_date),
                "Issuer": issuer or "",
                "Current Price": f"{Decimal(str(latest_price)):.2f} {latest_ccy}" if latest_price is not None else "",
                "Value (PLN)": f"{position.value_pln:,.2f}" if position else "",
                "Source": latest_source or "",
            })

        return pd.DataFrame(data)
    finally:
        conn.close()


def load_bond(bond_choice: str | None) -> tuple[str, str, str, float, float, int, str, str, str]:
    """Load a bond into the edit form."""
    holding_id = _parse_bond_choice(bond_choice)
    if holding_id is None:
        return "", "", "PLN", 100.0, 0.0, 1, "", "", ""

    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT
                h.symbol,
                h.name,
                h.currency,
                b.face,
                b.coupon_rate,
                b.coupon_freq,
                b.maturity_date,
                b.issuer
            FROM holdings h
            JOIN bond_meta b ON b.holding_id = h.id
            WHERE h.id = ?
        """, [holding_id]).fetchone()

        if not row:
            return "", "", "PLN", 100.0, 0.0, 1, "", "", "✗ Bond not found."

        return row[0], row[1] or "", row[2], float(row[3]), float(row[4]), int(row[5]), str(row[6]), row[7] or "", f"Loaded bond #{holding_id}."
    finally:
        conn.close()


def save_bond(
    bond_choice: str | None,
    symbol: str,
    name: str,
    currency: str,
    face,
    coupon_rate,
    coupon_freq,
    maturity_date_text: str,
    issuer: str,
) -> str:
    """Create or update bond metadata."""
    if not symbol or not symbol.strip():
        return "✗ Bond symbol is required."
    if not currency or not currency.strip():
        return "✗ Bond currency is required."

    try:
        face_dec = Decimal(str(face))
        coupon_rate_dec = Decimal(str(coupon_rate))
        maturity_date = _parse_date(maturity_date_text)
        coupon_freq_int = int(coupon_freq)
    except Exception as exc:
        return f"✗ Invalid bond data: {exc}"

    holding_id = _parse_bond_choice(bond_choice)

    conn = get_connection()
    try:
        if holding_id is None:
            conn.execute("""
                INSERT INTO holdings (asset_type, symbol, name, currency)
                VALUES ('bond', ?, ?, ?)
            """, [symbol.strip().upper(), name.strip() or None, currency.strip().upper()])
            holding_id = conn.execute("""
                SELECT id
                FROM holdings
                WHERE asset_type = 'bond' AND symbol = ?
            """, [symbol.strip().upper()]).fetchone()[0]

            conn.execute("""
                INSERT INTO bond_meta (holding_id, face, coupon_rate, coupon_freq, maturity_date, issuer)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [holding_id, face_dec, coupon_rate_dec, coupon_freq_int, maturity_date, issuer.strip() or None])
            conn.commit()
            return f"✓ Added bond: {symbol.strip().upper()}"

        conn.execute("""
            UPDATE holdings
            SET symbol = ?, name = ?, currency = ?
            WHERE id = ?
        """, [symbol.strip().upper(), name.strip() or None, currency.strip().upper(), holding_id])
        conn.execute("""
            UPDATE bond_meta
            SET face = ?, coupon_rate = ?, coupon_freq = ?, maturity_date = ?, issuer = ?
            WHERE holding_id = ?
        """, [face_dec, coupon_rate_dec, coupon_freq_int, maturity_date, issuer.strip() or None, holding_id])
        conn.commit()
        return f"✓ Updated bond: {symbol.strip().upper()}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def delete_bond(bond_choice: str | None) -> str:
    """Delete a bond if it is unused."""
    holding_id = _parse_bond_choice(bond_choice)
    if holding_id is None:
        return "✗ Select a bond to delete."

    conn = get_connection()
    try:
        txn_count = conn.execute("""
            SELECT COUNT(*)
            FROM transactions
            WHERE holding_id = ?
        """, [holding_id]).fetchone()[0]
        if txn_count:
            return "✗ Cannot delete a bond that already has transactions."

        price_count = conn.execute("""
            SELECT COUNT(*)
            FROM prices
            WHERE holding_id = ?
        """, [holding_id]).fetchone()[0]
        if price_count:
            return "✗ Cannot delete a bond that already has price history."

        conn.execute("DELETE FROM bond_meta WHERE holding_id = ?", [holding_id])
        conn.execute("DELETE FROM holdings WHERE id = ?", [holding_id])
        conn.commit()
        return f"✓ Deleted bond #{holding_id}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def save_bond_valuation(
    bond_choice: str | None,
    mode: str,
    value,
    valuation_ts: str | None = None,
) -> str:
    """Store a manual bond valuation in the shared prices table."""
    holding_id = _parse_bond_choice(bond_choice)
    if holding_id is None:
        return "✗ Select a bond before saving a valuation."

    try:
        input_value = Decimal(str(value))
    except Exception:
        return "✗ Enter a numeric valuation."

    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT h.currency, b.face
            FROM holdings h
            JOIN bond_meta b ON b.holding_id = h.id
            WHERE h.id = ?
        """, [holding_id]).fetchone()

        if not row:
            return "✗ Bond not found."

        currency, face = row[0], Decimal(str(row[1]))
        if mode == "Percent of face":
            unit_price = face * input_value / Decimal("100")
        else:
            unit_price = input_value

        timestamp = datetime.now()
        if valuation_ts and str(valuation_ts).strip():
            timestamp = datetime.fromisoformat(str(valuation_ts).strip())

        conn.execute("""
            INSERT INTO prices (holding_id, ts, price, price_ccy, source)
            VALUES (?, ?, ?, ?, 'manual')
        """, [holding_id, timestamp, unit_price, currency])
        conn.commit()
        return f"✓ Saved manual bond valuation at {unit_price:.2f} {currency}"
    except ValueError:
        return "✗ Invalid valuation timestamp. Use ISO format like 2026-03-25 14:30:00."
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def get_bond_valuation_details(bond_choice: str | None) -> str:
    """Describe the latest bond valuation."""
    holding_id = _parse_bond_choice(bond_choice)
    if holding_id is None:
        return "No bond selected."

    conn = get_connection()
    try:
        latest_price, latest_ccy, latest_source, latest_ts = get_latest_price_info(conn, holding_id)
        if latest_price is None:
            return "No manual valuation saved yet."
        return f"Latest valuation: {latest_price:.2f} {latest_ccy} from {latest_source} on {latest_ts}"
    finally:
        conn.close()
