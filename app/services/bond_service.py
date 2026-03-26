"""Manual bond metadata and valuation helpers."""
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from app.core.bonds import POLISH_BOND_PRESETS, parse_series_code, get_preset
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


def _get_bond_meta_id(conn, holding_id: int) -> int | None:
    row = conn.execute(
        "SELECT id FROM bond_meta WHERE holding_id = ?", [holding_id]
    ).fetchone()
    return row[0] if row else None


def get_bonds_df() -> pd.DataFrame:
    """Get bonds table matching PKO dashboard layout."""
    cols = ["Series", "Qty", "Nominal (PLN)", "Current (PLN)", "Maturity", "Rates"]
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT h.id, h.symbol, b.id AS bm_id, b.face, b.maturity_date
            FROM holdings h
            JOIN bond_meta b ON b.holding_id = h.id
            WHERE h.asset_type = 'bond'
            ORDER BY h.symbol
        """).fetchall()

        if not rows:
            return pd.DataFrame(columns=cols)

        positions = {
            p.holding.id: p
            for p in calculate_positions(conn)
            if p.holding.asset_type == "bond"
        }

        total_qty = 0
        total_nominal = Decimal("0")
        total_current = Decimal("0")
        data = []

        for holding_id, symbol, bm_id, face, maturity_date in rows:
            position = positions.get(holding_id)
            qty = int(position.qty) if position else 0
            face_dec = Decimal(str(face))
            nominal = qty * face_dec
            latest_price, _, _, _ = get_latest_price_info(conn, holding_id)
            current = qty * latest_price if latest_price else Decimal("0")

            # Period rates
            rates_rows = conn.execute("""
                SELECT rate FROM bond_period_rates
                WHERE bond_meta_id = ? ORDER BY period_num
            """, [bm_id]).fetchall()
            rates_str = ", ".join(f"{Decimal(str(r[0])):.2f}%" for r in rates_rows)

            total_qty += qty
            total_nominal += nominal
            total_current += current

            data.append({
                "Series": symbol,
                "Qty": qty,
                "Nominal (PLN)": f"{nominal:,.2f}",
                "Current (PLN)": f"{current:,.2f}" if latest_price else "",
                "Maturity": str(maturity_date),
                "Rates": rates_str,
            })

        data.append({
            "Series": "Total",
            "Qty": total_qty,
            "Nominal (PLN)": f"{total_nominal:,.2f}",
            "Current (PLN)": f"{total_current:,.2f}",
            "Maturity": "",
            "Rates": "",
        })

        return pd.DataFrame(data)
    finally:
        conn.close()


def load_bond(bond_choice: str | None) -> tuple:
    """Load a bond into the edit form.

    Returns: (symbol, name, currency, face, coupon_rate, coupon_freq,
              maturity_date, issuer, bond_type, rate_type, status)
    """
    holding_id = _parse_bond_choice(bond_choice)
    if holding_id is None:
        return "", "", "PLN", 100.0, 0.0, 1, "", "", "", "", ""

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
                b.issuer,
                b.bond_type,
                b.rate_type
            FROM holdings h
            JOIN bond_meta b ON b.holding_id = h.id
            WHERE h.id = ?
        """, [holding_id]).fetchone()

        if not row:
            return "", "", "PLN", 100.0, 0.0, 1, "", "", "", "", "Bond not found."

        return (
            row[0], row[1] or "", row[2],
            float(row[3]), float(row[4]), int(row[5]),
            str(row[6]), row[7] or "",
            row[8] or "", row[9] or "",
            f"Loaded bond #{holding_id}.",
        )
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
    bond_type: str | None = None,
    rate_type: str | None = None,
) -> str:
    """Create or update bond metadata."""
    if not symbol or not symbol.strip():
        return "✗ Symbol is required."
    if not currency or not currency.strip():
        return "✗ Currency is required."

    try:
        face_dec = Decimal(str(face))
        coupon_rate_dec = Decimal(str(coupon_rate))
        maturity_date = _parse_date(maturity_date_text)
        coupon_freq_int = int(coupon_freq)
    except Exception as exc:
        return f"✗ Invalid bond data: {exc}"

    holding_id = _parse_bond_choice(bond_choice)
    bond_type_val = bond_type.strip().upper() if bond_type and bond_type.strip() else None
    rate_type_val = rate_type.strip().lower() if rate_type and rate_type.strip() else None
    series_code = symbol.strip().upper()

    conn = get_connection()
    try:
        if holding_id is None:
            conn.execute("""
                INSERT INTO holdings (id, asset_type, symbol, name, currency)
                VALUES (nextval('seq_holdings_id'), 'bond', ?, ?, ?)
            """, [series_code, name.strip() or None, currency.strip().upper()])
            holding_id = conn.execute("""
                SELECT id FROM holdings
                WHERE asset_type = 'bond' AND symbol = ?
            """, [series_code]).fetchone()[0]

            conn.execute("""
                INSERT INTO bond_meta
                    (id, holding_id, face, coupon_rate, coupon_freq, maturity_date, issuer,
                     bond_type, rate_type, series_code)
                VALUES (nextval('seq_bond_meta_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [holding_id, face_dec, coupon_rate_dec, coupon_freq_int,
                  maturity_date, issuer.strip() or None,
                  bond_type_val, rate_type_val, series_code])
            conn.commit()
            return f"✓ Added bond: {series_code}"

        conn.execute("""
            UPDATE holdings
            SET symbol = ?, name = ?, currency = ?
            WHERE id = ?
        """, [series_code, name.strip() or None, currency.strip().upper(), holding_id])
        conn.execute("""
            UPDATE bond_meta
            SET face = ?, coupon_rate = ?, coupon_freq = ?, maturity_date = ?,
                issuer = ?, bond_type = ?, rate_type = ?, series_code = ?
            WHERE holding_id = ?
        """, [face_dec, coupon_rate_dec, coupon_freq_int, maturity_date,
              issuer.strip() or None, bond_type_val, rate_type_val, series_code,
              holding_id])
        conn.commit()
        return f"✓ Updated bond: {series_code}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def save_bond_from_preset(series_code_input: str) -> str:
    """Quick-add a Polish treasury bond from its series code (e.g. COI0528)."""
    if not series_code_input or not series_code_input.strip():
        return "Enter a series code (e.g. COI0528)."

    try:
        type_code, maturity = parse_series_code(series_code_input)
    except ValueError as exc:
        return str(exc)

    preset = get_preset(type_code)
    if not preset:
        return f"Unknown bond type: {type_code}"

    series = series_code_input.strip().upper()
    auto_name = preset.full_name

    conn = get_connection()
    try:
        existing = conn.execute("""
            SELECT id FROM holdings
            WHERE asset_type = 'bond' AND symbol = ?
        """, [series]).fetchone()
        if existing:
            return f"✗ Bond {series} already exists."

        conn.execute("""
            INSERT INTO holdings (id, asset_type, symbol, name, currency)
            VALUES (nextval('seq_holdings_id'), 'bond', ?, ?, ?)
        """, [series, auto_name, preset.currency])
        holding_id = conn.execute("""
            SELECT id FROM holdings
            WHERE asset_type = 'bond' AND symbol = ?
        """, [series]).fetchone()[0]

        conn.execute("""
            INSERT INTO bond_meta
                (id, holding_id, face, coupon_rate, coupon_freq, maturity_date, issuer,
                 bond_type, rate_type, series_code)
            VALUES (nextval('seq_bond_meta_id'), ?, ?, 0, ?, ?, ?, ?, ?, ?)
        """, [holding_id, preset.face, preset.coupon_freq, maturity,
              preset.issuer, preset.code, preset.rate_type, series])
        conn.commit()
        return f"✓ Added {series} ({preset.full_name}), maturity {maturity}, face {preset.face} {preset.currency}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def record_bond_purchase(bond_choice: str | None, qty, purchase_date) -> str:
    """Record a bond purchase (always at 100 PLN face value)."""
    holding_id = _parse_bond_choice(bond_choice)
    if holding_id is None:
        return "✗ Select a bond first."

    try:
        qty_int = int(qty)
    except (TypeError, ValueError):
        return "✗ Enter a valid quantity."
    if qty_int < 1:
        return "✗ Quantity must be at least 1."

    if isinstance(purchase_date, datetime):
        ts = purchase_date
    elif purchase_date:
        try:
            ts = datetime.fromisoformat(str(purchase_date).strip())
        except ValueError:
            return "✗ Invalid date."
    else:
        return "✗ Select a purchase date."

    if ts.date() > date.today():
        return "✗ Purchase date cannot be in the future."

    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO transactions (id, holding_id, ts, action, qty, price, fee)
            VALUES (nextval('seq_transactions_id'), ?, ?, 'buy', ?, 100, 0)
        """, [holding_id, ts, qty_int])
        conn.commit()
        return f"✓ Recorded purchase: {qty_int} bonds on {ts.date()}"
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
            SELECT COUNT(*) FROM transactions WHERE holding_id = ?
        """, [holding_id]).fetchone()[0]
        if txn_count:
            return "✗ Cannot delete a bond that already has transactions."

        price_count = conn.execute("""
            SELECT COUNT(*) FROM prices WHERE holding_id = ?
        """, [holding_id]).fetchone()[0]
        if price_count:
            return "✗ Cannot delete a bond that already has price history."

        bond_meta_id = _get_bond_meta_id(conn, holding_id)
        if bond_meta_id:
            conn.execute("DELETE FROM bond_period_rates WHERE bond_meta_id = ?", [bond_meta_id])
        conn.execute("DELETE FROM bond_meta WHERE holding_id = ?", [holding_id])
        conn.execute("DELETE FROM holdings WHERE id = ?", [holding_id])
        conn.commit()
        return f"✓ Deleted bond #{holding_id}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


# --- Manual valuation (unchanged logic) ---

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

    if input_value <= 0:
        return "✗ Value must be positive."

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
            INSERT INTO prices (id, holding_id, ts, price, price_ccy, source)
            VALUES (nextval('seq_prices_id'), ?, ?, ?, ?, 'manual')
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


# --- Period interest rates ---

def get_period_rates_df(bond_choice: str | None) -> pd.DataFrame:
    """Get period rates for a bond as a DataFrame."""
    cols = ["Period", "Rate (%)"]
    holding_id = _parse_bond_choice(bond_choice)
    if holding_id is None:
        return pd.DataFrame(columns=cols)

    conn = get_connection()
    try:
        bond_meta_id = _get_bond_meta_id(conn, holding_id)
        if bond_meta_id is None:
            return pd.DataFrame(columns=cols)

        rows = conn.execute("""
            SELECT period_num, rate
            FROM bond_period_rates
            WHERE bond_meta_id = ?
            ORDER BY period_num
        """, [bond_meta_id]).fetchall()

        if not rows:
            return pd.DataFrame(columns=cols)

        return pd.DataFrame(
            [{"Period": r[0], "Rate (%)": f"{Decimal(str(r[1])):.2f}"} for r in rows]
        )
    finally:
        conn.close()


def save_period_rate(bond_choice: str | None, period_num, rate) -> str:
    """Add or update a period interest rate for a bond."""
    holding_id = _parse_bond_choice(bond_choice)
    if holding_id is None:
        return "Select a bond first."

    try:
        period = int(period_num)
        rate_dec = Decimal(str(rate))
    except (TypeError, ValueError):
        return "Enter a valid period number and rate."

    if period < 1:
        return "Period must be 1 or greater."
    if rate_dec < 0:
        return "Rate cannot be negative."

    conn = get_connection()
    try:
        bond_meta_id = _get_bond_meta_id(conn, holding_id)
        if bond_meta_id is None:
            return "Bond metadata not found."

        conn.execute("""
            INSERT INTO bond_period_rates (id, bond_meta_id, period_num, rate)
            VALUES (nextval('seq_bond_period_rates_id'), ?, ?, ?)
            ON CONFLICT (bond_meta_id, period_num)
            DO UPDATE SET rate = EXCLUDED.rate
        """, [bond_meta_id, period, rate_dec])
        conn.commit()
        return f"Saved period {period} rate: {rate_dec:.2f}%"
    except Exception as exc:
        return f"Error: {exc}"
    finally:
        conn.close()


def delete_period_rate(bond_choice: str | None, period_num) -> str:
    """Delete a period interest rate."""
    holding_id = _parse_bond_choice(bond_choice)
    if holding_id is None:
        return "Select a bond first."

    try:
        period = int(period_num)
    except (TypeError, ValueError):
        return "Enter a valid period number."

    conn = get_connection()
    try:
        bond_meta_id = _get_bond_meta_id(conn, holding_id)
        if bond_meta_id is None:
            return "Bond metadata not found."

        count = conn.execute("""
            DELETE FROM bond_period_rates
            WHERE bond_meta_id = ? AND period_num = ?
        """, [bond_meta_id, period]).fetchone()[0]
        conn.commit()
        if count == 0:
            return f"No rate found for period {period}."
        return f"Deleted period {period} rate."
    except Exception as exc:
        return f"Error: {exc}"
    finally:
        conn.close()
