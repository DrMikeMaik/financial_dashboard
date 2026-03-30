"""Portfolio position calculations with FIFO cost basis."""
import duckdb
from decimal import Decimal
from datetime import date, datetime, time, timedelta
from typing import List, Dict, Tuple

from app.adapters import fx_nbp
from app.core.models import AssetType, Position, Holding, TransactionAction


def calculate_positions(conn: duckdb.DuckDBPyConnection) -> List[Position]:
    """
    Calculate current positions for all holdings using FIFO.

    Returns list of Position objects with current qty, avg cost, and unrealized P/L.
    """
    # Get all holdings
    holdings_data = conn.execute("""
        SELECT id, asset_type, symbol, name, currency, exchange_label
        FROM holdings
        WHERE asset_type != 'bond'
        ORDER BY symbol
    """).fetchall()

    positions = []

    for holding_data in holdings_data:
        holding_id, asset_type, symbol, name, currency, exchange_label = holding_data

        holding = Holding(
            id=holding_id,
            asset_type=asset_type,
            symbol=symbol,
            name=name,
            currency=currency,
            exchange_label=exchange_label,
        )

        # Calculate position for this holding
        position = _calculate_holding_position(conn, holding)
        if position and position.qty > 0:
            positions.append(position)

    return positions


def _calculate_holding_position(conn: duckdb.DuckDBPyConnection, holding: Holding) -> Position | None:
    """Calculate position for a single holding using FIFO."""

    # Get all transactions for this holding, ordered by timestamp (FIFO)
    txns_data = conn.execute("""
        SELECT id, ts, action, qty, price, fee, fee_currency
        FROM transactions
        WHERE holding_id = ?
        ORDER BY ts ASC, id ASC
    """, [holding.id]).fetchall()

    if not txns_data:
        return None

    # FIFO queue: list of (qty, cost_per_unit) tuples
    fifo_queue: List[Tuple[Decimal, Decimal]] = []
    total_realized_pl = Decimal("0")

    for txn_data in txns_data:
        txn_id, ts, action, qty, price, fee, fee_currency = txn_data

        if qty is None:
            qty = Decimal("0")
        if price is None:
            price = Decimal("0")
        if fee is None:
            fee = Decimal("0")
        effective_fee = _convert_fee_to_trade_currency(conn, holding, ts, fee, fee_currency)

        if action == TransactionAction.BUY.value:
            # Add to FIFO queue
            # Cost per unit includes proportional fee
            if qty > 0:
                cost_per_unit = price + (effective_fee / qty)
                fifo_queue.append((qty, cost_per_unit))

        elif action == TransactionAction.SELL.value:
            # Remove from FIFO queue and calculate realized P/L
            remaining_to_sell = qty

            while remaining_to_sell > 0 and fifo_queue:
                oldest_qty, oldest_cost = fifo_queue[0]

                if oldest_qty <= remaining_to_sell:
                    # Consume entire oldest lot
                    fifo_queue.pop(0)
                    proceeds = oldest_qty * price
                    cost = oldest_qty * oldest_cost
                    realized_pl = proceeds - cost - effective_fee * (oldest_qty / qty)
                    total_realized_pl += realized_pl
                    remaining_to_sell -= oldest_qty
                else:
                    # Partially consume oldest lot
                    fifo_queue[0] = (oldest_qty - remaining_to_sell, oldest_cost)
                    proceeds = remaining_to_sell * price
                    cost = remaining_to_sell * oldest_cost
                    realized_pl = proceeds - cost - effective_fee * (remaining_to_sell / qty)
                    total_realized_pl += realized_pl
                    remaining_to_sell = Decimal("0")

    # Calculate current position from remaining FIFO queue
    total_qty = sum(q for q, _ in fifo_queue)
    if total_qty == 0:
        return None

    total_cost = sum(q * c for q, c in fifo_queue)
    avg_cost = total_cost / total_qty

    valuation_warnings: List[str] = []

    # Get latest price for this holding and preserve the cached price currency.
    latest_price, price_ccy, price_source, price_ts = get_latest_price_info(conn, holding.id)
    if latest_price is None:
        latest_price = avg_cost
        price_ccy = holding.currency
        price_source = "cost_basis"
        valuation_warnings.append("Using cost basis because no cached market price is available.")

    value_native = total_qty * latest_price

    # Value and cost may be denominated in different currencies.
    value_fx_rate, value_fx_found, _, _ = get_fx_rate_info(conn, price_ccy, "PLN")
    cost_fx_rate, cost_fx_found, _, _ = get_fx_rate_info(conn, holding.currency, "PLN")

    if price_ccy != "PLN" and not value_fx_found:
        valuation_warnings.append(f"Missing FX rate for {price_ccy}/PLN; using 1.0 fallback.")

    if holding.currency != "PLN" and not cost_fx_found:
        valuation_warnings.append(f"Missing FX rate for {holding.currency}/PLN cost conversion; using 1.0 fallback.")

    value_pln = value_native * value_fx_rate
    cost_pln = total_cost * cost_fx_rate
    unrealized_pl = value_pln - cost_pln

    return Position(
        holding=holding,
        qty=total_qty,
        avg_cost=avg_cost,
        current_price=latest_price,
        current_price_ccy=price_ccy,
        value_native=value_native,
        value_pln=value_pln,
        unrealized_pl=unrealized_pl,
        price_source=price_source,
        price_ts=price_ts,
        valuation_warning=" ".join(valuation_warnings) if valuation_warnings else None,
    )


def get_latest_price_info(
    conn: duckdb.DuckDBPyConnection,
    holding_id: int,
) -> Tuple[Decimal | None, str | None, str | None, datetime | None]:
    """Get the most recent cached price for a holding."""
    result = conn.execute("""
        SELECT price, price_ccy, source, ts
        FROM prices
        WHERE holding_id = ?
        ORDER BY ts DESC
        LIMIT 1
    """, [holding_id]).fetchone()

    if not result:
        return None, None, None, None

    return Decimal(str(result[0])), result[1], result[2], result[3]


def get_fx_rate_info(
    conn: duckdb.DuckDBPyConnection,
    from_ccy: str,
    to_ccy: str,
) -> Tuple[Decimal, bool, str | None, datetime | None]:
    """Get latest FX rate details. Returns 1.0 when currencies match or rate is missing."""
    if from_ccy == to_ccy:
        return Decimal("1.0"), True, "identity", None

    # Look for direct rate
    result = conn.execute("""
        SELECT rate, source, ts
        FROM fx_rates
        WHERE base_ccy = ? AND quote_ccy = ?
        ORDER BY ts DESC
        LIMIT 1
    """, [from_ccy, to_ccy]).fetchone()

    if result:
        return Decimal(str(result[0])), True, result[1], result[2]

    # Try inverse rate
    result = conn.execute("""
        SELECT rate, source, ts
        FROM fx_rates
        WHERE base_ccy = ? AND quote_ccy = ?
        ORDER BY ts DESC
        LIMIT 1
    """, [to_ccy, from_ccy]).fetchone()

    if result:
        return Decimal("1.0") / Decimal(str(result[0])), True, result[1], result[2]

    # Not found, return 1.0 as fallback
    return Decimal("1.0"), False, None, None


def get_historical_fx_rate_info(
    conn: duckdb.DuckDBPyConnection,
    from_ccy: str,
    to_ccy: str,
    target_date: date,
    fetch_missing: bool = False,
) -> Tuple[Decimal, bool, str | None, datetime | None]:
    """Get same-day FX details, optionally fetching from NBP for PLN pairs."""
    if from_ccy == to_ccy:
        return Decimal("1.0"), True, "identity", datetime.combine(target_date, time.min)

    result = conn.execute("""
        SELECT rate, source, ts
        FROM fx_rates
        WHERE base_ccy = ? AND quote_ccy = ? AND CAST(ts AS DATE) = ?
        ORDER BY CASE WHEN source = 'NBP_HIST' THEN 0 ELSE 1 END, ts DESC
        LIMIT 1
    """, [from_ccy, to_ccy, target_date]).fetchone()
    if result:
        return Decimal(str(result[0])), True, result[1], result[2]

    result = conn.execute("""
        SELECT rate, source, ts
        FROM fx_rates
        WHERE base_ccy = ? AND quote_ccy = ? AND CAST(ts AS DATE) = ?
        ORDER BY CASE WHEN source = 'NBP_HIST' THEN 0 ELSE 1 END, ts DESC
        LIMIT 1
    """, [to_ccy, from_ccy, target_date]).fetchone()
    if result:
        return Decimal("1.0") / Decimal(str(result[0])), True, result[1], result[2]

    if not fetch_missing:
        return Decimal("1.0"), False, None, None

    normalized_ts = datetime.combine(target_date, time.min)
    if to_ccy == "PLN":
        rate = fx_nbp.get_rate_on_date(from_ccy, "PLN", target_date)
        if rate is not None:
            conn.execute("""
                INSERT INTO fx_rates (id, ts, base_ccy, quote_ccy, rate, source)
                VALUES (nextval('seq_fx_rates_id'), ?, ?, ?, ?, 'NBP_HIST')
                ON CONFLICT DO NOTHING
            """, [normalized_ts, from_ccy, "PLN", float(rate)])
            conn.commit()
            return rate, True, "NBP_HIST", normalized_ts

    if from_ccy == "PLN":
        rate = fx_nbp.get_rate_on_date(to_ccy, "PLN", target_date)
        if rate is not None:
            conn.execute("""
                INSERT INTO fx_rates (id, ts, base_ccy, quote_ccy, rate, source)
                VALUES (nextval('seq_fx_rates_id'), ?, ?, ?, ?, 'NBP_HIST')
                ON CONFLICT DO NOTHING
            """, [normalized_ts, to_ccy, "PLN", float(rate)])
            conn.commit()
            return Decimal("1.0") / rate, True, "NBP_HIST", normalized_ts

    return Decimal("1.0"), False, None, None


def _convert_fee_to_trade_currency(
    conn: duckdb.DuckDBPyConnection,
    holding: Holding,
    txn_ts: datetime,
    fee: Decimal,
    fee_currency: str | None,
) -> Decimal:
    """Convert stock/ETF PLN commissions into the holding currency for FIFO math."""
    if fee == 0:
        return Decimal("0")

    normalized_fee_currency = (fee_currency or holding.currency).upper()
    if normalized_fee_currency == holding.currency:
        return fee

    if normalized_fee_currency == "PLN" and holding.asset_type in {AssetType.STOCK.value, AssetType.ETF.value}:
        rate, found, _, _ = get_historical_fx_rate_info(conn, holding.currency, "PLN", txn_ts.date(), fetch_missing=True)
        if found and rate != 0:
            return fee / rate

    return fee


def get_portfolio_warnings(
    conn: duckdb.DuckDBPyConnection,
    stale_after_hours: int = 24,
) -> List[str]:
    """Collect dashboard warnings for stale or missing cached market data."""
    warnings: List[str] = []
    stale_cutoff = datetime.now() - timedelta(hours=stale_after_hours)

    missing_price_rows = conn.execute("""
        SELECT h.symbol
        FROM holdings h
        WHERE h.asset_type IN ('crypto', 'stock', 'etf')
          AND NOT EXISTS (
              SELECT 1
              FROM prices p
              WHERE p.holding_id = h.id
          )
        ORDER BY h.symbol
    """).fetchall()

    if missing_price_rows:
        missing_symbols = ", ".join(row[0] for row in missing_price_rows[:6])
        warnings.append(f"Missing cached prices for: {missing_symbols}.")

    stale_price_rows = conn.execute("""
        SELECT h.symbol, latest.ts
        FROM holdings h
        JOIN (
            SELECT holding_id, MAX(ts) AS ts
            FROM prices
            GROUP BY holding_id
        ) latest ON latest.holding_id = h.id
        WHERE h.asset_type IN ('crypto', 'stock', 'etf')
          AND latest.ts < ?
        ORDER BY latest.ts ASC, h.symbol
    """, [stale_cutoff]).fetchall()

    if stale_price_rows:
        stale_symbols = ", ".join(row[0] for row in stale_price_rows[:6])
        warnings.append(f"Stale cached prices older than {stale_after_hours}h: {stale_symbols}.")

    return warnings


def get_portfolio_summary(conn: duckdb.DuckDBPyConnection) -> Dict:
    """
    Calculate portfolio-wide summary metrics in PLN.

    Returns dict with net_worth, total_unrealized_pl, total_cash.
    """
    positions = calculate_positions(conn)

    total_value = sum(p.value_pln for p in positions)
    total_unrealized_pl = sum(p.unrealized_pl for p in positions)
    warnings = get_portfolio_warnings(conn)

    for position in positions:
        if position.valuation_warning:
            warnings.append(f"{position.holding.symbol}: {position.valuation_warning}")

    # Get total cash from accounts and convert each account currency to PLN.
    cash_rows = conn.execute("""
        SELECT currency, COALESCE(SUM(balance), 0)
        FROM accounts
        WHERE active = TRUE
        GROUP BY currency
        ORDER BY currency
    """).fetchall()

    total_cash = Decimal("0")
    for currency, balance in cash_rows:
        balance_dec = Decimal(str(balance))
        fx_rate, found, _, _ = get_fx_rate_info(conn, currency, "PLN")
        if currency != "PLN" and not found:
            warnings.append(f"Missing FX rate for cash account currency {currency}/PLN; using 1.0 fallback.")
        total_cash += balance_dec * fx_rate

    # Net worth = holdings value + cash
    net_worth = total_value + total_cash

    latest_price_ts = conn.execute("SELECT MAX(ts) FROM prices").fetchone()[0]
    latest_fx_ts = conn.execute("SELECT MAX(ts) FROM fx_rates").fetchone()[0]

    return {
        "net_worth": net_worth,
        "holdings_value": total_value,
        "unrealized_pl": total_unrealized_pl,
        "cash": total_cash,
        "warnings": list(dict.fromkeys(warnings)),
        "latest_price_ts": latest_price_ts,
        "latest_fx_ts": latest_fx_ts,
    }
