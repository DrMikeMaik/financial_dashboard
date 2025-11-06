"""Portfolio position calculations with FIFO cost basis."""
import duckdb
from decimal import Decimal
from datetime import datetime
from typing import List, Dict, Tuple
from app.core.models import Position, Holding, Transaction, TransactionAction


def calculate_positions(conn: duckdb.DuckDBPyConnection) -> List[Position]:
    """
    Calculate current positions for all holdings using FIFO.

    Returns list of Position objects with current qty, avg cost, and unrealized P/L.
    """
    # Get all holdings
    holdings_data = conn.execute("""
        SELECT id, asset_type, symbol, name, currency
        FROM holdings
        ORDER BY symbol
    """).fetchall()

    positions = []

    for holding_data in holdings_data:
        holding_id, asset_type, symbol, name, currency = holding_data

        holding = Holding(
            id=holding_id,
            asset_type=asset_type,
            symbol=symbol,
            name=name,
            currency=currency
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
        SELECT id, ts, action, qty, price, fee
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
        txn_id, ts, action, qty, price, fee = txn_data

        if qty is None:
            qty = Decimal("0")
        if price is None:
            price = Decimal("0")
        if fee is None:
            fee = Decimal("0")

        if action == TransactionAction.BUY.value:
            # Add to FIFO queue
            # Cost per unit includes proportional fee
            if qty > 0:
                cost_per_unit = price + (fee / qty)
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
                    realized_pl = proceeds - cost - fee * (oldest_qty / qty)  # proportional fee
                    total_realized_pl += realized_pl
                    remaining_to_sell -= oldest_qty
                else:
                    # Partially consume oldest lot
                    fifo_queue[0] = (oldest_qty - remaining_to_sell, oldest_cost)
                    proceeds = remaining_to_sell * price
                    cost = remaining_to_sell * oldest_cost
                    realized_pl = proceeds - cost - fee * (remaining_to_sell / qty)
                    total_realized_pl += realized_pl
                    remaining_to_sell = Decimal("0")

    # Calculate current position from remaining FIFO queue
    total_qty = sum(q for q, _ in fifo_queue)
    if total_qty == 0:
        return None

    total_cost = sum(q * c for q, c in fifo_queue)
    avg_cost = total_cost / total_qty

    # Get latest price for this holding
    latest_price = _get_latest_price(conn, holding.id, holding.currency)
    if latest_price is None:
        latest_price = avg_cost  # Fallback to cost basis if no price available

    value_native = total_qty * latest_price

    # Get FX rate to PLN
    fx_rate = _get_fx_rate(conn, holding.currency, "PLN")
    value_pln = value_native * fx_rate
    cost_pln = total_cost * fx_rate
    unrealized_pl = value_pln - cost_pln

    return Position(
        holding=holding,
        qty=total_qty,
        avg_cost=avg_cost,
        current_price=latest_price,
        value_native=value_native,
        value_pln=value_pln,
        unrealized_pl=unrealized_pl
    )


def _get_latest_price(conn: duckdb.DuckDBPyConnection, holding_id: int, currency: str) -> Decimal | None:
    """Get the most recent cached price for a holding."""
    result = conn.execute("""
        SELECT price
        FROM prices
        WHERE holding_id = ?
        ORDER BY ts DESC
        LIMIT 1
    """, [holding_id]).fetchone()

    return Decimal(str(result[0])) if result else None


def _get_fx_rate(conn: duckdb.DuckDBPyConnection, from_ccy: str, to_ccy: str) -> Decimal:
    """Get latest FX rate. Returns 1.0 if same currency or not found."""
    if from_ccy == to_ccy:
        return Decimal("1.0")

    # Look for direct rate
    result = conn.execute("""
        SELECT rate
        FROM fx_rates
        WHERE base_ccy = ? AND quote_ccy = ?
        ORDER BY ts DESC
        LIMIT 1
    """, [from_ccy, to_ccy]).fetchone()

    if result:
        return Decimal(str(result[0]))

    # Try inverse rate
    result = conn.execute("""
        SELECT rate
        FROM fx_rates
        WHERE base_ccy = ? AND quote_ccy = ?
        ORDER BY ts DESC
        LIMIT 1
    """, [to_ccy, from_ccy]).fetchone()

    if result:
        return Decimal("1.0") / Decimal(str(result[0]))

    # Not found, return 1.0 as fallback
    return Decimal("1.0")


def get_portfolio_summary(conn: duckdb.DuckDBPyConnection) -> Dict:
    """
    Calculate portfolio-wide summary metrics in PLN.

    Returns dict with net_worth, total_unrealized_pl, total_cash.
    """
    positions = calculate_positions(conn)

    total_value = sum(p.value_pln for p in positions)
    total_unrealized_pl = sum(p.unrealized_pl for p in positions)

    # Get total cash from accounts
    cash_result = conn.execute("""
        SELECT COALESCE(SUM(balance), 0)
        FROM accounts
        WHERE active = TRUE
    """).fetchone()

    total_cash = Decimal(str(cash_result[0])) if cash_result else Decimal("0")

    # Net worth = holdings value + cash
    net_worth = total_value + total_cash

    return {
        "net_worth": net_worth,
        "holdings_value": total_value,
        "unrealized_pl": total_unrealized_pl,
        "cash": total_cash,
    }
