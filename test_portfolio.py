"""Test script for portfolio FIFO calculations."""
from app.core.db import init_db
from app.core.portfolio import calculate_positions, get_portfolio_summary
from datetime import datetime
from decimal import Decimal
from pathlib import Path


def main():
    print("Testing Portfolio FIFO Calculations\n")
    print("=" * 50)

    # Delete old test database if exists
    test_db = Path("data/test_portfolio.duckdb")
    if test_db.exists():
        test_db.unlink()

    # Initialize fresh database for testing
    conn = init_db("data/test_portfolio.duckdb")

    # Add a test holding (BTC)
    conn.execute("""
        INSERT INTO holdings (asset_type, symbol, name, currency)
        VALUES ('crypto', 'BTC', 'Bitcoin', 'USD')
    """)

    # Get the holding ID
    holding_id = conn.execute("SELECT id FROM holdings WHERE symbol = 'BTC'").fetchone()[0]

    # Add BTC transactions (FIFO test scenario)
    # Buy 1 BTC at $30,000
    conn.execute("""
        INSERT INTO transactions (holding_id, ts, action, qty, price, fee)
        VALUES (?, '2024-01-01 10:00:00', 'buy', 1.0, 30000.0, 100.0)
    """, [holding_id])

    # Buy 2 BTC at $40,000
    conn.execute("""
        INSERT INTO transactions (holding_id, ts, action, qty, price, fee)
        VALUES (?, '2024-02-01 10:00:00', 'buy', 2.0, 40000.0, 200.0)
    """, [holding_id])

    # Sell 1.5 BTC at $50,000 (should sell the first 1 BTC + 0.5 from second lot)
    conn.execute("""
        INSERT INTO transactions (holding_id, ts, action, qty, price, fee)
        VALUES (?, '2024-03-01 10:00:00', 'sell', 1.5, 50000.0, 150.0)
    """, [holding_id])

    # Add current BTC price
    conn.execute("""
        INSERT INTO prices (holding_id, ts, price, price_ccy, source)
        VALUES (?, '2024-10-08 15:00:00', 60000.0, 'USD', 'test')
    """, [holding_id])

    # Add USD -> PLN FX rate
    conn.execute("""
        INSERT INTO fx_rates (ts, base_ccy, quote_ccy, rate, source)
        VALUES ('2024-10-08 15:00:00', 'USD', 'PLN', 4.0, 'test')
    """)

    conn.commit()

    print("\n1. Test scenario:")
    print("   - Buy 1 BTC @ $30,000 (fee $100)")
    print("   - Buy 2 BTC @ $40,000 (fee $200)")
    print("   - Sell 1.5 BTC @ $50,000 (fee $150)")
    print("   - Current price: $60,000")
    print("   - USD/PLN rate: 4.0")

    # Calculate position
    print("\n2. Calculating position...")
    positions = calculate_positions(conn)

    if positions:
        pos = positions[0]
        print(f"\n   Remaining quantity: {pos.qty}")
        print(f"   Average cost: ${pos.avg_cost:.2f}")
        print(f"   Current price: ${pos.current_price:.2f}")
        print(f"   Value (USD): ${pos.value_native:.2f}")
        print(f"   Value (PLN): {pos.value_pln:.2f} PLN")
        print(f"   Unrealized P/L: {pos.unrealized_pl:.2f} PLN")

        # Expected: 1.5 BTC remaining
        # Remaining lots: 1.5 BTC from second purchase at $40,100/BTC (40000 + 100 fee)
        expected_qty = Decimal("1.5")
        expected_avg_cost = Decimal("40100")  # $40,000 + ($200 fee / 2 BTC)

        print(f"\n3. Verification:")
        print(f"   Expected qty: {expected_qty}, Got: {pos.qty}")
        print(f"   Expected avg cost: ${expected_avg_cost:.2f}, Got: ${pos.avg_cost:.2f}")

        if abs(pos.qty - expected_qty) < Decimal("0.001"):
            print("   ✓ Quantity matches!")
        else:
            print("   ✗ Quantity mismatch!")

        if abs(pos.avg_cost - expected_avg_cost) < Decimal("1"):
            print("   ✓ Average cost matches!")
        else:
            print("   ✗ Average cost mismatch!")

    else:
        print("   No positions found!")

    # Portfolio summary
    print("\n4. Portfolio summary:")
    summary = get_portfolio_summary(conn)
    print(f"   Net worth: {summary['net_worth']:.2f} PLN")
    print(f"   Holdings value: {summary['holdings_value']:.2f} PLN")
    print(f"   Unrealized P/L: {summary['unrealized_pl']:.2f} PLN")
    print(f"   Cash: {summary['cash']:.2f} PLN")

    conn.close()
    print("\n" + "=" * 50)
    print("✓ Portfolio FIFO test complete\n")


if __name__ == "__main__":
    main()
