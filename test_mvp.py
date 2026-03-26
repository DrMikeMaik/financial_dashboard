"""Regression coverage for the usable local MVP milestone."""
from pathlib import Path
from decimal import Decimal

import duckdb

from app.core.db import init_db
from app.core.portfolio import calculate_positions, get_latest_price_info, get_portfolio_summary
from app.services import account_service, bond_service, dashboard_service, holding_service, reference_service, transaction_service


SERVICE_MODULES = [
    account_service,
    bond_service,
    dashboard_service,
    holding_service,
    reference_service,
    transaction_service,
]


def patch_service_connections(db_path: str) -> None:
    """Point all service modules at a temporary DuckDB file."""
    for module in SERVICE_MODULES:
        module.get_connection = lambda path=db_path: duckdb.connect(path)


def fresh_db(name: str):
    """Create a fresh temporary database for a test section."""
    db_path = Path("data") / name
    if db_path.exists():
        db_path.unlink()
    conn = init_db(str(db_path))
    patch_service_connections(str(db_path))
    return conn


def test_price_currency_and_cash_summary():
    print("1. Testing price currency handling, cash FX conversion, and warnings...")
    conn = fresh_db("test_mvp_valuation.duckdb")

    conn.execute("""
        INSERT INTO holdings (asset_type, symbol, name, currency)
        VALUES ('stock', 'BMW', 'BMW AG', 'EUR')
    """)
    holding_id = conn.execute("SELECT id FROM holdings WHERE symbol = 'BMW'").fetchone()[0]

    conn.execute("""
        INSERT INTO transactions (holding_id, ts, action, qty, price, fee)
        VALUES (?, '2024-01-01 10:00:00', 'buy', 10, 100, 0)
    """, [holding_id])
    conn.execute("""
        INSERT INTO prices (holding_id, ts, price, price_ccy, source)
        VALUES (?, '2024-01-02 10:00:00', 120, 'USD', 'test')
    """, [holding_id])
    conn.execute("""
        INSERT INTO fx_rates (ts, base_ccy, quote_ccy, rate, source)
        VALUES
            ('2024-01-02 10:00:00', 'USD', 'PLN', 4.0, 'test'),
            ('2024-01-02 10:00:00', 'EUR', 'PLN', 4.5, 'test')
    """)
    conn.execute("""
        INSERT INTO accounts (name, type, currency, balance, active)
        VALUES
            ('PLN Cash', 'checking', 'PLN', 100, TRUE),
            ('USD Cash', 'checking', 'USD', 100, TRUE)
    """)
    conn.execute("""
        INSERT INTO accounts (name, type, currency, balance, active)
        VALUES ('GBP Cash', 'checking', 'GBP', 10, TRUE)
    """)
    conn.execute("""
        INSERT INTO holdings (asset_type, symbol, name, currency)
        VALUES ('crypto', 'ETH', 'Ethereum', 'USD')
    """)
    conn.commit()

    position = calculate_positions(conn)[0]
    assert position.current_price_ccy == "USD"
    assert position.value_pln == Decimal("4800")
    assert position.unrealized_pl == Decimal("300")

    summary = get_portfolio_summary(conn)
    assert summary["cash"] == Decimal("510")
    assert any("Missing cached prices" in warning for warning in summary["warnings"])
    assert any("GBP/PLN" in warning for warning in summary["warnings"])

    conn.close()
    print("   ✓ Price currency, cash FX, and warning handling are correct.")


def test_transaction_crud_and_oversell_protection():
    print("2. Testing transaction CRUD, oversell validation, and timestamp ordering...")
    conn = fresh_db("test_mvp_transactions.duckdb")

    conn.execute("""
        INSERT INTO holdings (asset_type, symbol, name, currency)
        VALUES ('crypto', 'BTC', 'Bitcoin', 'USD')
    """)
    conn.commit()

    result = transaction_service.save_transaction(None, "2024-01-01 10:00:00", "BTC", None, "buy", 1, 30000, 100, "first lot")
    assert result.startswith("✓")
    result = transaction_service.save_transaction(None, "2024-02-01 10:00:00", "BTC", None, "buy", 1, 40000, 0, "second lot")
    assert result.startswith("✓")
    result = transaction_service.save_transaction(None, "2024-03-01 10:00:00", "BTC", None, "sell", 1.5, 50000, 0, "partial exit")
    assert result.startswith("✓")

    oversell = transaction_service.save_transaction(None, "2024-04-01 10:00:00", "BTC", None, "sell", 1, 51000, 0, "too much")
    assert "exceed available quantity" in oversell

    sell_id = conn.execute("""
        SELECT id
        FROM transactions
        WHERE action = 'sell'
    """).fetchone()[0]
    invalid_update = transaction_service.save_transaction(f"{sell_id} | edit", "2024-01-15 10:00:00", "BTC", None, "sell", 1.5, 50000, 0, "bad order")
    assert "exceed available quantity" in invalid_update

    first_buy_id = conn.execute("""
        SELECT id
        FROM transactions
        WHERE action = 'buy'
        ORDER BY ts ASC
        LIMIT 1
    """).fetchone()[0]
    invalid_delete = transaction_service.delete_transaction(f"{first_buy_id} | delete")
    assert "later sell invalid" in invalid_delete

    loaded = transaction_service.load_transaction(f"{sell_id} | edit")
    assert loaded[1] == "BTC"
    assert loaded[3] == "sell"

    conn.close()
    print("   ✓ CRUD and oversell protections behave as expected.")


def test_manual_bond_valuation():
    print("3. Testing manual bond metadata and valuation flow...")
    conn = fresh_db("test_mvp_bonds.duckdb")

    result = bond_service.save_bond(None, "EDO2030", "Treasury Bond", "PLN", 100, 6.5, 1, "2030-01-01", "Poland")
    assert result.startswith("✓")

    holding_id = conn.execute("""
        SELECT id
        FROM holdings
        WHERE asset_type = 'bond' AND symbol = 'EDO2030'
    """).fetchone()[0]

    result = transaction_service.save_transaction(None, "2024-01-01 10:00:00", "EDO2030", None, "buy", 10, 95, 0, "bond buy")
    assert result.startswith("✓")

    result = bond_service.save_bond_valuation(f"{holding_id} | EDO2030", "Percent of face", 105, "2024-06-01 12:00:00")
    assert result.startswith("✓")

    latest_price, latest_ccy, latest_source, _ = get_latest_price_info(conn, holding_id)
    assert latest_price == Decimal("105")
    assert latest_ccy == "PLN"
    assert latest_source == "manual"

    positions = [position for position in calculate_positions(conn) if position.holding.symbol == "EDO2030"]
    assert len(positions) == 1
    assert positions[0].value_pln == Decimal("1050")

    bond_rows = bond_service.get_bonds_df()
    assert not bond_rows.empty
    assert bond_rows.iloc[0]["Source"] == "manual"

    conn.close()
    print("   ✓ Bond metadata and manual valuation flow are correct.")


def test_dashboard_payload_smoke():
    print("4. Testing dashboard payload smoke...")
    conn = fresh_db("test_mvp_dashboard.duckdb")
    conn.close()

    payload = dashboard_service.get_dashboard_payload(25)
    assert len(payload) == 9
    assert isinstance(payload[0], str)
    print("   ✓ Dashboard payload stays stable for the UI.")


def main():
    print("Testing MVP regressions\n")
    print("=" * 50)
    test_price_currency_and_cash_summary()
    test_transaction_crud_and_oversell_protection()
    test_manual_bond_valuation()
    test_dashboard_payload_smoke()
    print("\n" + "=" * 50)
    print("✓ MVP regression test complete\n")


if __name__ == "__main__":
    main()
