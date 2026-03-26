"""Regression coverage for the usable local MVP milestone."""
from datetime import date, datetime
from pathlib import Path
from decimal import Decimal

import duckdb

from app.core.bonds import parse_series_code
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


def test_bonds_simple_ledger():
    print("3. Testing bonds simple ledger...")
    conn = fresh_db("test_mvp_bonds.duckdb")

    # parse_series_code still works
    type_code, maturity = parse_series_code("COI0528")
    assert type_code == "COI"
    assert maturity == date(2028, 5, 1)

    # add bond
    result = bond_service.add_bond("COI0528", 50, datetime(2024, 1, 15), 5.75)
    assert result.startswith("✓")

    # same series, different date — should succeed
    result = bond_service.add_bond("COI0528", 30, datetime(2024, 6, 1), 5.75)
    assert result.startswith("✓")

    # table shows both rows + total
    df = bond_service.get_bonds_df()
    assert len(df) == 3  # 2 rows + total
    assert df.iloc[0]["Series"] == "COI0528"
    assert df.iloc[2]["Series"] == "Total"

    # portfolio integration — nominal value (qty * 100)
    total = bond_service.get_bonds_total()
    assert total == Decimal("8000")  # (50 + 30) * 100

    # delete
    choices = bond_service.list_bond_choices()
    assert len(choices) == 2
    result = bond_service.delete_bond(choices[1])
    assert result.startswith("✓")
    assert len(bond_service.list_bond_choices()) == 1

    # validation: future date rejected
    result = bond_service.add_bond("EDO1131", 10, datetime(2099, 1, 1), 5)
    assert "future" in result

    # validation: negative qty rejected
    result = bond_service.add_bond("EDO1131", -5, datetime(2024, 1, 1), 5)
    assert "at least 1" in result

    conn.close()
    print("   ✓ Bonds simple ledger works correctly.")


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
    test_bonds_simple_ledger()
    test_dashboard_payload_smoke()
    print("\n" + "=" * 50)
    print("✓ MVP regression test complete\n")


if __name__ == "__main__":
    main()
