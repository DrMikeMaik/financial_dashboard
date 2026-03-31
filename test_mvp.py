"""Regression coverage for the usable local MVP milestone."""
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from app import ui as app_ui
from app.adapters import crypto_coingecko, stocks_yfinance
from app.core import portfolio as portfolio_core
from app.core.bonds import parse_series_code
from app.core.db import init_db
from app.core.portfolio import calculate_positions, get_portfolio_summary
from app.services import account_service, bond_service, crypto_ledger_service, dashboard_service, holding_service, reference_service, stock_ledger_service, transaction_service


SERVICE_MODULES = [
    account_service,
    bond_service,
    crypto_ledger_service,
    dashboard_service,
    holding_service,
    reference_service,
    stock_ledger_service,
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
    first_bond_id = conn.execute("""
        SELECT id
        FROM bonds
        WHERE series = 'COI0528' AND purchase_date = '2024-01-15'
    """).fetchone()[0]
    first_period_rates = conn.execute("""
        SELECT period_num, rate
        FROM bond_year_rates
        WHERE bond_id = ?
        ORDER BY period_num
    """, [first_bond_id]).fetchall()
    assert first_period_rates == [(1, Decimal("5.7500"))]

    result = bond_service.append_bond_rate(first_bond_id, 7.15)
    assert result.startswith("✓")
    updated_period_rates = conn.execute("""
        SELECT period_num, rate
        FROM bond_year_rates
        WHERE bond_id = ?
        ORDER BY period_num
    """, [first_bond_id]).fetchall()
    assert updated_period_rates == [(1, Decimal("5.7500")), (2, Decimal("7.1500"))]

    # same series, different date — should succeed
    result = bond_service.add_bond("COI0528", 30, datetime(2024, 6, 1), 5.75)
    assert result.startswith("✓")

    # table shows both rows + total
    df, ids = bond_service.get_bonds_df()
    assert len(df) == 3  # 2 rows + total
    assert len(ids) == 2
    assert df.iloc[0]["Series"] == "COI0528"
    assert "Rates" in df.columns
    assert "Y1 5.75%  \nY2 7.15%" == df.iloc[0]["Rates"]
    assert df.iloc[2]["Series"] == "Total"

    # portfolio integration — COI should never fall below nominal principal
    total = bond_service.get_bonds_total()
    assert total >= Decimal("8000")

    # delete
    _, ids = bond_service.get_bonds_df()
    assert len(ids) == 2
    result = bond_service.delete_bond_by_id(ids[1])
    assert result.startswith("✓")
    remaining_child_rates = conn.execute("SELECT COUNT(*) FROM bond_year_rates WHERE bond_id = ?", [ids[1]]).fetchone()[0]
    assert remaining_child_rates == 0
    _, ids = bond_service.get_bonds_df()
    assert len(ids) == 1

    # validation: future date rejected
    result = bond_service.add_bond("EDO1131", 10, datetime(2099, 1, 1), 5)
    assert "future" in result

    # validation: negative qty rejected
    result = bond_service.add_bond("EDO1131", -5, datetime(2024, 1, 1), 5)
    assert "at least 1" in result

    # validation: appending past the supported number of periods is rejected
    result = bond_service.append_bond_rate(first_bond_id, 6.65)
    assert result.startswith("✓")
    result = bond_service.append_bond_rate(first_bond_id, 5.55)
    assert result.startswith("✓")
    result = bond_service.append_bond_rate(first_bond_id, 5.25)
    assert "already has all 4 yearly rates" in result

    conn.close()
    print("   ✓ Bonds simple ledger works correctly.")


def test_bond_rate_schedule_valuation():
    print("4. Testing bond yearly-rate valuation logic...")
    assert bond_service._round_to_half_zloty(Decimal("123.24")) == Decimal("123.0")
    assert bond_service._round_to_half_zloty(Decimal("123.25")) == Decimal("123.5")
    assert bond_service._round_to_half_zloty(Decimal("123.74")) == Decimal("123.5")
    assert bond_service._round_to_half_zloty(Decimal("123.75")) == Decimal("124.0")

    edo_schedule = {
        1: Decimal("5.75"),
        2: Decimal("7.15"),
        3: Decimal("6.65"),
        4: Decimal("5.55"),
    }
    value, warning = bond_service._calc_actual_per_bond(
        "EDO0832",
        date(2023, 8, 8),
        edo_schedule,
        date(2026, 3, 30),
        date(2032, 8, 8),
    )
    expected = bond_service.FACE_VALUE
    expected *= Decimal("1.0575")
    expected *= Decimal("1.0715")
    elapsed_days = Decimal((date(2026, 3, 30) - date(2025, 8, 8)).days)
    expected *= Decimal("1") + Decimal("0.0665") * elapsed_days / Decimal("365")
    assert warning is None
    assert abs(value - expected) < Decimal("0.0001")

    frozen_value, frozen_warning = bond_service._calc_actual_per_bond(
        "EDO0832",
        date(2023, 8, 8),
        {1: Decimal("5.75")},
        date(2025, 3, 30),
        date(2032, 8, 8),
    )
    assert frozen_value == bond_service.FACE_VALUE * Decimal("1.0575")
    assert frozen_warning == "Need rate"

    coi_first_year_value, coi_first_year_warning = bond_service._calc_actual_per_bond(
        "COI0528",
        date(2024, 5, 14),
        {1: Decimal("6.55")},
        date(2025, 1, 14),
        date(2028, 5, 14),
    )
    coi_first_year_expected = bond_service.FACE_VALUE * (
        Decimal("1") + Decimal("0.0655") * Decimal((date(2025, 1, 14) - date(2024, 5, 14)).days) / Decimal("365")
    )
    assert coi_first_year_warning is None
    assert abs(coi_first_year_value - coi_first_year_expected) < Decimal("0.0001")

    coi_second_year_value, coi_second_year_warning = bond_service._calc_actual_per_bond(
        "COI0528",
        date(2024, 5, 14),
        {1: Decimal("6.55"), 2: Decimal("5.75")},
        date(2025, 11, 14),
        date(2028, 5, 14),
    )
    coi_second_year_expected = bond_service.FACE_VALUE * (
        Decimal("1") + Decimal("0.0575") * Decimal((date(2025, 11, 14) - date(2025, 5, 14)).days) / Decimal("365")
    )
    assert coi_second_year_warning is None
    assert abs(coi_second_year_value - coi_second_year_expected) < Decimal("0.0001")

    coi_missing_rate_value, coi_missing_rate_warning = bond_service._calc_actual_per_bond(
        "COI0528",
        date(2024, 5, 14),
        {1: Decimal("5.75")},
        date(2025, 11, 14),
        date(2028, 5, 14),
    )
    assert coi_missing_rate_value == bond_service.FACE_VALUE
    assert coi_missing_rate_warning == "Need rate"

    coi_maturity_value, coi_maturity_warning = bond_service._calc_actual_per_bond(
        "COI0528",
        date(2024, 5, 14),
        {
            1: Decimal("6.55"),
            2: Decimal("5.75"),
            3: Decimal("5.25"),
            4: Decimal("4.75"),
        },
        date(2030, 1, 1),
        date(2028, 5, 14),
    )
    assert coi_maturity_warning is None
    assert coi_maturity_value == bond_service.FACE_VALUE * Decimal("1.0475")
    print("   ✓ Yearly rate schedules value EDO and COI differently.")


def test_stock_search_adapter_and_ui_resolution():
    print("5. Testing Yahoo stock search normalization and UI field population...")

    original_search = stocks_yfinance.yf.Search
    original_get_info = stocks_yfinance.get_info
    original_service_search = stock_ledger_service.search_stock_candidates

    class FakeSearch:
        def __init__(self, query: str, max_results: int = 8):
            self.quotes = [{
                "symbol": "EUNM.DE",
                "longname": "iShares MSCI EM UCITS ETF USD (Acc)",
                "exchDisp": "XETRA",
                "quoteType": "ETF",
            }]

    stocks_yfinance.yf.Search = FakeSearch
    stocks_yfinance.get_info = lambda symbol: {
        "symbol": symbol.upper(),
        "name": "iShares MSCI EM UCITS ETF USD (Acc)",
        "currency": "EUR",
        "exchange": "GER",
        "exchange_display": "XETRA",
        "exchange_label": "XETRA EUR",
        "type": "ETF",
        "found": True,
    }
    try:
        results = stocks_yfinance.search_instruments("EUNM")
        assert results[0]["symbol"] == "EUNM.DE"
        assert results[0]["currency"] == "EUR"
        assert results[0]["exchange_display"] == "XETRA"
        assert results[0]["type"] == "ETF"

        normalized_results, _ = stock_ledger_service.search_stock_candidates("EUNM")
        assert normalized_results[0]["label"] == "EUNM.DE | iShares MSCI EM UCITS ETF USD (Acc) | XETRA | EUR"

        stock_ledger_service.search_stock_candidates = lambda query: (normalized_results, "✓ Found 1 Yahoo stock/ETF matches.")
        ui_payload = app_ui._search_stock_candidates("EUNM")
        assert ui_payload[3] == "EUNM.DE"
        assert ui_payload[4] == "EUR"
        assert ui_payload[5] == "iShares MSCI EM UCITS ETF USD (Acc)"
        assert ui_payload[6] == "XETRA EUR"
    finally:
        stocks_yfinance.yf.Search = original_search
        stocks_yfinance.get_info = original_get_info
        stock_ledger_service.search_stock_candidates = original_service_search

    print("   ✓ Search results expose ticker, currency, and UI-ready metadata.")


def test_crypto_search_and_ledger_rows():
    print("6. Testing CoinGecko crypto search normalization, persistence, and ledger math...")
    conn = fresh_db("test_mvp_crypto_ledger.duckdb")

    original_search = crypto_coingecko.search_coin
    original_service_search = crypto_ledger_service.search_crypto_candidates

    try:
        crypto_coingecko.search_coin = lambda query: [{
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
        }]

        normalized_results, _ = crypto_ledger_service.search_crypto_candidates("BTC")
        assert normalized_results[0]["label"] == "BTC | Bitcoin | bitcoin"

        crypto_ledger_service.search_crypto_candidates = lambda query: (normalized_results, "✓ Found 1 CoinGecko matches.")
        ui_payload = app_ui._search_crypto_candidates("BTC")
        assert ui_payload[3] == "BTC"
        assert ui_payload[4] == "PLN"
        assert ui_payload[5] == "Bitcoin"
        assert ui_payload[6] == "bitcoin"
    finally:
        crypto_coingecko.search_coin = original_search
        crypto_ledger_service.search_crypto_candidates = original_service_search

    blocked = crypto_ledger_service.save_crypto_order(
        None, None, [], "2025-01-03", "buy", 0.5, 200000, 100, "should fail"
    )
    assert "Search and select" in blocked

    resolved_result = {
        "id": "bitcoin",
        "symbol": "BTC",
        "name": "Bitcoin",
        "currency": "PLN",
        "label": "BTC | Bitcoin | bitcoin",
    }
    result = crypto_ledger_service.save_crypto_order(
        None, resolved_result["label"], [resolved_result], "2025-01-03", "buy", 0.5, 200000, 100, "first lot"
    )
    assert result.startswith("✓")
    result = crypto_ledger_service.save_crypto_order(
        None, resolved_result["label"], [resolved_result], "2025-02-03", "sell", 0.2, 240000, 50, "trim"
    )
    assert result.startswith("✓")
    oversell = crypto_ledger_service.save_crypto_order(
        None, resolved_result["label"], [resolved_result], "2025-03-03", "sell", 1, 250000, 0, "too much"
    )
    assert "exceed available quantity" in oversell

    holding_row = conn.execute("""
        SELECT id, currency, coingecko_id
        FROM holdings
        WHERE symbol = 'BTC'
    """).fetchone()
    holding_id = holding_row[0]
    assert holding_row[1] == "PLN"
    assert holding_row[2] == "bitcoin"

    conn.execute("""
        INSERT INTO fx_rates (ts, base_ccy, quote_ccy, rate, source)
        VALUES ('2025-03-01 10:00:00', 'USD', 'PLN', 4.0000, 'NBP')
    """)
    conn.execute("""
        INSERT INTO prices (holding_id, ts, price, price_ccy, source)
        VALUES (?, '2025-03-01 10:00:00', 60000, 'USD', 'test')
    """, [holding_id])
    conn.commit()

    df, row_ids = crypto_ledger_service.get_crypto_orders_df()
    assert list(df.columns) == crypto_ledger_service.ORDER_COLUMNS
    assert df.iloc[0]["B/S"] == "S"
    assert df.iloc[1]["Date"] == "2025-01-03"
    assert "BTC" in df.iloc[1]["Asset"]
    assert "Bitcoin" in df.iloc[1]["Asset"]
    assert df.iloc[1]["Spot Price"] == "200,000.00"
    assert df.iloc[1]["CCY"] == "PLN"
    assert df.iloc[1]["Fee"] == "100.00 PLN"
    assert df.iloc[1]["Subtotal"] == "100,000.00 PLN"
    assert df.iloc[1]["Current Value"] == "72,000.00 PLN"
    assert df.iloc[1]["Change %"] == "19.88%"
    assert df.iloc[2]["Date"] == "Total"
    assert df.iloc[2]["Fee"] == "150.00 PLN"
    assert df.iloc[2]["Subtotal"] == "148,000.00 PLN"
    assert df.iloc[2]["Current Value"] == "72,000.00 PLN"
    assert df.iloc[2]["Change %"] == "19.88%"
    assert len(row_ids) == 2

    loaded_choice = crypto_ledger_service.list_crypto_order_choices()[0]
    loaded = crypto_ledger_service.load_crypto_order(loaded_choice)
    assert loaded["resolved_symbol"] == "BTC"
    assert loaded["trade_currency"] == "PLN"
    assert loaded["coingecko_id"] == "bitcoin"

    delete_result = crypto_ledger_service.delete_crypto_order_by_id(row_ids[0])
    assert delete_result.startswith("✓")

    conn.close()
    print("   ✓ Crypto orders persist CoinGecko ids and compute PLN ledger values correctly.")


def test_minor_unit_price_normalization():
    print("7. Testing minor-unit Yahoo price normalization...")
    original_ticker = stocks_yfinance.yf.Ticker

    class FakeTicker:
        def __init__(self, symbol: str):
            self.info = {
                "currency": "GBp",
                "regularMarketPrice": 6682.0,
                "quoteType": "EQUITY",
                "exchange": "LSE",
                "fullExchangeName": "LSE",
                "longName": "iShares Physical Gold ETC",
            }
            self.fast_info = {
                "currency": "GBp",
                "regularMarketPreviousClose": 65.97,
            }

        def history(self, period="5d", start=None, end=None):
            import pandas as pd

            return pd.DataFrame({
                "Close": [64.0, 66.019997, 63.82, 65.970001, 6682.0],
            })

    stocks_yfinance.yf.Ticker = FakeTicker
    try:
        info = stocks_yfinance.get_info("SGLN.L")
        assert info["currency"] == "GBP"
        price = stocks_yfinance.get_current_price("SGLN.L")
        assert price == Decimal("66.82")
    finally:
        stocks_yfinance.yf.Ticker = original_ticker

    print("   ✓ Minor-unit Yahoo prices are normalized to major currency values.")


def test_stock_ledger_rows_and_fifo_fee_conversion():
    print("8. Testing stock ledger rows, FIFO fee conversion, and current valuation...")
    conn = fresh_db("test_mvp_stock_ledger.duckdb")

    resolved_result = {
        "symbol": "EUNM.DE",
        "name": "EUNM GR ETF",
        "currency": "EUR",
        "exchange": "GER",
        "exchange_display": "XETRA",
        "exchange_label": "XETRA EUR",
        "type": "ETF",
        "label": "EUNM.DE | EUNM GR ETF | XETRA | EUR",
    }

    blocked = stock_ledger_service.save_stock_order(
        None, None, [], "2025-01-03", "buy", 10, 100, 20, "should fail"
    )
    assert "Search and select" in blocked

    result = stock_ledger_service.save_stock_order(
        None, resolved_result["label"], [resolved_result], "2025-01-03", "buy", 10, 100, 20, "first lot"
    )
    assert result.startswith("✓")
    result = stock_ledger_service.save_stock_order(
        None, resolved_result["label"], [resolved_result], "2025-02-03", "sell", 4, 120, 8, "partial exit"
    )
    assert result.startswith("✓")

    holding_id = conn.execute("""
        SELECT id
        FROM holdings
        WHERE symbol = 'EUNM.DE'
    """).fetchone()[0]
    exchange_label = conn.execute("""
        SELECT exchange_label
        FROM holdings
        WHERE id = ?
    """, [holding_id]).fetchone()[0]
    assert exchange_label == "XETRA EUR"
    saved_timestamps = conn.execute("""
        SELECT ts
        FROM transactions
        WHERE holding_id = ?
        ORDER BY ts ASC
    """, [holding_id]).fetchall()
    assert saved_timestamps[0][0].strftime("%Y-%m-%d %H:%M:%S") == "2025-01-03 23:59:59"
    assert saved_timestamps[1][0].strftime("%Y-%m-%d %H:%M:%S") == "2025-02-03 23:59:59"

    conn.execute("""
        INSERT INTO fx_rates (ts, base_ccy, quote_ccy, rate, source)
        VALUES
            ('2025-01-03 00:00:00', 'EUR', 'PLN', 4.0000, 'NBP_HIST'),
            ('2025-02-03 00:00:00', 'EUR', 'PLN', 4.1000, 'NBP_HIST'),
            ('2025-03-01 10:00:00', 'EUR', 'PLN', 4.2000, 'NBP')
    """)
    conn.execute("""
        INSERT INTO prices (holding_id, ts, price, price_ccy, source)
        VALUES (?, '2025-03-01 10:00:00', 130, 'EUR', 'test')
    """, [holding_id])
    conn.commit()

    df, row_ids = stock_ledger_service.get_stock_orders_df()
    assert list(df.columns) == stock_ledger_service.ORDER_COLUMNS
    assert df.iloc[0]["B/S"] == "S"
    assert df.iloc[0]["Current Value"] == ""
    assert df.iloc[1]["Date"] == "2025-01-03"
    assert "EUNM.DE" in df.iloc[1]["Symbol"]
    assert "XETRA EUR" in df.iloc[1]["Symbol"]
    assert df.iloc[1]["Price"] == "100.00"
    assert df.iloc[1]["CCY"] == "EUR"
    assert df.iloc[1]["FX PLN"] == "4.0000"
    assert df.iloc[1]["Trade Value"] == "4,000.00 PLN"
    assert df.iloc[1]["Current Value"] == "3,276.00 PLN"
    assert df.iloc[1]["Change %"] == "35.82%"
    assert df.iloc[1]["Delete"] == "🗑️"
    assert df.iloc[2]["Date"] == "Total"
    assert df.iloc[2]["Comm."] == "28.00 PLN"
    assert df.iloc[2]["Trade Value"] == "5,968.00 PLN"
    assert df.iloc[2]["Current Value"] == "3,276.00 PLN"
    assert df.iloc[2]["Change %"] == "35.82%"
    assert df.iloc[2]["Delete"] == ""
    assert len(row_ids) == 2

    position = calculate_positions(conn)[0]
    expected_avg_cost = Decimal("100") + (Decimal("20") / Decimal("4")) / Decimal("10")
    assert abs(position.avg_cost - expected_avg_cost) < Decimal("0.00000001")

    delete_result = stock_ledger_service.delete_stock_order_by_id(row_ids[0])
    assert delete_result.startswith("✓")
    df_after_delete, remaining_ids = stock_ledger_service.get_stock_orders_df()
    assert len(df_after_delete) == 2
    assert df_after_delete.iloc[1]["Date"] == "Total"
    assert len(remaining_ids) == 1
    assert conn.execute("SELECT COUNT(*) FROM holdings WHERE id = ?", [holding_id]).fetchone()[0] == 1

    loaded_choice = stock_ledger_service.list_stock_order_choices()[0]
    loaded = stock_ledger_service.load_stock_order(loaded_choice)
    assert loaded["resolved_symbol"] == "EUNM.DE"
    assert loaded["trade_currency"] == "EUR"
    assert loaded["timestamp_text"].date().isoformat() == "2025-01-03"

    final_delete_result = stock_ledger_service.delete_stock_order_by_id(remaining_ids[0])
    assert final_delete_result.startswith("✓")
    final_df, final_ids = stock_ledger_service.get_stock_orders_df()
    assert final_df.empty
    assert final_ids == []
    assert conn.execute("SELECT COUNT(*) FROM holdings WHERE id = ?", [holding_id]).fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM prices WHERE holding_id = ?", [holding_id]).fetchone()[0] == 0

    conn.close()
    print("   ✓ Stock ledger rows and FIFO fee conversion behave as expected.")


def test_stock_ledger_fetches_and_caches_historical_fx_once():
    print("9. Testing stock ledger historical FX fetch/caching and missing-price behavior...")
    conn = fresh_db("test_mvp_stock_fx_cache.duckdb")
    conn.execute("""
        INSERT INTO holdings (asset_type, symbol, name, currency, exchange_label)
        VALUES ('stock', 'BMW', 'BMW AG', 'EUR', 'XETRA EUR')
    """)
    holding_id = conn.execute("SELECT id FROM holdings WHERE symbol = 'BMW'").fetchone()[0]
    conn.execute("""
        INSERT INTO transactions (holding_id, ts, action, qty, price, fee, fee_currency)
        VALUES (?, '2025-01-15 10:00:00', 'buy', 2, 80, 10, 'PLN')
    """, [holding_id])
    conn.commit()

    original_get_rate_on_date = portfolio_core.fx_nbp.get_rate_on_date
    call_counter = {"count": 0}
    original_apply_stock_choice = app_ui._apply_stock_search_choice

    def fake_get_rate_on_date(base_ccy: str, quote_ccy: str, target_date):
        assert base_ccy == "EUR"
        assert quote_ccy == "PLN"
        assert target_date == date(2025, 1, 15)
        call_counter["count"] += 1
        return Decimal("4.2500")

    portfolio_core.fx_nbp.get_rate_on_date = fake_get_rate_on_date
    try:
        df, _ = stock_ledger_service.get_stock_orders_df()
        assert df.iloc[0]["FX PLN"] == "4.2500"
        assert df.iloc[0]["Current Value"] == ""
        df, _ = stock_ledger_service.get_stock_orders_df()
        assert df.iloc[0]["FX PLN"] == "4.2500"
        ui_selection = app_ui._apply_stock_search_choice(
            "BMW | BMW AG | XETRA | EUR",
            [{
                "symbol": "BMW",
                "name": "BMW AG",
                "currency": "EUR",
                "exchange_display": "XETRA",
                "exchange_label": "XETRA EUR",
                "type": "EQUITY",
                "label": "BMW | BMW AG | XETRA | EUR",
            }],
        )
        assert ui_selection[0] == "BMW"
        assert ui_selection[1] == "EUR"
    finally:
        portfolio_core.fx_nbp.get_rate_on_date = original_get_rate_on_date

    cached_rows = conn.execute("""
        SELECT COUNT(*)
        FROM fx_rates
        WHERE base_ccy = 'EUR' AND quote_ccy = 'PLN' AND source = 'NBP_HIST'
    """).fetchone()[0]
    assert call_counter["count"] == 1
    assert cached_rows == 1

    summary = get_portfolio_summary(conn)
    assert any("Missing cached prices" in warning for warning in summary["warnings"])
    conn.close()
    print("   ✓ Historical FX gets cached once and missing live prices keep today-value blank.")


def test_stock_save_helper_refreshes_dashboard_on_success():
    print("10. Testing stock save helper refresh behavior...")
    original_save = stock_ledger_service.save_stock_order
    original_refs = app_ui._reference_updates
    original_refresh = app_ui._refresh_dashboard
    original_payload = app_ui._dashboard_payload

    stock_ledger_service.save_stock_order = lambda *args, **kwargs: "✓ Added buy order for EUNM.DE"
    app_ui._reference_updates = lambda: ("tx", "sym", "acc", "acct", "bond", "crypto_order", "stock")
    app_ui._refresh_dashboard = lambda limit: ("refresh", "overview", "positions", "crypto", "crypto_ids", "stocks", "stock_ids", "bonds", "bond_ids", "accounts", "txns", "settings")
    app_ui._dashboard_payload = lambda limit: ("payload", "overview", "positions", "crypto", "crypto_ids", "stocks", "stock_ids", "bonds", "bond_ids", "accounts", "txns", "settings")

    try:
        success_result = app_ui._save_stock_order_and_refresh(
            25, None, "choice", [{"label": "choice"}], "2025-01-03", "buy", 1, 100, 0, ""
        )
        assert success_result[0].startswith("✓")
        assert success_result[8] == "refresh"

        stock_ledger_service.save_stock_order = lambda *args, **kwargs: "✗ nope"
        fail_result = app_ui._save_stock_order_and_refresh(
            25, None, "choice", [{"label": "choice"}], "2025-01-03", "buy", 1, 100, 0, ""
        )
        assert fail_result[0].startswith("✗")
        assert fail_result[8] == "payload"
    finally:
        stock_ledger_service.save_stock_order = original_save
        app_ui._reference_updates = original_refs
        app_ui._refresh_dashboard = original_refresh
        app_ui._dashboard_payload = original_payload

    print("   ✓ Successful stock saves trigger the refresh path.")


def test_crypto_save_helper_refreshes_dashboard_on_success():
    print("11. Testing crypto save helper refresh behavior...")
    original_save = crypto_ledger_service.save_crypto_order
    original_refs = app_ui._reference_updates
    original_refresh = app_ui._refresh_dashboard
    original_payload = app_ui._dashboard_payload

    crypto_ledger_service.save_crypto_order = lambda *args, **kwargs: "✓ Added buy order for BTC"
    app_ui._reference_updates = lambda: ("tx", "sym", "acc", "acct", "bond", "crypto_order", "stock")
    app_ui._refresh_dashboard = lambda limit: ("refresh", "overview", "positions", "crypto", "crypto_ids", "stocks", "stock_ids", "bonds", "bond_ids", "accounts", "txns", "settings")
    app_ui._dashboard_payload = lambda limit: ("payload", "overview", "positions", "crypto", "crypto_ids", "stocks", "stock_ids", "bonds", "bond_ids", "accounts", "txns", "settings")

    try:
        success_result = app_ui._save_crypto_order_and_refresh(
            25, None, "choice", [{"label": "choice"}], "2025-01-03", "buy", 1, 100000, 0, ""
        )
        assert success_result[0].startswith("✓")
        assert success_result[8] == "refresh"

        crypto_ledger_service.save_crypto_order = lambda *args, **kwargs: "✗ nope"
        fail_result = app_ui._save_crypto_order_and_refresh(
            25, None, "choice", [{"label": "choice"}], "2025-01-03", "buy", 1, 100000, 0, ""
        )
        assert fail_result[0].startswith("✗")
        assert fail_result[8] == "payload"
    finally:
        crypto_ledger_service.save_crypto_order = original_save
        app_ui._reference_updates = original_refs
        app_ui._refresh_dashboard = original_refresh
        app_ui._dashboard_payload = original_payload

    print("   ✓ Successful crypto saves trigger the refresh path.")


def test_fx_refresh_only_persists_used_currencies():
    print("12. Testing FX refresh only persists used currencies and supports CoinGecko ids...")
    conn = fresh_db("test_mvp_fx_refresh_filter.duckdb")
    conn.execute("""
        INSERT INTO holdings (asset_type, symbol, name, currency, coingecko_id)
        VALUES
            ('stock', 'BMW', 'BMW AG', 'EUR', NULL),
            ('crypto', 'BTC', 'Bitcoin', 'PLN', 'bitcoin'),
            ('crypto', 'ETH', 'Ethereum', 'USD', NULL)
    """)
    conn.execute("""
        INSERT INTO accounts (name, type, currency, balance, active)
        VALUES
            ('GBP Cash', 'checking', 'GBP', 100, TRUE),
            ('PLN Cash', 'checking', 'PLN', 50, TRUE)
    """)
    conn.commit()
    conn.close()

    original_get_rates = dashboard_service.fx_nbp.get_current_rates
    original_crypto_prices_by_ids = dashboard_service.crypto_coingecko.get_current_prices_by_ids
    original_crypto_prices = dashboard_service.crypto_coingecko.get_current_prices
    original_stock_price = dashboard_service.stocks_yfinance.get_current_price

    dashboard_service.fx_nbp.get_current_rates = lambda quote_currency="PLN": {
        "PLN": Decimal("1.0"),
        "EUR": Decimal("4.20"),
        "USD": Decimal("4.00"),
        "GBP": Decimal("5.10"),
        "JPY": Decimal("0.025"),
    }
    dashboard_service.crypto_coingecko.get_current_prices_by_ids = lambda ids, vs_currency="usd": {
        "bitcoin": Decimal("30000"),
    }
    dashboard_service.crypto_coingecko.get_current_prices = lambda symbols, vs_currency="usd": {
        "ETH": Decimal("2000"),
    }
    dashboard_service.stocks_yfinance.get_current_price = lambda symbol: Decimal("120")

    try:
        status = dashboard_service.refresh_market_data()
        assert "✓ Updated 3 FX rates from NBP" in status
        assert "✓ Updated 2 crypto prices from CoinGecko" in status
    finally:
        dashboard_service.fx_nbp.get_current_rates = original_get_rates
        dashboard_service.crypto_coingecko.get_current_prices_by_ids = original_crypto_prices_by_ids
        dashboard_service.crypto_coingecko.get_current_prices = original_crypto_prices
        dashboard_service.stocks_yfinance.get_current_price = original_stock_price

    verify_conn = duckdb.connect("data/test_mvp_fx_refresh_filter.duckdb")
    saved_currencies = {
        row[0]
        for row in verify_conn.execute("""
            SELECT DISTINCT base_ccy
            FROM fx_rates
            WHERE source = 'NBP'
            ORDER BY base_ccy
        """).fetchall()
    }
    assert saved_currencies == {"EUR", "GBP", "USD"}
    saved_crypto_prices = verify_conn.execute("""
        SELECT h.symbol, p.price
        FROM prices p
        JOIN holdings h ON h.id = p.holding_id
        WHERE h.asset_type = 'crypto'
        ORDER BY h.symbol
    """).fetchall()
    assert saved_crypto_prices == [("BTC", Decimal("30000.00000000")), ("ETH", Decimal("2000.00000000"))]
    verify_conn.close()
    print("   ✓ FX cache keeps only currencies actually used by the portfolio.")


def test_schema_migration_adds_stock_ledger_columns():
    print("13. Testing schema migration for ledger columns...")
    db_path = Path("data") / "test_mvp_schema_migration.duckdb"
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE settings (key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL)")
    conn.execute("CREATE SEQUENCE seq_holdings_id START 1")
    conn.execute("""
        CREATE TABLE holdings (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_holdings_id'),
            asset_type VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            name VARCHAR,
            currency VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE SEQUENCE seq_transactions_id START 1")
    conn.execute("""
        CREATE TABLE transactions (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_transactions_id'),
            holding_id INTEGER NOT NULL,
            account_id INTEGER,
            ts TIMESTAMP NOT NULL,
            action VARCHAR NOT NULL,
            qty DECIMAL(18, 8),
            price DECIMAL(18, 8),
            fee DECIMAL(18, 8) DEFAULT 0,
            note VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.close()

    migrated = init_db(str(db_path))
    holding_columns = {row[1] for row in migrated.execute("PRAGMA table_info('holdings')").fetchall()}
    transaction_columns = {row[1] for row in migrated.execute("PRAGMA table_info('transactions')").fetchall()}
    assert "coingecko_id" in holding_columns
    assert "exchange_label" in holding_columns
    assert "fee_currency" in transaction_columns
    migrated.close()
    print("   ✓ Existing databases pick up the new stock ledger columns.")


def test_dashboard_payload_smoke():
    print("14. Testing dashboard payload smoke...")
    conn = fresh_db("test_mvp_dashboard.duckdb")
    conn.close()

    payload = dashboard_service.get_dashboard_payload(25)
    assert len(payload) == 12
    assert isinstance(payload[0], str)
    assert payload[4] == []
    print("   ✓ Dashboard payload stays stable for the UI.")


def main():
    print("Testing MVP regressions\n")
    print("=" * 50)
    test_price_currency_and_cash_summary()
    test_transaction_crud_and_oversell_protection()
    test_bonds_simple_ledger()
    test_bond_rate_schedule_valuation()
    test_stock_search_adapter_and_ui_resolution()
    test_crypto_search_and_ledger_rows()
    test_minor_unit_price_normalization()
    test_stock_ledger_rows_and_fifo_fee_conversion()
    test_stock_ledger_fetches_and_caches_historical_fx_once()
    test_stock_save_helper_refreshes_dashboard_on_success()
    test_crypto_save_helper_refreshes_dashboard_on_success()
    test_fx_refresh_only_persists_used_currencies()
    test_schema_migration_adds_stock_ledger_columns()
    test_dashboard_payload_smoke()
    print("\n" + "=" * 50)
    print("✓ MVP regression test complete\n")


if __name__ == "__main__":
    main()
