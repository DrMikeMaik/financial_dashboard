"""Dashboard read models and refresh orchestration."""
from datetime import datetime

import pandas as pd

from app.adapters import crypto_coingecko, fx_nbp, stocks_yfinance
from app.core.db import get_connection, get_setting
from app.core.portfolio import calculate_positions, get_portfolio_summary
from app.services.account_service import get_accounts_df
from app.services import bond_service
from app.services.bond_service import get_bonds_df
from app.services.crypto_ledger_service import get_crypto_orders_df
from app.services.stock_ledger_service import get_stock_orders_df


def _format_cache_timestamp(value: datetime | None) -> str:
    if value is None:
        return "n/a"
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _collect_relevant_fx_currencies(conn, holdings: list[tuple[int, str, str, str, str | None]]) -> set[str]:
    currencies = {
        (currency or "").strip().upper()
        for _, _, _, currency, _ in holdings
        if (currency or "").strip().upper() and (currency or "").strip().upper() != "PLN"
    }

    account_rows = conn.execute("""
        SELECT DISTINCT currency
        FROM accounts
        WHERE active = TRUE
    """).fetchall()
    for (currency,) in account_rows:
        normalized = (currency or "").strip().upper()
        if normalized and normalized != "PLN":
            currencies.add(normalized)

    fee_rows = conn.execute("""
        SELECT DISTINCT fee_currency
        FROM transactions
        WHERE fee_currency IS NOT NULL
    """).fetchall()
    for (currency,) in fee_rows:
        normalized = (currency or "").strip().upper()
        if normalized and normalized != "PLN":
            currencies.add(normalized)

    if any(asset_type == "crypto" for _, asset_type, _, _, _ in holdings):
        currencies.add("USD")

    return currencies


def refresh_market_data() -> str:
    """Refresh all asset prices from external APIs."""
    conn = get_connection()
    try:
        messages = []
        holdings = conn.execute("""
            SELECT id, asset_type, symbol, currency, coingecko_id
            FROM holdings
            ORDER BY asset_type, symbol
        """).fetchall()
        relevant_fx_currencies = _collect_relevant_fx_currencies(conn, holdings)

        if not holdings and not relevant_fx_currencies:
            return "No holdings or foreign-currency accounts found to refresh."

        if relevant_fx_currencies:
            try:
                fx_rates = fx_nbp.get_current_rates("PLN")
                now = datetime.now()
                updated_fx = 0
                for currency in sorted(relevant_fx_currencies):
                    rate = fx_rates.get(currency)
                    if rate is None:
                        continue
                    conn.execute("""
                        INSERT INTO fx_rates (id, ts, base_ccy, quote_ccy, rate, source)
                        VALUES (nextval('seq_fx_rates_id'), ?, ?, 'PLN', ?, 'NBP')
                    """, [now, currency, float(rate)])
                    updated_fx += 1
                messages.append(f"✓ Updated {updated_fx} FX rates from NBP")
            except Exception as exc:
                messages.append(f"⚠ FX rates update failed: {exc}")

        crypto_holdings = [(row[0], row[2], row[4]) for row in holdings if row[1] == "crypto"]
        stock_holdings = [(row[0], row[2], row[3]) for row in holdings if row[1] in {"stock", "etf"}]

        if crypto_holdings:
            try:
                prices_by_id = crypto_coingecko.get_current_prices_by_ids(
                    [coingecko_id for _, _, coingecko_id in crypto_holdings if coingecko_id],
                    vs_currency="usd",
                )
                legacy_symbols = [symbol for _, symbol, coingecko_id in crypto_holdings if not coingecko_id]
                legacy_prices = crypto_coingecko.get_current_prices(legacy_symbols, vs_currency="usd")
                now = datetime.now()
                updated = 0
                for holding_id, symbol, coingecko_id in crypto_holdings:
                    price = prices_by_id.get(coingecko_id) if coingecko_id else legacy_prices.get(symbol.upper())
                    if price is None:
                        continue
                    conn.execute("""
                        INSERT INTO prices (id, holding_id, ts, price, price_ccy, source)
                        VALUES (nextval('seq_prices_id'), ?, ?, ?, 'USD', 'CoinGecko')
                    """, [holding_id, now, float(price)])
                    updated += 1
                messages.append(f"✓ Updated {updated} crypto prices from CoinGecko")
            except Exception as exc:
                messages.append(f"⚠ Crypto price update failed: {exc}")

        if stock_holdings:
            updated = 0
            stock_errors = []
            now = datetime.now()
            for holding_id, symbol, currency in stock_holdings:
                try:
                    price = stocks_yfinance.get_current_price(symbol)
                    if price is None:
                        stock_errors.append(symbol)
                        continue
                    conn.execute("""
                        INSERT INTO prices (id, holding_id, ts, price, price_ccy, source)
                        VALUES (nextval('seq_prices_id'), ?, ?, ?, ?, 'yfinance')
                    """, [holding_id, now, float(price), currency])
                    updated += 1
                except Exception:
                    stock_errors.append(symbol)

            messages.append(f"✓ Updated {updated} stock/ETF prices from yfinance")
            if stock_errors:
                messages.append(f"⚠ Missing stock price updates for: {', '.join(stock_errors[:8])}")

        conn.commit()
        return "\n".join(messages)
    finally:
        conn.close()


def _positions_to_dataframe(asset_types: set[str] | None = None) -> pd.DataFrame:
    conn = get_connection()
    try:
        positions = calculate_positions(conn)
    finally:
        conn.close()

    if asset_types is not None:
        positions = [position for position in positions if position.holding.asset_type in asset_types]

    if not positions:
        return pd.DataFrame(columns=["Asset Type", "Symbol", "Quantity", "Avg Cost (PLN)", "Current Price (PLN)", "Value (PLN)", "UPL", "Price Source"])

    positions.sort(key=lambda position: position.value_pln, reverse=True)
    rows = [
        {
            "Asset Type": position.holding.asset_type.upper(),
            "Symbol": position.holding.symbol,
            "Quantity": f"{position.qty:.4f}",
            "Avg Cost (PLN)": f"{((position.value_pln - position.unrealized_pl) / position.qty):,.2f}",
            "Current Price (PLN)": f"{(position.value_pln / position.qty):,.2f}",
            "Value (PLN)": f"{position.value_pln:,.2f}",
            "UPL": f"{position.unrealized_pl:,.2f}",
            "Price Source": position.price_source or "",
        }
        for position in positions
    ]

    total_value_pln = sum(position.value_pln for position in positions)
    total_upl = sum(position.unrealized_pl for position in positions)
    rows.append({
        "Asset Type": "",
        "Symbol": "Total",
        "Quantity": "",
        "Avg Cost (PLN)": "",
        "Current Price (PLN)": "",
        "Value (PLN)": f"{total_value_pln:,.2f}",
        "UPL": f"{total_upl:,.2f}",
        "Price Source": "",
    })

    return pd.DataFrame(rows)


def get_all_positions_df() -> pd.DataFrame:
    return _positions_to_dataframe()


def get_crypto_holdings_df() -> pd.DataFrame:
    return _positions_to_dataframe({"crypto"})


def get_stock_holdings_df() -> pd.DataFrame:
    return _positions_to_dataframe({"stock", "etf"})


def get_settings_markdown() -> str:
    """Get current settings."""
    conn = get_connection()
    try:
        base_currency = get_setting(conn, "base_currency")
        cost_basis = get_setting(conn, "cost_basis")
        return f"""
## Current Settings

**Base Currency:** {base_currency}
**Cost Basis Method:** {cost_basis}

### Database Info
- **Location:** `data/portfolio.duckdb`
- **Initialized:** ✓
"""
    finally:
        conn.close()


def get_overview_data() -> tuple[str, pd.DataFrame]:
    """Get overview markdown and full positions DataFrame."""
    conn = get_connection()
    try:
        summary = get_portfolio_summary(conn)
        positions = calculate_positions(conn)
    finally:
        conn.close()

    bonds_total = bond_service.get_bonds_total()
    net_worth = summary['net_worth'] + bonds_total

    warnings_md = ""
    if summary["warnings"]:
        warnings_md = "\n".join(f"- {warning}" for warning in summary["warnings"])
        warnings_md = f"\n### Warnings\n{warnings_md}\n"

    latest_price_ts = _format_cache_timestamp(summary["latest_price_ts"])
    latest_fx_ts = _format_cache_timestamp(summary["latest_fx_ts"])

    summary_text = f"""
## Portfolio Summary

**Net Worth:** {net_worth:,.2f} PLN

**Holdings Value:** {summary['holdings_value']:,.2f} PLN

**Bonds:** {bonds_total:,.2f} PLN

**Cash:** {summary['cash']:,.2f} PLN

### Cache Status
- **Latest price cache:** {latest_price_ts}
- **Latest FX cache:** {latest_fx_ts}
{warnings_md}
""".strip()

    if not positions:
        return summary_text, pd.DataFrame()

    return summary_text, get_all_positions_df()


def get_dashboard_payload(transaction_limit: int = 50, refresh_status: str = "") -> tuple:
    """Return all dashboard outputs used by the top-level refresh."""
    overview_md, positions_df = get_overview_data()
    crypto_orders_df, crypto_order_ids = get_crypto_orders_df()
    stock_orders_df, stock_order_ids = get_stock_orders_df()
    bonds_dataframe, bonds_ids = get_bonds_df()
    return (
        refresh_status,
        overview_md,
        positions_df,
        crypto_orders_df,
        crypto_order_ids,
        stock_orders_df,
        stock_order_ids,
        bonds_dataframe,
        bonds_ids,
        get_accounts_df(),
        get_settings_markdown(),
    )


def refresh_and_get_dashboard(transaction_limit: int = 50) -> tuple:
    """Refresh caches and return all dashboard outputs."""
    refresh_status = refresh_market_data()
    return get_dashboard_payload(transaction_limit, refresh_status=refresh_status)
