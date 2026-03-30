"""Dashboard read models and refresh orchestration."""
from datetime import datetime
from decimal import Decimal

import pandas as pd

from app.adapters import crypto_coingecko, fx_nbp, stocks_yfinance
from app.core.db import get_connection, get_setting
from app.core.portfolio import calculate_positions, get_portfolio_summary
from app.services.account_service import get_accounts_df
from app.services import bond_service
from app.services.bond_service import get_bonds_df
from app.services.stock_ledger_service import get_stock_orders_df
from app.services.transaction_service import get_transactions_df


def refresh_market_data() -> str:
    """Refresh all asset prices from external APIs."""
    conn = get_connection()
    try:
        messages = []
        holdings = conn.execute("""
            SELECT id, asset_type, symbol, currency
            FROM holdings
            ORDER BY asset_type, symbol
        """).fetchall()

        if not holdings:
            return "No holdings found to refresh."

        try:
            fx_rates = fx_nbp.get_current_rates("PLN")
            now = datetime.now()
            for currency, rate in fx_rates.items():
                if currency == "PLN":
                    continue
                conn.execute("""
                    INSERT INTO fx_rates (id, ts, base_ccy, quote_ccy, rate, source)
                    VALUES (nextval('seq_fx_rates_id'), ?, ?, 'PLN', ?, 'NBP')
                """, [now, currency, float(rate)])
            messages.append(f"✓ Updated {len(fx_rates)} FX rates from NBP")
        except Exception as exc:
            messages.append(f"⚠ FX rates update failed: {exc}")

        crypto_holdings = [(row[0], row[2]) for row in holdings if row[1] == "crypto"]
        stock_holdings = [(row[0], row[2], row[3]) for row in holdings if row[1] in {"stock", "etf"}]

        if crypto_holdings:
            try:
                symbols = [symbol for _, symbol in crypto_holdings]
                prices = crypto_coingecko.get_current_prices(symbols, vs_currency="usd")
                now = datetime.now()
                for holding_id, symbol in crypto_holdings:
                    price = prices.get(symbol.upper())
                    if price is None:
                        continue
                    conn.execute("""
                        INSERT INTO prices (id, holding_id, ts, price, price_ccy, source)
                        VALUES (nextval('seq_prices_id'), ?, ?, ?, 'USD', 'CoinGecko')
                    """, [holding_id, now, float(price)])
                messages.append(f"✓ Updated {len(prices)} crypto prices from CoinGecko")
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
        return pd.DataFrame(columns=["Asset Type", "Symbol", "Name", "Quantity", "Avg Cost", "Current Price", "Value (PLN)", "Unrealized P/L", "Price Source"])

    return pd.DataFrame([
        {
            "Asset Type": position.holding.asset_type.upper(),
            "Symbol": position.holding.symbol,
            "Name": position.holding.name or "",
            "Quantity": f"{position.qty:.8f}",
            "Avg Cost": f"{position.avg_cost:.2f} {position.holding.currency}",
            "Current Price": f"{position.current_price:.2f} {position.current_price_ccy}",
            "Value (PLN)": f"{position.value_pln:,.2f}",
            "Unrealized P/L": f"{position.unrealized_pl:,.2f}",
            "Price Source": position.price_source or "",
        }
        for position in positions
    ])


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

    latest_price_ts = summary["latest_price_ts"] or "n/a"
    latest_fx_ts = summary["latest_fx_ts"] or "n/a"

    summary_text = f"""
## Portfolio Summary

**Net Worth:** {net_worth:,.2f} PLN
**Holdings Value:** {summary['holdings_value']:,.2f} PLN
**Bonds:** {bonds_total:,.2f} PLN
**Cash:** {summary['cash']:,.2f} PLN
**Unrealized P/L:** {summary['unrealized_pl']:,.2f} PLN

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
    bonds_dataframe, bonds_ids = get_bonds_df()
    return (
        refresh_status,
        overview_md,
        positions_df,
        get_crypto_holdings_df(),
        get_stock_orders_df(),
        bonds_dataframe,
        bonds_ids,
        get_accounts_df(),
        get_transactions_df(transaction_limit),
        get_settings_markdown(),
    )


def refresh_and_get_dashboard(transaction_limit: int = 50) -> tuple:
    """Refresh caches and return all dashboard outputs."""
    refresh_status = refresh_market_data()
    return get_dashboard_payload(transaction_limit, refresh_status=refresh_status)
