"""Gradio UI for the Financial Dashboard."""
import gradio as gr
import duckdb
import pandas as pd
from datetime import datetime, date
from decimal import Decimal
from typing import List

from app.core.db import init_db, get_setting, set_setting
from app.core.portfolio import calculate_positions, get_portfolio_summary
from app.core.models import AssetType, TransactionAction
from app.adapters import crypto_coingecko, stocks_yfinance, fx_nbp


# Global database connection
conn = None


def get_conn():
    """Get or initialize database connection."""
    global conn
    if conn is None:
        conn = init_db()
    return conn


def refresh_prices():
    """Refresh all asset prices from external APIs."""
    conn = get_conn()

    messages = []

    # Get all holdings that need price updates
    holdings = conn.execute("""
        SELECT id, asset_type, symbol, currency
        FROM holdings
        ORDER BY asset_type, symbol
    """).fetchall()

    if not holdings:
        return "No holdings found to refresh."

    # Fetch FX rates first
    try:
        fx_rates = fx_nbp.get_current_rates("PLN")
        now = datetime.now()

        for ccy, rate in fx_rates.items():
            if ccy != "PLN":
                conn.execute("""
                    INSERT INTO fx_rates (ts, base_ccy, quote_ccy, rate, source)
                    VALUES (?, ?, ?, ?, ?)
                """, [now, ccy, "PLN", float(rate), "NBP"])

        messages.append(f"✓ Updated {len(fx_rates)} FX rates from NBP")
    except Exception as e:
        messages.append(f"⚠ FX rates update failed: {str(e)}")

    # Group holdings by asset type
    crypto_symbols = []
    stock_symbols = []

    for holding_id, asset_type, symbol, currency in holdings:
        if asset_type == "crypto":
            crypto_symbols.append((holding_id, symbol, currency))
        elif asset_type in ["stock", "etf"]:
            stock_symbols.append((holding_id, symbol, currency))

    # Fetch crypto prices
    if crypto_symbols:
        try:
            symbols = [s[1] for s in crypto_symbols]
            prices = crypto_coingecko.get_current_prices(symbols, vs_currency="usd")

            now = datetime.now()
            for holding_id, symbol, currency in crypto_symbols:
                if symbol.upper() in prices:
                    price = prices[symbol.upper()]
                    conn.execute("""
                        INSERT INTO prices (holding_id, ts, price, price_ccy, source)
                        VALUES (?, ?, ?, ?, ?)
                    """, [holding_id, now, float(price), "USD", "CoinGecko"])

            messages.append(f"✓ Updated {len(prices)} crypto prices from CoinGecko")
        except Exception as e:
            messages.append(f"⚠ Crypto prices update failed: {str(e)}")

    # Fetch stock prices
    if stock_symbols:
        try:
            updated = 0
            now = datetime.now()

            for holding_id, symbol, currency in stock_symbols:
                price = stocks_yfinance.get_current_price(symbol)
                if price:
                    conn.execute("""
                        INSERT INTO prices (holding_id, ts, price, price_ccy, source)
                        VALUES (?, ?, ?, ?, ?)
                    """, [holding_id, now, float(price), currency, "yfinance"])
                    updated += 1

            messages.append(f"✓ Updated {updated} stock prices from yfinance")
        except Exception as e:
            messages.append(f"⚠ Stock prices update failed: {str(e)}")

    conn.commit()
    return "\n".join(messages)


def get_overview_data():
    """Get portfolio overview data."""
    conn = get_conn()

    # Get portfolio summary
    summary = get_portfolio_summary(conn)

    # Format summary
    summary_text = f"""
## Portfolio Summary

**Net Worth:** {summary['net_worth']:,.2f} PLN
**Holdings Value:** {summary['holdings_value']:,.2f} PLN
**Cash:** {summary['cash']:,.2f} PLN
**Unrealized P/L:** {summary['unrealized_pl']:,.2f} PLN
"""

    # Get positions by asset type
    positions = calculate_positions(conn)

    if not positions:
        return summary_text, pd.DataFrame()

    # Create DataFrame for positions
    positions_data = []
    for pos in positions:
        positions_data.append({
            "Asset Type": pos.holding.asset_type.upper(),
            "Symbol": pos.holding.symbol,
            "Name": pos.holding.name or "",
            "Quantity": f"{pos.qty:.8f}",
            "Avg Cost": f"{pos.avg_cost:.2f} {pos.holding.currency}",
            "Current Price": f"{pos.current_price:.2f} {pos.holding.currency}",
            "Value (PLN)": f"{pos.value_pln:,.2f}",
            "Unrealized P/L": f"{pos.unrealized_pl:,.2f}",
        })

    df = pd.DataFrame(positions_data)

    return summary_text, df


def get_crypto_holdings():
    """Get all crypto holdings."""
    conn = get_conn()

    positions = calculate_positions(conn)
    crypto_positions = [p for p in positions if p.holding.asset_type == "crypto"]

    if not crypto_positions:
        return pd.DataFrame(columns=["Symbol", "Name", "Quantity", "Avg Cost", "Current Price", "Value (PLN)", "Unrealized P/L"])

    data = []
    for pos in crypto_positions:
        data.append({
            "Symbol": pos.holding.symbol,
            "Name": pos.holding.name or "",
            "Quantity": f"{pos.qty:.8f}",
            "Avg Cost": f"{pos.avg_cost:.2f} {pos.holding.currency}",
            "Current Price": f"{pos.current_price:.2f} {pos.holding.currency}",
            "Value (PLN)": f"{pos.value_pln:,.2f}",
            "Unrealized P/L": f"{pos.unrealized_pl:,.2f}",
        })

    return pd.DataFrame(data)


def get_stock_holdings():
    """Get all stock/ETF holdings."""
    conn = get_conn()

    positions = calculate_positions(conn)
    stock_positions = [p for p in positions if p.holding.asset_type in ["stock", "etf"]]

    if not stock_positions:
        return pd.DataFrame(columns=["Symbol", "Name", "Quantity", "Avg Cost", "Current Price", "Value (PLN)", "Unrealized P/L"])

    data = []
    for pos in stock_positions:
        data.append({
            "Symbol": pos.holding.symbol,
            "Name": pos.holding.name or "",
            "Quantity": f"{pos.qty:.8f}",
            "Avg Cost": f"{pos.avg_cost:.2f} {pos.holding.currency}",
            "Current Price": f"{pos.current_price:.2f} {pos.holding.currency}",
            "Value (PLN)": f"{pos.value_pln:,.2f}",
            "Unrealized P/L": f"{pos.unrealized_pl:,.2f}",
        })

    return pd.DataFrame(data)


def get_bond_holdings():
    """Get all bond holdings."""
    conn = get_conn()

    # Get bonds with metadata
    bonds = conn.execute("""
        SELECT
            h.symbol, h.name, h.currency,
            b.face, b.coupon_rate, b.coupon_freq, b.maturity_date, b.issuer
        FROM holdings h
        JOIN bond_meta b ON h.id = b.holding_id
        WHERE h.asset_type = 'bond'
        ORDER BY b.maturity_date
    """).fetchall()

    if not bonds:
        return pd.DataFrame(columns=["Symbol", "Name", "Face Value", "Coupon Rate", "Frequency", "Maturity", "Issuer"])

    data = []
    for bond in bonds:
        symbol, name, currency, face, coupon_rate, coupon_freq, maturity, issuer = bond
        data.append({
            "Symbol": symbol,
            "Name": name or "",
            "Face Value": f"{face:.2f} {currency}",
            "Coupon Rate": f"{coupon_rate:.2f}%",
            "Frequency": f"{coupon_freq}x/year",
            "Maturity": str(maturity),
            "Issuer": issuer or "",
        })

    return pd.DataFrame(data)


def get_accounts():
    """Get all accounts."""
    conn = get_conn()

    accounts = conn.execute("""
        SELECT name, type, currency, balance, active
        FROM accounts
        ORDER BY name
    """).fetchall()

    if not accounts:
        return pd.DataFrame(columns=["Name", "Type", "Currency", "Balance", "Active"])

    data = []
    for acc in accounts:
        name, acc_type, currency, balance, active = acc
        data.append({
            "Name": name,
            "Type": acc_type,
            "Currency": currency,
            "Balance": f"{balance:,.2f}",
            "Active": "✓" if active else "✗",
        })

    return pd.DataFrame(data)


def get_transactions(limit=50):
    """Get recent transactions."""
    conn = get_conn()

    txns = conn.execute(f"""
        SELECT
            t.ts, h.symbol, h.asset_type, t.action,
            t.qty, t.price, t.fee, a.name as account, t.note
        FROM transactions t
        JOIN holdings h ON t.holding_id = h.id
        LEFT JOIN accounts a ON t.account_id = a.id
        ORDER BY t.ts DESC
        LIMIT {limit}
    """).fetchall()

    if not txns:
        return pd.DataFrame(columns=["Date", "Symbol", "Type", "Action", "Quantity", "Price", "Fee", "Account", "Note"])

    data = []
    for txn in txns:
        ts, symbol, asset_type, action, qty, price, fee, account, note = txn
        data.append({
            "Date": str(ts),
            "Symbol": symbol,
            "Type": asset_type,
            "Action": action,
            "Quantity": f"{qty:.8f}" if qty else "",
            "Price": f"{price:.2f}" if price else "",
            "Fee": f"{fee:.2f}" if fee else "0.00",
            "Account": account or "",
            "Note": note or "",
        })

    return pd.DataFrame(data)


def add_crypto_holding(symbol, name, currency="USD"):
    """Add a new crypto holding."""
    conn = get_conn()

    try:
        conn.execute("""
            INSERT INTO holdings (asset_type, symbol, name, currency)
            VALUES (?, ?, ?, ?)
        """, ["crypto", symbol.upper(), name, currency.upper()])
        conn.commit()
        return f"✓ Added crypto holding: {symbol.upper()}"
    except Exception as e:
        return f"✗ Error: {str(e)}"


def add_stock_holding(symbol, currency="USD"):
    """Add a new stock/ETF holding."""
    conn = get_conn()

    try:
        # Try to get info from yfinance
        info = stocks_yfinance.get_info(symbol)
        name = info.get("name", symbol)
        detected_currency = info.get("currency", currency)
        asset_type = "etf" if info.get("type") == "ETF" else "stock"

        conn.execute("""
            INSERT INTO holdings (asset_type, symbol, name, currency)
            VALUES (?, ?, ?, ?)
        """, [asset_type, symbol.upper(), name, detected_currency])
        conn.commit()
        return f"✓ Added {asset_type} holding: {symbol.upper()} ({name})"
    except Exception as e:
        return f"✗ Error: {str(e)}"


def add_transaction(symbol, action, quantity, price, fee=0, note=""):
    """Add a new transaction."""
    conn = get_conn()

    try:
        # Get holding ID
        holding = conn.execute("""
            SELECT id FROM holdings WHERE symbol = ?
        """, [symbol.upper()]).fetchone()

        if not holding:
            return f"✗ Holding not found: {symbol}"

        holding_id = holding[0]

        conn.execute("""
            INSERT INTO transactions (holding_id, ts, action, qty, price, fee, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [holding_id, datetime.now(), action, quantity, price, fee, note])
        conn.commit()

        return f"✓ Added {action} transaction for {symbol.upper()}"
    except Exception as e:
        return f"✗ Error: {str(e)}"


def add_account(name, acc_type, currency, balance=0):
    """Add a new account."""
    conn = get_conn()

    try:
        conn.execute("""
            INSERT INTO accounts (name, type, currency, balance)
            VALUES (?, ?, ?, ?)
        """, [name, acc_type, currency.upper(), balance])
        conn.commit()
        return f"✓ Added account: {name}"
    except Exception as e:
        return f"✗ Error: {str(e)}"


def get_settings_info():
    """Get current settings."""
    conn = get_conn()

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


def create_ui():
    """Create and configure the Gradio interface."""

    with gr.Blocks(title="Financial Dashboard", theme=gr.themes.Soft()) as demo:
        gr.Markdown("# 💰 Financial Dashboard")
        gr.Markdown("A minimal, local-only snapshot of your finances in PLN")

        # Refresh button at the top
        with gr.Row():
            refresh_btn = gr.Button("🔄 Refresh Prices", variant="primary", size="sm")
            refresh_output = gr.Textbox(label="Refresh Status", lines=3, interactive=False)

        refresh_btn.click(fn=refresh_prices, outputs=refresh_output)

        # Tabs
        with gr.Tabs():
            # Overview Tab
            with gr.Tab("📊 Overview"):
                with gr.Row():
                    with gr.Column():
                        overview_refresh_btn = gr.Button("🔄 Refresh Overview", size="sm")

                summary_md = gr.Markdown()
                positions_df = gr.DataFrame(label="All Positions")

                overview_refresh_btn.click(
                    fn=get_overview_data,
                    outputs=[summary_md, positions_df]
                )

                # Load on startup
                demo.load(fn=get_overview_data, outputs=[summary_md, positions_df])

            # Crypto Tab
            with gr.Tab("₿ Crypto"):
                with gr.Row():
                    crypto_refresh_btn = gr.Button("🔄 Refresh", size="sm")

                crypto_df = gr.DataFrame(label="Crypto Holdings")

                gr.Markdown("### Add New Crypto Holding")
                with gr.Row():
                    crypto_symbol = gr.Textbox(label="Symbol (e.g., BTC)", scale=1)
                    crypto_name = gr.Textbox(label="Name", scale=2)
                    crypto_currency = gr.Textbox(label="Currency", value="USD", scale=1)

                crypto_add_btn = gr.Button("Add Crypto Holding")
                crypto_add_output = gr.Textbox(label="Result", interactive=False)

                crypto_refresh_btn.click(fn=get_crypto_holdings, outputs=crypto_df)
                crypto_add_btn.click(
                    fn=add_crypto_holding,
                    inputs=[crypto_symbol, crypto_name, crypto_currency],
                    outputs=crypto_add_output
                ).then(fn=get_crypto_holdings, outputs=crypto_df)

                demo.load(fn=get_crypto_holdings, outputs=crypto_df)

            # Stocks Tab
            with gr.Tab("📈 Stocks & ETFs"):
                with gr.Row():
                    stocks_refresh_btn = gr.Button("🔄 Refresh", size="sm")

                stocks_df = gr.DataFrame(label="Stock Holdings")

                gr.Markdown("### Add New Stock/ETF Holding")
                with gr.Row():
                    stock_symbol = gr.Textbox(label="Symbol (e.g., AAPL)", scale=2)
                    stock_currency = gr.Textbox(label="Currency (optional)", value="USD", scale=1)

                stock_add_btn = gr.Button("Add Stock/ETF Holding")
                stock_add_output = gr.Textbox(label="Result", interactive=False)

                stocks_refresh_btn.click(fn=get_stock_holdings, outputs=stocks_df)
                stock_add_btn.click(
                    fn=add_stock_holding,
                    inputs=[stock_symbol, stock_currency],
                    outputs=stock_add_output
                ).then(fn=get_stock_holdings, outputs=stocks_df)

                demo.load(fn=get_stock_holdings, outputs=stocks_df)

            # Bonds Tab
            with gr.Tab("🏦 Bonds"):
                with gr.Row():
                    bonds_refresh_btn = gr.Button("🔄 Refresh", size="sm")

                bonds_df = gr.DataFrame(label="Bond Holdings")

                gr.Markdown("### Bond Management")
                gr.Markdown("*Bond management UI coming soon. Bonds must be added directly to the database.*")

                bonds_refresh_btn.click(fn=get_bond_holdings, outputs=bonds_df)
                demo.load(fn=get_bond_holdings, outputs=bonds_df)

            # Accounts Tab
            with gr.Tab("💳 Accounts"):
                with gr.Row():
                    accounts_refresh_btn = gr.Button("🔄 Refresh", size="sm")

                accounts_df = gr.DataFrame(label="Accounts")

                gr.Markdown("### Add New Account")
                with gr.Row():
                    acc_name = gr.Textbox(label="Name", scale=2)
                    acc_type = gr.Dropdown(
                        label="Type",
                        choices=["checking", "savings", "investment", "credit", "other"],
                        scale=1
                    )
                    acc_currency = gr.Textbox(label="Currency", value="PLN", scale=1)
                    acc_balance = gr.Number(label="Initial Balance", value=0, scale=1)

                acc_add_btn = gr.Button("Add Account")
                acc_add_output = gr.Textbox(label="Result", interactive=False)

                accounts_refresh_btn.click(fn=get_accounts, outputs=accounts_df)
                acc_add_btn.click(
                    fn=add_account,
                    inputs=[acc_name, acc_type, acc_currency, acc_balance],
                    outputs=acc_add_output
                ).then(fn=get_accounts, outputs=accounts_df)

                demo.load(fn=get_accounts, outputs=accounts_df)

            # Transactions Tab
            with gr.Tab("📝 Transactions"):
                with gr.Row():
                    txn_refresh_btn = gr.Button("🔄 Refresh", size="sm")
                    txn_limit = gr.Slider(label="Show last N transactions", minimum=10, maximum=200, value=50, step=10)

                txn_df = gr.DataFrame(label="Recent Transactions")

                gr.Markdown("### Add New Transaction")
                with gr.Row():
                    txn_symbol = gr.Textbox(label="Symbol", scale=1)
                    txn_action = gr.Dropdown(
                        label="Action",
                        choices=["buy", "sell", "dividend", "transfer"],
                        scale=1
                    )
                    txn_qty = gr.Number(label="Quantity", scale=1)
                    txn_price = gr.Number(label="Price", scale=1)
                    txn_fee = gr.Number(label="Fee", value=0, scale=1)

                txn_note = gr.Textbox(label="Note (optional)")
                txn_add_btn = gr.Button("Add Transaction")
                txn_add_output = gr.Textbox(label="Result", interactive=False)

                txn_refresh_btn.click(fn=get_transactions, inputs=txn_limit, outputs=txn_df)
                txn_limit.change(fn=get_transactions, inputs=txn_limit, outputs=txn_df)
                txn_add_btn.click(
                    fn=add_transaction,
                    inputs=[txn_symbol, txn_action, txn_qty, txn_price, txn_fee, txn_note],
                    outputs=txn_add_output
                ).then(fn=get_transactions, inputs=txn_limit, outputs=txn_df)

                demo.load(fn=get_transactions, outputs=txn_df)

            # Settings Tab
            with gr.Tab("⚙️ Settings"):
                settings_md = gr.Markdown()

                gr.Markdown("### Database Management")
                with gr.Row():
                    gr.Button("Export to CSV", size="sm", variant="secondary")
                    gr.Button("Export to Parquet", size="sm", variant="secondary")

                gr.Markdown("*Export functionality coming soon*")

                demo.load(fn=get_settings_info, outputs=settings_md)

    return demo


def launch(share=False, server_port=7860):
    """Launch the Gradio UI."""
    demo = create_ui()
    demo.launch(share=share, server_port=server_port, server_name="0.0.0.0")
