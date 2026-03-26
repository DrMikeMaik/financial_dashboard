# Financial Snapshot Dashboard (Local, PLN-first)

A minimal, local-only Gradio app to view a **current snapshot** of your finances (crypto, stocks/ETFs, bonds-manual, cash) in **PLN**, with data stored in **DuckDB**.

## Features (MVP)
- Gradio dashboard with tabs for Overview, Crypto, Stocks/ETFs, Bonds, Accounts, Transactions, and Settings.
- Single user-triggered **Refresh** that updates FX and market prices and refreshes all visible tables.
- FIFO portfolio valuation in PLN, with cached price-currency support and warning banners for stale or missing data.
- Manual transaction ledger with create/edit/delete in the UI.
- Manual cash accounts for current balances.
- Manual bond metadata and current valuation entry.
- Crypto prices via CoinGecko, stock/ETF prices via yfinance, and FX via **NBP**.

> Snapshot only. No forecasting. No real-time streaming.

## Quickstart
```bash
poetry install

# Run
poetry run python -m app.main

# Smoke tests
poetry run python test_fx.py
poetry run python test_crypto.py
poetry run python test_stocks.py
poetry run python test_portfolio.py
poetry run python test_mvp.py
```

## Current focus
- Finish the manual daily-use MVP before adding CSV import/export or ECB fallback.
- Keep holdings ledger-driven and cash balances manual.
- Defer richer analytics until the core workflow is stable.
