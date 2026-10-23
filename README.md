# Financial Snapshot Dashboard (Local, PLN-first)

A minimal, local-only Gradio app to view a **current snapshot** of your finances (crypto, stocks/ETFs, bonds-manual, cash) in **PLN**, with data stored in **DuckDB**.

## Features (MVP)
- Gradio dashboard with tabs for Overview, Crypto, Stocks & ETFs, Fund, Bonds, Accounts, and Settings.
- Single user-triggered **Refresh** that updates FX and market prices and refreshes all visible tables.
- FIFO portfolio valuation in PLN, with cached price-currency support and warning banners for stale or missing data.
- Separate crypto and stock/ETF order editors with asset search, FIFO lot valuations, and oversell protection on create/edit/delete.
- Manual cash accounts for current balances.
- Manual PLN fund snapshots with monthly contribution tracking.
- Standalone bond ledger with yearly rates and manual current-value overrides.
- Crypto prices via CoinGecko, stock/ETF prices via yfinance, and FX via **NBP**.

> Snapshot only. No forecasting. No real-time streaming.

The dashboard listens on `127.0.0.1:7860` for access from this computer.

## Quickstart
```bash
poetry install

# Run
poetry run python -m app.main

# Local regression checks (temporary test databases under data/)
poetry run python test_portfolio.py
poetry run python test_mvp.py

# Adapter smoke tests (require network access)
poetry run python test_fx.py
poetry run python test_crypto.py
poetry run python test_stocks.py
```

## Current focus
- Finish the manual daily-use MVP before adding CSV import/export or ECB fallback.
- Keep holdings ledger-driven and cash balances manual.
- Defer richer analytics until the core workflow is stable.
