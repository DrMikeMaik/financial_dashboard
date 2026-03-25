# Financial Snapshot Dashboard (Local, PLN-first)

A minimal, local-only Gradio app to view a **current snapshot** of your finances (crypto, stocks/ETFs, bonds-manual, cash) in **PLN**, with data stored in **DuckDB**.

## Features (MVP)
- Tabs for Overview, Crypto, Stocks, Bonds, Accounts, Transactions, Settings.
- **Refresh** button (no background jobs).
- Crypto prices via CoinGecko (PLN supported).
- Stocks via yfinance (Yahoo! Finance public data).
- FX via **NBP** (PLN base); simple ECB fallback stub included.
- DuckDB storage with CSV/Parquet export helpers.

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
