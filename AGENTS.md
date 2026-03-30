# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Overview

Local-only financial snapshot dashboard (Gradio + DuckDB) for viewing current portfolio positions in PLN across crypto, stocks/ETFs, manually-tracked bonds, and cash accounts. No forecasting or projections—only "where am I right now?" with FIFO cost-basis P/L calculations.

## Running the App

```bash
# Setup
poetry install

# Initialize database (currently just tests schema)
poetry run python -m app.main

# Test individual adapters
poetry run python test_fx.py        # Test NBP FX rates
poetry run python test_crypto.py    # Test CoinGecko crypto prices
poetry run python test_stocks.py    # Test yfinance stock prices
poetry run python test_portfolio.py # Test FIFO calculations
```

## Architecture

### Data Flow
1. User clicks **Refresh** button in Gradio UI
2. FX rates fetched from NBP API (primary) or ECB (fallback) → cached in DuckDB
3. Asset prices fetched from CoinGecko (crypto) and yfinance (stocks) → cached in DuckDB
4. Portfolio engine recalculates positions using **pooled FIFO** (all holdings of same symbol treated as single pool regardless of account)
5. P/L and valuations computed in PLN using cached FX rates
6. UI updates with current snapshot

### Module Structure

- **`app/core/`** - Business logic core
  - `db.py` - ✅ DuckDB initialization with sequences for auto-increment IDs
  - `models.py` - ✅ Dataclasses for domain entities (AssetType, Transaction, Position, etc.)
  - `portfolio.py` - ✅ FIFO cost-basis calculations, unrealized/realized P/L
  - `bonds.py` - ⏳ TODO: Manual bond valuation and coupon scheduling

- **`app/adapters/`** - External data source integrations
  - `crypto_coingecko.py` - ✅ CoinGecko REST API (current prices, historical, search)
  - `stocks_yfinance.py` - ✅ Yahoo Finance via yfinance (prices, dividends, splits)
  - `fx_nbp.py` - ✅ Narodowy Bank Polski Web API for PLN FX rates (Tables A & B)
  - `fx_ecb.py` - ⏳ TODO: ECB fallback for missing currency pairs

- **`app/io/`** - Import/Export
  - `import_csv.py` - ⏳ TODO: Transaction CSV import with column mapping
  - `export.py` - ⏳ TODO: CSV/Parquet export from DuckDB

- **`app/ui/`** - Gradio interface
  - ✅ Tab-based layout (Overview, Crypto, Stocks/ETFs, Bonds, Accounts, Transactions, Settings)
  - ✅ Single **Refresh** button triggers all data fetches and updates all visible tables
  - ✅ Manual CRUD for transactions, accounts, and bond metadata/valuations

### DuckDB Schema

All tables use sequences for auto-incrementing IDs. Database stored at `data/portfolio.duckdb`.

- `accounts` - Bank/wallet accounts (name, currency, balance, active)
- `holdings` - Assets tracked (asset_type, symbol, name, currency) with UNIQUE constraint on (asset_type, symbol)
- `transactions` - Buy/sell/transfer/dividend/coupon events ordered by timestamp for FIFO
- `prices` - Cached last prices from CoinGecko/yfinance with timestamp
- `fx_rates` - Cached FX rates from NBP (primary) with timestamp and source
- `bond_meta` - Face value, coupon rate/frequency, maturity dates, issuer
- `settings` - Key-value store (base_currency='PLN', cost_basis='FIFO')

## Key Design Decisions

**PLN-first**: All valuations convert to PLN using NBP API as primary FX source (Polish central bank rates). ECB used only as fallback for currency pairs NBP doesn't publish.

**Pooled FIFO**: Cost basis calculated globally per symbol across all accounts. Example: BTC in wallet A + BTC in wallet B = single FIFO queue for P/L calculations.

**User-initiated refresh**: No cron jobs or background polling. User clicks Refresh when they want updated prices. Stale cached prices displayed with banner if fetch fails.

**Manual bonds**: Polish treasury bonds (obligacje skarbowe) lack accessible market data APIs, so user manually inputs current % of face value or mark-to-market price.

## Non-Goals (MVP)

- Projections, forecasts, modeling
- Real-time streaming prices
- Brokerage API sync
- Tax form generation
- Options, futures, derivatives
- Alerts or scheduled jobs

## Current Status

**Completed**
- ✅ Database schema with DuckDB (7 tables, all with auto-increment sequences)
- ✅ Data models for holdings, transactions, FX, prices, bonds, and computed positions
- ✅ NBP FX adapter, CoinGecko crypto adapter, and yfinance stock/ETF adapter
- ✅ Gradio dashboard with overview, holdings, bonds, accounts, transactions, and settings tabs
- ✅ Top-level refresh flow for FX and market prices
- ✅ FIFO-based holdings valuation in PLN, honoring cached `price_ccy`
- ✅ Multi-currency cash account conversion to PLN in portfolio summary
- ✅ Transaction create/edit/delete workflow with oversell protection
- ✅ Manual bond metadata editor and manual valuation flow stored through `prices`

**Next Steps**
1. Implement CSV import/export.
2. Add ECB fallback for FX rates not covered by NBP.
3. Improve analytics and presentation once the manual workflow is stable.

## Notes for Next Session

- Test scripts are in root directory (`test_*.py`) - these demonstrate adapter usage
- `test_mvp.py` covers the daily-use MVP regressions
- Database schema uses sequences (`seq_*_id`) for auto-increment - don't specify IDs in INSERT statements
- This is an early-stage, single-user internal tool with disposable test data
- Prefer simple code and clean schema changes over migration/backfill logic
- Destructive DB changes are acceptable; the user can reset the DuckDB file at any time
- Avoid over-investing in polish, defensive error handling, and exhaustive tests unless explicitly requested
- FIFO calculation is in `portfolio.py:_calculate_holding_position()` - handles buy/sell with fees
- All prices and FX rates are cached in DuckDB with timestamps
- User will run bash commands manually in separate terminal window with `poetry run ...`
