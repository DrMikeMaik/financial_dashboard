"""Holding creation helpers for crypto and stocks/ETFs."""
from app.adapters import stocks_yfinance
from app.core.db import get_connection


def add_crypto_holding(symbol: str, name: str, currency: str = "USD") -> str:
    """Add a new crypto holding."""
    if not symbol or not symbol.strip():
        return "✗ Crypto symbol is required."

    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO holdings (asset_type, symbol, name, currency)
            VALUES ('crypto', ?, ?, ?)
        """, [symbol.strip().upper(), name.strip() or None, currency.strip().upper()])
        conn.commit()
        return f"✓ Added crypto holding: {symbol.strip().upper()}"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()


def add_stock_holding(symbol: str, currency: str = "USD") -> str:
    """Add a new stock or ETF holding using yfinance metadata."""
    if not symbol or not symbol.strip():
        return "✗ Stock/ETF symbol is required."

    conn = get_connection()
    try:
        info = stocks_yfinance.get_info(symbol.strip())
        name = info.get("name") or symbol.strip().upper()
        detected_currency = info.get("currency") or currency.strip().upper()
        asset_type = "etf" if info.get("type") == "ETF" else "stock"

        conn.execute("""
            INSERT INTO holdings (asset_type, symbol, name, currency)
            VALUES (?, ?, ?, ?)
        """, [asset_type, symbol.strip().upper(), name, detected_currency])
        conn.commit()
        return f"✓ Added {asset_type} holding: {symbol.strip().upper()} ({name})"
    except Exception as exc:
        return f"✗ Error: {exc}"
    finally:
        conn.close()
