"""Yahoo Finance stocks/ETFs adapter using yfinance."""
import yfinance as yf
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple


def get_current_price(symbol: str) -> Optional[Decimal]:
    """
    Fetch current price for a stock/ETF.

    Args:
        symbol: Stock ticker (e.g., "AAPL", "SPY")

    Returns:
        Current price as Decimal, or None if not found
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        # Try multiple price fields (Yahoo API is inconsistent)
        price = info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose")

        if price:
            return Decimal(str(price))

        # Fallback: get latest close from history
        hist = ticker.history(period="1d")
        if not hist.empty and "Close" in hist.columns:
            return Decimal(str(hist["Close"].iloc[-1]))

        return None

    except Exception as e:
        print(f"Error fetching price for {symbol}: {e}")
        return None


def get_current_prices(symbols: List[str]) -> Dict[str, Decimal]:
    """
    Fetch current prices for multiple stocks/ETFs.

    Args:
        symbols: List of stock tickers

    Returns:
        Dict of {symbol: price}. Symbols not found will be omitted.
    """
    prices = {}

    for symbol in symbols:
        price = get_current_price(symbol)
        if price:
            prices[symbol.upper()] = price

    return prices


def get_info(symbol: str) -> Dict:
    """
    Fetch basic info about a stock/ETF.

    Args:
        symbol: Stock ticker

    Returns:
        Dict with name, currency, exchange, etc.
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        return {
            "symbol": symbol.upper(),
            "name": info.get("longName") or info.get("shortName"),
            "currency": info.get("currency", "USD"),
            "exchange": info.get("exchange"),
            "type": info.get("quoteType"),  # EQUITY, ETF, MUTUALFUND, etc.
        }

    except Exception as e:
        print(f"Error fetching info for {symbol}: {e}")
        return {"symbol": symbol.upper(), "currency": "USD"}


def get_historical_prices(
    symbol: str,
    start_date: date,
    end_date: Optional[date] = None
) -> List[Tuple[datetime, Decimal]]:
    """
    Fetch historical daily closing prices.

    Args:
        symbol: Stock ticker
        start_date: Start date for history
        end_date: End date (defaults to today)

    Returns:
        List of (timestamp, close_price) tuples
    """
    if end_date is None:
        end_date = date.today()

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(start=start_date, end=end_date + timedelta(days=1))

        prices = []
        if not hist.empty and "Close" in hist.columns:
            for idx, row in hist.iterrows():
                # idx is a pandas Timestamp
                dt = idx.to_pydatetime()
                price = Decimal(str(row["Close"]))
                prices.append((dt, price))

        return prices

    except Exception as e:
        print(f"Error fetching historical prices for {symbol}: {e}")
        return []


def get_dividends(symbol: str, start_date: Optional[date] = None) -> List[Tuple[datetime, Decimal]]:
    """
    Fetch dividend history.

    Args:
        symbol: Stock ticker
        start_date: Optional start date (defaults to 1 year ago)

    Returns:
        List of (date, dividend_amount) tuples
    """
    if start_date is None:
        start_date = date.today() - timedelta(days=365)

    try:
        ticker = yf.Ticker(symbol)
        divs = ticker.dividends

        if divs.empty:
            return []

        # Filter by date
        divs = divs[divs.index >= str(start_date)]

        dividends = []
        for idx, amount in divs.items():
            dt = idx.to_pydatetime()
            dividends.append((dt, Decimal(str(amount))))

        return dividends

    except Exception as e:
        print(f"Error fetching dividends for {symbol}: {e}")
        return []


def get_splits(symbol: str, start_date: Optional[date] = None) -> List[Tuple[datetime, str]]:
    """
    Fetch stock split history.

    Args:
        symbol: Stock ticker
        start_date: Optional start date (defaults to 5 years ago)

    Returns:
        List of (date, split_ratio) tuples (e.g., "2:1")
    """
    if start_date is None:
        start_date = date.today() - timedelta(days=365 * 5)

    try:
        ticker = yf.Ticker(symbol)
        splits = ticker.splits

        if splits.empty:
            return []

        # Filter by date
        splits = splits[splits.index >= str(start_date)]

        split_list = []
        for idx, ratio in splits.items():
            dt = idx.to_pydatetime()
            split_list.append((dt, f"{ratio}:1"))

        return split_list

    except Exception as e:
        print(f"Error fetching splits for {symbol}: {e}")
        return []
