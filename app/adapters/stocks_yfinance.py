"""Yahoo Finance stocks/ETFs adapter using yfinance."""
import yfinance as yf
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple


SUPPORTED_QUOTE_TYPES = {"ETF", "EQUITY"}


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
    base = {
        "symbol": symbol.upper(),
        "name": None,
        "currency": None,
        "exchange": None,
        "exchange_display": None,
        "exchange_label": None,
        "type": None,
        "found": False,
    }

    try:
        ticker = yf.Ticker(symbol)
        try:
            info = ticker.info or {}
        except Exception:
            info = {}

        try:
            fast_info = dict(ticker.fast_info or {})
        except Exception:
            fast_info = {}

        name = info.get("longName") or info.get("shortName") or info.get("displayName")
        currency = (info.get("currency") or fast_info.get("currency") or "").upper() or None
        quote_type = info.get("quoteType")
        exchange = info.get("exchange")
        exchange_display = info.get("exchDisp") or info.get("fullExchangeName") or exchange
        exchange_label = build_exchange_label({
            "exchDisp": exchange_display,
            "exchange": exchange,
            "currency": currency,
        })

        found = any([name, currency, quote_type, exchange_display])
        if not found:
            hist = ticker.history(period="5d")
            found = not hist.empty

        return {
            "symbol": symbol.upper(),
            "name": name,
            "currency": currency,
            "exchange": exchange,
            "exchange_display": exchange_display,
            "exchange_label": exchange_label,
            "type": quote_type,
            "found": found,
        }

    except Exception as e:
        print(f"Error fetching info for {symbol}: {e}")
        return base


def build_exchange_label(info: Dict) -> str | None:
    """Build a compact exchange/market label for display in the stock ledger."""
    exchange = info.get("exchDisp") or info.get("exchange_display") or info.get("fullExchangeName") or info.get("exchange") or info.get("market")
    currency = info.get("currency")

    parts = [part for part in (exchange, currency) if part]
    return " ".join(parts) if parts else None


def search_instruments(query: str, max_results: int = 8) -> List[Dict]:
    """Search Yahoo Finance for stock/ETF symbols and enrich results with currency metadata."""
    normalized_query = (query or "").strip()
    if not normalized_query:
        return []

    try:
        search = yf.Search(normalized_query, max_results=max_results)
        quotes = getattr(search, "quotes", None) or []
    except Exception as e:
        print(f"Error searching for stock/ETF: {e}")
        return []

    results = []
    seen = set()

    for quote in quotes:
        symbol = (quote.get("symbol") or "").strip().upper()
        if not symbol or symbol in seen:
            continue

        quote_type = (quote.get("quoteType") or quote.get("typeDisp") or "").upper()
        if quote_type and quote_type not in SUPPORTED_QUOTE_TYPES:
            continue

        info = get_info(symbol)
        resolved_type = (info.get("type") or quote_type or "").upper()
        if resolved_type and resolved_type not in SUPPORTED_QUOTE_TYPES:
            continue

        currency = (quote.get("currency") or info.get("currency") or "").upper() or None
        name = quote.get("longname") or quote.get("shortname") or info.get("name") or symbol
        exchange_display = quote.get("exchDisp") or info.get("exchange_display") or quote.get("exchange") or info.get("exchange")
        result = {
            "symbol": symbol,
            "name": name,
            "currency": currency,
            "exchange": info.get("exchange") or quote.get("exchange"),
            "exchange_display": exchange_display,
            "exchange_label": build_exchange_label({
                "exchDisp": exchange_display,
                "exchange": info.get("exchange") or quote.get("exchange"),
                "currency": currency,
            }),
            "type": resolved_type or None,
        }
        results.append(result)
        seen.add(symbol)

    return results


def resolve_exact_symbol(symbol: str) -> Dict | None:
    """Resolve an exact Yahoo ticker for stock/ETF entry when search has no matches."""
    normalized_symbol = (symbol or "").strip().upper()
    if not normalized_symbol:
        return None

    info = get_info(normalized_symbol)
    if not info.get("found"):
        return None

    quote_type = (info.get("type") or "").upper()
    if quote_type and quote_type not in SUPPORTED_QUOTE_TYPES:
        return None

    return {
        "symbol": normalized_symbol,
        "name": info.get("name") or normalized_symbol,
        "currency": info.get("currency"),
        "exchange": info.get("exchange"),
        "exchange_display": info.get("exchange_display") or info.get("exchange"),
        "exchange_label": info.get("exchange_label"),
        "type": quote_type or None,
    }


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
