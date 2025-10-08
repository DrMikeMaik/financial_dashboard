"""NBP (Narodowy Bank Polski) FX rate adapter."""
import requests
from datetime import datetime, date
from decimal import Decimal
from typing import Dict


NBP_BASE_URL = "https://api.nbp.pl/api"


def get_current_rates(quote_currency: str = "PLN") -> Dict[str, Decimal]:
    """
    Fetch current NBP exchange rates.

    Returns dict of {currency_code: rate_to_PLN}.
    For PLN itself, rate is 1.0.

    NBP publishes tables:
    - Table A: major currencies (USD, EUR, GBP, etc.) - most common
    - Table B: less common currencies
    - Table C: non-convertible currencies (rare)
    """
    if quote_currency != "PLN":
        raise ValueError("NBP adapter only supports PLN as quote currency")

    rates = {"PLN": Decimal("1.0")}

    # Fetch Table A (major currencies)
    try:
        resp = requests.get(f"{NBP_BASE_URL}/exchangerates/tables/A?format=json", timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data and len(data) > 0:
            for rate_info in data[0].get("rates", []):
                code = rate_info["code"]
                rate = Decimal(str(rate_info["mid"]))
                rates[code] = rate
    except Exception as e:
        print(f"Warning: Failed to fetch NBP Table A: {e}")

    # Fetch Table B (additional currencies)
    try:
        resp = requests.get(f"{NBP_BASE_URL}/exchangerates/tables/B?format=json", timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data and len(data) > 0:
            for rate_info in data[0].get("rates", []):
                code = rate_info["code"]
                rate = Decimal(str(rate_info["mid"]))
                rates[code] = rate
    except Exception as e:
        print(f"Warning: Failed to fetch NBP Table B: {e}")

    return rates


def get_rate_on_date(base_ccy: str, quote_ccy: str, target_date: date) -> Decimal | None:
    """
    Fetch historical exchange rate for a specific date.

    Args:
        base_ccy: Base currency code (e.g., "USD")
        quote_ccy: Quote currency code (must be "PLN")
        target_date: Date to fetch rate for

    Returns:
        Exchange rate as Decimal, or None if not available
    """
    if quote_ccy != "PLN":
        raise ValueError("NBP adapter only supports PLN as quote currency")

    if base_ccy == "PLN":
        return Decimal("1.0")

    date_str = target_date.strftime("%Y-%m-%d")

    # Try Table A first (most common currencies)
    try:
        url = f"{NBP_BASE_URL}/exchangerates/rates/A/{base_ccy}/{date_str}?format=json"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if "rates" in data and len(data["rates"]) > 0:
            return Decimal(str(data["rates"][0]["mid"]))
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            print(f"Warning: NBP Table A request failed for {base_ccy}: {e}")
    except Exception as e:
        print(f"Warning: Failed to fetch NBP Table A rate for {base_ccy}: {e}")

    # Try Table B (less common currencies)
    try:
        url = f"{NBP_BASE_URL}/exchangerates/rates/B/{base_ccy}/{date_str}?format=json"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if "rates" in data and len(data["rates"]) > 0:
            return Decimal(str(data["rates"][0]["mid"]))
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            print(f"Warning: NBP Table B request failed for {base_ccy}: {e}")
    except Exception as e:
        print(f"Warning: Failed to fetch NBP Table B rate for {base_ccy}: {e}")

    return None


def convert_to_pln(amount: Decimal, from_ccy: str, rates: Dict[str, Decimal]) -> Decimal | None:
    """
    Convert amount from given currency to PLN using provided rates.

    Args:
        amount: Amount to convert
        from_ccy: Source currency code
        rates: Dict of currency rates (from get_current_rates)

    Returns:
        Amount in PLN, or None if rate not available
    """
    if from_ccy == "PLN":
        return amount

    if from_ccy not in rates:
        return None

    return amount * rates[from_ccy]
