"""Polish treasury retail bond type definitions and series code parser."""
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import re


@dataclass(frozen=True)
class BondTypePreset:
    code: str
    full_name: str
    term_months: int
    rate_type: str  # "fixed", "variable", "inflation"
    coupon_freq: int
    num_periods: int
    face: Decimal
    currency: str
    issuer: str


POLISH_BOND_PRESETS: dict[str, BondTypePreset] = {
    "OTS": BondTypePreset(
        code="OTS",
        full_name="3-miesięczne stałoprocentowe",
        term_months=3,
        rate_type="fixed",
        coupon_freq=4,
        num_periods=1,
        face=Decimal("100"),
        currency="PLN",
        issuer="Skarb Państwa",
    ),
    "DOS": BondTypePreset(
        code="DOS",
        full_name="2-letnie stałoprocentowe",
        term_months=24,
        rate_type="fixed",
        coupon_freq=1,
        num_periods=1,
        face=Decimal("100"),
        currency="PLN",
        issuer="Skarb Państwa",
    ),
    "TOZ": BondTypePreset(
        code="TOZ",
        full_name="3-letnie zmiennoprocentowe",
        term_months=36,
        rate_type="variable",
        coupon_freq=2,
        num_periods=6,
        face=Decimal("100"),
        currency="PLN",
        issuer="Skarb Państwa",
    ),
    "COI": BondTypePreset(
        code="COI",
        full_name="4-letnie indeksowane inflacją",
        term_months=48,
        rate_type="inflation",
        coupon_freq=1,
        num_periods=4,
        face=Decimal("100"),
        currency="PLN",
        issuer="Skarb Państwa",
    ),
    "EDO": BondTypePreset(
        code="EDO",
        full_name="10-letnie indeksowane inflacją",
        term_months=120,
        rate_type="inflation",
        coupon_freq=1,
        num_periods=10,
        face=Decimal("100"),
        currency="PLN",
        issuer="Skarb Państwa",
    ),
    "ROR": BondTypePreset(
        code="ROR",
        full_name="1-roczne zmiennoprocentowe",
        term_months=12,
        rate_type="variable",
        coupon_freq=12,
        num_periods=12,
        face=Decimal("100"),
        currency="PLN",
        issuer="Skarb Państwa",
    ),
    "ROD": BondTypePreset(
        code="ROD",
        full_name="2-letnie zmiennoprocentowe",
        term_months=24,
        rate_type="variable",
        coupon_freq=12,
        num_periods=24,
        face=Decimal("100"),
        currency="PLN",
        issuer="Skarb Państwa",
    ),
    "TOS": BondTypePreset(
        code="TOS",
        full_name="3-letnie indeksowane inflacją",
        term_months=36,
        rate_type="inflation",
        coupon_freq=1,
        num_periods=3,
        face=Decimal("100"),
        currency="PLN",
        issuer="Skarb Państwa",
    ),
}

_KNOWN_CODES = sorted(POLISH_BOND_PRESETS.keys(), key=len, reverse=True)
_SERIES_RE = re.compile(r"^([A-Z]{2,3})(\d{4})$")


def parse_series_code(series: str) -> tuple[str, date]:
    """Parse a Polish bond series code into (type_code, maturity_date).

    Example: "COI0528" -> ("COI", date(2028, 5, 1))
    Format: {TYPE_CODE}{MM}{YY}
    """
    series = series.strip().upper()
    m = _SERIES_RE.match(series)
    if not m:
        raise ValueError(f"Invalid series code format: '{series}'. Expected e.g. COI0528.")

    type_code = m.group(1)
    digits = m.group(2)

    if type_code not in POLISH_BOND_PRESETS:
        raise ValueError(f"Unknown bond type '{type_code}'. Known types: {', '.join(_KNOWN_CODES)}.")

    month = int(digits[:2])
    year = 2000 + int(digits[2:])

    if month < 1 or month > 12:
        raise ValueError(f"Invalid month {month:02d} in series code '{series}'.")

    return type_code, date(year, month, 1)


def get_preset(bond_type: str) -> BondTypePreset | None:
    return POLISH_BOND_PRESETS.get(bond_type.strip().upper())


def list_bond_type_codes() -> list[str]:
    return list(POLISH_BOND_PRESETS.keys())
