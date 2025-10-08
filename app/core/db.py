"""DuckDB initialization and schema management."""
import duckdb
from pathlib import Path


def get_db_path(custom_path: str | None = None) -> Path:
    """Get the database file path."""
    if custom_path:
        return Path(custom_path)
    return Path(__file__).parent.parent.parent / "data" / "portfolio.duckdb"


def init_db(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Initialize database and create schema if needed."""
    path = get_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(path))
    _create_schema(conn)
    return conn


def _create_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all tables if they don't exist."""

    # Settings table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key VARCHAR PRIMARY KEY,
            value VARCHAR NOT NULL
        )
    """)

    # Initialize default settings
    conn.execute("""
        INSERT INTO settings (key, value)
        VALUES ('base_currency', 'PLN')
        ON CONFLICT DO NOTHING
    """)
    conn.execute("""
        INSERT INTO settings (key, value)
        VALUES ('cost_basis', 'FIFO')
        ON CONFLICT DO NOTHING
    """)

    # Accounts table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            type VARCHAR NOT NULL,
            currency VARCHAR NOT NULL,
            balance DECIMAL(18, 8) DEFAULT 0,
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Holdings table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY,
            asset_type VARCHAR NOT NULL CHECK (asset_type IN ('crypto', 'stock', 'etf', 'bond', 'cash')),
            symbol VARCHAR NOT NULL,
            name VARCHAR,
            currency VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(asset_type, symbol)
        )
    """)

    # Transactions table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY,
            holding_id INTEGER NOT NULL,
            account_id INTEGER,
            ts TIMESTAMP NOT NULL,
            action VARCHAR NOT NULL CHECK (action IN ('buy', 'sell', 'transfer', 'dividend', 'coupon', 'adjustment')),
            qty DECIMAL(18, 8),
            price DECIMAL(18, 8),
            fee DECIMAL(18, 8) DEFAULT 0,
            note VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (holding_id) REFERENCES holdings(id),
            FOREIGN KEY (account_id) REFERENCES accounts(id)
        )
    """)

    # Prices cache table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY,
            holding_id INTEGER NOT NULL,
            ts TIMESTAMP NOT NULL,
            price DECIMAL(18, 8) NOT NULL,
            price_ccy VARCHAR NOT NULL,
            source VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (holding_id) REFERENCES holdings(id)
        )
    """)

    # FX rates cache table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fx_rates (
            id INTEGER PRIMARY KEY,
            ts TIMESTAMP NOT NULL,
            base_ccy VARCHAR NOT NULL,
            quote_ccy VARCHAR NOT NULL,
            rate DECIMAL(18, 8) NOT NULL,
            source VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts, base_ccy, quote_ccy, source)
        )
    """)

    # Bond metadata table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bond_meta (
            id INTEGER PRIMARY KEY,
            holding_id INTEGER NOT NULL UNIQUE,
            face DECIMAL(18, 8) NOT NULL,
            coupon_rate DECIMAL(8, 4) NOT NULL,
            coupon_freq INTEGER NOT NULL,
            maturity_date DATE NOT NULL,
            issuer VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (holding_id) REFERENCES holdings(id)
        )
    """)

    conn.commit()


def get_setting(conn: duckdb.DuckDBPyConnection, key: str) -> str | None:
    """Get a setting value by key."""
    result = conn.execute("SELECT value FROM settings WHERE key = ?", [key]).fetchone()
    return result[0] if result else None


def set_setting(conn: duckdb.DuckDBPyConnection, key: str, value: str) -> None:
    """Set a setting value."""
    conn.execute("""
        INSERT INTO settings (key, value) VALUES (?, ?)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """, [key, value])
    conn.commit()
