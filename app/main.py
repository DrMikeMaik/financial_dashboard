"""Main entry point for the financial dashboard."""
from app.core.db import init_db, get_setting


def main():
    """Initialize and run the application."""
    print("Initializing Financial Dashboard...")

    # Initialize database
    conn = init_db()
    print(f"✓ Database initialized at: {conn.execute('SELECT current_database()').fetchone()[0]}")

    # Check settings
    base_currency = get_setting(conn, "base_currency")
    cost_basis = get_setting(conn, "cost_basis")
    print(f"✓ Base currency: {base_currency}")
    print(f"✓ Cost basis method: {cost_basis}")

    # List tables
    tables = conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()

    print(f"\n✓ Created {len(tables)} tables:")
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
        print(f"  - {table[0]} ({count} rows)")

    conn.close()
    print("\n✓ Database initialized successfully!")


if __name__ == "__main__":
    main()
