from __future__ import annotations

import os
from pathlib import Path

import psycopg2

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


if load_dotenv is not None:
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def main() -> int:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "moex_dwh"),
        user=os.getenv("POSTGRES_USER", "moex"),
        password=os.getenv("POSTGRES_PASSWORD", "moex_pass"),
    )

    checks = [
        ("stg.raw_moex_data", "SELECT COUNT(*) FROM stg.raw_moex_data"),
        ("core.minute_candles", "SELECT COUNT(*) FROM core.minute_candles"),
        ("core.hourly_candles", "SELECT COUNT(*) FROM core.hourly_candles"),
        ("core.daily_candles", "SELECT COUNT(*) FROM core.daily_candles"),
        ("core.weekly_candles", "SELECT COUNT(*) FROM core.weekly_candles"),
    ]

    print("Pipeline table row counts:")
    with conn:
        with conn.cursor() as cur:
            for table_name, sql in checks:
                cur.execute(sql)
                count = cur.fetchone()[0]
                print(f"  {table_name}: {count}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
