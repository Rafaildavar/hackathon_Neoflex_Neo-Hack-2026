from __future__ import annotations

import argparse
import os
import sys
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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analyze.multiplication import DbConfig, calculate_technical_indicators


DDL = """
CREATE TABLE IF NOT EXISTS mart.technical_indicators (
    bucket TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    interval_type TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    price_change_pct NUMERIC,
    volume NUMERIC,
    sma_7 NUMERIC,
    sma_20 NUMERIC,
    sma_50 NUMERIC,
    rsi NUMERIC,
    ema_12 NUMERIC,
    ema_26 NUMERIC,
    macd_line NUMERIC,
    macd_signal NUMERIC,
    macd_histogram NUMERIC,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker, interval_type, bucket)
);

SELECT create_hypertable(
    'mart.technical_indicators',
    'bucket',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize mart.technical_indicators and backfill indicators"
    )
    parser.add_argument(
        "--interval",
        default="daily",
        choices=["minute", "hourly", "daily", "weekly"],
        help="Source interval for backfill",
    )
    parser.add_argument(
        "--db-host",
        default=os.getenv("POSTGRES_HOST", "localhost"),
        help="PostgreSQL host",
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", "5432")),
        help="PostgreSQL port",
    )
    parser.add_argument(
        "--db-name",
        default=os.getenv("POSTGRES_DB", "moex_dwh"),
        help="PostgreSQL database",
    )
    parser.add_argument(
        "--db-user",
        default=os.getenv("POSTGRES_USER", "moex"),
        help="PostgreSQL user",
    )
    parser.add_argument(
        "--db-password",
        default=os.getenv("POSTGRES_PASSWORD", "moex_pass"),
        help="PostgreSQL password",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    conn = psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(DDL)
    finally:
        conn.close()

    db_cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )
    stats = calculate_technical_indicators(interval=args.interval, db_config=db_cfg)

    print("Technical indicators initialized.")
    print(f"Loaded: {stats.rows_loaded}")
    print(f"Saved: {stats.rows_saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


