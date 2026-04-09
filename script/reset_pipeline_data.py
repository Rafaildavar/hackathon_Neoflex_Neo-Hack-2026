from __future__ import annotations

import argparse
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reset pipeline tables before full historical reload."
    )
    parser.add_argument("--db-host", default=os.getenv("POSTGRES_HOST", "localhost"))
    parser.add_argument(
        "--db-port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432"))
    )
    parser.add_argument("--db-name", default=os.getenv("POSTGRES_DB", "moex_dwh"))
    parser.add_argument("--db-user", default=os.getenv("POSTGRES_USER", "moex"))
    parser.add_argument(
        "--db-password", default=os.getenv("POSTGRES_PASSWORD", "moex_pass")
    )
    parser.add_argument(
        "--include-mart",
        action="store_true",
        help="Also truncate mart tables (dashboard_metrics, daily_metrics, anomaly_events).",
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

    truncate_statements = [
        "TRUNCATE TABLE stg.raw_moex_data;",
        "TRUNCATE TABLE core.minute_candles;",
    ]
    if args.include_mart:
        truncate_statements.extend(
            [
                "TRUNCATE TABLE mart.dashboard_metrics;",
                "TRUNCATE TABLE mart.daily_metrics;",
                "TRUNCATE TABLE mart.anomaly_events RESTART IDENTITY;",
            ]
        )

    with conn:
        with conn.cursor() as cur:
            for stmt in truncate_statements:
                cur.execute(stmt)

    conn.close()
    print("Reset complete.")
    for stmt in truncate_statements:
        print(f"  OK {stmt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
