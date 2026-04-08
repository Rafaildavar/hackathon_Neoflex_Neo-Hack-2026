from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import Json

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Загружаем переменные из .env
if load_dotenv is not None:
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)


@dataclass
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass
class TransformStats:
    raw_payloads_processed: int = 0
    candles_inserted: int = 0
    errors: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform raw MOEX payloads from stg.raw_moex_data to core.minute_candles"
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
    parser.add_argument(
        "--ticker",
        default=None,
        help="Process only specific ticker (optional)",
    )
    return parser.parse_args()


def parse_candles_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Parse candles array from MOEX payload.
    Expected format: candles.data = [[open, close, high, low, value, volume, begin, end], ...]
    Expected columns: [open, close, high, low, value, volume, begin, end]
    """
    candles_section = payload.get("candles", {})
    if not isinstance(candles_section, dict):
        return []

    columns = candles_section.get("columns", [])
    data = candles_section.get("data", [])

    if not columns or not data:
        return []

    # Create column indices
    col_idx = {col: i for i, col in enumerate(columns)}

    required = {"open", "close", "high", "low", "volume", "begin"}
    if not required.issubset(set(col_idx.keys())):
        return []

    candles = []
    for row in data:
        try:
            begin_str = row[col_idx["begin"]]  # "2026-04-07 06:59:00"
            # Parse to datetime, assume UTC
            bucket = datetime.fromisoformat(begin_str).replace(
                tzinfo=ZoneInfo("UTC")
            )

            candle = {
                "bucket": bucket,
                "open": float(row[col_idx["open"]]) if row[col_idx["open"]] else None,
                "close": float(row[col_idx["close"]])
                if row[col_idx["close"]]
                else None,
                "high": float(row[col_idx["high"]]) if row[col_idx["high"]] else None,
                "low": float(row[col_idx["low"]]) if row[col_idx["low"]] else None,
                "volume": float(row[col_idx["volume"]])
                if row[col_idx["volume"]]
                else None,
            }
            candles.append(candle)
        except (IndexError, ValueError, TypeError) as e:
            print(f"    WARN Skipping malformed candle row: {e}")
            continue

    return candles


def insert_candles(
    conn: psycopg2.extensions.connection,
    ticker: str,
    candles: list[dict[str, Any]],
) -> int:
    """
    Insert candles into core.minute_candles (each in separate transaction).
    Returns count of inserted rows.
    """
    if not candles:
        return 0

    inserted_count = 0
    first_error = None

    for candle in candles:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.minute_candles (
                        bucket, ticker, open, high, low, close, volume
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, bucket) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                    """,
                    (
                        candle["bucket"],
                        ticker,
                        candle["open"],
                        candle["high"],
                        candle["low"],
                        candle["close"],
                        candle["volume"],
                    ),
                )
            conn.commit()
            inserted_count += 1
        except Exception as e:
            conn.rollback()
            if not first_error:
                first_error = e
            continue

    if first_error:
        print(f"    WARN First error (subsequent errors suppressed): {first_error}")

    return inserted_count


def main() -> int:
    args = parse_args()

    db_cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    stats = TransformStats()

    conn = psycopg2.connect(
        host=db_cfg.host,
        port=db_cfg.port,
        dbname=db_cfg.dbname,
        user=db_cfg.user,
        password=db_cfg.password,
    )

    try:
        # Get list of unprocessed payloads
        with conn.cursor() as cur:
            if args.ticker:
                cur.execute(
                    "SELECT ticker, payload FROM stg.raw_moex_data WHERE ticker = %s ORDER BY ticker",
                    (args.ticker,),
                )
            else:
                cur.execute(
                    "SELECT ticker, payload FROM stg.raw_moex_data ORDER BY ticker"
                )

            rows = cur.fetchall()

        print(f"Processing {len(rows)} raw payload(s)...")

        for ticker, payload in rows:
            print(f"\n{ticker}:")
            candles = parse_candles_from_payload(payload)
            print(f"  Parsed {len(candles)} candles from payload")

            if candles:
                count = insert_candles(conn, ticker, candles)
                print(f"  Inserted/updated {count} candles")
                stats.candles_inserted += count
            else:
                print("  ERROR No candles to insert")
                stats.errors += 1

            stats.raw_payloads_processed += 1

        conn.commit()
    finally:
        conn.close()

    print(
        f"\nOK Done. "
        f"Payloads processed: {stats.raw_payloads_processed}, "
        f"candles inserted: {stats.candles_inserted}, "
        f"errors: {stats.errors}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

