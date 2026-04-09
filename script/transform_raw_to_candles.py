from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
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
    min_bucket: datetime | None = None
    max_bucket: datetime | None = None


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
    parser.add_argument(
        "--from-date",
        dest="from_date",
        default=None,
        help="Filter payloads by request_params.from >= YYYY-MM-DD",
    )
    parser.add_argument(
        "--till-date",
        dest="till_date",
        default=None,
        help="Filter payloads by request_params.till <= YYYY-MM-DD",
    )
    parser.add_argument(
        "--refresh-aggregates",
        action="store_true",
        help="Force refresh core.hourly_candles and core.daily_candles for loaded range",
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


def refresh_aggregates(
    conn: psycopg2.extensions.connection,
    start_ts: datetime,
    end_ts: datetime,
) -> None:
    hour_start = start_ts.replace(minute=0, second=0, microsecond=0)
    hour_end = end_ts.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    day_start = start_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=2)
    week_start = (day_start - timedelta(days=day_start.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    week_end = week_start + timedelta(days=14)

    original_autocommit = conn.autocommit
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "CALL refresh_continuous_aggregate('core.hourly_candles', %s, %s)",
                (hour_start, hour_end),
            )
            cur.execute(
                "CALL refresh_continuous_aggregate('core.daily_candles', %s, %s)",
                (day_start, day_end),
            )
            cur.execute(
                "CALL refresh_continuous_aggregate('core.weekly_candles', %s, %s)",
                (week_start, week_end),
            )
    finally:
        conn.autocommit = original_autocommit


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
        # Get list of payloads to process
        with conn.cursor() as cur:
            where_parts: list[str] = []
            query_args: list[Any] = []

            if args.ticker:
                where_parts.append("ticker = %s")
                query_args.append(args.ticker.upper())
            if args.from_date:
                where_parts.append("(request_params->>'from')::date >= %s::date")
                query_args.append(args.from_date)
            if args.till_date:
                where_parts.append("(request_params->>'till')::date <= %s::date")
                query_args.append(args.till_date)

            where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
            cur.execute(
                f"""
                SELECT ticker, payload
                FROM stg.raw_moex_data
                {where_sql}
                ORDER BY ticker, ingested_at
                """,
                tuple(query_args),
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
                buckets = [c["bucket"] for c in candles if c.get("bucket") is not None]
                if buckets:
                    payload_min = min(buckets)
                    payload_max = max(buckets)
                    stats.min_bucket = (
                        payload_min
                        if stats.min_bucket is None
                        else min(stats.min_bucket, payload_min)
                    )
                    stats.max_bucket = (
                        payload_max
                        if stats.max_bucket is None
                        else max(stats.max_bucket, payload_max)
                    )
            else:
                print("  ERROR No candles to insert")
                stats.errors += 1

            stats.raw_payloads_processed += 1

        if args.refresh_aggregates and stats.min_bucket and stats.max_bucket:
            # End is exclusive for refresh window, add one minute to include last candle.
            refresh_start = stats.min_bucket
            refresh_end = stats.max_bucket.replace(second=0, microsecond=0)
            refresh_end = refresh_end + timedelta(minutes=1)
            print(
                f"\nRefreshing aggregates for range {refresh_start.isoformat()} .. {refresh_end.isoformat()}"
            )
            refresh_aggregates(conn, refresh_start, refresh_end)
            print(
                "  OK Refreshed core.hourly_candles, core.daily_candles and core.weekly_candles"
            )

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

