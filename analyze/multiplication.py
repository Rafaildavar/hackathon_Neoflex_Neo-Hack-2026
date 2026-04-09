from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Загружаем переменные из .env
if load_dotenv is not None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)


IntervalType = Literal["minute", "hourly", "daily", "weekly"]


@dataclass
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "DbConfig":
        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "moex_dwh"),
            user=os.getenv("POSTGRES_USER", "moex"),
            password=os.getenv("POSTGRES_PASSWORD", "moex_pass"),
        )


@dataclass
class IndicatorStats:
    rows_loaded: int = 0
    rows_saved: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calculate RSI, MACD and SMA indicators")
    parser.add_argument(
        "--interval",
        default="daily",
        choices=["minute", "hourly", "daily", "weekly"],
        help="Source interval",
    )
    parser.add_argument("--ticker", default=None, help="Ticker filter, e.g. SBER")
    parser.add_argument("--from-ts", dest="from_ts", default=None, help="Start timestamp")
    parser.add_argument("--to-ts", dest="to_ts", default=None, help="End timestamp")
    parser.add_argument("--db-host", default=os.getenv("POSTGRES_HOST", "localhost"), help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")), help="PostgreSQL port")
    parser.add_argument("--db-name", default=os.getenv("POSTGRES_DB", "moex_dwh"), help="PostgreSQL database")
    parser.add_argument("--db-user", default=os.getenv("POSTGRES_USER", "moex"), help="PostgreSQL user")
    parser.add_argument("--db-password", default=os.getenv("POSTGRES_PASSWORD", "moex_pass"), help="PostgreSQL password")
    return parser.parse_args()


def source_table_name(interval: IntervalType) -> str:
    table_map = {
        "minute": "core.minute_candles",
        "hourly": "core.hourly_candles",
        "daily": "core.daily_candles",
        "weekly": "core.weekly_candles",
    }
    if interval not in table_map:
        raise ValueError(f"Некорректный interval: {interval}")
    return table_map[interval]


def load_candles(
    interval: IntervalType = "daily",
    ticker: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
    db_config: DbConfig | None = None,
) -> pd.DataFrame:
    cfg = db_config or DbConfig.from_env()
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )

    query_parts = [
        "SELECT bucket, ticker, open, high, low, close, volume",
        f"FROM {source_table_name(interval)}",
        "WHERE 1=1",
    ]
    params: list[Any] = []

    if ticker:
        query_parts.append("AND ticker = %s")
        params.append(ticker.upper())
    if from_ts:
        query_parts.append("AND bucket >= %s")
        params.append(from_ts)
    if to_ts:
        query_parts.append("AND bucket <= %s")
        params.append(to_ts)

    query_parts.append("ORDER BY ticker, bucket")
    query = "\n".join(query_parts)

    try:
        return pd.read_sql_query(query, conn, params=params or None)
    finally:
        conn.close()


def calculate_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.where(avg_loss != 0, 100.0)
    rsi = rsi.where(avg_gain != 0, 0.0)
    return rsi


def enrich_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    required = {"bucket", "ticker", "close"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Не хватает колонок: {sorted(missing)}")

    result = df.copy()
    result["bucket"] = pd.to_datetime(result["bucket"], utc=True, errors="coerce")
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result = result.sort_values(["ticker", "bucket"])

    def _per_ticker(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values("bucket").copy()
        close = group["close"]

        group["price_change_pct"] = close.pct_change() * 100

        group["sma_7"] = close.rolling(window=7, min_periods=7).mean()
        group["sma_20"] = close.rolling(window=20, min_periods=20).mean()
        group["sma_50"] = close.rolling(window=50, min_periods=50).mean()

        group["ema_12"] = close.ewm(span=12, adjust=False, min_periods=12).mean()
        group["ema_26"] = close.ewm(span=26, adjust=False, min_periods=26).mean()
        group["macd_line"] = group["ema_12"] - group["ema_26"]
        group["macd_signal"] = group["macd_line"].ewm(span=9, adjust=False, min_periods=9).mean()
        group["macd_histogram"] = group["macd_line"] - group["macd_signal"]
        group["rsi"] = calculate_rsi(close, period=14)
        return group

    enriched = result.groupby("ticker", group_keys=False).apply(_per_ticker).reset_index(drop=True)
    return enriched


def save_indicators(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    interval_type: IntervalType,
) -> int:
    if df.empty:
        return 0

    now = datetime.now(timezone.utc)
    records: list[tuple[Any, ...]] = []
    for row in df.itertuples(index=False):
        records.append(
            (
                row.bucket.to_pydatetime() if hasattr(row.bucket, "to_pydatetime") else row.bucket,
                row.ticker,
                interval_type,
                row.open if hasattr(row, "open") else None,
                row.high if hasattr(row, "high") else None,
                row.low if hasattr(row, "low") else None,
                row.close,
                row.price_change_pct if hasattr(row, "price_change_pct") else None,
                row.volume if hasattr(row, "volume") else None,
                row.sma_7,
                row.sma_20,
                row.sma_50,
                row.rsi,
                row.ema_12,
                row.ema_26,
                row.macd_line,
                row.macd_signal,
                row.macd_histogram,
                now,
            )
        )

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO mart.technical_indicators (
                bucket,
                ticker,
                interval_type,
                open,
                high,
                low,
                close,
                price_change_pct,
                volume,
                sma_7,
                sma_20,
                sma_50,
                rsi,
                ema_12,
                ema_26,
                macd_line,
                macd_signal,
                macd_histogram,
                calculated_at
            ) VALUES %s
            ON CONFLICT (ticker, interval_type, bucket) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                price_change_pct = EXCLUDED.price_change_pct,
                volume = EXCLUDED.volume,
                sma_7 = EXCLUDED.sma_7,
                sma_20 = EXCLUDED.sma_20,
                sma_50 = EXCLUDED.sma_50,
                rsi = EXCLUDED.rsi,
                ema_12 = EXCLUDED.ema_12,
                ema_26 = EXCLUDED.ema_26,
                macd_line = EXCLUDED.macd_line,
                macd_signal = EXCLUDED.macd_signal,
                macd_histogram = EXCLUDED.macd_histogram,
                calculated_at = EXCLUDED.calculated_at
            """,
            records,
            page_size=1000,
        )
    return len(records)


def calculate_technical_indicators(
    interval: IntervalType = "daily",
    ticker: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
    db_config: DbConfig | None = None,
) -> IndicatorStats:
    cfg = db_config or DbConfig.from_env()
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )

    stats = IndicatorStats()

    try:
        candles = load_candles(
            interval=interval,
            ticker=ticker,
            from_ts=from_ts,
            to_ts=to_ts,
            db_config=cfg,
        )
        stats.rows_loaded = len(candles)

        enriched = enrich_indicators(candles)
        stats.rows_saved = save_indicators(conn, enriched, interval)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


def main() -> int:
    args = parse_args()
    db_cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    stats = calculate_technical_indicators(
        interval=args.interval,
        ticker=args.ticker,
        from_ts=args.from_ts,
        to_ts=args.to_ts,
        db_config=db_cfg,
    )

    print(f"Loaded: {stats.rows_loaded}")
    print(f"Saved: {stats.rows_saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



