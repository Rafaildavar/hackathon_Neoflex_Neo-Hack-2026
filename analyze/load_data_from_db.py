from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Sequence

import pandas as pd
import psycopg2

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

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


def _execute_query(
    query: str,
    params: Sequence[Any] | None = None,
    db_config: DbConfig | None = None,
) -> pd.DataFrame:
    if not query or not query.strip():
        raise ValueError("SQL запрос не может быть пуст")

    cfg = db_config or DbConfig.from_env()
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


def _load_table(
    table_name: str,
    columns: str,
    ticker: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
    time_column: str = "bucket",
    db_config: DbConfig | None = None,
) -> pd.DataFrame:
    query_parts = [f"SELECT {columns}", f"FROM {table_name}", "WHERE 1=1"]
    params: list[Any] = []

    if ticker:
        query_parts.append("AND ticker = %s")
        params.append(ticker.upper())
    if from_ts:
        query_parts.append(f"AND {time_column} >= %s")
        params.append(from_ts)
    if to_ts:
        query_parts.append(f"AND {time_column} <= %s")
        params.append(to_ts)

    query_parts.append(f"ORDER BY {time_column} DESC")
    return _execute_query("\n".join(query_parts), params=params or None, db_config=db_config)


def load_candles(
    interval: IntervalType = "minute",
    ticker: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
    db_config: DbConfig | None = None,
) -> pd.DataFrame:
    table_map = {
        "minute": "core.minute_candles",
        "hourly": "core.hourly_candles",
        "daily": "core.daily_candles",
        "weekly": "core.weekly_candles",
    }
    if interval not in table_map:
        raise ValueError(f"Некорректный interval: {interval}")
    return _load_table(
        table_name=table_map[interval],
        columns="bucket, ticker, open, high, low, close, volume",
        ticker=ticker,
        from_ts=from_ts,
        to_ts=to_ts,
        db_config=db_config,
    )


def load_daily_metrics(
    ticker: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    db_config: DbConfig | None = None,
) -> pd.DataFrame:
    return _load_table(
        table_name="mart.daily_metrics",
        columns="trade_date, ticker, close, price_change_pct, volume, volatility_pct",
        ticker=ticker,
        from_ts=from_date,
        to_ts=to_date,
        time_column="trade_date",
        db_config=db_config,
    )


def load_dashboard_metrics(
    ticker: str | None = None,
    interval_type: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
    db_config: DbConfig | None = None,
) -> pd.DataFrame:
    query_parts = [
        "SELECT bucket, ticker, interval_type, open, high, low, close, volume, price_change_pct, sma_7, sma_20, sma_50, rsi, ema_12, ema_26, macd_line, macd_signal, macd_histogram",
        "FROM mart.technical_indicators",
        "WHERE 1=1",
    ]
    params: list[Any] = []

    if ticker:
        query_parts.append("AND ticker = %s")
        params.append(ticker.upper())
    if interval_type:
        query_parts.append("AND interval_type = %s")
        params.append(interval_type)
    if from_ts:
        query_parts.append("AND bucket >= %s")
        params.append(from_ts)
    if to_ts:
        query_parts.append("AND bucket <= %s")
        params.append(to_ts)

    query_parts.append("ORDER BY bucket DESC")
    return _execute_query("\n".join(query_parts), params=params or None, db_config=db_config)


def load_raw_payloads(
    ticker: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
    db_config: DbConfig | None = None,
) -> pd.DataFrame:
    return _load_table(
        table_name="stg.raw_moex_data",
        columns="raw_id, ingested_at, source_endpoint, ticker, request_params, payload",
        ticker=ticker,
        from_ts=from_ts,
        to_ts=to_ts,
        time_column="ingested_at",
        db_config=db_config,
    )

