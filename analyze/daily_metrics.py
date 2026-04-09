from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
class MetricsStats:
    rows_loaded: int = 0
    rows_saved: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calculate daily metrics and save to mart.daily_metrics")
    parser.add_argument("--ticker", default=None, help="Ticker filter, e.g. SBER")
    parser.add_argument("--from-date", dest="from_date", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--to-date", dest="to_date", default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--db-host", default=os.getenv("POSTGRES_HOST", "localhost"), help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")), help="PostgreSQL port")
    parser.add_argument("--db-name", default=os.getenv("POSTGRES_DB", "moex_dwh"), help="PostgreSQL database")
    parser.add_argument("--db-user", default=os.getenv("POSTGRES_USER", "moex"), help="PostgreSQL user")
    parser.add_argument("--db-password", default=os.getenv("POSTGRES_PASSWORD", "moex_pass"), help="PostgreSQL password")
    return parser.parse_args()


def build_daily_metrics_query(ticker: str | None = None, from_date: str | None = None, to_date: str | None = None) -> tuple[str, list[Any]]:
    where_parts: list[str] = []
    params: list[Any] = []

    if ticker:
        where_parts.append("ticker = %s")
        params.append(ticker.upper())

    cte_where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    outer_where_parts: list[str] = []
    outer_params: list[Any] = []
    if from_date:
        outer_where_parts.append("trade_date >= %s")
        outer_params.append(from_date)
    if to_date:
        outer_where_parts.append("trade_date <= %s")
        outer_params.append(to_date)

    outer_where_sql = f"WHERE {' AND '.join(outer_where_parts)}" if outer_where_parts else ""

    query = f"""
        WITH ordered AS (
            SELECT
                bucket::date AS trade_date,
                ticker,
                close,
                volume,
                volatility,
                LAG(close) OVER (PARTITION BY ticker ORDER BY bucket) AS prev_close
            FROM core.daily_candles
            {cte_where_sql}
        )
        SELECT
            trade_date,
            ticker,
            close,
            CASE
                WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
                ELSE ROUND(((close - prev_close) / prev_close) * 100, 6)
            END AS price_change_pct,
            volume,
            volatility AS volatility_pct
        FROM ordered
        {outer_where_sql}
        ORDER BY trade_date, ticker
    """.strip()

    return query, params + outer_params


def save_daily_metrics(
    conn: psycopg2.extensions.connection,
    rows: list[tuple[Any, ...]],
) -> int:
    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO mart.daily_metrics (
                trade_date,
                ticker,
                close,
                price_change_pct,
                volume,
                volatility_pct
            ) VALUES %s
            ON CONFLICT (trade_date, ticker) DO UPDATE SET
                close = EXCLUDED.close,
                price_change_pct = EXCLUDED.price_change_pct,
                volume = EXCLUDED.volume,
                volatility_pct = EXCLUDED.volatility_pct
            """,
            rows,
            page_size=1000,
        )
    return len(rows)


def calculate_daily_metrics(
    ticker: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    db_config: DbConfig | None = None,
) -> MetricsStats:
    cfg = db_config or DbConfig.from_env()
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )

    stats = MetricsStats()

    try:
        query, params = build_daily_metrics_query(
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
        )

        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        stats.rows_loaded = len(rows)
        stats.rows_saved = save_daily_metrics(conn, rows)
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

    stats = calculate_daily_metrics(
        ticker=args.ticker,
        from_date=args.from_date,
        to_date=args.to_date,
        db_config=db_cfg,
    )

    print(f"Loaded: {stats.rows_loaded}")
    print(f"Saved: {stats.rows_saved}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

