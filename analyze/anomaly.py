from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json, execute_values

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
class AnomalyStats:
    rows_checked: int = 0
    anomalies_found: int = 0
    anomalies_saved: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Find anomalies and save them to mart.anomaly_events")
    parser.add_argument("--ticker", default=None, help="Ticker filter, e.g. SBER")
    parser.add_argument("--from-date", dest="from_date", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--to-date", dest="to_date", default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--window-size", type=int, default=int(os.getenv("ANOMALY_WINDOW_SIZE", "20")), help="Window for 3-sigma check")
    parser.add_argument("--db-host", default=os.getenv("POSTGRES_HOST", "localhost"), help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")), help="PostgreSQL port")
    parser.add_argument("--db-name", default=os.getenv("POSTGRES_DB", "moex_dwh"), help="PostgreSQL database")
    parser.add_argument("--db-user", default=os.getenv("POSTGRES_USER", "moex"), help="PostgreSQL user")
    parser.add_argument("--db-password", default=os.getenv("POSTGRES_PASSWORD", "moex_pass"), help="PostgreSQL password")
    return parser.parse_args()


def build_anomaly_query(
    ticker: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    window_size: int = 20,
) -> tuple[str, list[Any]]:
    if window_size <= 0:
        raise ValueError("window_size must be positive")

    where_parts: list[str] = []
    params: list[Any] = []

    if ticker:
        where_parts.append("ticker = %s")
        params.append(ticker.upper())

    base_where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

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
        WITH base AS (
            SELECT
                trade_date,
                ticker,
                close,
                price_change_pct,
                volume,
                volatility_pct,
                AVG(volume) OVER (
                    PARTITION BY ticker
                    ORDER BY trade_date
                    ROWS BETWEEN {window_size} PRECEDING AND 1 PRECEDING
                ) AS volume_mean,
                STDDEV_SAMP(volume) OVER (
                    PARTITION BY ticker
                    ORDER BY trade_date
                    ROWS BETWEEN {window_size} PRECEDING AND 1 PRECEDING
                ) AS volume_std
            FROM mart.daily_metrics
            {base_where_sql}
        )
        SELECT
            trade_date,
            ticker,
            close,
            price_change_pct,
            volume,
            volatility_pct,
            volume_mean,
            volume_std,
            (ABS(price_change_pct) > 2) AS price_anomaly,
            (volume_std IS NOT NULL AND volume_std > 0 AND ABS(volume - volume_mean) > 3 * volume_std) AS volume_anomaly
        FROM base
        WHERE ABS(price_change_pct) > 2
           OR (volume_std IS NOT NULL AND volume_std > 0 AND ABS(volume - volume_mean) > 3 * volume_std)
        {outer_where_sql}
        ORDER BY trade_date, ticker
    """.strip()

    return query, params + outer_params


def build_anomaly_events(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    events: list[tuple[Any, ...]] = []

    def to_json_value(value: Any) -> Any:
        if isinstance(value, Decimal):
            return float(value)
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value

    for (
        trade_date,
        ticker,
        close,
        price_change_pct,
        volume,
        volatility_pct,
        volume_mean,
        volume_std,
        price_anomaly,
        volume_anomaly,
    ) in rows:
        details: dict[str, Any] = {
            "trade_date": to_json_value(trade_date),
            "close": to_json_value(close),
            "price_change_pct": to_json_value(price_change_pct),
            "volume": to_json_value(volume),
            "volatility_pct": to_json_value(volatility_pct),
        }

        if price_anomaly:
            events.append(
                (
                    ticker,
                    "price_change_gt_2pct",
                    "high",
                    to_json_value(price_change_pct),
                    2,
                    {**details, "threshold_pct": 2},
                )
            )

        if volume_anomaly:
            events.append(
                (
                    ticker,
                    "volume_3sigma",
                    "high",
                    to_json_value(volume),
                    3,
                    {
                        **details,
                        "volume_mean": to_json_value(volume_mean),
                        "volume_std": to_json_value(volume_std),
                        "threshold_sigma": 3,
                    },
                )
            )

    return events


def save_anomalies(
    conn: psycopg2.extensions.connection,
    events: list[tuple[Any, ...]],
) -> int:
    if not events:
        return 0

    with conn.cursor() as cur:
        for ticker, anomaly_type, _severity, _metric_value, _threshold_value, details in events:
            trade_date = details["trade_date"]
            cur.execute(
                """
                DELETE FROM mart.anomaly_events
                WHERE ticker = %s
                  AND anomaly_type = %s
                  AND details->>'trade_date' = %s
                """,
                (ticker, anomaly_type, trade_date),
            )

        execute_values(
            cur,
            """
            INSERT INTO mart.anomaly_events (
                detected_at,
                ticker,
                anomaly_type,
                severity,
                metric_value,
                threshold_value,
                details
            ) VALUES %s
            """,
            [
                (
                    datetime.now(timezone.utc),
                    ticker,
                    anomaly_type,
                    severity,
                    metric_value,
                    threshold_value,
                    Json(details),
                )
                for ticker, anomaly_type, severity, metric_value, threshold_value, details in events
            ],
            template="(%s, %s, %s, %s, %s, %s, %s)",
            page_size=1000,
        )

    return len(events)


def run_anomaly_detection(
    ticker: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    window_size: int = 20,
    db_config: DbConfig | None = None,
) -> AnomalyStats:
    cfg = db_config or DbConfig.from_env()
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )

    stats = AnomalyStats()

    try:
        query, params = build_anomaly_query(
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
            window_size=window_size,
        )

        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        stats.rows_checked = len(rows)
        events = build_anomaly_events(rows)
        stats.anomalies_found = len(events)
        stats.anomalies_saved = save_anomalies(conn, events)
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

    stats = run_anomaly_detection(
        ticker=args.ticker,
        from_date=args.from_date,
        to_date=args.to_date,
        window_size=args.window_size,
        db_config=db_cfg,
    )

    print(f"Checked: {stats.rows_checked}")
    print(f"Found: {stats.anomalies_found}")
    print(f"Saved: {stats.anomalies_saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




