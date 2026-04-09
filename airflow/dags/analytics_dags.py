from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator


def _pg_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "moex_dwh"),
        user=os.getenv("POSTGRES_USER", "moex"),
        password=os.getenv("POSTGRES_PASSWORD", "moex_pass"),
    )


def run_daily_metrics_upsert() -> None:
    query = """
        WITH ordered AS (
            SELECT
                bucket::date AS trade_date,
                ticker,
                close,
                volume,
                volatility,
                LAG(close) OVER (PARTITION BY ticker ORDER BY bucket) AS prev_close
            FROM core.daily_candles
        )
        INSERT INTO mart.daily_metrics (
            trade_date,
            ticker,
            close,
            price_change_pct,
            volume,
            volatility_pct
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
        ON CONFLICT (trade_date, ticker) DO UPDATE SET
            close = EXCLUDED.close,
            price_change_pct = EXCLUDED.price_change_pct,
            volume = EXCLUDED.volume,
            volatility_pct = EXCLUDED.volatility_pct
    """
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()
    finally:
        conn.close()


def _to_json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def has_new_data_since_last_run(**context: dict[str, Any]) -> bool:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(bucket) FROM core.minute_candles")
            latest_bucket = cur.fetchone()[0]
    finally:
        conn.close()

    if latest_bucket is None:
        return False

    latest_iso = latest_bucket.astimezone(timezone.utc).isoformat()
    last_seen = Variable.get("anomaly_last_seen_bucket", default_var=None)

    if last_seen is None:
        context["ti"].xcom_push(key="latest_bucket", value=latest_iso)
        return True

    try:
        last_seen_dt = datetime.fromisoformat(last_seen)
    except ValueError:
        context["ti"].xcom_push(key="latest_bucket", value=latest_iso)
        return True

    if latest_bucket.astimezone(timezone.utc) > last_seen_dt.astimezone(timezone.utc):
        context["ti"].xcom_push(key="latest_bucket", value=latest_iso)
        return True

    return False


def run_anomaly_detection(window_size: int = 20) -> None:
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
        ORDER BY trade_date, ticker
    """

    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        events: list[tuple[Any, ...]] = []
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
            details = {
                "trade_date": _to_json_value(trade_date),
                "close": _to_json_value(close),
                "price_change_pct": _to_json_value(price_change_pct),
                "volume": _to_json_value(volume),
                "volatility_pct": _to_json_value(volatility_pct),
            }

            if price_anomaly:
                events.append(
                    (
                        ticker,
                        "price_change_gt_2pct",
                        "high",
                        _to_json_value(price_change_pct),
                        2,
                        details,
                    )
                )

            if volume_anomaly:
                events.append(
                    (
                        ticker,
                        "volume_3sigma",
                        "high",
                        _to_json_value(volume),
                        3,
                        {
                            **details,
                            "volume_mean": _to_json_value(volume_mean),
                            "volume_std": _to_json_value(volume_std),
                            "threshold_sigma": 3,
                        },
                    )
                )

        with conn.cursor() as cur:
            for ticker, anomaly_type, _, _, _, details in events:
                cur.execute(
                    """
                    DELETE FROM mart.anomaly_events
                    WHERE ticker = %s
                      AND anomaly_type = %s
                      AND details->>'trade_date' = %s
                    """,
                    (ticker, anomaly_type, details["trade_date"]),
                )

            for ticker, anomaly_type, severity, metric_value, threshold_value, details in events:
                cur.execute(
                    """
                    INSERT INTO mart.anomaly_events (
                        detected_at,
                        ticker,
                        anomaly_type,
                        severity,
                        metric_value,
                        threshold_value,
                        details
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        datetime.now(timezone.utc),
                        ticker,
                        anomaly_type,
                        severity,
                        metric_value,
                        threshold_value,
                        json.dumps(details, ensure_ascii=False),
                    ),
                )
        conn.commit()
    finally:
        conn.close()


def mark_latest_data_processed(**context: dict[str, Any]) -> None:
    latest_bucket = context["ti"].xcom_pull(
        task_ids="check_new_data", key="latest_bucket"
    )
    if latest_bucket:
        Variable.set("anomaly_last_seen_bucket", latest_bucket)


with DAG(
    dag_id="daily_metrics_dag",
    start_date=datetime(2026, 1, 1),
    schedule="10 0 * * *",
    catchup=False,
    tags=["analytics", "daily", "metrics"],
) as daily_metrics_dag:
    PythonOperator(
        task_id="calculate_daily_metrics",
        python_callable=run_daily_metrics_upsert,
    )


with DAG(
    dag_id="anomaly_on_new_data_dag",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    tags=["analytics", "anomaly", "event-driven"],
) as anomaly_on_new_data_dag:
    check_new_data = ShortCircuitOperator(
        task_id="check_new_data",
        python_callable=has_new_data_since_last_run,
    )

    detect_anomalies = PythonOperator(
        task_id="detect_anomalies",
        python_callable=run_anomaly_detection,
    )

    mark_processed = PythonOperator(
        task_id="mark_latest_bucket_processed",
        python_callable=mark_latest_data_processed,
    )

    check_new_data >> detect_anomalies >> mark_processed
