from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


def _pg_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "moex_dwh"),
        user=os.getenv("POSTGRES_USER", "moex"),
        password=os.getenv("POSTGRES_PASSWORD", "moex_pass"),
    )


def _ensure_notification_table(cur: psycopg2.extensions.cursor) -> None:
    cur.execute(
        """
        ALTER TABLE auth.users
        ADD COLUMN IF NOT EXISTS telegram_chat_id TEXT
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mart.anomaly_telegram_notifications (
            notification_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            event_id BIGINT NOT NULL,
            chat_id TEXT NOT NULL,
            sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (event_id, chat_id)
        )
        """
    )


def _build_telegram_message(event: tuple[Any, ...]) -> str:
    (
        event_id,
        detected_at,
        ticker,
        anomaly_type,
        severity,
        metric_value,
        threshold_value,
        details,
    ) = event
    details_text = json.dumps(details, ensure_ascii=False, indent=2)
    return (
        "🚨 Новая аномалия\n\n"
        f"Event ID: {event_id}\n"
        f"Время: {detected_at}\n"
        f"Тикер: {ticker}\n"
        f"Тип: {anomaly_type}\n"
        f"Severity: {severity}\n"
        f"Metric value: {metric_value}\n"
        f"Threshold: {threshold_value}\n"
        f"Details:\n{details_text}"
    )


def _send_telegram_message(bot_token: str, chat_id: str, text: str) -> None:
    response = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=20,
    )
    response.raise_for_status()


def send_anomaly_telegram_alerts() -> None:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        raise ValueError("TELEGRAM_BOT_TOKEN must be set for Telegram alerts.")

    conn = _pg_connect()
    sent = 0
    try:
        with conn.cursor() as cur:
            _ensure_notification_table(cur)
            conn.commit()

            cur.execute(
                """
                SELECT
                    e.event_id,
                    e.detected_at,
                    e.ticker,
                    e.anomaly_type,
                    e.severity,
                    e.metric_value,
                    e.threshold_value,
                    e.details,
                    u.telegram_chat_id
                FROM mart.anomaly_events e
                JOIN auth.users u
                  ON u.is_active = TRUE
                 AND u.telegram_chat_id IS NOT NULL
                LEFT JOIN mart.anomaly_telegram_notifications n
                  ON n.event_id = e.event_id
                 AND n.chat_id = u.telegram_chat_id
                WHERE n.notification_id IS NULL
                ORDER BY e.detected_at ASC
                LIMIT 500
                """
            )
            rows = cur.fetchall()

        for row in rows:
            event_id = row[0]
            chat_id = row[8]
            message = _build_telegram_message(row[:8])

            _send_telegram_message(bot_token=bot_token, chat_id=chat_id, text=message)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO mart.anomaly_telegram_notifications (event_id, chat_id)
                    VALUES (%s, %s)
                    ON CONFLICT (event_id, chat_id) DO NOTHING
                    """,
                    (event_id, chat_id),
                )
            conn.commit()
            sent += 1

        print(f"Anomaly telegram DAG: sent={sent}, pending_checked={len(rows)}")
    finally:
        conn.close()


with DAG(
    dag_id="anomaly_telegram_alert_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "anomaly", "telegram"],
) as anomaly_telegram_alert_dag:
    PythonOperator(
        task_id="send_anomaly_telegram_alerts",
        python_callable=send_anomaly_telegram_alerts,
    )
