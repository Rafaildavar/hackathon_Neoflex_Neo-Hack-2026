from __future__ import annotations

import os
import smtplib
from datetime import datetime
from email.message import EmailMessage
from typing import Any

import psycopg2
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
        CREATE TABLE IF NOT EXISTS mart.anomaly_email_notifications (
            notification_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            event_id BIGINT NOT NULL,
            email TEXT NOT NULL,
            sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (event_id, email)
        )
        """
    )


def _smtp_send(recipient: str, subject: str, body: str) -> None:
    smtp_host = os.getenv("SMTP_HOST", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", "1025"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    smtp_sender = os.getenv("ALERT_EMAIL_SENDER", "alerts@neo-invest.local")
    smtp_use_tls = os.getenv("SMTP_USE_TLS", "false").lower() == "true"

    msg = EmailMessage()
    msg["From"] = smtp_sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
        if smtp_use_tls:
            server.starttls()
        if smtp_user and smtp_password:
            server.login(smtp_user, smtp_password)
        server.send_message(msg)


def _build_email(event: tuple[Any, ...]) -> tuple[str, str]:
    (
        event_id,
        detected_at,
        ticker,
        anomaly_type,
        severity,
        metric_value,
        threshold_value,
        details,
        recipient,
    ) = event

    subject = f"[NeoInvest] Новая аномалия: {ticker} / {anomaly_type}"
    body = (
        "Обнаружена новая аномалия.\n\n"
        f"Event ID: {event_id}\n"
        f"Время: {detected_at}\n"
        f"Тикер: {ticker}\n"
        f"Тип: {anomaly_type}\n"
        f"Severity: {severity}\n"
        f"Metric value: {metric_value}\n"
        f"Threshold: {threshold_value}\n"
        f"Details: {details}\n\n"
        f"Получатель: {recipient}\n"
    )
    return subject, body


def send_anomaly_email_alerts() -> None:
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
                    u.email
                FROM mart.anomaly_events e
                JOIN auth.users u
                  ON u.is_active = TRUE
                LEFT JOIN mart.anomaly_email_notifications n
                  ON n.event_id = e.event_id
                 AND n.email = u.email
                WHERE n.notification_id IS NULL
                ORDER BY e.detected_at ASC
                LIMIT 500
                """
            )
            rows = cur.fetchall()

        for row in rows:
            event_id = row[0]
            recipient = row[8]
            subject, body = _build_email(row)

            _smtp_send(recipient=recipient, subject=subject, body=body)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO mart.anomaly_email_notifications (event_id, email)
                    VALUES (%s, %s)
                    ON CONFLICT (event_id, email) DO NOTHING
                    """,
                    (event_id, recipient),
                )
            conn.commit()
            sent += 1

        print(f"Anomaly email DAG: sent={sent}, pending_checked={len(rows)}")
    finally:
        conn.close()


with DAG(
    dag_id="anomaly_email_alert_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "anomaly", "email"],
) as anomaly_email_alert_dag:
    PythonOperator(
        task_id="send_anomaly_email_alerts",
        python_callable=send_anomaly_email_alerts,
    )
