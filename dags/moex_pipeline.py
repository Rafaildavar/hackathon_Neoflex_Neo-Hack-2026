from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "neo_invest", "depends_on_past": False, "retries": 2}


def _pg_connect():
    """Подключение к Postgres: в Docker-сети хост `postgres`, порт 5432 внутри контейнера."""
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = (
        5432
        if host == "postgres"
        else int(os.getenv("POSTGRES_PORT", "5432"))
    )
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=os.getenv("POSTGRES_DB", "neo_invest"),
        user=os.getenv("POSTGRES_USER", "neo"),
        password=os.getenv("POSTGRES_PASSWORD", "neo_pass"),
    )


def load_history_to_stg() -> None:
    tickers = ["SBER", "GAZP", "LKOH", "YDEX", "VTBR", "ROSN", "NVTK", "TATN", "GMKN", "NLMK"]
    date_till = datetime.utcnow().date()
    date_from = date_till - timedelta(days=30)

    conn = _pg_connect()
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS stg")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg.raw_moex_data (
            id BIGSERIAL PRIMARY KEY,
            load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            source_endpoint TEXT NOT NULL,
            ticker TEXT NOT NULL,
            event_ts TIMESTAMPTZ,
            payload_json JSONB NOT NULL
        )
        """
    )

    for ticker in tickers:
        url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/{ticker}.json"
        payload = requests.get(
            url,
            params={"from": date_from.isoformat(), "till": date_till.isoformat(), "iss.meta": "off"},
            timeout=20,
        ).json()
        cur.execute(
            """
            INSERT INTO stg.raw_moex_data (source_endpoint, ticker, payload_json)
            VALUES (%s, %s, %s::jsonb)
            """,
            ("history", ticker, json.dumps(payload, ensure_ascii=False)),
        )

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id="moex_manual_load",
    default_args=default_args,
    description="Manual MOEX history load (30 days) into stg.raw_moex_data",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["moex", "manual", "neo_invest"],
) as dag:
    PythonOperator(
        task_id="load_history_30_days",
        python_callable=load_history_to_stg,
    )
