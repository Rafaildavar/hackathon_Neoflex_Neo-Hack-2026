from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.loader import load_history, load_realtime_once

default_args = {
    "owner": "neo_invest",
    "depends_on_past": False,
    "retries": 2,
}

with DAG(
    dag_id="moex_pipeline",
    default_args=default_args,
    description="MOEX ingestion pipeline into Postgres/Timescale",
    start_date=datetime(2026, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["moex", "neo_invest"],
) as dag:
    history_task = PythonOperator(
        task_id="load_history_30d",
        python_callable=load_history,
        op_kwargs={"days_back": 30},
    )

    realtime_task = PythonOperator(
        task_id="load_realtime_snapshot",
        python_callable=load_realtime_once,
    )

    history_task >> realtime_task
