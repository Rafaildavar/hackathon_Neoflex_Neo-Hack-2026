from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="bootstrap_healthcheck",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bootstrap", "infra"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    start >> finish

