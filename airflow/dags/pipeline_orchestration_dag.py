from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="pipeline_orchestration_dag",
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["orchestration", "pipeline"],
) as dag:
    run_minute_sync = TriggerDagRunOperator(
        task_id="run_minute_sync",
        trigger_dag_id="moex_minute_incremental_sync",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=True,
    )

    run_anomaly_detection = TriggerDagRunOperator(
        task_id="run_anomaly_detection",
        trigger_dag_id="anomaly_on_new_data_dag",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=True,
    )

    run_anomaly_email_alerts = TriggerDagRunOperator(
        task_id="run_anomaly_email_alerts",
        trigger_dag_id="anomaly_email_alert_dag",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=True,
    )

    run_anomaly_telegram_alerts = TriggerDagRunOperator(
        task_id="run_anomaly_telegram_alerts",
        trigger_dag_id="anomaly_telegram_alert_dag",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=True,
    )

    run_minute_sync >> run_anomaly_detection >> run_anomaly_email_alerts >> run_anomaly_telegram_alerts
