"""
Intraday Quote Ingestion DAG.

Schedule: Every 5 minutes, 09:15–15:30 IST (03:45–10:00 UTC), Mon–Fri.
Purpose: Poll live NSE quotes for equity prices during market hours.

Produces to nse.equities.raw with source='intraday_quote'.
Flink's deduplication state prevents duplicates if a symbol appears
in both intraday and daily Bhavcopy.
"""
from __future__ import annotations

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "neith",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="neith_intraday_quotes",
    description="5-minute intraday equity quote ingestion during market hours",
    schedule_interval="*/5 3-10 * * 1-5",  # Every 5 min, 03:45-10:00 UTC
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["neith", "ingestion", "intraday"],
    max_active_runs=1,
    concurrency=1,
) as dag:

    run_intraday = BashOperator(
        task_id="intraday_equity_quotes",
        bash_command=(
            "cd /opt/airflow/neith && "
            "python -m producers.runner --date {{ ds }} --domains equity index --metrics-port 0"
        ),
        execution_timeout=timedelta(minutes=4),  # Must finish before next 5-min run
    )

    run_intraday
