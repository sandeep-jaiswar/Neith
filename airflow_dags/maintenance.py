"""
Maintenance DAG — Nightly Iceberg Compaction & Snapshot Expiry.

Schedule: 23:30 IST (18:00 UTC) daily.
"""
from __future__ import annotations

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "neith",
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
}

with DAG(
    dag_id="neith_maintenance",
    description="Nightly Iceberg compaction, snapshot expiry, orphan file removal",
    schedule_interval="0 18 * * *",  # 18:00 UTC = 23:30 IST
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["neith", "maintenance"],
    max_active_runs=1,
) as dag:

    compact = BashOperator(
        task_id="iceberg_compaction",
        bash_command=(
            "spark-submit"
            " --master spark://spark-master:7077"
            " --conf spark.executor.memory=2g"
            " /opt/spark-apps/compaction.py"
        ),
        execution_timeout=timedelta(hours=3),
    )

    compact
