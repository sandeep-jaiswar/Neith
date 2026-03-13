"""
Daily Market Close Ingestion DAG.

Schedule: 16:30 IST (11:00 UTC) daily, Monday–Friday.
Purpose: Triggered after NSE market close (15:30 IST + 1hr buffer for file publication).

Steps:
  1. Run all producers in parallel (subprocess via runner.py)
  2. Wait 5 min for Flink to process and sink to Iceberg
  3. Submit Spark OHLCV adjuster (silver layer)
  4. Submit Spark market breadth (gold layer)
"""
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "neith",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="neith_daily_ingestion",
    description="Post-market NSE data ingestion: Bhavcopy, FO, Indices, Corporate, Surveillance",
    schedule_interval="0 11 * * 1-5",  # 11:00 UTC = 16:30 IST Mon-Fri
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["neith", "ingestion", "daily"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Step 1: Run all producers in parallel ─────────────────────────────
    run_producers = BashOperator(
        task_id="run_producers",
        bash_command=(
            "cd /opt/airflow/neith && "
            "python -m producers.runner --date {{ ds }} --metrics-port 0"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # ── Step 2: Wait for Flink watermark to settle ─────────────────────────
    wait_flink = BashOperator(
        task_id="wait_flink_settle",
        bash_command="sleep 300",  # 5 min buffer
    )

    # ── Step 3: Spark — Silver OHLCV Adjustment ───────────────────────────
    spark_silver = BashOperator(
        task_id="spark_ohlcv_adjuster",
        bash_command=(
            "spark-submit"
            " --master spark://spark-master:7077"
            " --conf spark.executor.memory=2g"
            " --conf spark.executor.cores=2"
            " /opt/spark-apps/ohlcv_adjuster.py"
            " --start-date {{ ds }}"
        ),
        execution_timeout=timedelta(minutes=60),
    )

    # ── Step 4: Spark — Gold Market Breadth ──────────────────────────────
    spark_gold = BashOperator(
        task_id="spark_market_breadth",
        bash_command=(
            "spark-submit"
            " --master spark://spark-master:7077"
            " --conf spark.executor.memory=2g"
            " /opt/spark-apps/market_breadth.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    done = EmptyOperator(task_id="done")

    start >> run_producers >> wait_flink >> [spark_silver] >> spark_gold >> done
