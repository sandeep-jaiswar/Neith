"""
Historical Backfill DAG.

One-shot DAG to ingest NSE data from BACKFILL_START_DATE to today.
Designed for initial population of the datalake.

Strategy:
  - Chunks date range into weekly batches to avoid memory pressure.
  - Respects NSE rate limits (PRODUCER_RATE_LIMIT_PER_SEC).
  - Skips weekends and NSE holidays automatically (financeindia returns
    empty results for non-trading days, which the producer handles gracefully).
  - Can be re-run safely: duplicate records are deduplicated by Flink.

Trigger manually:
  airflow dags trigger neith_historical_backfill
"""
from __future__ import annotations

from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

BACKFILL_START = "2020-01-01"

default_args = {
    "owner": "neith",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}


def generate_date_range(**context) -> list[str]:
    """Generate all trading weekdays from BACKFILL_START to today."""
    start = datetime.strptime(BACKFILL_START, "%Y-%m-%d").date()
    end = date.today() - timedelta(days=1)
    dates = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Mon–Fri only
            dates.append(current.isoformat())
        current += timedelta(days=1)

    context["ti"].xcom_push(key="backfill_dates", value=dates)
    return dates


def run_backfill_batch(**context) -> None:
    """Run the producer runner for a range of dates sequentially."""
    import subprocess
    import sys

    dates: list[str] = context["ti"].xcom_pull(key="backfill_dates")
    if not dates:
        print("No dates to backfill.")
        return

    print(f"[backfill] Total dates to process: {len(dates)}")
    failed = []
    for trade_date in dates:
        print(f"[backfill] Processing {trade_date} ...")
        result = subprocess.run(
            [
                sys.executable, "-m", "producers.runner",
                "--date", trade_date,
                "--metrics-port", "0",
            ],
            cwd="/opt/airflow/neith",
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"[backfill] FAILED {trade_date}: {result.stderr[:500]}")
            failed.append(trade_date)
        else:
            print(f"[backfill] OK {trade_date}")

    if failed:
        raise RuntimeError(f"Backfill failed for {len(failed)} dates: {failed[:10]}")


with DAG(
    dag_id="neith_historical_backfill",
    description="One-shot historical backfill from 2020 to today",
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["neith", "backfill", "historical"],
    max_active_runs=1,
) as dag:

    gen_dates = PythonOperator(
        task_id="generate_date_range",
        python_callable=generate_date_range,
    )

    run_backfill = PythonOperator(
        task_id="run_backfill",
        python_callable=run_backfill_batch,
        execution_timeout=timedelta(hours=48),
    )

    spark_silver = BashOperator(
        task_id="spark_ohlcv_adjuster_full",
        bash_command=(
            "spark-submit"
            " --master spark://spark-master:7077"
            " --conf spark.executor.memory=3g"
            " --conf spark.executor.cores=4"
            " /opt/spark-apps/ohlcv_adjuster.py"
            f" --start-date {BACKFILL_START}"
        ),
        execution_timeout=timedelta(hours=4),
    )

    compact = BashOperator(
        task_id="compact_iceberg_tables",
        bash_command=(
            "spark-submit"
            " --master spark://spark-master:7077"
            " --conf spark.executor.memory=3g"
            " /opt/spark-apps/compaction.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    gen_dates >> run_backfill >> spark_silver >> compact
