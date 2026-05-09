from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"
PYTHON = "python"
MINIO_ENDPOINT = "minio:9000"


default_args = {
    "owner": "econom_arch",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="econom_financial_news_pipeline",
    description="Simple bronze to silver to Spark gold pipeline for financial news.",
    default_args=default_args,
    start_date=datetime(2026, 5, 8),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["financial-news", "minio", "spark"],
) as dag:
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} bronze_to_silver.py "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--include-invalid"
        ),
    )

    silver_to_gold_spark = BashOperator(
        task_id="silver_to_gold_spark",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} silver_to_gold.py "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--spark-master spark://spark-master:7077"
        ),
    )

    load_gold_warehouse = BashOperator(
        task_id="load_gold_warehouse",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} warehouse_loader.py "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--pg-host postgres "
            f"--dashboard-output dashboard/data.json"
        ),
    )

    bronze_to_silver >> silver_to_gold_spark >> load_gold_warehouse
