from __future__ import annotations

# NOTE: Airflow is Linux-only and runs inside Docker — these imports
# will show IDE errors on Windows but work correctly at runtime.
from datetime import datetime, timedelta

from airflow import DAG  # type: ignore[import-untyped]
from airflow.operators.bash import BashOperator  # type: ignore[import-untyped]


PROJECT_DIR = "/opt/airflow/project"
PYTHON = "python"
MINIO_ENDPOINT = "minio:9000"
KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC = "financial-news-raw"


default_args = {
    "owner": "econom_arch",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="econom_financial_news_pipeline",
    description="Full pipeline: scrape → Kafka → bronze → silver → Spark gold → warehouse → dashboard.",
    default_args=default_args,
    start_date=datetime(2026, 5, 8),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["financial-news", "minio", "spark", "kafka"],
) as dag:

    # ── Stage 1: scrape financial news and publish events to Kafka ──────────
    scrape_to_kafka = BashOperator(
        task_id="scrape_to_kafka",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} scrape_financial_news.py "
            f"--mode stream "
            f"--stream-once "
            f"--kafka-bootstrap-servers {KAFKA_BOOTSTRAP} "
            f"--kafka-topic {KAFKA_TOPIC} "
            f"--max-per-source 50 "
            f"--delay-seconds 0.5"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    # ── Stage 2: move Kafka events into MinIO bronze bucket ─────────────────
    kafka_to_bronze = BashOperator(
        task_id="kafka_to_bronze",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} kafka_to_bronze.py "
            f"--kafka-bootstrap-servers {KAFKA_BOOTSTRAP} "
            f"--kafka-topic {KAFKA_TOPIC} "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--once "
            f"--idle-timeout-ms 15000"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    # ── Stage 3: flatten bronze JSONL into clean silver records ─────────────
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} bronze_to_silver.py "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--include-invalid"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── Stage 4: aggregate silver records into gold JSON via Spark ───────────
    silver_to_gold_spark = BashOperator(
        task_id="silver_to_gold_spark",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} silver_to_gold.py "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--spark-master spark://spark-master:7077"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # ── Stage 5: load gold analytics into PostgreSQL + export dashboard ──────
    load_gold_warehouse = BashOperator(
        task_id="load_gold_warehouse",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} warehouse_loader.py "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--pg-host postgres "
            f"--dashboard-output dashboard/data.json"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    # ── DAG topology ─────────────────────────────────────────────────────────
    scrape_to_kafka >> kafka_to_bronze >> bronze_to_silver >> silver_to_gold_spark >> load_gold_warehouse
