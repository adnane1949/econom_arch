#!/usr/bin/env python3
"""
Build gold analytics from silver financial news records using PySpark.

The script keeps MinIO access simple: it downloads silver JSONL objects to a
temporary local file, uses Spark for the aggregations, then writes gold JSON
objects back to MinIO.

Example:
  python silver_to_gold.py
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_z(value: datetime | None = None) -> str:
    value = value or utc_now()
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def create_minio_client(endpoint: str, access_key: str, secret_key: str, secure: bool) -> Any:
    try:
        from minio import Minio
    except ImportError as exc:
        raise RuntimeError("MinIO support requires minio. Install it with: pip install -r requirements.txt") from exc

    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def download_silver_jsonl(client: Any, bucket: str, prefix: str, output_path: Path) -> int:
    seen_article_ids: set[str] = set()
    written = 0
    objects = list(client.list_objects(bucket, prefix=prefix.strip("/") + "/", recursive=True))

    with output_path.open("w", encoding="utf-8") as output:
        for obj in objects:
            if not obj.object_name.endswith(".jsonl"):
                continue
            response = client.get_object(bucket, obj.object_name)
            try:
                for raw_line in response.stream(32 * 1024):
                    for line in raw_line.decode("utf-8").splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        article_id = str(record.get("article_id") or "")
                        if not article_id or article_id in seen_article_ids:
                            continue
                        seen_article_ids.add(article_id)
                        output.write(json.dumps(record, ensure_ascii=False) + "\n")
                        written += 1
            finally:
                response.close()
                response.release_conn()

    return written


def create_spark_session(master: str = "local[*]") -> Any:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError("Spark gold processing requires pyspark. Install it with: pip install -r requirements.txt") from exc

    return (
        SparkSession.builder.appName("econom-arch-silver-to-gold")
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def collect_count_rows(df: Any, value_column: str = "value", count_column: str = "count") -> list[dict[str, Any]]:
    return [
        {value_column: row[value_column], count_column: row[count_column]}
        for row in df.collect()
    ]


def build_gold_with_spark(spark: Any, input_path: Path, generated_at: str, top_limit: int) -> dict[str, Any]:
    from pyspark.sql import functions as F

    df = spark.read.json(str(input_path))
    df = df.dropDuplicates(["article_id"])
    record_count = df.count()

    if record_count == 0:
        return {
            "generated_at": generated_at,
            "engine": "spark",
            "record_count": 0,
            "average_quality_score": 0.0,
            "articles_by_source": [],
            "articles_by_topic": [],
            "articles_by_sentiment": [],
            "articles_by_language": [],
            "articles_by_source_country": [],
            "average_sentiment_by_source": [],
            "top_tags": [],
            "top_companies": [],
            "top_currencies": [],
            "top_countries_mentioned": [],
            "latest_articles": [],
        }

    def group_count(column: str) -> Any:
        return (
            df.select(F.coalesce(F.col(column).cast("string"), F.lit("unknown")).alias("value"))
            .groupBy("value")
            .count()
            .orderBy(F.desc("count"), F.asc("value"))
            .limit(top_limit)
        )

    def explode_count(column: str) -> Any:
        return (
            df.select(F.explode_outer(F.col(column)).alias("value"))
            .where(F.col("value").isNotNull() & (F.length(F.trim(F.col("value").cast("string"))) > 0))
            .groupBy("value")
            .count()
            .orderBy(F.desc("count"), F.asc("value"))
            .limit(top_limit)
        )

    average_quality = df.select(F.avg(F.col("quality_score").cast("double")).alias("value")).first()["value"]
    source_sentiment = (
        df.groupBy("source")
        .agg(
            F.round(F.avg(F.col("sentiment_score").cast("double")), 4).alias("average_sentiment_score"),
            F.count("*").alias("article_count"),
        )
        .orderBy("source")
    )
    latest_articles = (
        df.select(
            "article_id",
            "source",
            "title",
            "url",
            "publication_timestamp",
            "sentiment",
            "topic",
            "quality_score",
        )
        .orderBy(F.desc(F.coalesce(F.col("publication_timestamp"), F.col("scraping_timestamp"))))
        .limit(top_limit)
    )

    return {
        "generated_at": generated_at,
        "engine": "spark",
        "record_count": record_count,
        "average_quality_score": round(float(average_quality or 0.0), 4),
        "articles_by_source": collect_count_rows(group_count("source")),
        "articles_by_topic": collect_count_rows(group_count("topic")),
        "articles_by_sentiment": collect_count_rows(group_count("sentiment")),
        "articles_by_language": collect_count_rows(group_count("language")),
        "articles_by_source_country": collect_count_rows(group_count("source_country")),
        "average_sentiment_by_source": [row.asDict() for row in source_sentiment.collect()],
        "top_tags": collect_count_rows(explode_count("tags")),
        "top_companies": collect_count_rows(explode_count("companies_mentioned")),
        "top_currencies": collect_count_rows(explode_count("currencies_mentioned")),
        "top_countries_mentioned": collect_count_rows(explode_count("countries_mentioned")),
        "latest_articles": [row.asDict() for row in latest_articles.collect()],
    }


def write_json_object(client: Any, bucket: str, object_name: str, value: dict[str, Any]) -> None:
    payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    client.put_object(bucket, object_name, BytesIO(payload), length=len(payload), content_type="application/json")


def build_gold_layer(args: argparse.Namespace) -> int:
    client = create_minio_client(args.minio_endpoint, args.minio_access_key, args.minio_secret_key, args.minio_secure)
    if not client.bucket_exists(args.bucket):
        raise RuntimeError(f"bucket does not exist: {args.bucket}")

    generated_at_dt = utc_now()
    generated_at = isoformat_z(generated_at_dt)

    # Use a fixed path in the shared data directory if spark-master is remote
    if "spark-master" in args.spark_master:
        # We are in Docker, use the shared volume path
        work_dir = Path("/opt/airflow/project/data/transfer")
        work_dir.mkdir(parents=True, exist_ok=True)
        silver_path = work_dir / "silver.jsonl"
        spark_read_path = args.spark_read_path or "/opt/spark-apps/data/transfer/silver.jsonl"
    else:
        # Local mode or host mode, use temp dir
        tmp_dir_obj = tempfile.TemporaryDirectory()
        silver_path = Path(tmp_dir_obj.name) / "silver.jsonl"
        spark_read_path = str(silver_path)

    try:
        record_count = download_silver_jsonl(client, args.bucket, args.silver_prefix, silver_path)
        spark = create_spark_session(args.spark_master)
        try:
            gold = build_gold_with_spark(spark, spark_read_path, generated_at, args.top_limit)
        finally:
            spark.stop()
    finally:
        # Cleanup if we used a temp dir
        if "spark-master" not in args.spark_master:
            tmp_dir_obj.cleanup()

    date_path = generated_at_dt.strftime("%Y/%m/%d/%H")
    snapshot_object = f"{args.gold_prefix.strip('/')}/financial-news/{date_path}/gold_{generated_at_dt:%Y%m%dT%H%M%SZ}.json"
    latest_object = f"{args.gold_prefix.strip('/')}/financial-news/latest.json"
    write_json_object(client, args.bucket, snapshot_object, gold)
    write_json_object(client, args.bucket, latest_object, gold)

    print(f"gold_written engine=spark records={record_count} object=s3://{args.bucket}/{snapshot_object}")
    print(f"gold_latest object=s3://{args.bucket}/{latest_object}")
    return record_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Spark gold aggregate analytics from silver financial news records.")
    parser.add_argument("--minio-endpoint", default="localhost:9000", help="MinIO API endpoint.")
    parser.add_argument("--minio-access-key", default="minioadmin", help="MinIO access key.")
    parser.add_argument("--minio-secret-key", default="minioadmin", help="MinIO secret key.")
    parser.add_argument("--minio-secure", action="store_true", help="Use HTTPS for MinIO.")
    parser.add_argument("--bucket", default="econom-raw", help="MinIO bucket containing silver and gold data.")
    parser.add_argument("--silver-prefix", default="silver/financial-news", help="Silver object prefix to read.")
    parser.add_argument("--gold-prefix", default="gold", help="Gold object prefix to write.")
    parser.add_argument("--top-limit", type=int, default=10, help="Number of top values to keep in each metric.")
    parser.add_argument("--spark-master", default="local[*]", help="Spark master URL.")
    parser.add_argument("--spark-read-path", help="Internal Spark path to the input data (if different from silver-path).")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        build_gold_layer(args)
    except Exception as exc:
        print(f"gold_failed error={exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
