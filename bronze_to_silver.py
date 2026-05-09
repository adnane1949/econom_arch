#!/usr/bin/env python3
"""
Transform raw bronze financial news events in MinIO into flattened silver JSONL.

Example:
  python bronze_to_silver.py
  python bronze_to_silver.py --include-invalid
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from io import BytesIO
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


def read_jsonl_object(client: Any, bucket: str, object_name: str) -> list[dict[str, Any]]:
    response = client.get_object(bucket, object_name)
    try:
        payload = response.read().decode("utf-8")
    finally:
        response.close()
        response.release_conn()

    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(payload.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            print(f"skipping_invalid_json object={object_name} line={line_number}", file=sys.stderr)
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def flatten_record(record: dict[str, Any], processed_at: str, bronze_object: str) -> dict[str, Any] | None:
    metadata = record.get("metadata") or {}
    article = record.get("article") or {}
    entities = record.get("entities") or {}
    analytics = record.get("analytics") or {}
    quality = record.get("quality") or {}
    governance = record.get("governance") or {}

    article_id = metadata.get("article_id")
    url = article.get("url")
    title = article.get("title")
    if not article_id or not url or not title:
        return None

    return {
        "article_id": article_id,
        "source": metadata.get("source"),
        "source_country": metadata.get("source_country"),
        "source_type": metadata.get("source_type"),
        "scraping_timestamp": metadata.get("scraping_timestamp"),
        "ingestion_mode": metadata.get("ingestion_mode"),
        "pipeline_version": metadata.get("pipeline_version"),
        "title": title,
        "subtitle": article.get("subtitle"),
        "author": article.get("author"),
        "publication_timestamp": article.get("publication_timestamp"),
        "category": article.get("category"),
        "tags": as_list(article.get("tags")),
        "language": article.get("language"),
        "url": url,
        "content": article.get("content"),
        "summary": article.get("summary"),
        "reading_time_minutes": article.get("reading_time_minutes"),
        "countries_mentioned": as_list(entities.get("countries_mentioned")),
        "companies_mentioned": as_list(entities.get("companies_mentioned")),
        "currencies_mentioned": as_list(entities.get("currencies_mentioned")),
        "people_mentioned": as_list(entities.get("people_mentioned")),
        "sentiment": analytics.get("sentiment"),
        "sentiment_score": analytics.get("sentiment_score"),
        "topic": analytics.get("topic"),
        "keyword_frequency": analytics.get("keyword_frequency") or {},
        "importance_score": analytics.get("importance_score"),
        "is_valid": quality.get("is_valid"),
        "missing_fields": as_list(quality.get("missing_fields")),
        "content_length": quality.get("content_length"),
        "quality_score": quality.get("quality_score"),
        "raw_storage_path": governance.get("raw_storage_path"),
        "lineage_id": governance.get("lineage_id"),
        "bronze_object": bronze_object,
        "silver_processed_at": processed_at,
    }


def output_object_name(prefix: str, processed_at: datetime) -> str:
    date_path = processed_at.strftime("%Y/%m/%d/%H")
    unique_id = uuid.uuid4().hex[:12]
    return f"{prefix.strip('/')}/financial-news/{date_path}/silver_{processed_at:%Y%m%dT%H%M%SZ}_{unique_id}.jsonl"


def write_jsonl_object(client: Any, bucket: str, object_name: str, records: list[dict[str, Any]]) -> None:
    payload = "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records).encode("utf-8")
    client.put_object(
        bucket,
        object_name,
        BytesIO(payload),
        length=len(payload),
        content_type="application/x-ndjson",
    )


def transform_bronze_to_silver(args: argparse.Namespace) -> int:
    client = create_minio_client(args.minio_endpoint, args.minio_access_key, args.minio_secret_key, args.minio_secure)
    if not client.bucket_exists(args.bucket):
        raise RuntimeError(f"bucket does not exist: {args.bucket}")

    processed_at_dt = utc_now()
    processed_at = isoformat_z(processed_at_dt)
    silver_records: list[dict[str, Any]] = []
    seen_article_ids: set[str] = set()

    objects = list(client.list_objects(args.bucket, prefix=args.bronze_prefix.strip("/") + "/", recursive=True))
    for obj in objects:
        if not obj.object_name.endswith(".jsonl"):
            continue
        for raw_record in read_jsonl_object(client, args.bucket, obj.object_name):
            if not args.include_invalid and not (raw_record.get("quality") or {}).get("is_valid"):
                continue
            silver_record = flatten_record(raw_record, processed_at, obj.object_name)
            if silver_record is None:
                continue
            article_id = str(silver_record["article_id"])
            if article_id in seen_article_ids:
                continue
            seen_article_ids.add(article_id)
            silver_records.append(silver_record)

    if not silver_records:
        print("silver_done records=0")
        return 0

    target = output_object_name(args.silver_prefix, processed_at_dt)
    write_jsonl_object(client, args.bucket, target, silver_records)
    print(f"silver_written records={len(silver_records)} object=s3://{args.bucket}/{target}")
    return len(silver_records)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flatten bronze financial news JSONL from MinIO into a silver JSONL dataset.")
    parser.add_argument("--minio-endpoint", default="localhost:9000", help="MinIO API endpoint.")
    parser.add_argument("--minio-access-key", default="minioadmin", help="MinIO access key.")
    parser.add_argument("--minio-secret-key", default="minioadmin", help="MinIO secret key.")
    parser.add_argument("--minio-secure", action="store_true", help="Use HTTPS for MinIO.")
    parser.add_argument("--bucket", default="econom-raw", help="MinIO bucket containing bronze and silver data.")
    parser.add_argument("--bronze-prefix", default="bronze/financial-news-raw", help="Bronze object prefix to read.")
    parser.add_argument("--silver-prefix", default="silver", help="Silver object prefix to write.")
    parser.add_argument("--include-invalid", action="store_true", help="Include records marked invalid by the scraper quality checks.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        transform_bronze_to_silver(args)
    except Exception as exc:
        print(f"silver_failed error={exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
