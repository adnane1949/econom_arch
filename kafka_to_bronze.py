#!/usr/bin/env python3
"""
Move raw financial news events from Kafka into the MinIO bronze bucket.

Example:
  python kafka_to_bronze.py --once
  python kafka_to_bronze.py
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def object_name(prefix: str, topic: str, created_at: datetime) -> str:
    date_path = created_at.strftime("%Y/%m/%d/%H")
    unique_id = uuid.uuid4().hex[:12]
    return f"{prefix.strip('/')}/{topic}/{date_path}/events_{created_at:%Y%m%dT%H%M%SZ}_{unique_id}.jsonl"


def create_minio_client(endpoint: str, access_key: str, secret_key: str, secure: bool) -> Any:
    try:
        from minio import Minio
    except ImportError as exc:
        raise RuntimeError("MinIO support requires minio. Install it with: pip install -r requirements.txt") from exc

    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def ensure_bucket(client: Any, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def create_kafka_consumer(args: argparse.Namespace) -> Any:
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError("Kafka support requires kafka-python. Install it with: pip install -r requirements.txt") from exc

    return KafkaConsumer(
        args.kafka_topic,
        bootstrap_servers=[server.strip() for server in args.kafka_bootstrap_servers.split(",") if server.strip()],
        group_id=args.kafka_group_id,
        auto_offset_reset=args.auto_offset_reset,
        enable_auto_commit=False,
        max_poll_records=args.batch_size,
    )


def decode_message(value: bytes) -> dict[str, Any] | None:
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def upload_batch(client: Any, bucket_name: str, prefix: str, topic: str, records: list[dict[str, Any]]) -> str:
    created_at = utc_now()
    payload = "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records).encode("utf-8")
    target = object_name(prefix, topic, created_at)
    client.put_object(
        bucket_name,
        target,
        BytesIO(payload),
        length=len(payload),
        content_type="application/x-ndjson",
    )
    return target


def consume_to_bronze(args: argparse.Namespace) -> int:
    minio_client = create_minio_client(args.minio_endpoint, args.minio_access_key, args.minio_secret_key, args.minio_secure)
    ensure_bucket(minio_client, args.bucket)

    consumer = create_kafka_consumer(args)
    batch: list[dict[str, Any]] = []
    uploaded = 0
    last_flush = time.monotonic()
    last_message = time.monotonic()

    try:
        while True:
            polled = consumer.poll(timeout_ms=1000, max_records=args.batch_size)
            now = time.monotonic()

            if not polled and args.once and (now - last_message) * 1000 >= args.idle_timeout_ms:
                break

            for messages in polled.values():
                for message in messages:
                    record = decode_message(message.value)
                    if record is None:
                        print(f"skipping_invalid_json topic={message.topic} partition={message.partition} offset={message.offset}", file=sys.stderr)
                        consumer.commit()
                        continue

                    batch.append(record)
                    last_message = now

            if batch and (len(batch) >= args.batch_size or now - last_flush >= args.flush_seconds):
                target = upload_batch(minio_client, args.bucket, args.prefix, args.kafka_topic, batch)
                consumer.commit()
                print(f"bronze_written records={len(batch)} object=s3://{args.bucket}/{target}")
                uploaded += len(batch)
                batch = []
                last_flush = now

        if batch:
            target = upload_batch(minio_client, args.bucket, args.prefix, args.kafka_topic, batch)
            consumer.commit()
            print(f"bronze_written records={len(batch)} object=s3://{args.bucket}/{target}")
            uploaded += len(batch)
    finally:
        consumer.close()

    return uploaded


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write raw Kafka financial news events into a MinIO bronze bucket.")
    parser.add_argument("--kafka-bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers.")
    parser.add_argument("--kafka-topic", default="financial-news-raw", help="Kafka topic to consume.")
    parser.add_argument("--kafka-group-id", default="bronze-writer", help="Kafka consumer group id.")
    parser.add_argument("--auto-offset-reset", choices=["earliest", "latest"], default="earliest", help="Where to start if this group has no committed offset.")
    parser.add_argument("--minio-endpoint", default="localhost:9000", help="MinIO API endpoint.")
    parser.add_argument("--minio-access-key", default="minioadmin", help="MinIO access key.")
    parser.add_argument("--minio-secret-key", default="minioadmin", help="MinIO secret key.")
    parser.add_argument("--minio-secure", action="store_true", help="Use HTTPS for MinIO.")
    parser.add_argument("--bucket", default="econom-raw", help="Bronze bucket name.")
    parser.add_argument("--prefix", default="bronze", help="Object prefix inside the bucket.")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of events per bronze object.")
    parser.add_argument("--flush-seconds", type=float, default=10.0, help="Maximum seconds before flushing a partial batch.")
    parser.add_argument("--idle-timeout-ms", type=int, default=10000, help="Stop --once after this many milliseconds without messages.")
    parser.add_argument("--once", action="store_true", help="Consume available messages, write them, then stop.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        count = consume_to_bronze(args)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"bronze_failed error={exc}", file=sys.stderr)
        return 1

    if args.once:
        print(f"bronze_done records={count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
