#!/usr/bin/env python3
"""
Load gold financial-news analytics into PostgreSQL warehouse tables.

The loader accepts either a local gold JSON file or a MinIO object. It also
exports a compact dashboard/data.json file so the UI can run without extra
backend dependencies.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any


COUNT_METRICS = {
    "articles_by_source": "source",
    "articles_by_topic": "topic",
    "articles_by_sentiment": "sentiment",
    "articles_by_language": "language",
    "articles_by_source_country": "source_country",
    "top_tags": "tag",
    "top_companies": "company",
    "top_currencies": "currency",
    "top_countries_mentioned": "country_mentioned",
}

ALLOWED_SENTIMENTS = {"positive", "neutral", "negative"}
ALLOWED_LANGUAGES = {"en", "fr", "ar", "es", "de", "it", "pt", "unknown"}


def parse_iso_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def create_minio_client(endpoint: str, access_key: str, secret_key: str, secure: bool) -> Any:
    try:
        from minio import Minio
    except ImportError as exc:
        raise RuntimeError("MinIO support requires minio. Install it with: pip install -r requirements.txt") from exc

    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def read_gold_from_minio(args: argparse.Namespace) -> dict[str, Any]:
    client = create_minio_client(args.minio_endpoint, args.minio_access_key, args.minio_secret_key, args.minio_secure)
    response = client.get_object(args.bucket, args.gold_object)
    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def read_gold_payload(args: argparse.Namespace) -> dict[str, Any]:
    if args.gold_file:
        return json.loads(Path(args.gold_file).read_text(encoding="utf-8"))
    return read_gold_from_minio(args)


def count_total(rows: Any) -> int:
    if not isinstance(rows, list):
        return 0
    return sum(int(row.get("count") or 0) for row in rows if isinstance(row, dict))


def add_check(
    checks: list[dict[str, Any]],
    check_name: str,
    dimension: str,
    passed: bool,
    observed: Any,
    expected: Any,
    details: str,
    severity: str = "error",
) -> None:
    checks.append(
        {
            "check_name": check_name,
            "dimension": dimension,
            "status": "passed" if passed else "failed",
            "severity": severity,
            "observed_value": None if observed is None else str(observed),
            "expected_value": None if expected is None else str(expected),
            "details": details,
        }
    )


def build_quality_checks(gold: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    record_count = gold.get("record_count")

    add_check(
        checks,
        "required_header_fields",
        "completeness",
        bool(gold.get("generated_at")) and isinstance(record_count, int),
        f"generated_at={gold.get('generated_at')} record_count={record_count}",
        "generated_at present and record_count integer",
        "Gold snapshot header must identify the run and row count.",
    )
    add_check(
        checks,
        "non_empty_snapshot",
        "completeness",
        isinstance(record_count, int) and record_count > 0,
        record_count,
        "> 0",
        "Gold snapshot should contain at least one article.",
    )

    for section in COUNT_METRICS:
        rows = gold.get(section)
        add_check(
            checks,
            "section_present",
            section,
            isinstance(rows, list),
            type(rows).__name__,
            "list",
            f"{section} must be present as a list.",
        )
        if section.startswith("articles_by_") and isinstance(record_count, int):
            total = count_total(rows)
            add_check(
                checks,
                "count_total_matches_record_count",
                section,
                total == record_count,
                total,
                record_count,
                f"{section} counts should add up to record_count.",
            )

    source_counts = {
        row.get("value"): int(row.get("count") or 0)
        for row in gold.get("articles_by_source", [])
        if isinstance(row, dict)
    }
    for row in gold.get("average_sentiment_by_source", []):
        source = row.get("source")
        article_count = int(row.get("article_count") or 0)
        add_check(
            checks,
            "source_sentiment_count_matches_source_count",
            "average_sentiment_by_source",
            source_counts.get(source) == article_count,
            f"{source}={article_count}",
            source_counts.get(source),
            "Per-source sentiment article_count should match articles_by_source.",
        )
        score = row.get("average_sentiment_score")
        add_check(
            checks,
            "average_sentiment_score_range",
            "average_sentiment_by_source",
            isinstance(score, (int, float)) and -1 <= float(score) <= 1,
            score,
            "between -1 and 1",
            "Average sentiment score must stay in the normalized sentiment range.",
        )

    avg_quality = gold.get("average_quality_score")
    add_check(
        checks,
        "average_quality_score_range",
        "validity",
        isinstance(avg_quality, (int, float)) and 0 <= float(avg_quality) <= 1,
        avg_quality,
        "between 0 and 1",
        "Average quality score must be normalized.",
    )

    latest = gold.get("latest_articles", [])
    seen_ids: set[str] = set()
    for index, article in enumerate(latest, start=1):
        article_id = article.get("article_id")
        required_values = [article_id, article.get("source"), article.get("title"), article.get("url")]
        add_check(
            checks,
            "latest_article_required_fields",
            "latest_articles",
            all(required_values),
            f"row={index}",
            "article_id, source, title, url present",
            "Latest article rows must keep drill-down identifiers.",
        )
        add_check(
            checks,
            "latest_article_unique_id",
            "latest_articles",
            bool(article_id) and article_id not in seen_ids,
            article_id,
            "unique non-empty article_id",
            "Latest article identifiers should not repeat.",
        )
        if article_id:
            seen_ids.add(article_id)

        url = article.get("url")
        add_check(
            checks,
            "latest_article_url_valid",
            "latest_articles",
            isinstance(url, str) and url.startswith(("http://", "https://")),
            url,
            "http(s) URL",
            "Article URL must be usable for navigation.",
        )
        sentiment = article.get("sentiment")
        add_check(
            checks,
            "latest_article_sentiment_valid",
            "latest_articles",
            sentiment in ALLOWED_SENTIMENTS,
            sentiment,
            sorted(ALLOWED_SENTIMENTS),
            "Sentiment must be in the accepted categories.",
        )
        quality_score = article.get("quality_score")
        add_check(
            checks,
            "latest_article_quality_score_range",
            "latest_articles",
            isinstance(quality_score, (int, float)) and 0 <= float(quality_score) <= 1,
            quality_score,
            "between 0 and 1",
            "Article quality score must be normalized.",
        )
        timestamp = article.get("publication_timestamp")
        add_check(
            checks,
            "latest_article_timestamp_parseable",
            "latest_articles",
            timestamp is None or parse_iso_timestamp(timestamp) is not None,
            timestamp,
            "ISO-8601 timestamp or null",
            "Publication timestamp must be parseable when present.",
            severity="warning",
        )

    language_values = [row.get("value") for row in gold.get("articles_by_language", []) if isinstance(row, dict)]
    invalid_languages = [value for value in language_values if value not in ALLOWED_LANGUAGES]
    add_check(
        checks,
        "language_codes_valid",
        "articles_by_language",
        not invalid_languages,
        invalid_languages,
        sorted(ALLOWED_LANGUAGES),
        "Language breakdown should use expected ISO-like codes.",
        severity="warning",
    )

    return checks


def connect_postgres(args: argparse.Namespace) -> Any:
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError("PostgreSQL loading requires psycopg2-binary. Install it with: pip install -r requirements.txt") from exc

    return psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
    )


def ensure_schema(cur: Any) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dw_gold_runs (
            run_id BIGSERIAL PRIMARY KEY,
            generated_at TIMESTAMPTZ NOT NULL UNIQUE,
            engine TEXT NOT NULL,
            record_count INTEGER NOT NULL CHECK (record_count >= 0),
            average_quality_score NUMERIC(8,4) NOT NULL CHECK (average_quality_score BETWEEN 0 AND 1),
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS dw_metric_counts (
            run_id BIGINT NOT NULL REFERENCES dw_gold_runs(run_id) ON DELETE CASCADE,
            metric_name TEXT NOT NULL,
            value TEXT NOT NULL,
            article_count INTEGER NOT NULL CHECK (article_count >= 0),
            PRIMARY KEY (run_id, metric_name, value)
        );

        CREATE TABLE IF NOT EXISTS dw_source_sentiment (
            run_id BIGINT NOT NULL REFERENCES dw_gold_runs(run_id) ON DELETE CASCADE,
            source TEXT NOT NULL,
            average_sentiment_score NUMERIC(8,4) NOT NULL CHECK (average_sentiment_score BETWEEN -1 AND 1),
            article_count INTEGER NOT NULL CHECK (article_count >= 0),
            PRIMARY KEY (run_id, source)
        );

        CREATE TABLE IF NOT EXISTS dw_latest_articles (
            run_id BIGINT NOT NULL REFERENCES dw_gold_runs(run_id) ON DELETE CASCADE,
            article_id TEXT NOT NULL,
            source TEXT,
            title TEXT,
            url TEXT,
            publication_timestamp TIMESTAMPTZ,
            sentiment TEXT,
            topic TEXT,
            quality_score NUMERIC(8,4) CHECK (quality_score BETWEEN 0 AND 1),
            PRIMARY KEY (run_id, article_id)
        );

        CREATE TABLE IF NOT EXISTS dw_data_quality_checks (
            run_id BIGINT NOT NULL REFERENCES dw_gold_runs(run_id) ON DELETE CASCADE,
            check_name TEXT NOT NULL,
            dimension TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('passed', 'failed')),
            severity TEXT NOT NULL CHECK (severity IN ('warning', 'error')),
            observed_value TEXT,
            expected_value TEXT,
            details TEXT,
            checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE OR REPLACE VIEW vw_latest_gold_run AS
        SELECT *
        FROM dw_gold_runs
        ORDER BY generated_at DESC
        LIMIT 1;

        CREATE OR REPLACE VIEW mart_source_performance AS
        SELECT
            r.generated_at,
            c.value AS source,
            c.article_count,
            s.average_sentiment_score,
            ROUND(c.article_count::NUMERIC / NULLIF(r.record_count, 0), 4) AS share_of_articles
        FROM dw_metric_counts c
        JOIN dw_gold_runs r ON r.run_id = c.run_id
        LEFT JOIN dw_source_sentiment s ON s.run_id = c.run_id AND s.source = c.value
        WHERE c.metric_name = 'source';

        CREATE OR REPLACE VIEW mart_topic_distribution AS
        SELECT
            r.generated_at,
            c.value AS topic,
            c.article_count,
            ROUND(c.article_count::NUMERIC / NULLIF(r.record_count, 0), 4) AS share_of_articles
        FROM dw_metric_counts c
        JOIN dw_gold_runs r ON r.run_id = c.run_id
        WHERE c.metric_name = 'topic';

        CREATE OR REPLACE VIEW mart_sentiment_mix AS
        SELECT
            r.generated_at,
            c.value AS sentiment,
            c.article_count,
            ROUND(c.article_count::NUMERIC / NULLIF(r.record_count, 0), 4) AS share_of_articles
        FROM dw_metric_counts c
        JOIN dw_gold_runs r ON r.run_id = c.run_id
        WHERE c.metric_name = 'sentiment';

        CREATE OR REPLACE VIEW mart_entity_leaderboard AS
        SELECT r.generated_at, c.metric_name, c.value, c.article_count
        FROM dw_metric_counts c
        JOIN dw_gold_runs r ON r.run_id = c.run_id
        WHERE c.metric_name IN ('tag', 'company', 'currency', 'country_mentioned');

        CREATE OR REPLACE VIEW mart_latest_articles AS
        SELECT r.generated_at, a.*
        FROM dw_latest_articles a
        JOIN dw_gold_runs r ON r.run_id = a.run_id;

        CREATE OR REPLACE VIEW mart_quality_summary AS
        SELECT
            r.generated_at,
            q.dimension,
            q.severity,
            q.status,
            COUNT(*) AS check_count
        FROM dw_data_quality_checks q
        JOIN dw_gold_runs r ON r.run_id = q.run_id
        GROUP BY r.generated_at, q.dimension, q.severity, q.status;
        """
    )


def upsert_run(cur: Any, gold: dict[str, Any]) -> int:
    generated_at = parse_iso_timestamp(gold.get("generated_at"))
    if generated_at is None:
        generated_at = datetime.now(timezone.utc)
    cur.execute(
        """
        INSERT INTO dw_gold_runs (generated_at, engine, record_count, average_quality_score)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (generated_at) DO UPDATE SET
            engine = EXCLUDED.engine,
            record_count = EXCLUDED.record_count,
            average_quality_score = EXCLUDED.average_quality_score,
            loaded_at = now()
        RETURNING run_id
        """,
        (
            generated_at,
            str(gold.get("engine") or "unknown"),
            int(gold.get("record_count") or 0),
            float(gold.get("average_quality_score") or 0),
        ),
    )
    return int(cur.fetchone()[0])


def replace_run_details(cur: Any, run_id: int, gold: dict[str, Any], quality_checks: list[dict[str, Any]]) -> None:
    for table in ("dw_metric_counts", "dw_source_sentiment", "dw_latest_articles", "dw_data_quality_checks"):
        cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))

    for section, metric_name in COUNT_METRICS.items():
        for row in gold.get(section, []):
            cur.execute(
                """
                INSERT INTO dw_metric_counts (run_id, metric_name, value, article_count)
                VALUES (%s, %s, %s, %s)
                """,
                (run_id, metric_name, str(row.get("value") or "unknown"), int(row.get("count") or 0)),
            )

    for row in gold.get("average_sentiment_by_source", []):
        cur.execute(
            """
            INSERT INTO dw_source_sentiment (run_id, source, average_sentiment_score, article_count)
            VALUES (%s, %s, %s, %s)
            """,
            (
                run_id,
                str(row.get("source") or "unknown"),
                float(row.get("average_sentiment_score") or 0),
                int(row.get("article_count") or 0),
            ),
        )

    for row in gold.get("latest_articles", []):
        cur.execute(
            """
            INSERT INTO dw_latest_articles (
                run_id, article_id, source, title, url, publication_timestamp, sentiment, topic, quality_score
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                str(row.get("article_id") or ""),
                row.get("source"),
                row.get("title"),
                row.get("url"),
                parse_iso_timestamp(row.get("publication_timestamp")),
                row.get("sentiment"),
                row.get("topic"),
                row.get("quality_score"),
            ),
        )

    for check in quality_checks:
        cur.execute(
            """
            INSERT INTO dw_data_quality_checks (
                run_id, check_name, dimension, status, severity, observed_value, expected_value, details
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                check["check_name"],
                check["dimension"],
                check["status"],
                check["severity"],
                check["observed_value"],
                check["expected_value"],
                check["details"],
            ),
        )


def export_dashboard_data(path: Path, gold: dict[str, Any], quality_checks: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    failed_errors = sum(1 for check in quality_checks if check["status"] == "failed" and check["severity"] == "error")
    failed_warnings = sum(1 for check in quality_checks if check["status"] == "failed" and check["severity"] == "warning")
    payload = {
        "gold": gold,
        "quality": {
            "checks": quality_checks,
            "summary": {
                "total_checks": len(quality_checks),
                "passed_checks": sum(1 for check in quality_checks if check["status"] == "passed"),
                "failed_errors": failed_errors,
                "failed_warnings": failed_warnings,
                "status": "failed" if failed_errors else "warning" if failed_warnings else "passed",
            },
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_warehouse(args: argparse.Namespace) -> int:
    gold = read_gold_payload(args)
    quality_checks = build_quality_checks(gold)
    export_dashboard_data(Path(args.dashboard_output), gold, quality_checks)

    if args.skip_db:
        print(
            "warehouse_skipped "
            f"dashboard={args.dashboard_output} quality_failed="
            f"{sum(1 for check in quality_checks if check['status'] == 'failed')}"
        )
        return 0

    conn = connect_postgres(args)
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_schema(cur)
                run_id = upsert_run(cur, gold)
                replace_run_details(cur, run_id, gold, quality_checks)
    finally:
        conn.close()

    print(
        "warehouse_loaded "
        f"run_generated_at={gold.get('generated_at')} "
        f"records={gold.get('record_count')} "
        f"dashboard={args.dashboard_output}"
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load gold analytics into PostgreSQL analytical warehouse tables.")
    parser.add_argument("--gold-file", help="Local gold JSON file. If omitted, --gold-object is read from MinIO.")
    parser.add_argument("--dashboard-output", default="dashboard/data.json", help="Dashboard JSON output path.")
    parser.add_argument("--skip-db", action="store_true", help="Only run quality checks and export dashboard JSON.")
    parser.add_argument("--pg-host", default="localhost", help="PostgreSQL host.")
    parser.add_argument("--pg-port", type=int, default=5432, help="PostgreSQL port.")
    parser.add_argument("--pg-database", default="econom_arch", help="PostgreSQL database.")
    parser.add_argument("--pg-user", default="econom_user", help="PostgreSQL user.")
    parser.add_argument("--pg-password", default="econom_password", help="PostgreSQL password.")
    parser.add_argument("--minio-endpoint", default="localhost:9000", help="MinIO API endpoint.")
    parser.add_argument("--minio-access-key", default="minioadmin", help="MinIO access key.")
    parser.add_argument("--minio-secret-key", default="minioadmin", help="MinIO secret key.")
    parser.add_argument("--minio-secure", action="store_true", help="Use HTTPS for MinIO.")
    parser.add_argument("--bucket", default="econom-raw", help="MinIO bucket containing gold data.")
    parser.add_argument("--gold-object", default="gold/financial-news/latest.json", help="Gold object to read from MinIO.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        return load_warehouse(args)
    except Exception as exc:
        print(f"warehouse_failed error={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
