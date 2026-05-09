# Econom Architecture Pipeline

A financial-news data pipeline: scrape → Kafka → MinIO (bronze/silver/gold) → PostgreSQL → dashboard.

```
scrape_financial_news.py
        ↓ (Kafka topic: financial-news-raw)
kafka_to_bronze.py  →  MinIO  bronze/financial-news-raw/
        ↓
bronze_to_silver.py  →  MinIO  silver/financial-news/
        ↓
silver_to_gold.py (PySpark)  →  MinIO  gold/financial-news/latest.json
        ↓
warehouse_loader.py  →  PostgreSQL + dashboard/data.json
        ↓
dashboard/index.html  served on http://localhost:8050
```

---

## Quick Start

All commands are centralised in `run.ps1`. You only need Docker Desktop running.

### 1 — Start infrastructure

```powershell
.\run.ps1 up
```

Services started:

| Service   | URL                        | Credentials                |
|-----------|----------------------------|----------------------------|
| Airflow   | http://localhost:8082       | admin / admin              |
| MinIO     | http://localhost:9001       | minioadmin / minioadmin    |
| Spark UI  | http://localhost:8085       | —                          |
| Postgres  | localhost:5432              | econom_user / econom_password |

### 2 — Run the pipeline (two options)

**Option A — Trigger the Airflow DAG (automated, recommended)**

```powershell
.\run.ps1 trigger
```

Then monitor progress at http://localhost:8082. The DAG runs all 5 stages automatically and repeats every hour.

**Option B — Run all stages manually in sequence**

```powershell
.\run.ps1 pipeline
```

This calls all 5 stages one after another, printing output for each.

### 3 — View the dashboard

```powershell
.\run.ps1 dashboard
```

Open http://localhost:8050 in your browser.

---

## All Available Commands

```powershell
# ── Infrastructure ───────────────────────────────────────────────────────────
.\run.ps1 up              # Build and start all Docker services
.\run.ps1 down            # Stop all services
.\run.ps1 logs            # Follow live logs for all containers
.\run.ps1 ps              # Show container status

# ── Full pipeline ─────────────────────────────────────────────────────────────
.\run.ps1 trigger         # Trigger the Airflow DAG (automated, recommended)
.\run.ps1 pipeline        # Run all 5 stages manually in sequence

# ── Individual stages (manual, inside Docker) ─────────────────────────────────
.\run.ps1 scrape          # Stage 1: Scrape news → publish to Kafka
.\run.ps1 kafka-to-bronze # Stage 2: Kafka → MinIO bronze
.\run.ps1 bronze          # Stage 3: Bronze → Silver (flatten + dedupe)
.\run.ps1 silver          # Stage 4: Silver → Gold  (PySpark aggregation)
.\run.ps1 load            # Stage 5: Gold → PostgreSQL + dashboard/data.json

# ── Local testing (no full Docker stack needed) ───────────────────────────────
.\run.ps1 test            # Validate gold_buck_test_1.json, skip Postgres
.\run.ps1 test-db         # Load gold_buck_test_1.json into local Postgres

# ── Dashboard ─────────────────────────────────────────────────────────────────
.\run.ps1 dashboard       # Serve dashboard at http://localhost:8050

# ── Postgres queries ──────────────────────────────────────────────────────────
.\run.ps1 query-latest    # Latest gold run summary
.\run.ps1 query-sources   # Source performance (article counts + sentiment)
.\run.ps1 query-quality   # Data quality check summary

# ── Open service UIs in browser ───────────────────────────────────────────────
.\run.ps1 open-airflow
.\run.ps1 open-minio
.\run.ps1 open-spark
```

---

## Analytical Warehouse Tables

| Table | Description |
|---|---|
| `dw_gold_runs` | One row per gold snapshot (timestamp, engine, record count, avg quality) |
| `dw_metric_counts` | Count facts by source, topic, sentiment, language, country, tags, companies, currencies |
| `dw_source_sentiment` | Average sentiment score per source |
| `dw_latest_articles` | Article drill-down rows (URL, topic, sentiment, quality score) |
| `dw_data_quality_checks` | Completeness, coherence, and validity checks per gold run |

Marts / views automatically created: `vw_latest_gold_run`, `mart_source_performance`, `mart_topic_distribution`, `mart_sentiment_mix`, `mart_entity_leaderboard`, `mart_latest_articles`, `mart_quality_summary`.

---

## Data Quality Checks

`warehouse_loader.py` verifies every gold snapshot before loading:

- **Completeness** — required header fields, non-empty snapshot, expected sections, required article drill-down fields.
- **Coherence** — aggregate totals match `record_count`, per-source sentiment counts match source counts, latest article IDs are unique.
- **Validity** — quality scores in `[0, 1]`, sentiment scores in `[-1, 1]`, accepted sentiment labels, parseable ISO-8601 timestamps, valid `http(s)` URLs, expected language codes.
