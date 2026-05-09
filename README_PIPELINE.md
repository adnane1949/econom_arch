# Econom Architecture Pipeline

## Analytical Warehouse Tables

The gold JSON is loaded into PostgreSQL with these analytical tables:

- `dw_gold_runs`: one row per gold snapshot, with `generated_at`, engine, `record_count`, and average quality.
- `dw_metric_counts`: reusable count facts for source, topic, sentiment, language, source country, tags, companies, currencies, and mentioned countries.
- `dw_source_sentiment`: average sentiment score by source.
- `dw_latest_articles`: latest article drill-down table with article URL, topic, sentiment, and quality.
- `dw_data_quality_checks`: completeness, coherence, and validity checks for each gold run.

The loader also creates marts/views: `mart_source_performance`, `mart_topic_distribution`, `mart_sentiment_mix`, `mart_entity_leaderboard`, `mart_latest_articles`, and `mart_quality_summary`.

## Commands

Start infrastructure:

```powershell
docker compose up -d --build
```

Open services:

```powershell
# Airflow
http://localhost:8082
# MinIO console
http://localhost:9001

# Spark master
http://localhost:8085
```

Run the full Airflow DAG from the command line:

```powershell
docker compose exec airflow airflow dags trigger econom_financial_news_pipeline
```

Run stages manually inside Airflow container:

```powershell
docker compose exec airflow python /opt/airflow/project/bronze_to_silver.py --minio-endpoint minio:9000 --include-invalid
docker compose exec airflow python /opt/airflow/project/silver_to_gold.py --minio-endpoint minio:9000
docker compose exec airflow python /opt/airflow/project/warehouse_loader.py --minio-endpoint minio:9000 --pg-host postgres
```

Load the provided local test gold file into Postgres from your host:

```powershell
.\.venv\Scripts\python.exe warehouse_loader.py --gold-file gold_buck_test_1.json
```

Only validate the test gold file and generate dashboard data, without Postgres:

```powershell
.\.venv\Scripts\python.exe warehouse_loader.py --gold-file gold_buck_test_1.json --skip-db
```

Serve the interactive dashboard:

```powershell
.\.venv\Scripts\python.exe -m http.server 8050 --directory dashboard
```

Then open:

```text
http://localhost:8050
```

Useful warehouse queries:

```powershell
docker compose exec postgres psql -U econom_user -d econom_arch -c "select * from vw_latest_gold_run;"
docker compose exec postgres psql -U econom_user -d econom_arch -c "select * from mart_source_performance order by generated_at desc, article_count desc;"
docker compose exec postgres psql -U econom_user -d econom_arch -c "select * from mart_quality_summary order by generated_at desc, dimension, severity, status;"
```

## Data Quality Scope

The loader verifies:

- Completeness: required header fields, non-empty snapshot, expected sections, required article drill-down fields.
- Coherence: aggregate totals match `record_count`, source sentiment counts match source counts, latest article IDs are unique.
- Validity: quality scores in `[0, 1]`, sentiment scores in `[-1, 1]`, accepted sentiment labels, parseable timestamps, valid `http(s)` URLs, expected language codes.
