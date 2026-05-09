#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Centralised workflow script for the Econom Architecture pipeline.

.DESCRIPTION
    Run any stage of the pipeline with a single short command.

.EXAMPLE
    .\run.ps1 up            # Start all Docker services
    .\run.ps1 pipeline      # Run all 5 stages manually inside Docker
    .\run.ps1 trigger       # Trigger the Airflow DAG (automated)
    .\run.ps1 dashboard     # Serve the dashboard on http://localhost:8050
    .\run.ps1 down          # Stop all services
#>

param(
    [Parameter(Position = 0, Mandatory = $true)]
    [ValidateSet(
        "up", "down", "logs", "ps",
        "scrape", "kafka-to-bronze", "bronze", "silver", "gold", "load",
        "pipeline", "trigger",
        "test", "test-db",
        "dashboard",
        "query-latest", "query-sources", "query-quality",
        "open-airflow", "open-minio", "open-spark"
    )]
    [string]$Task
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# -- Constants ----------------------------------------------------------------

$AIRFLOW_PROJECT = "/opt/airflow/project"
$MINIO_ENDPOINT  = "minio:9000"
$KAFKA_BOOTSTRAP = "kafka:29092"
$KAFKA_TOPIC     = "financial-news-raw"

# -- Helpers ------------------------------------------------------------------

function Invoke-DockerCmd {
    param([string[]]$Arguments)
    Write-Host ""
    Write-Host ">> docker $Arguments" -ForegroundColor Cyan
    Write-Host ""
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed with exit code $LASTEXITCODE" }
}

function Invoke-PsqlQuery {
    param([string]$Query)
    Invoke-DockerCmd @(
        "compose", "exec", "postgres",
        "psql", "-U", "econom_user", "-d", "econom_arch", "-c", $Query
    )
}

# -- Tasks --------------------------------------------------------------------

switch ($Task) {

    # -- Infrastructure -------------------------------------------------------

    "up" {
        Write-Host "Starting all services..." -ForegroundColor Green
        Invoke-DockerCmd @("compose", "up", "-d", "--build")
        Write-Host ""
        Write-Host "Services started:" -ForegroundColor Green
        Write-Host "   Airflow   : http://localhost:8082  (admin / admin)"
        Write-Host "   MinIO     : http://localhost:9001  (minioadmin / minioadmin)"
        Write-Host "   Spark UI  : http://localhost:8085"
        Write-Host "   Postgres  : localhost:5432"
    }

    "down" {
        Write-Host "Stopping all services..." -ForegroundColor Yellow
        Invoke-DockerCmd @("compose", "down")
    }

    "logs" {
        Invoke-DockerCmd @("compose", "logs", "--tail=100", "-f")
    }

    "ps" {
        Invoke-DockerCmd @("compose", "ps")
    }

    # -- Individual pipeline stages -------------------------------------------

    "scrape" {
        Write-Host "Stage 1 - Scraping financial news and pushing to Kafka..." -ForegroundColor Green
        Invoke-DockerCmd @(
            "compose", "exec", "airflow",
            "python", "$AIRFLOW_PROJECT/scrape_financial_news.py",
            "--mode", "stream",
            "--stream-once",
            "--kafka-bootstrap-servers", $KAFKA_BOOTSTRAP,
            "--kafka-topic", $KAFKA_TOPIC,
            "--max-per-source", "50",
            "--delay-seconds", "0.5"
        )
    }

    "kafka-to-bronze" {
        Write-Host "Stage 2 - Kafka to MinIO bronze..." -ForegroundColor Green
        Invoke-DockerCmd @(
            "compose", "exec", "airflow",
            "python", "$AIRFLOW_PROJECT/kafka_to_bronze.py",
            "--kafka-bootstrap-servers", $KAFKA_BOOTSTRAP,
            "--kafka-topic", $KAFKA_TOPIC,
            "--minio-endpoint", $MINIO_ENDPOINT,
            "--once",
            "--idle-timeout-ms", "15000"
        )
    }

    "bronze" {
        Write-Host "Stage 3 - Bronze to Silver..." -ForegroundColor Green
        Invoke-DockerCmd @(
            "compose", "exec", "airflow",
            "python", "$AIRFLOW_PROJECT/bronze_to_silver.py",
            "--minio-endpoint", $MINIO_ENDPOINT,
            "--include-invalid"
        )
    }

    "silver" {
        Write-Host "Stage 4 - Silver to Gold (Spark)..." -ForegroundColor Green
        Invoke-DockerCmd @(
            "compose", "exec", "airflow",
            "python", "$AIRFLOW_PROJECT/silver_to_gold.py",
            "--minio-endpoint", $MINIO_ENDPOINT,
            "--spark-master", "spark://spark-master:7077"
        )
    }

    "gold" {
        Write-Host "Stage 4 - Silver to Gold (Spark)..." -ForegroundColor Green
        Invoke-DockerCmd @(
            "compose", "exec", "airflow",
            "python", "$AIRFLOW_PROJECT/silver_to_gold.py",
            "--minio-endpoint", $MINIO_ENDPOINT,
            "--spark-master", "spark://spark-master:7077"
        )
    }

    "load" {
        Write-Host "Stage 5 - Gold to PostgreSQL + Dashboard JSON..." -ForegroundColor Green
        Invoke-DockerCmd @(
            "compose", "exec", "airflow",
            "python", "$AIRFLOW_PROJECT/warehouse_loader.py",
            "--minio-endpoint", $MINIO_ENDPOINT,
            "--pg-host", "postgres",
            "--dashboard-output", "dashboard/data.json"
        )
    }

    # -- Run all 5 stages in sequence -----------------------------------------

    "pipeline" {
        Write-Host "Running full pipeline (all 5 stages)..." -ForegroundColor Green
        Write-Host ""

        Write-Host "=== Stage 1/5: Scrape to Kafka ===" -ForegroundColor Cyan
        & $PSCommandPath "scrape"

        Write-Host ""
        Write-Host "=== Stage 2/5: Kafka to Bronze ===" -ForegroundColor Cyan
        & $PSCommandPath "kafka-to-bronze"

        Write-Host ""
        Write-Host "=== Stage 3/5: Bronze to Silver ===" -ForegroundColor Cyan
        & $PSCommandPath "bronze"

        Write-Host ""
        Write-Host "=== Stage 4/5: Silver to Gold (Spark) ===" -ForegroundColor Cyan
        & $PSCommandPath "silver"

        Write-Host ""
        Write-Host "=== Stage 5/5: Gold to Warehouse ===" -ForegroundColor Cyan
        & $PSCommandPath "load"

        Write-Host ""
        Write-Host "Full pipeline complete!" -ForegroundColor Green
    }

    # -- Trigger via Airflow --------------------------------------------------

    "trigger" {
        Write-Host "Triggering Airflow DAG: econom_financial_news_pipeline..." -ForegroundColor Green
        Invoke-DockerCmd @(
            "compose", "exec", "airflow",
            "airflow", "dags", "trigger", "econom_financial_news_pipeline"
        )
        Write-Host ""
        Write-Host "DAG triggered. Monitor at: http://localhost:8082" -ForegroundColor Green
    }

    # -- Local test -----------------------------------------------------------

    "test" {
        Write-Host "Local test - validating gold_buck_test_1.json (no Postgres)..." -ForegroundColor Green
        & .\.venv\Scripts\python.exe warehouse_loader.py `
            --gold-file gold_buck_test_1.json `
            --skip-db `
            --dashboard-output dashboard/data.json
        Write-Host ""
        Write-Host "Dashboard data written to dashboard/data.json" -ForegroundColor Green
        Write-Host "Run '.\run.ps1 dashboard' to view it." -ForegroundColor Yellow
    }

    "test-db" {
        Write-Host "Local test - loading gold_buck_test_1.json into local Postgres..." -ForegroundColor Green
        & .\.venv\Scripts\python.exe warehouse_loader.py `
            --gold-file gold_buck_test_1.json `
            --dashboard-output dashboard/data.json
    }

    # -- Dashboard ------------------------------------------------------------

    "dashboard" {
        Write-Host "Serving dashboard at http://localhost:8050 ..." -ForegroundColor Green
        Write-Host "Press Ctrl+C to stop." -ForegroundColor Yellow
        Write-Host ""
        & .\.venv\Scripts\python.exe -m http.server 8050 --directory dashboard
    }

    # -- Postgres queries -----------------------------------------------------

    "query-latest" {
        Write-Host "Latest gold run:" -ForegroundColor Cyan
        Invoke-PsqlQuery "SELECT * FROM vw_latest_gold_run;"
    }

    "query-sources" {
        Write-Host "Source performance:" -ForegroundColor Cyan
        Invoke-PsqlQuery "SELECT * FROM mart_source_performance ORDER BY generated_at DESC, article_count DESC;"
    }

    "query-quality" {
        Write-Host "Quality summary:" -ForegroundColor Cyan
        Invoke-PsqlQuery "SELECT * FROM mart_quality_summary ORDER BY generated_at DESC, dimension, severity, status;"
    }

    # -- Open service URLs ----------------------------------------------------

    "open-airflow" { Start-Process "http://localhost:8082" }
    "open-minio"   { Start-Process "http://localhost:9001" }
    "open-spark"   { Start-Process "http://localhost:8085" }
}
