# Databricks Retail Lakehouse

Production-style data engineering portfolio project for a retail orders pipeline using the public UCI Online Retail dataset and Databricks-style Bronze, Silver, and Gold Delta layers.

This project intentionally avoids AWS/S3 to keep cost near zero. All lakehouse layers are designed to live in Databricks Delta tables, with a local development fallback that mirrors the same table structure and merge logic.

## Project Goal

Build an incremental lakehouse pipeline that ingests public retail transaction data, stores raw events in Bronze Delta, cleans and deduplicates into Silver Delta, aggregates into Gold Delta, and exposes dashboard-ready business metrics and pipeline health.

The main interview-relevant features are:

- Medallion architecture: Bronze, Silver, Gold
- Incremental `MERGE INTO`, not full reloads
- Late-arriving and corrected record handling
- Data quality gates that fail loudly
- Backfill command for arbitrary date ranges
- Orchestration with GitHub Actions
- Databricks SQL/dashboard-ready Gold tables as the final consumption layer

## Dataset

This project uses the [UCI Online Retail dataset](https://archive.ics.uci.edu/dataset/352/online%2Bretail), a transactional dataset from a UK-based online retailer. UCI lists 541,909 rows covering transactions between December 1, 2010 and December 9, 2011. The dataset is licensed under CC BY 4.0.

The source fields include:

- `InvoiceNo`
- `StockCode`
- `Description`
- `Quantity`
- `InvoiceDate`
- `UnitPrice`
- `CustomerID`
- `Country`

The ingestion layer maps these rows into order events and adds controlled duplicate, correction, and late-arrival records so the pipeline can demonstrate incremental merge behavior.

## Phase Status

Current phase: **Phase 6 - GitHub Actions Orchestration**

Phase 6 adds GitHub Actions workflows for CI, scheduled pipeline entry points, manual backfill runs, and Databricks deployment. Pushes to `main` can update Databricks workspace files, serverless job definitions, and the Lakeview dashboard when Databricks secrets are configured in GitHub.

## Target Architecture

```text
GitHub Actions schedule / manual dispatch
        |
        v
Python ingestion job
        |
        v
Bronze Delta table
raw order events, ingestion metadata, batch id
        |
        v
Silver Delta table
typed, validated, deduped current order records
        |
        v
Gold Delta tables
daily sales, product performance, order status metrics, pipeline health
```

## GitHub-to-Databricks Automation

The deployment workflow is:

```text
.github/workflows/deploy_databricks.yml
```

It runs tests, installs the Databricks CLI, uploads project files to Workspace Files, creates or resets Databricks jobs, and publishes the Lakeview dashboard.

Required GitHub secrets:

```text
DATABRICKS_HOST
DATABRICKS_TOKEN
```

## Cost Strategy

No AWS resources are required.

Primary path:

- Databricks Free Edition, Community Edition, or trial credits
- Small datasets only
- Short-lived compute
- No always-on services

Fallback path:

- Local PySpark plus Delta Lake
- Local `data/` directory for development
- Same table names and merge semantics as Databricks

## Planned Repo Structure

```text
.
├── config/
├── data/
│   └── landing/
├── docs/
├── notebooks/
├── src/
├── tests/
├── .github/
│   └── workflows/
├── .gitignore
├── README.md
└── requirements.txt
```

## Phase 1 Quickstart

Download the public dataset:

```bash
python -m src.ingest.download_public_dataset
```

Generate raw landing events from UCI Online Retail:

```bash
python -m src.ingest.generate_public_orders --date 2010-12-01 --limit 1000 --batch-id demo-20101201
```

Ingest Bronze Delta:

```bash
python -m src.ingest.bronze_orders --batch-id bronze-demo-20101201
```

Run smoke tests:

```bash
pytest
```

## Phase 2 Quickstart

Run the Silver merge after generating landing data and ingesting Bronze:

```bash
python -m src.transform.silver_orders --run-id silver-demo-20101201
```

## Phase 3 Quickstart

Run data quality checks after the Silver merge:

```bash
python -m src.quality.run_quality_checks --run-id dq-demo-20101201
```

## Phase 4 Quickstart

Build Gold aggregates after Silver and DQ:

```bash
python -m src.transform.gold_aggregates --run-id gold-demo-20101201
```

## Phase 5 Quickstart

Backfill a historical date range:

```bash
python -m src.orchestration.backfill \
  --start-date 2010-12-01 \
  --end-date 2010-12-07 \
  --limit-per-day 1000 \
  --run-prefix demo-backfill
```

## Phase 6 Workflows

GitHub Actions workflows:

- `.github/workflows/ci.yml`
- `.github/workflows/pipeline.yml`
- `.github/workflows/backfill.yml`

Default workflow behavior is lightweight:

```text
run tests
download public dataset
generate landing JSON artifacts
```

Full Spark/Delta execution is guarded by a manual `run_spark=true` input and is intended for Databricks Free or a prepared Spark runner.

## Databricks Deployment

This project has been deployed to the Databricks workspace for `shailus2002@gmail.com`.

Workspace path:

```text
/Users/shailus2002@gmail.com/retail-lakehouse-files
```

Jobs:

- `retail-lakehouse-daily`: `1087673142742809`
- `retail-lakehouse-backfill`: `659734928704448`

## Next Phase

Phase 7 will implement portfolio polish and Databricks SQL dashboard notes.

Expected Phase 7 output:

- Databricks SQL dashboard notes
- screenshots or demo notes
- final README polish
- Databricks migration guide
