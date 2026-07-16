# Architecture

## High-Level Flow

```text
UCI Online Retail public dataset
        |
        v
Landing JSON files
        |
        v
Bronze Delta: raw order events
        |
        v
Silver Delta: current cleaned orders
        |
        v
Gold Delta: analytics aggregates and dashboard-ready tables
```

## Bronze Layer

Purpose: preserve ingested events with minimal transformation.

Expected columns:

- `order_id`
- `customer_id`
- `product_id`
- `order_status`
- `quantity`
- `unit_price`
- `currency`
- `event_time`
- `updated_at`
- `source_system`
- `source_file`
- `batch_id`
- `ingested_at`
- `event_date`
- `raw_payload`

Bronze is append-oriented. It records what arrived and when.

## Silver Layer

Purpose: create a clean current-state table for each order.

Rules:

- Natural key: `order_id`
- Sequence column: `updated_at`
- Insert records that do not exist
- Update existing records only when incoming `updated_at` is newer
- Ignore older duplicate or replayed records
- Keep typed numeric and timestamp fields
- Add audit columns such as `silver_updated_at`

Core interview feature:

```sql
MERGE INTO silver.orders AS target
USING staged_orders AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## Gold Layer

Purpose: aggregate Silver into business-friendly tables.

Planned tables:

- `gold.daily_sales`
- `gold.product_performance`
- `gold.country_revenue`
- `gold.order_status_summary`
- `gold.pipeline_health`

Gold should be recomputed only for affected dates during normal incremental runs and backfills.

## Operational Tables

Planned tables:

- `ops.pipeline_run_log`
- `ops.dq_check_results`
- `ops.rejected_records`

These tables make the pipeline observable and help tell the project story during interviews.

## Data Quality Layer

Data quality runs after Silver and before Gold.

Critical checks stop the pipeline:

- missing `order_id`
- duplicate `order_id`
- missing `updated_at`
- zero/null quantity
- negative/null unit price

Warning checks are logged:

- missing invoice number
- missing country

Results are written to `ops.dq_check_results`, and failed rows are captured in `ops.rejected_records`.

## Backfill Flow

Backfill reuses the same pipeline functions as a daily run.

```text
date range
   |
   v
for each date:
  generate landing JSON
  append Bronze
  merge Silver
  run DQ
  rebuild Gold
```

Silver `MERGE` makes reruns safe because the natural key is `order_id` and newer records are chosen by `updated_at`.

## GitHub Actions Orchestration

GitHub Actions provides CI and pipeline entry points:

- `ci.yml` runs tests and source compilation
- `pipeline.yml` runs scheduled/manual landing generation and optional Spark steps
- `backfill.yml` runs manual date-range landing generation and optional full backfill

Spark execution is opt-in in GitHub Actions. The intended final compute target for Delta workloads is Databricks Free Edition.
