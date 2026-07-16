# Phase 4 - Gold Aggregates

## Goal

Phase 4 builds dashboard-ready Gold tables from the cleaned Silver orders table.

Gold is the reporting layer. It should answer common business and pipeline-health questions without forcing the dashboard to perform heavy transformations.

## Pipeline Placement

```text
Bronze ingestion
        |
        v
Silver incremental merge
        |
        v
Data quality gates
        |
        v
Gold aggregations
        |
        v
Dashboard
```

Gold should run only after critical data quality checks pass.

## Gold Tables

### `gold.daily_sales`

Daily business metrics:

- invoice count
- customer count
- line count
- net units
- net revenue
- cancellation lines

### `gold.product_performance`

Product-level metrics:

- line count
- invoice count
- net units
- net revenue

### `gold.country_revenue`

Country-level metrics:

- line count
- invoice count
- customer count
- net revenue

### `gold.order_status_summary`

Status and cancellation metrics:

- order status
- cancellation flag
- line count
- invoice count
- net units
- net revenue

### `gold.pipeline_health`

Operational metrics for the dashboard:

- latest run per pipeline step
- latest status
- latest message
- data quality summary status

## Run Command

Install dependencies first:

```bash
python -m pip install -r requirements.txt
```

Run Gold aggregations after Silver and DQ:

```bash
python -m src.transform.gold_aggregates --run-id gold-demo-20101201
```

## Output Paths

Local Delta output:

```text
data/delta/gold/daily_sales
data/delta/gold/product_performance
data/delta/gold/country_revenue
data/delta/gold/order_status_summary
data/delta/gold/pipeline_health
```

Databricks target tables:

```text
gold.daily_sales
gold.product_performance
gold.country_revenue
gold.order_status_summary
gold.pipeline_health
```

## Dashboard Plan

The future Databricks SQL dashboard should read only Gold tables:

- revenue trend from `gold.daily_sales`
- top products from `gold.product_performance`
- revenue by country from `gold.country_revenue`
- cancellation rate from `gold.order_status_summary`
- pipeline status from `gold.pipeline_health`

## Phase 4 Exit Criteria

- Gold aggregation job exists
- Dashboard-facing table contracts are documented
- Gold paths follow the medallion layout
- Pipeline run logging records the Gold step
- Contract tests pass without local Spark
