# Phase 3 - Data Quality Gates

## Goal

Phase 3 adds data quality gates after the Silver merge and before Gold aggregations.

The purpose is to make the pipeline fail loudly when critical assumptions break, while still logging warning-level issues that should be monitored.

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
```

Gold should only run when critical Silver checks pass.

## Check Types

Critical checks fail the pipeline:

- `silver_order_id_not_null`
- `silver_no_duplicate_order_ids`
- `silver_updated_at_not_null`
- `silver_quantity_not_zero`
- `silver_unit_price_non_negative`

Warning checks are logged but do not fail the pipeline:

- `silver_invoice_no_present`
- `silver_country_present`

## Output Tables

Data quality results:

```text
data/delta/ops/dq_check_results
```

Rejected records:

```text
data/delta/ops/rejected_records
```

Pipeline run log:

```text
data/delta/ops/pipeline_run_log
```

In Databricks, these should become Delta tables under the `ops` schema:

```text
ops.dq_check_results
ops.rejected_records
ops.pipeline_run_log
```

## Run Command

Install dependencies first:

```bash
python -m pip install -r requirements.txt
```

Run checks:

```bash
python -m src.quality.run_quality_checks --run-id dq-demo-20101201
```

Run checks without failing the process on critical issues:

```bash
python -m src.quality.run_quality_checks --run-id dq-demo-20101201 --warn-only
```

## Why This Matters

This makes the project feel closer to production data engineering:

- broken assumptions become visible
- failures are logged to tables
- Gold does not quietly build on bad data
- dashboard users can see pipeline health later

## Phase 3 Exit Criteria

- Data quality check definitions exist
- Critical vs warning severity is explicit
- Results are written to an ops Delta table
- Rejected records are captured when checks fail
- Pipeline run log records the DQ step
- Critical failures can stop the pipeline
- Contract tests pass without local Spark
