# Phase 5 - Backfill Capability

## Goal

Phase 5 adds one command that reprocesses an arbitrary historical date range.

Backfill is important because real pipelines need to recover from missing data, fixed bugs, changed business logic, late-arriving records, and new metrics that must be rebuilt historically.

## Command

```bash
python -m src.orchestration.backfill \
  --start-date 2010-12-01 \
  --end-date 2010-12-07 \
  --limit-per-day 1000 \
  --run-prefix demo-backfill
```

## What It Runs

For each date in the range, backfill runs the same pipeline steps as a normal daily run:

```text
1. Generate raw landing JSON from UCI Online Retail
2. Append to Bronze Delta
3. Merge into Silver Delta
4. Run data quality gates
5. Rebuild Gold aggregate tables
```

During backfill, Bronze ingestion reads only the JSONL file generated for that specific date and batch. This avoids accidentally re-ingesting every existing landing folder on each date in the range.

## Why It Is Idempotent

The backfill command reuses the same Silver merge logic:

```sql
ON target.order_id = source.order_id
WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

That means:

- new historical records insert
- corrected historical records update only when newer
- replayed records do not create duplicate Silver rows
- older records do not overwrite newer records

## Run IDs

Each step gets a deterministic run ID when `--run-prefix` is supplied.

Example:

```text
demo-backfill-landing-2010-12-01
demo-backfill-bronze-2010-12-01
demo-backfill-silver-2010-12-01
demo-backfill-dq-2010-12-01
demo-backfill-gold-2010-12-01
```

This makes demos easier because the same backfill can be re-run and traced clearly in logs.

## Demo Plan

1. Run backfill for `2010-12-01` through `2010-12-03`.
2. Check Silver row count and Gold metrics.
3. Run the same backfill again with the same `--run-prefix`.
4. Confirm Silver row count does not duplicate.
5. Confirm Gold metrics remain consistent.

## Databricks Notes

In Databricks Free Edition, this can become a job or notebook with widgets:

- `start_date`
- `end_date`
- `limit_per_day`
- `run_prefix`

The same orchestration logic applies, but each step can also become a separate Databricks task in a workflow.

## Phase 5 Exit Criteria

- Date-range backfill command exists
- Backfill reuses normal pipeline functions
- Run IDs are deterministic for demos
- Date range behavior is tested
- Contract tests pass without local Spark
