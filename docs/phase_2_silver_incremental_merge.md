# Phase 2 - Silver Incremental Merge

## Goal

Phase 2 turns append-only Bronze events into a clean Silver current-state orders table.

This is the most interview-relevant phase of the project because it demonstrates incremental processing instead of full reloads.

## Silver Responsibilities

Silver does the following:

- Reads Bronze Delta order events
- Parses timestamp and numeric fields
- Deduplicates each staged batch by `order_id`
- Keeps the newest version using `updated_at`
- Merges into Silver on the natural key
- Updates existing rows only when the source record is newer
- Inserts new orders
- Ignores older duplicate replays
- Logs the pipeline run

## Merge Contract

Natural key:

```text
order_id
```

For the public UCI dataset, `order_id` is a deterministic line-level key built from:

```text
InvoiceNo + StockCode + InvoiceDate
```

Sequence column:

```text
updated_at
```

Merge behavior:

```sql
MERGE INTO silver.orders AS target
USING staged_orders AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## Local Command

Install dependencies first:

```bash
python -m pip install -r requirements.txt
```

Generate Phase 1 landing events:

```bash
python -m src.ingest.generate_public_orders \
  --date 2010-12-01 \
  --limit 1000 \
  --batch-id demo-20101201
```

Run Bronze ingestion:

```bash
python -m src.ingest.bronze_orders --batch-id bronze-demo-20260715
```

Run Silver merge:

```bash
python -m src.transform.silver_orders --run-id silver-demo-20260715
```

## Databricks Migration Notes

In Databricks Free Edition, this phase should become a notebook or job task that:

1. Reads the Bronze Delta table
2. Builds the staged Silver dataframe
3. Executes the same `MERGE INTO` logic
4. Appends to `ops.pipeline_run_log`

The local implementation writes to:

```text
data/delta/silver/orders
```

The Databricks version should write to:

```text
silver.orders
```

or to a managed Delta table in the workspace catalog/schema available in the free environment.

## Phase 2 Exit Criteria

- Silver job exists
- Silver table uses `order_id` as the natural key
- Merge updates only when source `updated_at` is newer
- New records are inserted
- Older replays are ignored
- Silver schema includes both business fields and audit fields
- Tests document the merge contract

## Local Verification Notes

Verified on the current machine:

- `python -m pytest` passes
- Silver merge contract tests pass without requiring local Spark

Not yet executed on the current machine:

- Actual Delta `MERGE`, because local `pyspark`/`delta-spark` dependencies are not installed yet
