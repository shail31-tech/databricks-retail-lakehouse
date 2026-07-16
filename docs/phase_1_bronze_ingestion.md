# Phase 1 - Source Events and Bronze Ingestion

## Goal

Phase 1 creates raw retail order events from the public UCI Online Retail dataset and a Bronze ingestion job.

The public dataset provides real transaction rows, and the event simulator intentionally creates realistic pipeline scenarios:

- New order events
- Duplicate replays
- Upstream corrections
- Late-arriving events

These records make later phases meaningful because Silver will need incremental merge logic instead of full reloads.

## Download the Public Dataset

```bash
python -m src.ingest.download_public_dataset
```

This downloads and extracts `Online Retail.xlsx` from UCI into `data/source/`.

## Generate Raw JSON Landing Files

```bash
python -m src.ingest.generate_public_orders \
  --date 2010-12-01 \
  --limit 1000 \
  --batch-id demo-20101201
```

Output shape:

```text
data/landing/orders/event_date=2010-12-01/batch_id=demo-20101201/orders.jsonl
```

The landing area represents raw source arrival before Bronze.

## Ingest Bronze Delta

Install dependencies first:

```bash
python -m pip install -r requirements.txt
```

```bash
python -m src.ingest.bronze_orders --batch-id bronze-demo-20260715
```

Output tables are stored locally under:

```text
data/delta/bronze/orders_raw
data/delta/ops/pipeline_run_log
```

In Databricks, the same logic can be used in a job or notebook with table paths changed to workspace/catalog paths.

## Bronze Columns

Bronze keeps source fields and adds ingestion metadata:

- `raw_payload`
- `source_file`
- `batch_id`
- `ingested_at`
- `event_date`

## Smoke Tests

```bash
pytest
```

The Phase 1 tests verify that event generation is deterministic and includes duplicates, corrections, and late arrivals.

## Phase 1 Exit Criteria

- Raw order events can be generated for a chosen date
- Landing files use date and batch partitions
- Bronze ingestion can append landing JSON to Delta
- Pipeline run logging stub exists
- Generator tests pass

## Local Verification Notes

Verified on the current machine:

- `python -m pytest` passes
- Public dataset mapping and event simulation tests pass

Not yet executed on the current machine:

- Bronze Delta write, because local `pyspark`/`delta-spark` dependencies are not installed yet
