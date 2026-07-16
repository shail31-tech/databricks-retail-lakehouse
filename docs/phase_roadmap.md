# Phase Roadmap

## Phase 0 - Definition and Skeleton

- Create project folder
- Define architecture
- Define cost strategy
- Define table names and repo layout
- Document success criteria

Status: complete

## Phase 1 - Public Source Event Generation and Bronze Ingestion

- Download UCI Online Retail public dataset
- Generate deterministic order events from public source rows
- Include new records, duplicates, corrections, and late arrivals
- Write landing JSON files
- Load landing files into Bronze Delta
- Add run metadata
- Add smoke test

Status: implementation complete; local Bronze execution pending PySpark/Delta dependency install

## Phase 2 - Silver Incremental Merge

- Parse and type Bronze records
- Deduplicate each batch by `order_id` and latest `updated_at`
- Implement Delta `MERGE INTO`
- Prove reruns are idempotent
- Prove corrected records update Silver

Status: implementation complete; local Delta merge execution pending PySpark/Delta dependency install

## Phase 3 - Data Quality Gates

- Implement critical and warning-level checks
- Log all checks to `ops.dq_check_results`
- Fail the pipeline on critical failures
- Store rejected records when useful

Status: implementation complete; local Delta execution pending PySpark/Delta dependency install

## Phase 4 - Gold Aggregates

- Build daily sales metrics
- Build product performance metrics
- Build order status metrics
- Recompute affected partitions after corrections

Status: implementation complete; local Delta execution pending PySpark/Delta dependency install

## Phase 5 - Backfill Capability

- Add date range parameters
- Reprocess selected landing dates
- Reuse the same merge logic
- Prove no duplicate Silver rows
- Prove Gold updates after backfill

Status: implementation complete; local Delta execution pending PySpark/Delta dependency install

## Phase 6 - GitHub Actions Orchestration

- Add scheduled workflow
- Add manual workflow dispatch
- Add backfill workflow inputs
- Upload pipeline logs as artifacts if running locally

Status: complete

## Phase 7 - Portfolio Polish and Databricks SQL Dashboard Notes

- Add Databricks SQL dashboard notes
- Add screenshots
- Add architecture diagram
- Add demo commands
- Add interview talking points
