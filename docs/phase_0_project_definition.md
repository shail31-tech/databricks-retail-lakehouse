# Phase 0 - Project Definition

## Project Name

Databricks Retail Lakehouse

## One-Line Pitch

A cost-conscious Databricks-style lakehouse pipeline that incrementally processes the public UCI Online Retail dataset through Bronze, Silver, and Gold Delta layers with data quality gates, late-arriving update handling, and idempotent backfills.

## Why This Project

This project is designed to show the kind of data engineering work that appears in real teams:

- Landing raw events
- Designing medallion layers
- Handling incremental updates
- Preventing duplicate rows during reruns
- Validating data before it reaches analytics tables
- Reprocessing history safely
- Exposing operational health and business metrics

The project avoids AWS because previous AWS plus Databricks integration created unnecessary monthly cost. The architecture keeps Databricks as the primary platform and makes local development possible.

## In Scope

- UCI Online Retail public transaction data
- A small event simulator that creates duplicate, corrected, and late-arriving records from public rows
- Bronze Delta table for raw ingested records
- Silver Delta table with typed and deduplicated current records
- Gold Delta aggregate tables
- Incremental `MERGE INTO` logic
- Late-arriving records and corrected upstream records
- Data quality checks and failure logging
- Backfill command for date ranges
- GitHub Actions workflows
- Databricks SQL/dashboard-ready Gold tables

## Out of Scope

- AWS S3, Lambda, Glue, or Athena
- Always-on cloud resources
- Large-scale datasets
- Complex BI semantic modeling
- Production secrets management beyond documented placeholders

## Success Criteria

By the end of the project, the repo should demonstrate:

- A scheduled pipeline can ingest new order events
- Rerunning the same batch does not duplicate rows
- Corrected records update existing Silver rows only when newer
- Late-arriving records are merged correctly
- Bad data fails data quality gates and is logged
- A backfill date range can be reprocessed safely
- Gold aggregates reflect corrections after merge/backfill
- Dashboard shows both business KPIs and pipeline health

## Resume Bullet Draft

Built a Databricks-style retail lakehouse using the public UCI Online Retail dataset, PySpark, and Delta Lake with Bronze/Silver/Gold tables, incremental `MERGE INTO` processing, late-arriving record handling, data quality gates, idempotent backfills, GitHub Actions orchestration, and dashboard-ready Gold metrics.
