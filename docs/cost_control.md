# Cost Control

## Decision

This project does not use AWS. All three lakehouse layers are modeled as Databricks/Delta tables.

## Reason

AWS plus Databricks integration can create recurring cost through storage, compute, networking, permissions experiments, and forgotten resources. The project goal is to demonstrate data engineering patterns, not cloud billing setup.

## Primary Low-Cost Execution Options

1. Databricks Free Edition, Community Edition, or trial workspace
2. Local PySpark and Delta Lake fallback
3. GitHub Actions for orchestration and CI
4. Databricks SQL/dashboard-ready Gold tables for the reporting layer

## Guardrails

- Keep datasets small
- Avoid always-on compute
- Stop clusters immediately after runs
- Avoid external cloud storage
- Avoid managed BI tools with monthly billing
- Keep local fallback runnable from the repo

## Portfolio Framing

Use this wording in the README and interviews:

> I designed the project to avoid cloud cost while preserving production lakehouse patterns. The Bronze, Silver, and Gold tables use Delta Lake semantics, and the same PySpark jobs can be moved into a Databricks workspace with minimal path and job configuration changes.
