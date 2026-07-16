# Phase 6 - GitHub Actions Orchestration

## Goal

Phase 6 adds visible orchestration and deployment in GitHub Actions.

The workflows are designed to show real CI, pipeline entry points, and a Databricks deployment path. By default, the data-generation workflows stay lightweight to avoid accidental compute usage. The deployment workflow updates Databricks workspace files, jobs, and the Lakeview dashboard when code lands on `main`.

## Workflows

### `CI`

File:

```text
.github/workflows/ci.yml
```

Runs on:

- push
- pull request
- manual dispatch

Steps:

- install lightweight dependencies from `requirements-ci.txt`
- run unit and contract tests
- compile source files

This workflow is safe and cheap.

### `Retail Lakehouse Pipeline`

File:

```text
.github/workflows/pipeline.yml
```

Runs on:

- daily schedule
- manual dispatch

Inputs:

- `run_date`
- `limit`
- `run_spark`

Default behavior:

```text
tests
download UCI dataset
generate landing JSON
upload landing artifact
```

Optional Spark behavior when `run_spark=true`:

```text
Bronze ingestion
Silver merge
Data quality checks
Gold aggregations
upload local Delta artifacts
```

### `Backfill`

File:

```text
.github/workflows/backfill.yml
```

Runs on:

- manual dispatch

Inputs:

- `start_date`
- `end_date`
- `limit_per_day`
- `run_prefix`
- `run_spark`

Default behavior:

```text
tests
download UCI dataset
generate landing JSON for every date in the range
upload landing artifacts
```

Optional Spark behavior when `run_spark=true`:

```text
full date-range backfill
upload local Delta artifacts
```

### `Deploy to Databricks`

File:

```text
.github/workflows/deploy_databricks.yml
```

Runs on:

- push to `main`
- manual dispatch

Steps:

- run tests
- compile source, Databricks wrappers, and deployment scripts
- install the Databricks CLI
- authenticate using GitHub secrets
- upload repo assets to Workspace Files
- create or reset the serverless daily and backfill jobs
- create or update and publish the Lakeview dashboard

Required GitHub secrets:

```text
DATABRICKS_HOST
DATABRICKS_TOKEN
```

Optional GitHub repository variables:

```text
DATABRICKS_WORKSPACE_ROOT
DATABRICKS_WAREHOUSE_ID
```

## Why Spark Is Optional

GitHub-hosted runners are not the intended long-term compute engine for this project. The final portfolio version should run Spark/Delta work in Databricks Free Edition where possible.

GitHub Actions is used for:

- visible CI
- scheduled orchestration entry points
- manual pipeline/backfill controls
- Databricks deployment after pushes to `main`
- artifact generation
- proof that the repo has real operational wiring

Databricks is intended for:

- Bronze Delta writes
- Silver `MERGE INTO`
- data quality tables
- Gold Delta tables

## Portfolio Story

In interviews, describe this phase like this:

> I used GitHub Actions for CI and orchestration entry points. Lightweight jobs validate code and generate landing artifacts for free, while Spark/Delta execution is guarded behind a manual flag and intended to run in Databricks Free Edition for the final demo.

For deployment:

> Pushes to `main` run tests and then deploy the repo to Databricks. GitHub is the source of truth for code, job definitions, and the Lakeview dashboard definition.

## Phase 6 Exit Criteria

- CI workflow exists
- scheduled pipeline workflow exists
- manual backfill workflow exists
- Databricks deployment workflow exists
- workflow inputs are documented
- Spark execution is opt-in
- tests pass locally
