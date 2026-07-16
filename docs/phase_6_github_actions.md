# Phase 6 - GitHub Actions Orchestration

## Goal

Phase 6 adds visible orchestration in GitHub Actions.

The workflows are designed to show real CI and pipeline entry points without accidentally creating cloud cost. By default, they run lightweight checks and generate landing artifacts. Spark/Delta execution is optional.

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

## Why Spark Is Optional

GitHub-hosted runners are not the intended long-term compute engine for this project. The final portfolio version should run Spark/Delta work in Databricks Free Edition where possible.

GitHub Actions is used for:

- visible CI
- scheduled orchestration entry points
- manual pipeline/backfill controls
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

## Phase 6 Exit Criteria

- CI workflow exists
- scheduled pipeline workflow exists
- manual backfill workflow exists
- workflow inputs are documented
- Spark execution is opt-in
- tests pass locally
