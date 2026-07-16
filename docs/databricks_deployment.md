# Databricks Deployment

## Account

The project is deployed in the Databricks workspace authenticated as:

```text
shailus2002@gmail.com
```

## Workspace Files

Uploaded project files:

```text
/Users/shailus2002@gmail.com/retail-lakehouse-files
```

Important entrypoints:

```text
/Users/shailus2002@gmail.com/retail-lakehouse-files/databricks/run_daily_databricks.py
/Users/shailus2002@gmail.com/retail-lakehouse-files/databricks/backfill_databricks.py
```

## Jobs

Daily job in the current serverless workspace:

```text
Name: retail-lakehouse-daily
Job ID: 1087673142742809
```

Backfill job in the current serverless workspace:

```text
Name: retail-lakehouse-backfill
Job ID: 659734928704448
```

## GitHub Actions Deployment

GitHub is the source of truth for Databricks code, job settings, and the dashboard definition.

Workflow:

```text
.github/workflows/deploy_databricks.yml
```

On every push to `main`, the workflow:

1. runs tests,
2. installs the Databricks CLI,
3. uploads `src/`, `config/`, `databricks/`, `deploy/`, and `dashboards/` to Workspace Files,
4. creates or resets the serverless daily and backfill jobs,
5. creates or updates and publishes the Lakeview dashboard.

Required GitHub repository secrets:

```text
DATABRICKS_HOST=https://dbc-b6e9996e-8dc3.cloud.databricks.com
DATABRICKS_TOKEN=<personal-access-token>
```

Optional GitHub repository variables:

```text
DATABRICKS_WORKSPACE_ROOT=/Users/shailus2002@gmail.com/retail-lakehouse-files
DATABRICKS_WAREHOUSE_ID=542b132076a46f4c
```

Local equivalent:

```bash
DATABRICKS_PROFILE=shailus2002-current python scripts/deploy_to_databricks.py
```

## Cost Note

The jobs are created but should be run deliberately. This workspace supports serverless jobs, so running either job may consume serverless/trial compute credits depending on the workspace plan.

## Useful CLI Commands

```bash
databricks current-user me --profile shailus2002
databricks jobs list --profile shailus2002
databricks jobs run-now 1087673142742809 --profile shailus2002-current
databricks jobs run-now 659734928704448 --profile shailus2002-current
```
