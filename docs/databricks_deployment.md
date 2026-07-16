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

## Cost Note

The jobs are created but should be run deliberately. This workspace supports serverless jobs, so running either job may consume serverless/trial compute credits depending on the workspace plan.

## Useful CLI Commands

```bash
databricks current-user me --profile shailus2002
databricks jobs list --profile shailus2002
databricks jobs run-now 1087673142742809 --profile shailus2002-current
databricks jobs run-now 659734928704448 --profile shailus2002-current
```
