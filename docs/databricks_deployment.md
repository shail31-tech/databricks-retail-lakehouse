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

Daily job:

```text
Name: retail-lakehouse-daily
Job ID: 278944769186859
```

Backfill job:

```text
Name: retail-lakehouse-backfill
Job ID: 138253664882787
```

## Cost Note

The jobs are created but should be run deliberately. They use a single-node job cluster definition to keep the demo small, but running a Databricks job may still consume trial/serverless/compute credits depending on the workspace plan.

## Useful CLI Commands

```bash
databricks current-user me --profile shailus2002
databricks jobs list --profile shailus2002
databricks jobs run-now 278944769186859 --profile shailus2002
databricks jobs run-now 138253664882787 --profile shailus2002
```
