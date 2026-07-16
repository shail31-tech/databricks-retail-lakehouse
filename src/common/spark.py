from __future__ import annotations

from typing import Any


def create_spark_session(app_name: str) -> Any:
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as exc:  # pragma: no cover - local setup state
        raise SystemExit(
            "This pipeline step requires PySpark. Run it in Databricks or install project "
            "dependencies locally with: python -m pip install -r requirements.txt"
        ) from exc

    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    try:
        from delta import configure_spark_with_delta_pip
    except ImportError:
        return builder.getOrCreate()

    return configure_spark_with_delta_pip(builder).getOrCreate()


def require_pyspark_functions() -> Any:
    try:
        from pyspark.sql import functions as F
    except ModuleNotFoundError as exc:  # pragma: no cover - local setup state
        raise SystemExit(
            "This pipeline step requires PySpark. Run it in Databricks or install project "
            "dependencies locally with: python -m pip install -r requirements.txt"
        ) from exc
    return F


def require_pyspark_window() -> Any:
    try:
        from pyspark.sql import Window
    except ModuleNotFoundError as exc:  # pragma: no cover - local setup state
        raise SystemExit(
            "This pipeline step requires PySpark. Run it in Databricks or install project "
            "dependencies locally with: python -m pip install -r requirements.txt"
        ) from exc
    return Window
