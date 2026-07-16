from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.common.config import load_config
from src.common.spark import create_spark_session, require_pyspark_functions, require_pyspark_window


MERGE_CONDITION = "target.order_id = source.order_id"
MATCHED_UPDATE_CONDITION = "source.updated_at > target.updated_at"

SILVER_ORDER_COLUMNS = [
    "order_id",
    "invoice_no",
    "stock_code",
    "customer_id",
    "product_id",
    "product_name",
    "order_status",
    "quantity",
    "unit_price",
    "currency",
    "channel",
    "country",
    "is_cancellation",
    "event_time",
    "updated_at",
    "source_system",
    "source_file",
    "bronze_batch_id",
    "bronze_ingested_at",
    "event_date",
    "order_amount",
    "silver_updated_at",
]


def bronze_orders_path(delta_root: Path) -> Path:
    return delta_root / "bronze" / "orders_raw"


def silver_orders_path(delta_root: Path) -> Path:
    return delta_root / "silver" / "orders"


def pipeline_run_log_path(delta_root: Path) -> Path:
    return delta_root / "ops" / "pipeline_run_log"


def create_spark(app_name: str = "retail-lakehouse-silver") -> Any:
    return create_spark_session(app_name)


def stage_silver_orders(bronze_orders: Any) -> Any:
    functions = require_pyspark_functions()
    window = require_pyspark_window()
    required_columns = [column for column in bronze_orders.columns if column != "raw_payload"]
    selected = bronze_orders.select(*required_columns)

    typed = (
        selected.withColumn("event_time_ts", functions.to_timestamp("event_time"))
        .withColumn("updated_at_ts", functions.to_timestamp("updated_at"))
        .withColumn("quantity_int", functions.col("quantity").cast("int"))
        .withColumn("unit_price_decimal", functions.col("unit_price").cast("decimal(12,2)"))
        .withColumn(
            "order_amount_decimal",
            functions.col("quantity_int") * functions.col("unit_price_decimal"),
        )
        .withColumnRenamed("batch_id", "bronze_batch_id")
        .withColumnRenamed("ingested_at", "bronze_ingested_at")
    )

    latest_per_order = window.partitionBy("order_id").orderBy(
        functions.col("updated_at_ts").desc_nulls_last(),
        functions.col("bronze_ingested_at").desc_nulls_last(),
    )

    return (
        typed.filter(functions.col("order_id").isNotNull())
        .filter(functions.col("updated_at_ts").isNotNull())
        .withColumn("_row_number", functions.row_number().over(latest_per_order))
        .filter(functions.col("_row_number") == 1)
        .select(
            functions.col("order_id"),
            functions.col("invoice_no"),
            functions.col("stock_code"),
            functions.col("customer_id"),
            functions.col("product_id"),
            functions.col("product_name"),
            functions.col("order_status"),
            functions.col("quantity_int").alias("quantity"),
            functions.col("unit_price_decimal").alias("unit_price"),
            functions.col("currency"),
            functions.col("channel"),
            functions.col("country"),
            functions.col("is_cancellation"),
            functions.col("event_time_ts").alias("event_time"),
            functions.col("updated_at_ts").alias("updated_at"),
            functions.col("source_system"),
            functions.col("source_file"),
            functions.col("bronze_batch_id"),
            functions.col("bronze_ingested_at"),
            functions.col("event_date"),
            functions.col("order_amount_decimal").alias("order_amount"),
            functions.current_timestamp().alias("silver_updated_at"),
        )
    )


def merge_silver_orders(spark: Any, delta_root: Path, run_id: str) -> int:
    delta_table = require_delta_table()
    bronze_path = bronze_orders_path(delta_root)
    silver_path = silver_orders_path(delta_root)

    bronze_orders = spark.read.format("delta").load(str(bronze_path))
    staged_orders = stage_silver_orders(bronze_orders)
    staged_count = staged_orders.count()

    if delta_table.isDeltaTable(spark, str(silver_path)):
        (
            delta_table.forPath(spark, str(silver_path))
            .alias("target")
            .merge(staged_orders.alias("source"), MERGE_CONDITION)
            .whenMatchedUpdateAll(condition=MATCHED_UPDATE_CONDITION)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            staged_orders.select(*SILVER_ORDER_COLUMNS)
            .write.format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .save(str(silver_path))
        )

    log_pipeline_run(
        spark=spark,
        delta_root=delta_root,
        run_id=run_id,
        status="success",
        row_count=staged_count,
        message=f"Merged staged Bronze orders into {silver_path}",
    )
    return staged_count


def log_pipeline_run(
    spark: Any,
    delta_root: Path,
    run_id: str,
    status: str,
    row_count: int,
    message: str,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    log_df = spark.createDataFrame(
        [
            {
                "run_id": run_id,
                "pipeline_step": "silver_orders",
                "status": status,
                "row_count": row_count,
                "message": message,
                "created_at": now,
            }
        ]
    )
    log_df.write.format("delta").mode("append").save(str(pipeline_run_log_path(delta_root)))


def require_delta_table() -> Any:
    try:
        from delta.tables import DeltaTable
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised by local setup state
        raise SystemExit(
            "Silver merge requires PySpark and Delta Lake. "
            "Install project dependencies with: python -m pip install -r requirements.txt"
        ) from exc

    return DeltaTable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge Bronze orders into Silver Delta.")
    parser.add_argument("--config", default="config/pipeline.yml", help="Pipeline config path.")
    parser.add_argument("--run-id", default=None, help="Optional pipeline run id.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    run_id = args.run_id or f"silver-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    spark = create_spark()
    try:
        count = merge_silver_orders(spark=spark, delta_root=config.delta_root, run_id=run_id)
        print(f"Merged {count} staged rows into Silver Delta with run_id={run_id}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
