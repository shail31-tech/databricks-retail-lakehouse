from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.common.config import load_config
from src.common.spark import create_spark_session, require_pyspark_functions
from src.quality.run_quality_checks import dq_check_results_path
from src.transform.silver_orders import pipeline_run_log_path, silver_orders_path


GOLD_TABLE_PATHS = {
    "daily_sales": ("gold", "daily_sales"),
    "product_performance": ("gold", "product_performance"),
    "country_revenue": ("gold", "country_revenue"),
    "order_status_summary": ("gold", "order_status_summary"),
    "pipeline_health": ("gold", "pipeline_health"),
}


def gold_table_path(delta_root: Path, table_name: str) -> Path:
    layer, name = GOLD_TABLE_PATHS[table_name]
    return delta_root / layer / name


def create_spark(app_name: str = "retail-lakehouse-gold") -> Any:
    return create_spark_session(app_name)


def build_gold_tables(spark: Any, delta_root: Path, run_id: str) -> dict[str, int]:
    functions = require_pyspark_functions()
    silver = spark.read.format("delta").load(str(silver_orders_path(delta_root)))

    gold_frames = {
        "daily_sales": build_daily_sales(silver, functions),
        "product_performance": build_product_performance(silver, functions),
        "country_revenue": build_country_revenue(silver, functions),
        "order_status_summary": build_order_status_summary(silver, functions),
        "pipeline_health": build_pipeline_health(spark, delta_root, functions),
    }

    row_counts: dict[str, int] = {}
    for name, frame in gold_frames.items():
        output_path = gold_table_path(delta_root, name)
        row_count = frame.count()
        frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(output_path))
        row_counts[name] = row_count

    log_pipeline_run(
        spark=spark,
        delta_root=delta_root,
        run_id=run_id,
        status="success",
        row_count=sum(row_counts.values()),
        message=f"Built Gold tables: {', '.join(sorted(row_counts))}",
    )
    return row_counts


def build_daily_sales(silver: Any, functions: Any) -> Any:
    return (
        silver.groupBy("event_date")
        .agg(
            functions.countDistinct("invoice_no").alias("invoice_count"),
            functions.countDistinct("customer_id").alias("customer_count"),
            functions.count("*").alias("line_count"),
            functions.sum("quantity").alias("net_units"),
            functions.sum("order_amount").alias("net_revenue"),
            functions.sum(functions.when(functions.col("is_cancellation"), 1).otherwise(0)).alias(
                "cancellation_lines"
            ),
        )
        .withColumn("gold_updated_at", functions.current_timestamp())
    )


def build_product_performance(silver: Any, functions: Any) -> Any:
    return (
        silver.groupBy("product_id", "product_name")
        .agg(
            functions.count("*").alias("line_count"),
            functions.countDistinct("invoice_no").alias("invoice_count"),
            functions.sum("quantity").alias("net_units"),
            functions.sum("order_amount").alias("net_revenue"),
        )
        .orderBy(functions.col("net_revenue").desc_nulls_last())
        .withColumn("gold_updated_at", functions.current_timestamp())
    )


def build_country_revenue(silver: Any, functions: Any) -> Any:
    return (
        silver.groupBy("country")
        .agg(
            functions.count("*").alias("line_count"),
            functions.countDistinct("invoice_no").alias("invoice_count"),
            functions.countDistinct("customer_id").alias("customer_count"),
            functions.sum("order_amount").alias("net_revenue"),
        )
        .orderBy(functions.col("net_revenue").desc_nulls_last())
        .withColumn("gold_updated_at", functions.current_timestamp())
    )


def build_order_status_summary(silver: Any, functions: Any) -> Any:
    return (
        silver.groupBy("order_status", "is_cancellation")
        .agg(
            functions.count("*").alias("line_count"),
            functions.countDistinct("invoice_no").alias("invoice_count"),
            functions.sum("quantity").alias("net_units"),
            functions.sum("order_amount").alias("net_revenue"),
        )
        .withColumn("gold_updated_at", functions.current_timestamp())
    )


def build_pipeline_health(spark: Any, delta_root: Path, functions: Any) -> Any:
    run_log = spark.read.format("delta").load(str(pipeline_run_log_path(delta_root)))
    dq_results = spark.read.format("delta").load(str(dq_check_results_path(delta_root)))

    latest_run = (
        run_log.groupBy("pipeline_step")
        .agg(
            functions.max("created_at").alias("last_run_at"),
            functions.max_by("status", "created_at").alias("last_status"),
            functions.max_by("message", "created_at").alias("last_message"),
        )
    )
    latest_dq = (
        dq_results.groupBy("table_name")
        .agg(
            functions.max("created_at").alias("last_dq_at"),
            functions.sum(functions.when(functions.col("status") == "fail", 1).otherwise(0)).alias(
                "failed_check_count"
            ),
            functions.sum(functions.when(functions.col("severity") == "critical", 1).otherwise(0)).alias(
                "critical_check_count"
            ),
        )
        .withColumn("pipeline_step", functions.lit("data_quality_summary"))
        .withColumn("last_status", functions.when(functions.col("failed_check_count") > 0, "failed").otherwise("success"))
        .withColumn("last_run_at", functions.col("last_dq_at"))
        .withColumn("last_message", functions.concat(functions.lit("DQ failures: "), functions.col("failed_check_count")))
        .select("pipeline_step", "last_run_at", "last_status", "last_message")
    )

    return latest_run.select("pipeline_step", "last_run_at", "last_status", "last_message").unionByName(latest_dq)


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
                "pipeline_step": "gold_aggregates",
                "status": status,
                "row_count": row_count,
                "message": message,
                "created_at": now,
            }
        ]
    )
    log_df.write.format("delta").mode("append").save(str(pipeline_run_log_path(delta_root)))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Gold aggregate Delta tables.")
    parser.add_argument("--config", default="config/pipeline.yml", help="Pipeline config path.")
    parser.add_argument("--run-id", default=None, help="Optional pipeline run id.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    run_id = args.run_id or f"gold-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    spark = create_spark()
    try:
        counts = build_gold_tables(spark=spark, delta_root=config.delta_root, run_id=run_id)
        print(f"Built Gold tables for run_id={run_id}: {counts}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
