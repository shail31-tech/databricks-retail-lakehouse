from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.common.config import load_config
from src.common.spark import create_spark_session, require_pyspark_functions


def create_spark(app_name: str = "retail-lakehouse-bronze") -> Any:
    return create_spark_session(app_name)


def prepare_bronze_orders(raw_orders: Any, batch_id: str) -> Any:
    functions = require_pyspark_functions()
    original_columns = raw_orders.columns
    return (
        raw_orders.withColumn("raw_payload", functions.to_json(functions.struct(*[functions.col(c) for c in original_columns])))
        .withColumn("source_file", functions.input_file_name())
        .withColumn("batch_id", functions.lit(batch_id))
        .withColumn("ingested_at", functions.current_timestamp())
        .withColumn("event_date", functions.to_date("event_time"))
    )


def ingest_bronze_orders(
    spark: Any,
    landing_path: Path,
    delta_root: Path,
    batch_id: str,
    source_path: Path | None = None,
) -> int:
    resolved_source_path = str(source_path or landing_path / "orders")
    raw_orders = spark.read.json(resolved_source_path)
    bronze_orders = prepare_bronze_orders(raw_orders, batch_id=batch_id)
    output_path = delta_root / "bronze" / "orders_raw"

    (
        bronze_orders.write.format("delta")
        .mode("append")
        .partitionBy("event_date")
        .save(str(output_path))
    )

    row_count = bronze_orders.count()
    log_pipeline_run(
        spark=spark,
        delta_root=delta_root,
        run_id=batch_id,
        status="success",
        row_count=row_count,
        message=f"Ingested Bronze orders from {resolved_source_path}",
    )
    return row_count


def log_pipeline_run(
    spark: Any,
    delta_root: Path,
    run_id: str,
    status: str,
    row_count: int,
    message: str,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    log_rows = [
        {
            "run_id": run_id,
            "pipeline_step": "bronze_orders",
            "status": status,
            "row_count": row_count,
            "message": message,
            "created_at": now,
        }
    ]
    log_df = spark.createDataFrame(log_rows)
    log_path = delta_root / "ops" / "pipeline_run_log"
    log_df.write.format("delta").mode("append").save(str(log_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest landing order JSON into Bronze Delta.")
    parser.add_argument("--config", default="config/pipeline.yml", help="Pipeline config path.")
    parser.add_argument("--batch-id", default=None, help="Optional pipeline run id.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    batch_id = args.batch_id or f"bronze-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    spark = create_spark()
    try:
        count = ingest_bronze_orders(
            spark=spark,
            landing_path=config.landing_path,
            delta_root=config.delta_root,
            batch_id=batch_id,
        )
        print(f"Ingested {count} rows into Bronze Delta with batch_id={batch_id}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
