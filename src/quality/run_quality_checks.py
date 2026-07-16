from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.common.config import load_config
from src.common.spark import create_spark_session, require_pyspark_functions, require_pyspark_window
from src.quality.check_definitions import DataQualityCheck, SILVER_ORDER_CHECKS
from src.transform.silver_orders import pipeline_run_log_path, silver_orders_path


def dq_check_results_path(delta_root: Path) -> Path:
    return delta_root / "ops" / "dq_check_results"


def rejected_records_path(delta_root: Path) -> Path:
    return delta_root / "ops" / "rejected_records"


def create_spark(app_name: str = "retail-lakehouse-quality") -> Any:
    return create_spark_session(app_name)


def run_silver_quality_checks(
    spark: Any,
    delta_root: Path,
    run_id: str,
    fail_on_critical: bool = True,
) -> list[dict[str, Any]]:
    functions = require_pyspark_functions()
    silver = spark.read.format("delta").load(str(silver_orders_path(delta_root)))
    silver_with_duplicate_flag = add_duplicate_flag(silver)

    results = [
        evaluate_check(
            dataframe=silver_with_duplicate_flag,
            check=check,
            run_id=run_id,
            functions=functions,
        )
        for check in SILVER_ORDER_CHECKS
    ]

    write_quality_results(spark=spark, delta_root=delta_root, results=results)
    write_rejected_records(
        dataframe=silver_with_duplicate_flag,
        delta_root=delta_root,
        run_id=run_id,
        functions=functions,
    )

    critical_failures = [
        result for result in results if result["severity"] == "critical" and result["status"] == "fail"
    ]
    status = "failed" if critical_failures else "success"
    log_pipeline_run(
        spark=spark,
        delta_root=delta_root,
        run_id=run_id,
        status=status,
        row_count=len(results),
        message=f"Completed {len(results)} Silver data quality checks",
    )

    if fail_on_critical and critical_failures:
        failed_names = ", ".join(result["check_name"] for result in critical_failures)
        raise SystemExit(f"Critical data quality checks failed: {failed_names}")

    return results


def add_duplicate_flag(dataframe: Any) -> Any:
    functions = require_pyspark_functions()
    window = require_pyspark_window()
    duplicate_window = window.partitionBy("order_id")
    return dataframe.withColumn(
        "__duplicate_order_id",
        functions.count("*").over(duplicate_window) > functions.lit(1),
    )


def evaluate_check(
    dataframe: Any,
    check: DataQualityCheck,
    run_id: str,
    functions: Any,
) -> dict[str, Any]:
    failed_rows = dataframe.filter(check.failure_expression)
    failed_count = failed_rows.count()
    total_count = dataframe.count()
    failure_rate = failed_count / total_count if total_count else 0.0

    return {
        "run_id": run_id,
        "check_name": check.name,
        "table_name": check.table_name,
        "severity": check.severity,
        "status": "fail" if failed_count else "pass",
        "failed_count": failed_count,
        "total_count": total_count,
        "failure_rate": failure_rate,
        "description": check.description,
        "failure_expression": check.failure_expression,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def write_quality_results(spark: Any, delta_root: Path, results: list[dict[str, Any]]) -> None:
    results_df = spark.createDataFrame(results)
    results_df.write.format("delta").mode("append").save(str(dq_check_results_path(delta_root)))


def write_rejected_records(dataframe: Any, delta_root: Path, run_id: str, functions: Any) -> None:
    failed = None
    for check in SILVER_ORDER_CHECKS:
        check_failures = (
            dataframe.filter(check.failure_expression)
            .withColumn("run_id", functions.lit(run_id))
            .withColumn("check_name", functions.lit(check.name))
            .withColumn("severity", functions.lit(check.severity))
            .withColumn("rejected_at", functions.current_timestamp())
        )
        failed = check_failures if failed is None else failed.unionByName(check_failures, allowMissingColumns=True)

    if failed is not None and failed.limit(1).count() > 0:
        failed.write.format("delta").mode("append").save(str(rejected_records_path(delta_root)))


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
                "pipeline_step": "quality_checks",
                "status": status,
                "row_count": row_count,
                "message": message,
                "created_at": now,
            }
        ]
    )
    log_df.write.format("delta").mode("append").save(str(pipeline_run_log_path(delta_root)))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run data quality checks against Silver Delta.")
    parser.add_argument("--config", default="config/pipeline.yml", help="Pipeline config path.")
    parser.add_argument("--run-id", default=None, help="Optional pipeline run id.")
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Do not fail process exit on critical data quality failures.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    run_id = args.run_id or f"dq-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    spark = create_spark()
    try:
        results = run_silver_quality_checks(
            spark=spark,
            delta_root=config.delta_root,
            run_id=run_id,
            fail_on_critical=config.fail_on_critical and not args.warn_only,
        )
        failed = [result for result in results if result["status"] == "fail"]
        print(f"Completed {len(results)} checks with {len(failed)} failures for run_id={run_id}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
