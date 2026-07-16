from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from uuid import uuid4

from src.common.config import load_config
from src.ingest.bronze_orders import ingest_bronze_orders
from src.ingest.download_public_dataset import download_public_dataset
from src.ingest.generate_public_orders import write_public_order_events
from src.orchestration.backfill import build_run_id, create_spark
from src.quality.run_quality_checks import run_silver_quality_checks
from src.transform.gold_aggregates import build_gold_tables
from src.transform.silver_orders import merge_silver_orders


def run_daily_pipeline(
    run_date: date,
    limit: int,
    config_path: str,
    run_prefix: str,
    include_edge_cases: bool = True,
) -> dict[str, object]:
    config = load_config(config_path)
    source_excel_path = download_public_dataset(config_path=config_path)
    landing_file = write_public_order_events(
        event_date=run_date,
        source_excel_path=source_excel_path,
        landing_root=config.landing_path,
        limit=limit,
        batch_id=build_run_id(run_prefix, "landing", run_date),
        include_edge_cases=include_edge_cases,
    )

    spark = create_spark("retail-lakehouse-daily")
    try:
        bronze_rows = ingest_bronze_orders(
            spark=spark,
            landing_path=config.landing_path,
            delta_root=config.delta_root,
            batch_id=build_run_id(run_prefix, "bronze", run_date),
            source_path=landing_file,
        )
        silver_rows = merge_silver_orders(
            spark=spark,
            delta_root=config.delta_root,
            run_id=build_run_id(run_prefix, "silver", run_date),
        )
        quality_results = run_silver_quality_checks(
            spark=spark,
            delta_root=config.delta_root,
            run_id=build_run_id(run_prefix, "dq", run_date),
            fail_on_critical=config.fail_on_critical,
        )
        gold_counts = build_gold_tables(
            spark=spark,
            delta_root=config.delta_root,
            run_id=build_run_id(run_prefix, "gold", run_date),
        )
    finally:
        spark.stop()

    return {
        "run_date": run_date.isoformat(),
        "run_prefix": run_prefix,
        "landing_file": str(landing_file),
        "bronze_rows": bronze_rows,
        "silver_staged_rows": silver_rows,
        "quality_failures": sum(1 for result in quality_results if result["status"] == "fail"),
        "gold_counts": gold_counts,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one daily retail lakehouse pipeline.")
    parser.add_argument("--date", required=True, help="Business date to process, YYYY-MM-DD.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum public rows to emit.")
    parser.add_argument("--config", default="config/pipeline.yml", help="Pipeline config path.")
    parser.add_argument("--run-prefix", default=None, help="Optional run prefix.")
    parser.add_argument("--no-edge-cases", action="store_true", help="Disable demo duplicate/correction/late events.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = date.fromisoformat(args.date)
    run_prefix = args.run_prefix or f"daily-{run_date.isoformat()}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    summary = run_daily_pipeline(
        run_date=run_date,
        limit=args.limit,
        config_path=args.config,
        run_prefix=run_prefix,
        include_edge_cases=not args.no_edge_cases,
    )
    print(summary)


if __name__ == "__main__":
    main()
