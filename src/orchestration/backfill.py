from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from src.common.config import PipelineConfig, load_config
from src.common.spark import create_spark_session
from src.ingest.bronze_orders import ingest_bronze_orders
from src.ingest.generate_public_orders import write_public_order_events
from src.quality.run_quality_checks import run_silver_quality_checks
from src.transform.gold_aggregates import build_gold_tables
from src.transform.silver_orders import merge_silver_orders


DEFAULT_BACKFILL_PREFIX = "backfill"


@dataclass(frozen=True)
class BackfillPlan:
    start_date: date
    end_date: date
    limit_per_day: int
    run_prefix: str
    include_edge_cases: bool

    @property
    def dates(self) -> list[date]:
        return list(iter_dates(self.start_date, self.end_date))


def iter_dates(start_date: date, end_date: date) -> Iterable[date]:
    if end_date < start_date:
        raise ValueError("end_date must be greater than or equal to start_date")

    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def build_run_id(prefix: str, step: str, run_date: date) -> str:
    return f"{prefix}-{step}-{run_date.isoformat()}"


def build_backfill_prefix(start_date: date, end_date: date, explicit_prefix: str | None = None) -> str:
    if explicit_prefix:
        return explicit_prefix
    return f"{DEFAULT_BACKFILL_PREFIX}-{start_date.isoformat()}-to-{end_date.isoformat()}"


def create_spark(app_name: str = "retail-lakehouse-backfill") -> Any:
    return create_spark_session(app_name)


def run_backfill(config: PipelineConfig, plan: BackfillPlan, spark: Any) -> dict[str, Any]:
    source_excel_path = config.source_path / config.dataset_excel_file
    summary: dict[str, Any] = {
        "run_prefix": plan.run_prefix,
        "start_date": plan.start_date.isoformat(),
        "end_date": plan.end_date.isoformat(),
        "dates": [],
    }

    for run_date in plan.dates:
        landing_batch_id = build_run_id(plan.run_prefix, "landing", run_date)
        landing_file = write_public_order_events(
            event_date=run_date,
            source_excel_path=source_excel_path,
            landing_root=config.landing_path,
            limit=plan.limit_per_day,
            batch_id=landing_batch_id,
            include_edge_cases=plan.include_edge_cases,
        )

        bronze_count = ingest_bronze_orders(
            spark=spark,
            landing_path=config.landing_path,
            delta_root=config.delta_root,
            batch_id=build_run_id(plan.run_prefix, "bronze", run_date),
            source_path=landing_file,
        )
        silver_count = merge_silver_orders(
            spark=spark,
            delta_root=config.delta_root,
            run_id=build_run_id(plan.run_prefix, "silver", run_date),
        )
        quality_results = run_silver_quality_checks(
            spark=spark,
            delta_root=config.delta_root,
            run_id=build_run_id(plan.run_prefix, "dq", run_date),
            fail_on_critical=config.fail_on_critical,
        )
        gold_counts = build_gold_tables(
            spark=spark,
            delta_root=config.delta_root,
            run_id=build_run_id(plan.run_prefix, "gold", run_date),
        )

        summary["dates"].append(
            {
                "date": run_date.isoformat(),
                "landing_batch_id": landing_batch_id,
                "bronze_rows": bronze_count,
                "silver_staged_rows": silver_count,
                "quality_failures": sum(1 for result in quality_results if result["status"] == "fail"),
                "gold_counts": gold_counts,
            }
        )

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill the retail lakehouse pipeline for a date range.")
    parser.add_argument("--start-date", required=True, help="First date to backfill, YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="Last date to backfill, YYYY-MM-DD.")
    parser.add_argument("--limit-per-day", type=int, default=1000, help="Maximum public rows to emit per date.")
    parser.add_argument("--config", default="config/pipeline.yml", help="Pipeline config path.")
    parser.add_argument("--run-prefix", default=None, help="Optional deterministic prefix for all step run ids.")
    parser.add_argument(
        "--no-edge-cases",
        action="store_true",
        help="Disable duplicate/correction/late-arrival demo records during backfill.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    prefix = build_backfill_prefix(
        start_date=start_date,
        end_date=end_date,
        explicit_prefix=args.run_prefix,
    )
    if args.run_prefix is None:
        prefix = f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"

    plan = BackfillPlan(
        start_date=start_date,
        end_date=end_date,
        limit_per_day=args.limit_per_day,
        run_prefix=prefix,
        include_edge_cases=not args.no_edge_cases,
    )
    spark = create_spark()
    try:
        summary = run_backfill(config=config, plan=plan, spark=spark)
        print(summary)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
