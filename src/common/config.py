from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class PipelineConfig:
    project_name: str
    timezone: str
    source_path: Path
    landing_path: Path
    delta_root: Path
    dataset_url: str
    dataset_archive_file: str
    dataset_excel_file: str
    bronze_orders_table: str
    silver_orders_table: str
    gold_daily_sales_table: str
    gold_product_performance_table: str
    gold_country_revenue_table: str
    gold_order_status_summary_table: str
    gold_pipeline_health_table: str
    dq_check_results_table: str
    pipeline_run_log_table: str
    fail_on_critical: bool
    freshness_hours: int


def load_config(config_path: str | Path = "config/pipeline.yml") -> PipelineConfig:
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)

    return PipelineConfig(
        project_name=raw["project"]["name"],
        timezone=raw["project"]["timezone"],
        source_path=Path(raw["local_paths"]["source"]),
        landing_path=Path(raw["local_paths"]["landing"]),
        delta_root=Path(raw["local_paths"]["delta_root"]),
        dataset_url=raw["dataset"]["url"],
        dataset_archive_file=raw["dataset"]["archive_file"],
        dataset_excel_file=raw["dataset"]["excel_file"],
        bronze_orders_table=raw["tables"]["bronze_orders"],
        silver_orders_table=raw["tables"]["silver_orders"],
        gold_daily_sales_table=raw["tables"]["gold_daily_sales"],
        gold_product_performance_table=raw["tables"]["gold_product_performance"],
        gold_country_revenue_table=raw["tables"]["gold_country_revenue"],
        gold_order_status_summary_table=raw["tables"]["gold_order_status_summary"],
        gold_pipeline_health_table=raw["tables"]["gold_pipeline_health"],
        dq_check_results_table=raw["tables"]["dq_check_results"],
        pipeline_run_log_table=raw["tables"]["pipeline_run_log"],
        fail_on_critical=bool(raw["quality"]["fail_on_critical"]),
        freshness_hours=int(raw["quality"]["freshness_hours"]),
    )
