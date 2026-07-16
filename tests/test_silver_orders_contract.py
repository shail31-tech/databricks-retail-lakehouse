from pathlib import Path

from src.common.config import load_config
from src.transform.silver_orders import (
    MATCHED_UPDATE_CONDITION,
    MERGE_CONDITION,
    SILVER_ORDER_COLUMNS,
    bronze_orders_path,
    pipeline_run_log_path,
    silver_orders_path,
)


def test_config_exposes_silver_table_name() -> None:
    config = load_config("config/pipeline.yml")

    assert config.silver_orders_table == "silver.orders"


def test_delta_paths_match_medallion_layout() -> None:
    root = Path("data/delta")

    assert bronze_orders_path(root) == Path("data/delta/bronze/orders_raw")
    assert silver_orders_path(root) == Path("data/delta/silver/orders")
    assert pipeline_run_log_path(root) == Path("data/delta/ops/pipeline_run_log")


def test_merge_contract_updates_only_newer_source_records() -> None:
    assert MERGE_CONDITION == "target.order_id = source.order_id"
    assert MATCHED_UPDATE_CONDITION == "source.updated_at > target.updated_at"


def test_silver_schema_keeps_business_and_audit_columns() -> None:
    required_columns = {
        "order_id",
        "invoice_no",
        "stock_code",
        "customer_id",
        "product_id",
        "country",
        "is_cancellation",
        "quantity",
        "unit_price",
        "event_time",
        "updated_at",
        "bronze_batch_id",
        "bronze_ingested_at",
        "order_amount",
        "silver_updated_at",
    }

    assert required_columns.issubset(SILVER_ORDER_COLUMNS)
