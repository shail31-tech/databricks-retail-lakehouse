from pathlib import Path

from src.common.config import load_config
from src.transform.gold_aggregates import GOLD_TABLE_PATHS, gold_table_path


def test_config_exposes_gold_table_names() -> None:
    config = load_config("config/pipeline.yml")

    assert config.gold_daily_sales_table == "gold.daily_sales"
    assert config.gold_product_performance_table == "gold.product_performance"
    assert config.gold_country_revenue_table == "gold.country_revenue"
    assert config.gold_order_status_summary_table == "gold.order_status_summary"
    assert config.gold_pipeline_health_table == "gold.pipeline_health"


def test_gold_table_paths_match_layer_layout() -> None:
    root = Path("data/delta")

    assert gold_table_path(root, "daily_sales") == Path("data/delta/gold/daily_sales")
    assert gold_table_path(root, "product_performance") == Path("data/delta/gold/product_performance")
    assert gold_table_path(root, "country_revenue") == Path("data/delta/gold/country_revenue")
    assert gold_table_path(root, "order_status_summary") == Path("data/delta/gold/order_status_summary")
    assert gold_table_path(root, "pipeline_health") == Path("data/delta/gold/pipeline_health")


def test_gold_table_contract_includes_dashboard_tables() -> None:
    assert set(GOLD_TABLE_PATHS) == {
        "daily_sales",
        "product_performance",
        "country_revenue",
        "order_status_summary",
        "pipeline_health",
    }
