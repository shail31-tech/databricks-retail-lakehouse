from pathlib import Path

from src.common.config import load_config
from src.quality.check_definitions import SILVER_ORDER_CHECKS, check_names
from src.quality.run_quality_checks import dq_check_results_path, rejected_records_path


def test_config_exposes_quality_settings() -> None:
    config = load_config("config/pipeline.yml")

    assert config.dq_check_results_table == "ops.dq_check_results"
    assert config.fail_on_critical is True
    assert config.freshness_hours == 24


def test_quality_paths_match_ops_layout() -> None:
    root = Path("data/delta")

    assert dq_check_results_path(root) == Path("data/delta/ops/dq_check_results")
    assert rejected_records_path(root) == Path("data/delta/ops/rejected_records")


def test_silver_quality_checks_include_critical_merge_safety_checks() -> None:
    names = set(check_names())

    assert "silver_order_id_not_null" in names
    assert "silver_no_duplicate_order_ids" in names
    assert "silver_updated_at_not_null" in names


def test_silver_quality_checks_include_warning_lineage_checks() -> None:
    warning_checks = {check.name for check in SILVER_ORDER_CHECKS if check.severity == "warning"}

    assert "silver_invoice_no_present" in warning_checks
    assert "silver_country_present" in warning_checks


def test_quality_check_names_are_unique() -> None:
    names = check_names()

    assert len(names) == len(set(names))
