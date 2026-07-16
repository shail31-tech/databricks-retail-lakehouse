from datetime import date

import pytest

from src.orchestration.backfill import (
    BackfillPlan,
    build_backfill_prefix,
    build_run_id,
    iter_dates,
)


def test_iter_dates_includes_start_and_end_dates() -> None:
    assert list(iter_dates(date(2010, 12, 1), date(2010, 12, 3))) == [
        date(2010, 12, 1),
        date(2010, 12, 2),
        date(2010, 12, 3),
    ]


def test_iter_dates_rejects_invalid_range() -> None:
    with pytest.raises(ValueError, match="end_date"):
        list(iter_dates(date(2010, 12, 3), date(2010, 12, 1)))


def test_backfill_plan_exposes_date_sequence() -> None:
    plan = BackfillPlan(
        start_date=date(2010, 12, 1),
        end_date=date(2010, 12, 2),
        limit_per_day=100,
        run_prefix="demo",
        include_edge_cases=True,
    )

    assert plan.dates == [date(2010, 12, 1), date(2010, 12, 2)]


def test_backfill_run_ids_are_deterministic_per_step_and_date() -> None:
    assert build_run_id("demo", "silver", date(2010, 12, 1)) == "demo-silver-2010-12-01"
    assert build_run_id("demo", "gold", date(2010, 12, 1)) == "demo-gold-2010-12-01"


def test_backfill_prefix_can_be_explicit_or_derived() -> None:
    assert build_backfill_prefix(date(2010, 12, 1), date(2010, 12, 7), "manual") == "manual"
    assert build_backfill_prefix(date(2010, 12, 1), date(2010, 12, 7)) == (
        "backfill-2010-12-01-to-2010-12-07"
    )
