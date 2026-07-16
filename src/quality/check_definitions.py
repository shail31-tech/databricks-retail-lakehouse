from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


Severity = Literal["critical", "warning"]


@dataclass(frozen=True)
class DataQualityCheck:
    name: str
    table_name: str
    severity: Severity
    description: str
    failure_expression: str


SILVER_ORDER_CHECKS = [
    DataQualityCheck(
        name="silver_order_id_not_null",
        table_name="silver.orders",
        severity="critical",
        description="Every Silver order row must have a natural key.",
        failure_expression="order_id IS NULL OR trim(order_id) = ''",
    ),
    DataQualityCheck(
        name="silver_no_duplicate_order_ids",
        table_name="silver.orders",
        severity="critical",
        description="Silver must contain at most one current row per order_id.",
        failure_expression="__duplicate_order_id = true",
    ),
    DataQualityCheck(
        name="silver_updated_at_not_null",
        table_name="silver.orders",
        severity="critical",
        description="Every Silver row must have an updated_at timestamp for merge ordering.",
        failure_expression="updated_at IS NULL",
    ),
    DataQualityCheck(
        name="silver_quantity_not_zero",
        table_name="silver.orders",
        severity="critical",
        description="UCI rows can be positive sales or negative cancellations, but quantity should not be zero.",
        failure_expression="quantity = 0 OR quantity IS NULL",
    ),
    DataQualityCheck(
        name="silver_unit_price_non_negative",
        table_name="silver.orders",
        severity="critical",
        description="Unit price should not be negative.",
        failure_expression="unit_price < 0 OR unit_price IS NULL",
    ),
    DataQualityCheck(
        name="silver_invoice_no_present",
        table_name="silver.orders",
        severity="warning",
        description="Invoice number should be present for lineage back to the public dataset.",
        failure_expression="invoice_no IS NULL OR trim(invoice_no) = ''",
    ),
    DataQualityCheck(
        name="silver_country_present",
        table_name="silver.orders",
        severity="warning",
        description="Country should be populated for country-level Gold aggregations.",
        failure_expression="country IS NULL OR trim(country) = ''",
    ),
]


def check_names() -> list[str]:
    return [check.name for check in SILVER_ORDER_CHECKS]
