from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


PUBLIC_SOURCE_SYSTEM = "uci_online_retail"


def row_to_order_event(row: dict[str, Any], arrival_date: date | None = None) -> dict[str, Any]:
    invoice_no = clean_string(row.get("InvoiceNo"))
    stock_code = clean_string(row.get("StockCode"))
    invoice_date = parse_datetime(row.get("InvoiceDate"))
    quantity = parse_int(row.get("Quantity"))
    unit_price = parse_decimal(row.get("UnitPrice"))
    customer_id = clean_string(row.get("CustomerID"))
    country = clean_string(row.get("Country"))
    description = clean_string(row.get("Description"))
    is_cancellation = invoice_no.upper().startswith("C")
    event_date = arrival_date or invoice_date.date()

    line_key = build_line_key(invoice_no=invoice_no, stock_code=stock_code, invoice_date=invoice_date)
    updated_at = datetime.combine(event_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(
        hours=12
    )

    return {
        "order_id": line_key,
        "invoice_no": invoice_no,
        "stock_code": stock_code,
        "customer_id": customer_id,
        "product_id": stock_code,
        "product_name": description,
        "order_status": "cancelled" if is_cancellation else "completed",
        "quantity": quantity,
        "unit_price": str(unit_price),
        "currency": "GBP",
        "channel": "online_retail",
        "country": country,
        "is_cancellation": is_cancellation,
        "event_time": invoice_date.isoformat().replace("+00:00", "Z"),
        "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
        "source_system": PUBLIC_SOURCE_SYSTEM,
    }


def add_pipeline_edge_cases(events: list[dict[str, Any]], event_date: date) -> list[dict[str, Any]]:
    if not events:
        return []

    output = [dict(event) for event in events]

    # Duplicate replay: same natural key and timestamp. Silver should ignore it.
    output.append(dict(output[0]))

    if len(output) > 1:
        # Upstream correction: same key, newer updated_at, changed quantity.
        correction = dict(output[1])
        correction["quantity"] = abs(int(correction["quantity"])) + 1
        correction["updated_at"] = (
            parse_datetime(correction["updated_at"]) + timedelta(hours=4)
        ).isoformat().replace("+00:00", "Z")
        correction["source_system"] = f"{PUBLIC_SOURCE_SYSTEM}_correction"
        output.append(correction)

    if len(output) > 2:
        # Late arrival: original event date stays historical, but updated_at lands in this batch.
        late = dict(output[2])
        late["updated_at"] = (
            datetime.combine(event_date, datetime.min.time(), tzinfo=timezone.utc)
            + timedelta(hours=23, minutes=30)
        ).isoformat().replace("+00:00", "Z")
        late["source_system"] = f"{PUBLIC_SOURCE_SYSTEM}_late_arrival"
        output.append(late)

    return output


def build_line_key(invoice_no: str, stock_code: str, invoice_date: datetime) -> str:
    timestamp = invoice_date.strftime("%Y%m%d%H%M%S")
    return f"{invoice_no}|{stock_code}|{timestamp}"


def clean_string(value: Any) -> str:
    if value is None or is_nan(value):
        return ""
    return str(value).strip()


def parse_int(value: Any) -> int:
    if value is None or is_nan(value):
        return 0
    return int(value)


def parse_decimal(value: Any) -> Decimal:
    if value is None or is_nan(value):
        return Decimal("0.00")
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_nan(value: Any) -> bool:
    return isinstance(value, float) and math.isnan(value)
