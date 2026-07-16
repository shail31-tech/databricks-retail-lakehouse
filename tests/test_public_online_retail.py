from datetime import date, datetime

from src.ingest.public_online_retail import (
    PUBLIC_SOURCE_SYSTEM,
    add_pipeline_edge_cases,
    build_line_key,
    row_to_order_event,
)


def sample_row() -> dict[str, object]:
    return {
        "InvoiceNo": "536365",
        "StockCode": "85123A",
        "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
        "Quantity": 6,
        "InvoiceDate": datetime(2010, 12, 1, 8, 26),
        "UnitPrice": 2.55,
        "CustomerID": 17850,
        "Country": "United Kingdom",
    }


def test_public_row_maps_to_pipeline_order_event_schema() -> None:
    event = row_to_order_event(sample_row(), arrival_date=date(2010, 12, 1))

    assert event["order_id"] == "536365|85123A|20101201082600"
    assert event["invoice_no"] == "536365"
    assert event["stock_code"] == "85123A"
    assert event["product_id"] == "85123A"
    assert event["product_name"] == "WHITE HANGING HEART T-LIGHT HOLDER"
    assert event["customer_id"] == "17850"
    assert event["country"] == "United Kingdom"
    assert event["currency"] == "GBP"
    assert event["source_system"] == PUBLIC_SOURCE_SYSTEM


def test_invoice_starting_with_c_is_marked_cancelled() -> None:
    row = sample_row()
    row["InvoiceNo"] = "C536365"

    event = row_to_order_event(row, arrival_date=date(2010, 12, 1))

    assert event["is_cancellation"] is True
    assert event["order_status"] == "cancelled"


def test_edge_case_simulator_adds_duplicate_correction_and_late_arrival() -> None:
    events = [
        row_to_order_event(sample_row(), arrival_date=date(2010, 12, 1)),
        row_to_order_event({**sample_row(), "StockCode": "71053"}, arrival_date=date(2010, 12, 1)),
        row_to_order_event({**sample_row(), "StockCode": "84406B"}, arrival_date=date(2010, 12, 1)),
    ]

    output = add_pipeline_edge_cases(events, event_date=date(2010, 12, 2))

    assert len(output) == 6
    assert output[0] == output[3]
    assert output[4]["source_system"].endswith("_correction")
    assert output[5]["source_system"].endswith("_late_arrival")


def test_line_key_uses_invoice_stock_and_timestamp() -> None:
    assert build_line_key("536365", "85123A", datetime(2010, 12, 1, 8, 26)) == (
        "536365|85123A|20101201082600"
    )
