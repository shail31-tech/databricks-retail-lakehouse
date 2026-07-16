from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.common.config import load_config
from src.ingest.public_online_retail import add_pipeline_edge_cases, row_to_order_event


def write_public_order_events(
    event_date: date,
    source_excel_path: Path,
    landing_root: Path,
    limit: int = 1000,
    batch_id: str | None = None,
    include_edge_cases: bool = True,
) -> Path:
    batch = batch_id or f"batch-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    output_dir = landing_root / "orders" / f"event_date={event_date.isoformat()}" / f"batch_id={batch}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "orders.jsonl"

    rows = load_public_rows(source_excel_path=source_excel_path, event_date=event_date, limit=limit)
    events = [row_to_order_event(row, arrival_date=event_date) for row in rows]
    if include_edge_cases:
        events = add_pipeline_edge_cases(events, event_date=event_date)

    with output_path.open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event, sort_keys=True) + "\n")

    return output_path


def load_public_rows(source_excel_path: Path, event_date: date, limit: int) -> list[dict[str, object]]:
    if not source_excel_path.exists():
        raise FileNotFoundError(
            f"Missing public dataset file: {source_excel_path}. "
            "Run: python -m src.ingest.download_public_dataset"
        )

    columns = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    ]
    frame = pd.read_excel(source_excel_path, usecols=columns, engine="openpyxl")
    frame["InvoiceDate"] = pd.to_datetime(frame["InvoiceDate"])
    filtered = frame[frame["InvoiceDate"].dt.date == event_date]

    if filtered.empty:
        available_date = frame["InvoiceDate"].dt.date.min()
        filtered = frame[frame["InvoiceDate"].dt.date == available_date]

    return filtered.head(limit).to_dict(orient="records")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate raw order events from UCI Online Retail.")
    parser.add_argument("--date", required=True, help="Business date to generate, YYYY-MM-DD.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum source rows to emit.")
    parser.add_argument("--config", default="config/pipeline.yml", help="Pipeline config path.")
    parser.add_argument("--batch-id", default=None, help="Optional batch id for idempotent demos.")
    parser.add_argument(
        "--no-edge-cases",
        action="store_true",
        help="Disable duplicate/correction/late-arrival demo records.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    output_path = write_public_order_events(
        event_date=date.fromisoformat(args.date),
        source_excel_path=config.source_path / config.dataset_excel_file,
        landing_root=config.landing_path,
        limit=args.limit,
        batch_id=args.batch_id,
        include_edge_cases=not args.no_edge_cases,
    )
    print(f"Wrote UCI Online Retail order events to {output_path}")


if __name__ == "__main__":
    main()
