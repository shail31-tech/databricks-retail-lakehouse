# Dataset

## Primary Source

This project uses the UCI Online Retail dataset:

- Source: https://archive.ics.uci.edu/dataset/352/online%2Bretail
- Dataset name: Online Retail
- Provider: UCI Machine Learning Repository
- Rows: 541,909
- Period: December 1, 2010 to December 9, 2011
- License: Creative Commons Attribution 4.0 International, according to UCI

## Source Columns

- `InvoiceNo`
- `StockCode`
- `Description`
- `Quantity`
- `InvoiceDate`
- `UnitPrice`
- `CustomerID`
- `Country`

## Pipeline Mapping

The UCI dataset is invoice-line oriented. The pipeline maps each source row into a lakehouse order event.

| UCI field | Pipeline field |
| --- | --- |
| `InvoiceNo` | `invoice_no` |
| `StockCode` | `stock_code`, `product_id` |
| `Description` | `product_name` |
| `Quantity` | `quantity` |
| `InvoiceDate` | `event_time` |
| `UnitPrice` | `unit_price` |
| `CustomerID` | `customer_id` |
| `Country` | `country` |

The pipeline creates a deterministic line-level natural key:

```text
order_id = InvoiceNo + StockCode + InvoiceDate
```

This keeps the existing Silver `MERGE` contract simple while still using real public data.

## Event Simulation

The public dataset is historical and static, so the project adds a small event simulation layer for data engineering demos:

- duplicate replay records
- corrected records with newer `updated_at`
- late-arriving records

These simulated events are derived from public rows. They exist only to demonstrate incremental merge, idempotency, and backfill behavior.
