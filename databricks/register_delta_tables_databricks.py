from __future__ import annotations

import argparse
import sys


PROJECT_ROOT = "/Workspace/Users/shailus2002@gmail.com/retail-lakehouse-files"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.common.config import load_config
from src.common.spark import create_spark_session


TABLE_LOCATIONS = {
    "workspace.bronze.orders_raw": "bronze/orders_raw",
    "workspace.silver.orders": "silver/orders",
    "workspace.gold.daily_sales": "gold/daily_sales",
    "workspace.gold.product_performance": "gold/product_performance",
    "workspace.gold.country_revenue": "gold/country_revenue",
    "workspace.gold.order_status_summary": "gold/order_status_summary",
    "workspace.gold.pipeline_health": "gold/pipeline_health",
    "workspace.ops.dq_check_results": "ops/dq_check_results",
    "workspace.ops.pipeline_run_log": "ops/pipeline_run_log",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register Delta paths as Unity Catalog tables.")
    parser.add_argument("--config", required=True, help="Pipeline config path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    spark = create_spark_session("retail-lakehouse-register-tables")
    try:
        for schema in ["bronze", "silver", "gold", "ops"]:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS workspace.{schema}")

        for table_name, relative_path in TABLE_LOCATIONS.items():
            table_path = config.delta_root / relative_path
            dataframe = spark.read.format("delta").load(str(table_path))
            (
                dataframe.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(table_name)
            )
            print(f"Created managed table {table_name} from {table_path}")

        spark.sql("SHOW TABLES IN workspace.gold").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
