"""First look at the raw data with DuckDB, before any pipeline exists.

Why do this in M0: every later design choice (partition column, primary keys,
which table needs SCD2, what the watermark column is) comes from facts about
the data. We check those facts here instead of guessing.
DuckDB docs for reading CSV: https://duckdb.org/docs/data/csv/overview
"""
from pathlib import Path
import duckdb

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
con = duckdb.connect()


def q(title: str, sql: str) -> None:
    print(f"\n-- {title}")
    print(con.sql(sql))


tables = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
}
for name, file in tables.items():
    con.sql(f"CREATE VIEW {name} AS SELECT * FROM read_csv_auto('{RAW / file}')")

q("1. Row counts", " UNION ALL ".join(
    f"SELECT '{n}' AS table_name, count(*) AS rows FROM {n}" for n in tables))

q("2. Is order_id unique in orders? (decides the primary key)",
  "SELECT count(*) AS rows, count(DISTINCT order_id) AS distinct_ids FROM orders")

q("3. Grain of order_items: (order_id, order_item_id) should be unique",
  "SELECT count(*) AS rows, count(DISTINCT (order_id, order_item_id)) AS distinct_keys FROM order_items")

q("4. Date range of purchases (this is our watermark column later)",
  "SELECT min(order_purchase_timestamp) AS first, max(order_purchase_timestamp) AS last FROM orders")

q("5. Orders per month (helps pick a partition granularity)",
  """SELECT date_trunc('month', order_purchase_timestamp) AS month, count(*) AS orders
     FROM orders GROUP BY 1 ORDER BY 1""")

q("6. Order status mix (statuses change over time, hence MERGE in M3)",
  "SELECT order_status, count(*) AS n FROM orders GROUP BY 1 ORDER BY n DESC")

q("7. customer_id vs customer_unique_id (one person, many customer_ids?)",
  "SELECT count(DISTINCT customer_id) AS customer_ids, count(DISTINCT customer_unique_id) AS people FROM customers")

q("8. Nulls in delivery dates (data quality tests in M4)",
  """SELECT count(*) FILTER (WHERE order_delivered_customer_date IS NULL) AS no_delivery_date,
            count(*) FILTER (WHERE order_status = 'delivered' AND order_delivered_customer_date IS NULL) AS delivered_but_no_date
     FROM orders""")
