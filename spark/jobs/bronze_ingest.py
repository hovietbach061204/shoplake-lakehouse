"""
M2: load the raw Olist CSVs from the landing bucket into bronze Iceberg tables.

    landing (MinIO, raw CSV)                 warehouse (MinIO, Iceberg)
    s3a://landing/olist/olist_orders_dataset.csv  ->  iceberg.bronze.orders
    ...                                              ...

Bronze rules used here:
  * Keep the data as it arrived: every column stays a STRING. Casting to real
    types happens in silver (M4), where a bad value becomes a test failure
    instead of a crashed load.
  * Add lineage columns: _source_file (which file) and _ingested_at (when).
  * Idempotent: each run REPLACES the table (createOrReplace). Running the job
    twice gives the same result, not duplicated rows. (M3 switches to
    incremental loads with MERGE.)

Run it:
    docker compose exec spark spark-submit spark/jobs/bronze_ingest.py
    docker compose exec spark spark-submit spark/jobs/bronze_ingest.py orders customers
"""
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LANDING = "s3a://landing/olist"
NAMESPACE = "iceberg.bronze"

# bronze table name -> raw file name in the landing bucket
SOURCES = {
    "orders":         "olist_orders_dataset.csv",
    "order_items":    "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews":  "olist_order_reviews_dataset.csv",
    "customers":      "olist_customers_dataset.csv",
    "sellers":        "olist_sellers_dataset.csv",
    "products":       "olist_products_dataset.csv",
    "geolocation":    "olist_geolocation_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}


def read_raw_csv(spark, path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)   # bronze: all strings, no guessing
        # Reviews contain free text with line breaks and quotes inside fields.
        # Without these two options one review becomes several broken rows.
        .option("multiLine", True)
        .option("escape", '"')
        .csv(path)
    )


def load_table(spark, table, file_name):
    path = f"{LANDING}/{file_name}"
    started = time.time()

    df = (
        read_raw_csv(spark, path)
        .withColumn("_source_file", F.lit(path))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    # DataFrameWriterV2: create the Iceberg table, or atomically replace it.
    # Replace = one new snapshot; readers never see a half-written table.
    (
        df.writeTo(f"{NAMESPACE}.{table}")
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    rows = spark.table(f"{NAMESPACE}.{table}").count()
    print(f"  {table:<36} {rows:>9,} rows   {time.time() - started:5.1f}s")
    return rows


def main(selected):
    spark = SparkSession.builder.appName("shoplake-bronze-ingest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE} "
              "LOCATION 's3://warehouse/bronze'")

    unknown = [t for t in selected if t not in SOURCES]
    if unknown:
        sys.exit(f"Unknown table(s): {unknown}. Choose from {list(SOURCES)}")

    print(f"Loading {len(selected)} table(s) from {LANDING} into {NAMESPACE}:")
    for table in selected:
        load_table(spark, table, SOURCES[table])

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:] or list(SOURCES))
