"""
M3: incremental load of the daily order extracts, with a watermark and MERGE.

    s3a://landing/olist/orders_daily/dt=YYYY-MM-DD/   (one folder per day, from replay_orders.py)
          |
          |  1. find days newer than the watermark
          v
    iceberg.bronze.orders_changes        APPEND-ONLY history: every row of every extract,
                                         partitioned by _batch_date (one partition per day)
          |
          |  2. MERGE INTO (upsert on order_id, newest updated_at wins)
          v
    iceberg.bronze.orders_current_cow    one row per order, latest state   (copy-on-write)
    iceberg.bronze.orders_current_mor    the same table, merge-on-read     (to compare files)

Watermark = the newest _batch_date already in orders_changes. Each run loads
only the landing days after it, in date order. So a normal run is incremental,
and a re-run with nothing new does nothing.

Idempotency, two layers:
  * orders_changes: a day is written with overwritePartitions(), so re-loading
    a day REPLACES that day's partition instead of appending it twice.
  * MERGE only updates a row when the incoming updated_at is NEWER, so
    replaying an old day can never move an order back to an older status.

Run:
    docker compose exec spark spark-submit spark/jobs/orders_incremental.py            # all new days
    docker compose exec spark spark-submit spark/jobs/orders_incremental.py --date 2017-03-02   # re-load one day
"""
import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LANDING = "s3a://landing/olist/orders_daily"
NS = "iceberg.bronze"
CHANGES = f"{NS}.orders_changes"
TARGETS = {
    # table name -> how row-level changes are written (Iceberg write.*.mode)
    f"{NS}.orders_current_cow": "copy-on-write",
    f"{NS}.orders_current_mor": "merge-on-read",
}
TS_COLS = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
           "order_delivered_customer_date", "order_estimated_delivery_date", "updated_at"]

COLUMNS_DDL = """
    order_id                      STRING,
    customer_id                   STRING,
    order_status                  STRING,
    order_purchase_timestamp      TIMESTAMP,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    updated_at                    TIMESTAMP,
    _batch_date                   DATE,
    _ingested_at                  TIMESTAMP
"""


def create_tables(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS} LOCATION 's3://warehouse/bronze'")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CHANGES} ({COLUMNS_DDL})
        USING iceberg
        PARTITIONED BY (_batch_date)
        TBLPROPERTIES ('format-version' = '2')
    """)
    for table, mode in TARGETS.items():
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table} ({COLUMNS_DDL})
            USING iceberg
            PARTITIONED BY (months(order_purchase_timestamp))
            TBLPROPERTIES (
                'format-version'    = '2',
                'write.merge.mode'  = '{mode}',
                'write.update.mode' = '{mode}',
                'write.delete.mode' = '{mode}'
            )
        """)


def landing_days(spark):
    """List the dt=YYYY-MM-DD folders in the landing bucket (no data is read)."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(LANDING)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return []
    names = [s.getPath().getName() for s in fs.listStatus(path) if s.isDirectory()]
    return sorted(n.split("=", 1)[1] for n in names if n.startswith("dt="))


def watermark(spark):
    row = spark.sql(f"SELECT CAST(max(_batch_date) AS STRING) AS wm FROM {CHANGES}").first()
    return row.wm  # None on the very first run


def read_day(spark, day):
    df = spark.read.option("header", True).csv(f"{LANDING}/dt={day}")
    for c in TS_COLS:
        df = df.withColumn(c, F.to_timestamp(c))
    return (df.withColumn("_batch_date", F.lit(day).cast("date"))
              .withColumn("_ingested_at", F.current_timestamp()))


def load_day(spark, day):
    started = time.time()
    batch = read_day(spark, day).cache()
    n = batch.count()

    # 1. History: replace this day's partition (idempotent append)
    batch.writeTo(CHANGES).overwritePartitions()

    # 2. Latest state: upsert into both tables
    batch.createOrReplaceTempView("batch")
    for table in TARGETS:
        spark.sql(f"""
            MERGE INTO {table} AS t
            USING batch AS s
            ON t.order_id = s.order_id
            WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    batch.unpersist()
    print(f"  {day}: {n:>5} rows  ->  changes + MERGE into {len(TARGETS)} tables  ({time.time() - started:.1f}s)")


def main(only_date):
    spark = SparkSession.builder.appName("shoplake-orders-incremental").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)

    if only_date:
        todo = [only_date]
        print(f"Re-loading one day on request: {only_date}")
    else:
        wm = watermark(spark)
        todo = [d for d in landing_days(spark) if wm is None or d > wm]
        print(f"Watermark (newest day already loaded): {wm or 'none, first run'}")
        print(f"New days in landing: {len(todo)}")

    for day in todo:
        load_day(spark, day)

    for table in [CHANGES, *TARGETS]:
        rows = spark.table(table).count()
        print(f"  {table:<36} {rows:>7} rows")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="re-load exactly this landing day (YYYY-MM-DD)")
    main(p.parse_args().date)
