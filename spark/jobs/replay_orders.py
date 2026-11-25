"""
M3: the "source system" simulator.

Olist gives us ONE final snapshot of every order. Real sources don't work that
way: every day the order service exports the orders that CHANGED that day,
with their status as it was at the end of that day. This job fakes exactly
that, one day at a time, from the full table we loaded in M2:

    iceberg.bronze.orders (final state of 99k orders)
        -> s3a://landing/olist/orders_daily/dt=2017-03-01/part-...csv
        -> s3a://landing/olist/orders_daily/dt=2017-03-02/part-...csv
        ...

For day D, an order is in the extract if one of its lifecycle events happened
on D (purchase, approval, handed to carrier, delivered). Its row shows the
order AS OF the end of D:
  * timestamps that happen after D are blanked out (not known yet),
  * order_status is derived from the events so far:
        created -> approved -> shipped -> delivered
    and the final status (e.g. canceled) appears on the order's last event day,
  * updated_at = the latest event time so far (the source's "last modified").

So the same order shows up on several days with a newer status each time.
That is what forces us to MERGE (upsert) instead of append.

Run:
    docker compose exec spark spark-submit spark/jobs/replay_orders.py --start 2017-03-01 --days 7
Re-running a day overwrites that day's folder (idempotent).
"""
import argparse
import datetime as dt
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE_TABLE = "iceberg.bronze.orders"
LANDING = "s3a://landing/olist/orders_daily"

EVENTS = [  # (column, status reached when this event has happened)
    ("order_approved_at", "approved"),
    ("order_delivered_carrier_date", "shipped"),
    ("order_delivered_customer_date", "delivered"),
]
TS_COLS = ["order_purchase_timestamp"] + [c for c, _ in EVENTS]


def day_extract(orders, day):
    end_of_day = F.lit(f"{day} 23:59:59").cast("timestamp")

    # 1. The orders that had at least one event on this day
    on_day = [F.to_date(c) == F.lit(day).cast("date") for c in TS_COLS]
    touched = orders.where(reduce(lambda a, b: a | b, on_day))

    # 2. Blank out what had not happened yet at the end of the day
    as_of = touched
    for c in TS_COLS:
        as_of = as_of.withColumn(c, F.when(F.col(c) <= end_of_day, F.col(c)))

    # 3. Status as of the end of the day
    derived = F.lit("created")
    for col, status in EVENTS:  # later events win
        derived = F.when(F.col(col).isNotNull(), F.lit(status)).otherwise(derived)
    last_event = F.greatest(*[F.col(c) for c in TS_COLS])
    final_event = F.col("_final_last_event")
    status = F.when(last_event >= final_event, F.col("order_status")).otherwise(derived)

    return (
        as_of.withColumn("order_status", status)
        .withColumn("updated_at", last_event)
        .select("order_id", "customer_id", "order_status", *TS_COLS,
                "order_estimated_delivery_date", "updated_at")
    )


def main(start, days):
    spark = SparkSession.builder.appName("shoplake-replay-orders").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    orders = spark.table(SOURCE_TABLE)
    for c in TS_COLS:  # bronze keeps strings; the simulator needs real timestamps
        orders = orders.withColumn(c, F.to_timestamp(c))
    orders = orders.withColumn("_final_last_event", F.greatest(*[F.col(c) for c in TS_COLS])).cache()

    for i in range(days):
        day = (start + dt.timedelta(days=i)).isoformat()
        out = f"{LANDING}/dt={day}"
        extract = day_extract(orders, day)
        (extract.coalesce(1)                      # one file per day, like a real export
         .write.mode("overwrite")
         .option("header", True)
         .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
         .csv(out))
        print(f"  {day}: {spark.read.option('header', True).csv(out).count():>5} changed orders -> {out}")

    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, type=dt.date.fromisoformat)
    p.add_argument("--days", type=int, default=1)
    a = p.parse_args()
    main(a.start, a.days)
