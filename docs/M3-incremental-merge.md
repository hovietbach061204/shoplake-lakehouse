# M3: Daily replay, incremental loads and `MERGE INTO`

**Goal:** stop reloading everything. A simulator drops one file per day of *changed* orders into the landing bucket, like a real source system would. A Spark job loads only the days it hasn't seen yet, keeps every version in a history table, and **upserts** the latest state of each order with `MERGE INTO`. You run the same MERGE on two tables, one **copy-on-write** and one **merge-on-read**, and compare the files each one writes. Finally you change a table's partitioning without rewriting it.

Time: 2 to 3 hours.

Prerequisite: M2 done (`iceberg.bronze.orders` loaded with the real Olist data).

---

## 0. The mental model

```
iceberg.bronze.orders (M2, final state of 99k orders)
        |
        |  make replay START=2017-03-01 DAYS=7      spark/jobs/replay_orders.py
        v                                            "the source system"
landing/olist/orders_daily/dt=2017-03-01/part-*.csv
landing/olist/orders_daily/dt=2017-03-02/part-*.csv  one file per day = the orders that CHANGED that day
        ...
        |
        |  make incremental                          spark/jobs/orders_incremental.py
        |    1. watermark = newest day already loaded
        |    2. for each newer day:
        v
iceberg.bronze.orders_changes        history, append-only, one partition per day (every version of every order)
        |
        |    3. MERGE INTO ... (newest updated_at wins)
        v
iceberg.bronze.orders_current_cow    one row per order, latest state, copy-on-write
iceberg.bronze.orders_current_mor    the same rows, merge-on-read
```

### Answers to the M2 questions

1. **Replacing the whole table every run** reads and rewrites all history to add one day. With 1,000 days, a run that should take seconds takes as long as the full history, costs 1,000× the I/O, and creates a full copy each time. Instead: load **only what's new** (incremental) and **change only the rows that changed** (MERGE).
2. **Append-only + status changes:** an order that goes `created → approved → shipped → delivered` gets **4 rows**. To find the latest, take the row with the max `updated_at` per `order_id`. That's exactly what `orders_changes` (all versions) vs `orders_current_*` (the latest one) gives you.
3. **`_ingested_at` vs `order_purchase_timestamp`:** `_ingested_at` is when *we* loaded the row; `order_purchase_timestamp` is when the *business event* happened. An incremental load filters on **when the data arrived or changed**: here the batch day (`_batch_date`) and the source's `updated_at`. Filtering on purchase time would miss an order bought last month and delivered today.

### Four concepts for this milestone

**Watermark.** A bookmark that says "I've loaded everything up to here". Our watermark is `max(_batch_date)` in `orders_changes`. It is stored **in the table itself**, not in a separate state file, so it can never disagree with the data: if a write fails, the watermark doesn't move, and the next run retries that day.

**Upsert with MERGE.** The source sends the same order many times with a newer status. Appending would create duplicates. `MERGE INTO` matches on a key and either updates the existing row or inserts a new one, in **one atomic Iceberg commit**:
```sql
MERGE INTO iceberg.bronze.orders_current_cow AS t
USING batch AS s                                  -- today's extract
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *   -- only if newer
WHEN NOT MATCHED THEN INSERT *
```
The `s.updated_at > t.updated_at` guard is what makes replays safe: loading an old day again can never move an order back to an older status. ([Iceberg: Spark MERGE INTO](https://iceberg.apache.org/docs/latest/spark-writes/#merge-into))

**History table vs current table.** Real pipelines usually keep both. The history (`orders_changes`) answers "what did we know, and when?" and lets you rebuild anything. The current table answers "what is the status now?" quickly. In M4, dbt builds silver from these.

**Copy-on-write vs merge-on-read.** Parquet files are immutable, so how do you "update" one row?
| | Copy-on-write (CoW) | Merge-on-read (MoR) |
| --- | --- | --- |
| On UPDATE/MERGE | Rewrite **every data file** that contains a changed row, with the new values | Write the **new row versions** in a new data file + a small **delete file** that says "row 17 of file X is dead" |
| Write cost | High (rewrites whole files for one changed row) | Low |
| Read cost | Low (just read the data files) | Higher (readers must apply delete files) |
| Needs compaction? | No | Yes, from time to time |
| Good for | Read-heavy tables, rare updates | Frequent small updates, streaming |

Iceberg chooses per table with the properties `write.merge.mode`, `write.update.mode`, `write.delete.mode` ([Iceberg write properties](https://iceberg.apache.org/docs/latest/configuration/#write-properties)). Our job creates the two tables with the two modes, then runs exactly the same MERGE on both. The delete files are the "position deletes" of the Iceberg v2 spec ([row-level deletes](https://iceberg.apache.org/spec/#row-level-deletes)). Iceberg v3 replaces them with compact *deletion vectors*: same idea, smaller files.

## 1. What's new in the repo

| File | What it does |
| --- | --- |
| `spark/jobs/replay_orders.py` | The source simulator: writes one CSV per day of changed orders, each "as of the end of that day" |
| `spark/jobs/orders_incremental.py` | The incremental job: watermark → history table → MERGE into the CoW and MoR tables |
| `sql/m3_inspect.sql` | Trino queries to compare the three tables, their files and their snapshots |
| `Makefile` | New targets: `replay`, `incremental`, `m3-inspect`, `compact` |

### How the simulator fakes daily extracts

Olist only gives the final state of each order. The replayer turns it back into a stream of daily changes. For day D, an order is in that day's file if one of its events happened on D (purchase, approval, handed to carrier, delivered). The row shows the order **as of the end of D**:
- timestamps after D are blanked (not known yet),
- `order_status` is derived from the events so far: `created → approved → shipped → delivered`, and the real final status (e.g. `canceled`) appears on the order's last event day,
- `updated_at` = the latest event time so far, like a `last_modified` column in a source database.

So the same order shows up on several days with a newer status each time. That's what forces MERGE.

### Why the incremental job casts timestamps (when bronze M2 kept strings)

M2's rule was "bronze keeps strings". Here we break it on purpose for a few columns: the MERGE condition compares `updated_at`, and the current tables are partitioned by `order_purchase_timestamp`. Both need real timestamps. Everything else about cleaning still waits for silver.

Spark's `TIMESTAMP` is stored as Iceberg `timestamptz`, which Trino shows as `timestamp(6) with time zone` in UTC.

## 2. Run it

```bash
cd /Volumes/SandiskSSD/Developer/Data_Engineering/ShopLake
git pull
make up && make ps        # everything healthy (Trino takes ~60 s)
```
No image rebuild is needed: the jobs folder is mounted into the Spark container.

### Step 1: replay a week of changes into landing

```bash
make replay START=2017-03-01 DAYS=7
# = docker compose exec spark spark-submit spark/jobs/replay_orders.py --start 2017-03-01 --days 7
```
You'll see one line per day:
```
  2017-03-01:   NNN changed orders -> s3a://landing/olist/orders_daily/dt=2017-03-01
  ...
```
Expect a few hundred orders per day in March 2017 with the real data (I tested with synthetic data, so your counts will differ from the examples in this guide).

Look at one file in the MinIO console (http://localhost:9001 → `landing` → `olist/orders_daily/dt=2017-03-01/`). Note:
- one CSV per day (plus an empty `_SUCCESS` marker that Spark writes when a job finishes),
- orders bought in February that were *approved/shipped/delivered* on March 1st are in it too,
- the later timestamps of each row are empty.

The `dt=YYYY-MM-DD` folder naming is the Hive convention for date partitions. Most tools (Spark, Trino's Hive connector, Athena) understand it.

### Step 2: the first incremental run

```bash
make incremental
# = docker compose exec spark spark-submit spark/jobs/orders_incremental.py
```
Expected output (shape):
```
Watermark (newest day already loaded): none, first run
New days in landing: 7
  2017-03-01:   NNN rows  ->  changes + MERGE into 2 tables  (4.2s)
  ...
  2017-03-07:   NNN rows  ->  changes + MERGE into 2 tables  (1.5s)
  iceberg.bronze.orders_changes            <sum of the 7 days>
  iceberg.bronze.orders_current_cow        <distinct orders>
  iceberg.bronze.orders_current_mor        <same number>
```
On the first run the job creates the three tables (`CREATE TABLE IF NOT EXISTS`). Read `create_tables()` in the job: the history table is `PARTITIONED BY (_batch_date)`, and the two current tables are `PARTITIONED BY (months(order_purchase_timestamp))` with different `write.*.mode` properties.

`months(...)` is an Iceberg **hidden partitioning** transform: you query `WHERE order_purchase_timestamp >= ...` and Iceberg prunes months by itself. There's no extra `purchase_month` column to remember. ([Iceberg partitioning](https://iceberg.apache.org/docs/latest/partitioning/))

### Step 3: prove it's idempotent

Run it again with nothing new:
```bash
make incremental
```
```
Watermark (newest day already loaded): 2017-03-07
New days in landing: 0
```
Now force a re-load of a day that's already in:
```bash
make incremental DATE=2017-03-03
```
The three row counts are **the same as before**. Two mechanisms make that true:
1. **History:** a day is written with `overwritePartitions()`, so loading 2017-03-03 again *replaces* the `_batch_date=2017-03-03` partition instead of appending it twice ([Iceberg: overwriting data](https://iceberg.apache.org/docs/latest/spark-writes/#overwriting-data)).
2. **Current tables:** the MERGE guard `s.updated_at > t.updated_at` is false for every row (they're older or equal), so nothing changes.

Idempotency is the most important property of a pipeline. Airflow (M6) **will** retry failed tasks and re-run old days in backfills, and the result must be the same every time.

### Step 4: inspect the results from Trino

```bash
make m3-inspect
# = docker compose exec -T trino trino --output-format=ALIGNED < sql/m3_inspect.sql
```
Read the output query by query:

**1. Row counts.** `orders_changes` has more rows than orders (many versions). The two current tables have exactly one row per order (`rows = orders`).

**2. History partitions.** One partition per loaded day, with that day's row count.

**3. The life of one order.** For example (from my test run):
```
 order_id | _batch_date | order_status | updated_at
 ord00479 | 2017-01-01  | created      | 2017-01-01 02:29:08 UTC
 ord00479 | 2017-01-02  | approved     | 2017-01-02 11:29:08 UTC
 ord00479 | 2017-01-04  | shipped      | 2017-01-04 02:29:08 UTC
 ord00479 | 2017-01-08  | delivered    | 2017-01-08 04:29:08 UTC
```
Four rows in the history, and only the `delivered` one in the current tables. Check it:
```sql
SELECT order_status, updated_at FROM iceberg.bronze.orders_current_cow WHERE order_id = '<the id you got>';
```

**4. CoW vs MoR content:** 0 rows differ. Same data, different files underneath.

**5 and 6. The files.** This is the main lesson of M3, see the next step.

### Step 5: copy-on-write vs merge-on-read, in the files

From my test (9 days of data, all purchases in one month, so one month partition):

**Live files now (query 5):**
```
 mode | content | files | records
 cow  |       0 |     1 |     404     <- one data file, exactly the live rows
 mor  |       0 |     8 |     841     <- 8 data files with ALL versions ever written
 mor  |       1 |     7 |     437     <- 7 delete files marking 437 of them dead
```
841 − 437 = 404: an MoR reader reads 841 rows and throws 437 away. (`content` 0 = data, 1 = position deletes, 2 = equality deletes.)

**What each daily MERGE wrote (query 6, `rows_written` = `added-records`):**
```
 mode | operation | added_data_files | removed_data_files | added_delete_files | rows_written
 cow  | append    | 1                |                    |                    |  48
 cow  | overwrite | 1                | 1                  |                    |  92   <- 72 new/changed + 20 untouched, rewritten
 cow  | overwrite | 1                | 1                  |                    | 132
 ...                                                                                  (the whole file, every day)
 cow  | overwrite | 1                | 1                  |                    | 349   <- re-load of an old day: rewrote everything, changed nothing
 mor  | append    | 1                |                    |                    |  48
 mor  | overwrite | 1                |                    | 1                  |  72   <- only that day's rows
 mor  | overwrite | 1                |                    | 2                  |  83
 ...
 mor  | overwrite |                  |                    |                    |       <- re-load of an old day: writes nothing
```
What to take from it:
- **CoW** removes and rewrites the data file every day. `rows_written` grows with the size of the table, not the size of the change. Even the no-op re-load rewrote 349 rows: Spark rewrites every file that has a *matched* row, before the `updated_at` condition is applied.
- **MoR** writes only the day's rows plus small delete files. Writes stay cheap, but the dead rows and delete files pile up, and every read pays for them.
- MoR writes one delete file per data file it touched (Iceberg's default `write.delete.granularity=file`), which is why `added_delete_files` grows by one each day.

With the real data, the month partitions matter: a March 1st batch touches orders bought in February and March, so CoW rewrites the files of **both** months, and leaves every older month alone. That's what partitioning buys you on writes.

**Compaction fixes MoR's read side:**
```bash
make compact
# = spark-sql -e "CALL iceberg.system.rewrite_data_files(table => 'bronze.orders_current_mor', options => map('delete-file-threshold', '1'))"
```
Output columns: `rewritten_data_files_count, added_data_files_count, rewritten_bytes_count, ...`, e.g. `8  1  51210 ...`. Run `make m3-inspect` again: the MoR table now has **1 data file and no delete files**, like CoW. `delete-file-threshold=1` means "rewrite any file that has at least one delete file". ([Iceberg: rewrite_data_files](https://iceberg.apache.org/docs/latest/spark-procedures/#rewrite_data_files))

The usual production pattern: MoR for cheap frequent writes + a scheduled compaction (M6/M7 with Airflow, and the same idea as Tableflow's background compaction in M3b).

> You may see `CatalogNotFoundException: The catalog 'bronze' not found` in the log before the result. Spark tries the name as a catalog first, then falls back to the default catalog `iceberg`. It's noise; the result table is what counts.

### Step 6: one more day, the normal daily run

This is what Airflow will do every morning in M6:
```bash
make replay START=2017-03-08          # the source drops tomorrow's file
make incremental                      # watermark 2017-03-07 -> loads only 2017-03-08
```
```
Watermark (newest day already loaded): 2017-03-07
New days in landing: 1
  2017-03-08:   NNN rows  ->  changes + MERGE into 2 tables
```

### Step 7: partition evolution

Say analysts now filter by day, and month partitions are too coarse. In most older table formats you'd have to rebuild the table. In Iceberg it's a metadata change:
```bash
make trino
```
```sql
ALTER TABLE iceberg.bronze.orders_current_cow
SET PROPERTIES partitioning = ARRAY['day(order_purchase_timestamp)'];
```
No data moves. Iceberg adds a new **partition spec** (spec 1) to `metadata.json`. Old files keep spec 0 (month); new writes use spec 1 (day). Queries work across both, and Iceberg plans each file with its own spec. ([Iceberg: partition evolution](https://iceberg.apache.org/docs/latest/evolution/#partition-evolution), [Trino Iceberg connector: ALTER TABLE SET PROPERTIES](https://trino.io/docs/current/connector/iceberg.html))

Now load another day and look at the files:
```bash
make replay START=2017-03-09 && make incremental
```
```sql
SELECT spec_id, partition, record_count
FROM iceberg.bronze."orders_current_cow$files" ORDER BY spec_id, partition;
```
You'll see a mix:
- `spec_id 0`, `{order_purchase_timestamp_month=...}` for months the MERGE didn't touch (e.g. 2016, early 2017),
- `spec_id 1`, `{order_purchase_timestamp_month=NULL, order_purchase_timestamp_day=2017-03-09}` for files written after the change.

In my test, all orders were in one month, so the MERGE rewrote that one file (CoW!) and every row moved to the day spec, in 9 day folders: `data/order_purchase_timestamp_day=2017-01-01/...`. With the real data, only the touched months move. To move everything, you'd run `rewrite_data_files` on the table.

Check that Trino prunes by the new partitions:
```sql
EXPLAIN ANALYZE
SELECT count(*) FROM iceberg.bronze.orders_current_cow
WHERE order_purchase_timestamp >= TIMESTAMP '2017-03-09 00:00:00 UTC'
  AND order_purchase_timestamp <  TIMESTAMP '2017-03-10 00:00:00 UTC';
```
Look for `Input: N rows ... Splits: 1` in the scan line: Trino read one file, not the table.

Notice that **Trino** changed the partitioning, and the **Spark** MERGE that ran next used the new spec straight away. Both engines read the same `metadata.json` through the REST catalog.

## 3. Behind the scenes of one MERGE (CoW)

What happens when the job runs `MERGE INTO orders_current_cow` for one day:
1. Spark asks iceberg-rest for the table → gets the current `metadata.json` location → reads the current snapshot's manifests from MinIO.
2. It uses partition values and column min/max stats in the manifests to find which data files **might** contain the batch's `order_id`s. With the month partitioning, a file of 2016 orders is skipped when the batch only has 2017 orders.
3. It joins the batch with those files. For CoW it writes **new** Parquet files with: untouched rows, updated rows and inserted rows. For MoR it writes the updated + inserted rows and a position-delete file.
4. It writes a new manifest, manifest list and `metadata.json` to MinIO.
5. It sends the commit to iceberg-rest: "replace data files A with B, based on snapshot S". iceberg-rest checks that S is still the current snapshot (optimistic concurrency) and swaps the pointer.
6. If someone else committed in between, the commit is rejected and Spark retries from step 1. The old files stay until snapshot expiration removes them, so time travel to yesterday still works:
   ```sql
   SELECT count(*) FROM iceberg.bronze.orders_current_cow FOR VERSION AS OF <an older snapshot_id>;
   ```

## 4. Memory and time

Each daily load takes a few seconds; the first Spark run of a session is slower (JVM start, catalog connection). Memory is the same as M2: the Spark container peaks around 2 GB.

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `[TABLE_OR_VIEW_NOT_FOUND] ... bronze.orders` in `make replay` | M2's bronze orders isn't loaded | `make landing && make bronze` |
| `New days in landing: 0` but you expected some | Those days are older than the watermark | `make incremental DATE=YYYY-MM-DD` loads one day explicitly |
| You skipped a day (e.g. replayed 03-10 before 03-09) | The watermark moved past 03-09 | `make incremental DATE=2017-03-09`. The MERGE guard keeps the newer status, so order doesn't matter for the current tables |
| MERGE fails with a cardinality error (`MERGE_CARDINALITY_VIOLATION`: one target row matched by several source rows) | The batch has the same `order_id` twice | Can't happen with the replayer; in real data, dedupe the batch first (keep the max `updated_at` per key) |
| Want to start M3 over | | In Trino: `DROP TABLE iceberg.bronze.orders_changes;` and the two `orders_current_*` tables. In the MinIO console, delete `landing/olist/orders_daily/` |

## Done when

- [ ] `make replay` wrote 7 day folders, and you looked at one file
- [ ] `make incremental` loaded 7 days; a re-run loads 0; `DATE=` re-load leaves all counts unchanged
- [ ] You found an order with several versions in `orders_changes` and one in `orders_current_cow`
- [ ] You can explain the CoW vs MoR numbers from `make m3-inspect`, and what `make compact` changed
- [ ] You evolved the partitioning in Trino and saw files with two `spec_id`s

Questions before M4:
1. The MoR table needs compaction and CoW doesn't. For a table that gets 50,000 small updates an hour and is queried twice a day, which mode would you choose, and when would you compact?
2. Our watermark is the newest `_batch_date`. What breaks if a source file for an **old** day arrives late (e.g. 03-05 lands after 03-08 was loaded)? How would you detect and handle it?
3. `orders_changes` keeps every version. Write a Trino query that rebuilds `orders_current` from it using `row_number()`. That's how dbt will do it in M4.
4. Why do we not want both Spark and dbt writing to the same table at the same time? What does Iceberg do if they try?

Next: **M3b, the same orders as a Kafka stream into Iceberg** ([plan](M3b-kafka-to-iceberg-plan.md)), then **M4, dbt silver models.**
