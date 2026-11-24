# M2: PySpark loads raw CSVs into bronze Iceberg tables

**Goal:** the 9 real Olist CSVs go from your laptop into the `landing` bucket, and a PySpark job turns each one into an Iceberg table in `iceberg.bronze`. Then you query the same tables from Trino. Two engines, one copy of the data, one catalog.

Time: 1 to 2 hours.

---

## 0. The mental model

```
data/raw/*.csv  --make landing-->  MinIO landing/olist/*.csv      (raw files, as received)
                                          |
                                          |  Spark reads with s3a://
                                          v
                              spark/jobs/bronze_ingest.py
                                          |
                                          |  Spark writes Iceberg via the REST catalog
                                          v
                     MinIO warehouse/bronze/<table>-<uuid>/   +  pointer in iceberg-rest
                                          ^
                                          |  Trino reads the same tables
                                     make trino
```

### What "bronze" means here

The medallion layers each have one job:

| Layer | Job | In ShopLake |
| --- | --- | --- |
| **Bronze** | Keep a faithful copy of the source, plus "where/when did this come from" | M2 (this), M3 incremental |
| Silver | Clean, typed, deduplicated, tested | M4 (dbt) |
| Gold | Business models: facts, dimensions, marts | M5 (dbt) |

So the bronze job deliberately does **very little**:

1. **Every column stays a STRING.** Casting `"2017-10-02 10:56:33"` to a timestamp is silver's job. If one row has a bad value, a cast in bronze would crash the whole load. A cast in silver becomes a failed data test you can investigate, while the raw value is still safe in bronze.
2. **Two lineage columns** are added: `_source_file` (which file) and `_ingested_at` (when the job loaded it). When a number looks wrong in a dashboard six months from now, this is how you trace it back.
3. **Idempotent:** each run *replaces* the table (`createOrReplace`). Run the job twice and you still have one copy of the data, not two. (M3 switches to incremental `MERGE`.)

### Why Spark here and not Trino?

Trino could also run `INSERT INTO ... SELECT` from a CSV. We use Spark for ingestion because that's the usual split in a real lakehouse:
- **Spark:** batch/stream processing jobs written in code (Python). Good at reading messy files, custom logic and large rewrites.
- **Trino:** interactive SQL for people and for dbt.

ShopBack's stack lists both, and this is typically where each one is used.

## 1. What's new in the repo

| File | What it does |
| --- | --- |
| `spark/Dockerfile` | The official `apache/spark:4.1.4-python3` image plus 5 jars (see below) |
| `spark/conf/spark-defaults.conf` | Spark settings: the Iceberg catalog, S3 access to MinIO, laptop memory sizing |
| `spark/jobs/bronze_ingest.py` | The job |
| `docker-compose.yml` | New `spark` service |
| `Makefile` | New targets: `build`, `landing`, `bronze`, `pyspark` |

### The jars, and why each one is needed

Spark itself knows nothing about Iceberg or S3. Jars are Java libraries that add those abilities:

| Jar | Gives Spark |
| --- | --- |
| `iceberg-spark-runtime-4.1_2.13-1.12.0` | Iceberg tables, `writeTo(...)`, `MERGE INTO`, metadata tables. Its name encodes the Spark version (4.1) and Scala version (2.13), which must match the image |
| `iceberg-aws-bundle-1.12.0` | AWS SDK v2, used by Iceberg's `S3FileIO` to read and write table files in MinIO |
| `hadoop-aws-3.4.2` | The `s3a://` filesystem, used to read the raw CSVs. It must match Spark's Hadoop version (3.4.2) |
| `s3-transfer-manager`, `apache-client` (2.54.17) | Two AWS SDK pieces that S3A needs but the Iceberg bundle leaves out. Same SDK version as the bundle |

I found the last two the way you usually do in practice. The job failed with `ClassNotFoundException: software.amazon.awssdk.transfer.s3...`, I added the jar that contains that class, and repeated. Version-matching jars is a classic Spark chore. Baking them into the image means you do it once.

### Same catalog name in both engines

In `spark-defaults.conf`:
```properties
spark.sql.catalog.iceberg       org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type  rest
spark.sql.catalog.iceberg.uri   http://iceberg-rest:8181
```
This creates a Spark catalog named `iceberg`, the same name as Trino's `iceberg.properties`. So `iceberg.bronze.orders` means the same table in Spark and in Trino. Compare the keys with Trino's file: same REST URI, same `s3.endpoint`, same `path-style-access`. Only the prefix differs. ([Iceberg Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/))

Why two S3 configs (`spark.sql.catalog.iceberg.s3.*` and `spark.hadoop.fs.s3a.*`)? There are two different clients:
- **Iceberg's S3FileIO** reads and writes table files (`s3://warehouse/...`).
- **Hadoop's S3A** reads plain files that aren't Iceberg tables (`s3a://landing/...`).

## 2. Run it

```bash
cd /Volumes/SandiskSSD/Developer/Data_Engineering/ShopLake
git pull
make build      # = docker compose build spark   (first time: downloads ~1.3 GB image + ~120 MB jars)
make up         # starts everything, now including spark
make ps         # spark shows "Up" (it idles, waiting for jobs)
```

### Step 1: put the raw files in the landing bucket
```bash
make landing
```
This runs `mc mirror` in a one-off MinIO-client container with `data/raw` mounted, and copies every CSV to `landing/olist/`. Check it in the console: http://localhost:9001 → `landing` → `olist`.

Why copy them at all, when Spark could read `data/raw` from your disk? In production, sources never live on the laptop that runs Spark. They arrive in object storage (an S3 drop from a partner, an export from a database, files from Kafka Connect in M3b). Landing first makes our job identical to a production job.

### Step 2: run the Spark job
```bash
make bronze
# = docker compose exec spark spark-submit spark/jobs/bronze_ingest.py
```

You should see (with the real dataset):
```
Loading 9 table(s) from s3a://landing/olist into iceberg.bronze:
  orders                                  99,441 rows
  order_items                            112,650 rows
  order_payments                         103,886 rows
  order_reviews                           99,224 rows
  customers                               99,441 rows
  sellers                                  3,095 rows
  products                                32,951 rows
  geolocation                          1,000,163 rows
  product_category_name_translation           71 rows
```
**Compare these with your M0 `make explore` output (question 1).** They should match exactly. If `order_reviews` shows more rows than M0, the `multiLine` option isn't working (see section 3).

Load a single table: `docker compose exec spark spark-submit spark/jobs/bronze_ingest.py orders`

While a job runs, the Spark UI is at http://localhost:4040 (jobs, stages, the SQL plan).

### Step 3: query bronze from Trino
```bash
make trino
```
```sql
SHOW TABLES FROM iceberg.bronze;

SELECT order_status, count(*) FROM iceberg.bronze.orders GROUP BY 1 ORDER BY 2 DESC;

-- Every column is varchar, plus the two lineage columns:
DESCRIBE iceberg.bronze.orders;

-- Run `make bronze` twice, then:
SELECT committed_at, operation, element_at(summary, 'total-records') AS total
FROM iceberg.bronze."orders$snapshots" ORDER BY committed_at;
```
The last query shows one snapshot per run, each with the **same** total: an idempotent replace, not an append.

Spark wrote these tables and Trino reads them without any copy or export. That's the core idea of a lakehouse: storage and table format are shared, engines are interchangeable.

## 3. The one real data problem in this job: multi-line reviews

Customers wrote reviews with line breaks inside them:
```
review_id,order_id,review_score,review_comment_title,review_comment_message,...
abc,o1,5,,"Ótimo produto,
chegou rápido",...
```
A naive CSV reader treats each line break as a new row, so one review becomes two broken rows and the columns shift. The job reads with:
```python
.option("multiLine", True)   # a quoted field may contain line breaks
.option("escape", '"')       # "" inside a quoted field means a literal "
```
Try it yourself in `make pyspark`:
```python
naive = spark.read.option("header", True).csv("s3a://landing/olist/olist_order_reviews_dataset.csv")
good  = spark.read.option("header", True).option("multiLine", True).option("escape", '"') \
             .csv("s3a://landing/olist/olist_order_reviews_dataset.csv")
naive.count(), good.count()
```
This is why bronze row counts are always checked against the source.

## 4. Look at the files

In the MinIO console, open `warehouse/bronze/orders-<uuid>/`. Same structure as M1: `data/` with Parquet files, and `metadata/` with `metadata.json`, a manifest list and manifests.

Things to notice:
- **CSV 17 MB → Parquet a few MB.** Columnar storage plus compression (zstd by default in Iceberg 1.x).
- **No partition folders.** Bronze is unpartitioned for now. Partitioning by `order_purchase_timestamp` is a silver/M3 decision, after we know it's a valid timestamp.

## 5. Memory

| Container | Idle | During `make bronze` |
| --- | --- | --- |
| spark | ~2 MB (just `sleep`) | up to ~2 GB (capped at 2.5 GB) |
| trino | ~1 GB | ~1 GB |
| iceberg-rest + minio | ~0.35 GB | ~0.35 GB |

That's about 4 GB peak inside Colima's 8 GB. The Spark image is 2.4 GB on disk.

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `pull access denied for shoplake-spark` on `make up` | Image not built yet | `make build` |
| `make build` fails downloading a jar (HTTP 429) | Maven Central rate limit | Wait a minute, retry |
| `Path does not exist: s3a://landing/olist/...` | CSVs not uploaded | `make landing` |
| `ClassNotFoundException: software.amazon.awssdk...` | A jar is missing or has a mismatched version | Rebuild with `make build` and don't change one version alone |
| Job killed, exit 137 | Out of memory | Close other apps; or set `spark.driver.memory 1g` in `spark-defaults.conf` |

## Done when

- [ ] `make bronze` loads 9 tables, and the row counts match your M0 numbers
- [ ] `SHOW TABLES FROM iceberg.bronze` in Trino lists them
- [ ] Running `make bronze` twice gives 2 snapshots with the same `total-records`
- [ ] You saw the naive vs `multiLine` count difference for reviews

Questions before M3:
1. Bronze replaces the whole table each run. With 1,000 days of orders, why does that become a problem, and what would you do instead?
2. An order's `order_status` changes from `shipped` to `delivered` a week later. If bronze only *appends* new files, how many rows does that order have? Which column(s) would you use to find the latest version?
3. Why is `_ingested_at` different from `order_purchase_timestamp`, and which one should an incremental load filter on?

Next: **M3, daily replay + incremental loads with `MERGE INTO`**, then **M3b, streaming the same orders through Kafka into Iceberg.**
