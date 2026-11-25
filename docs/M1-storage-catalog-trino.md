# M1: MinIO + Iceberg REST catalog + Trino

**Goal:** run the three core pieces of a lakehouse on your Mac, create your first Iceberg table with plain SQL, and then open the actual files in MinIO to see what a "table" really is: Parquet files plus a tree of metadata files.

Time: 1 to 2 hours. No Spark yet: that is M2.

---

## 0. The mental model (read this first)

A lakehouse splits a database into separate parts. In Oracle all of these live inside one product; here each one is its own service:

| Layer | In Oracle | In ShopLake (M1) | Container |
| --- | --- | --- | --- |
| Storage: where bytes live | Datafiles on the DB server's disk | Buckets in MinIO (S3 API) | `minio` |
| Table format: how bytes become a table | Oracle's internal block format | Apache Iceberg (Parquet data + JSON/Avro metadata) | (a spec, not a service) |
| Catalog: which tables exist, where their metadata is | Oracle data dictionary | Iceberg REST catalog | `iceberg-rest` |
| Compute: runs SQL | Oracle server process | Trino | `trino` |

The key idea: **Iceberg is not a server.** It is a specification for how to lay out files. Any engine that implements the spec (Trino now, Spark in M2) can read and write the same tables. The catalog is the only shared "brain": it stores one pointer per table, "the current metadata file is X". Committing a write means atomically swapping that pointer.

Docs: [Iceberg spec overview](https://iceberg.apache.org/spec/#overview), [REST catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)

## 1. What is in the repo for M1

| File | What it does |
| --- | --- |
| `docker-compose.yml` | Defines 4 services: `minio`, `minio-init` (creates buckets once, then exits), `iceberg-rest`, `trino` |
| `infra/trino/catalog/iceberg.properties` | Tells Trino: "there is a catalog called `iceberg`; ask the REST catalog for table metadata; read/write files in MinIO via S3" |
| `sql/m1_first_table.sql` | The hands-on exercise: create, insert, update, inspect |
| `.env` | MinIO user/password (local demo values). Compose reads it automatically |

### Why these images

| Service | Image | Why |
| --- | --- | --- |
| MinIO | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` | MinIO stopped publishing official community images in 2025 (`minio/minio` is no longer on Docker Hub). `pgsty/minio` is a community build of the same MinIO server, with the same S3 API and `mc` client. Pinned to an exact release. |
| Iceberg REST catalog | `apache/iceberg-rest-fixture:1.10.1` | Apache Iceberg's own reference REST catalog. Simple and official. It stores its pointers in SQLite on a Docker volume. Production uses AWS Glue, Polaris, Nessie, Lakekeeper or Unity Catalog instead, but they speak the same REST API. |
| Trino | `trinodb/trino:484` | Official image, a current release, native arm64. |

Every version is pinned, never `latest`. You saw why with the `kaggle`/`six` bug in M0: an unpinned dependency changed under us.

### Memory and disk (measured)

| Container | Memory when idle | Image size |
| --- | --- | --- |
| trino | ~0.9 GB (capped at 3 GB) | 2.4 GB |
| iceberg-rest | ~0.25 GB | 0.8 GB |
| minio | ~0.1 GB | 0.2 GB |

About 3.5 GB of images, downloaded once into Colima's disk on your SSD.

## 2. Pull the new files and start the stack

```bash
cd /Volumes/SandiskSSD/Developer/Data_Engineering/ShopLake
git pull
colima status          # must say "colima is running"; if not, re-run your colima start command
cp -n .env.example .env  # only copies if .env does not exist yet
make up                # = docker compose up -d
```

The first `make up` downloads the images (a few minutes). Then:

```bash
make ps
```

You want to see:

```
minio          Up (healthy)
minio-init     Exited (0)      <- this is correct: it created the buckets and finished
iceberg-rest   Up (healthy)
trino          Up (healthy)
```

Trino takes 20 to 40 seconds to become `healthy`. If something is not healthy, `docker compose logs <service>` shows why.

**Startup order (why `depends_on` matters):** `minio` must be healthy, then `minio-init` creates buckets and exits successfully, then `iceberg-rest` starts (its warehouse bucket now exists), then `trino`. Compose enforces this order with `depends_on` conditions. Same idea as making a Kafka consumer wait for the broker.

## 3. Look around before writing anything

Open these in your browser:

| URL | What | Login |
| --- | --- | --- |
| http://localhost:9001 | MinIO console: browse buckets and files | `admin` / `password` (from `.env`) |
| http://localhost:8080 | Trino web UI: running and finished queries | any username, e.g. `kaiser` |
| http://localhost:8181/v1/namespaces | The REST catalog's raw API (JSON) | none |

You should see two empty buckets in MinIO: `landing` (raw files, from M2) and `warehouse` (Iceberg tables).

The catalog URL is plain HTTP + JSON, just like the REST services you built at MoMo. Spark and Trino call exactly these endpoints. Try `curl -s localhost:8181/v1/config` in a terminal.

## 4. Your first Iceberg table

Run the whole exercise once:

```bash
make m1-demo
```

Then read `sql/m1_first_table.sql` from top to bottom. It does five things:

1. **`CREATE SCHEMA ... WITH (location = 's3://warehouse/playground')`**: a schema (Iceberg calls it a *namespace*) is a folder of tables. The catalog now knows `playground` exists.
2. **`CREATE TABLE ... WITH (partitioning = ARRAY['day(order_purchase_timestamp)'])`**: an Iceberg table with **hidden partitioning**. You partition by a *transform* of a column (`day(...)`), not by a separate `dt` column. Queries filter on `order_purchase_timestamp` directly and Iceberg still prunes partitions. [Docs: partitioning](https://iceberg.apache.org/docs/latest/partitioning/)
3. **`INSERT` 3 rows**: one commit, so a new snapshot.
4. **`UPDATE` 1 row**: another commit, another snapshot.
5. **Queries on metadata tables** `"orders_demo$snapshots"` and `"orders_demo$files"`.

### What you should see, and what it means

**Snapshots: 3 rows**

| operation | added_records | added_delete_files | Means |
| --- | --- | --- | --- |
| append | NULL | NULL | `CREATE TABLE` made an empty first snapshot |
| append | 3 | NULL | the `INSERT` |
| overwrite | 1 | 1 | the `UPDATE` |

Every write creates a new, immutable snapshot that points to its parent. Nothing is overwritten in place. That is what makes time travel and safe concurrent readers possible.

**Files: 4 rows**

| content | partition | record_count | Means |
| --- | --- | --- | --- |
| 0 | day=2017-10-02 | 2 | data file from the INSERT (o1, o2) |
| 0 | day=2017-10-03 | 1 | data file from the INSERT (o3), its own partition folder |
| 1 | day=2017-10-02 | 1 | **position-delete file** from the UPDATE: "row 0 of file X is deleted" |
| 0 | day=2017-10-02 | 1 | new data file from the UPDATE: o1 with status `delivered` |

This is the most important observation in M1. **Trino did not rewrite the original Parquet file.** Parquet files are immutable. To "update" one row, Trino wrote a tiny delete file plus a new data file, and readers merge them at query time. This strategy is **merge-on-read (MoR)**: cheap writes, slightly more work on reads. The alternative, **copy-on-write (CoW)**, rewrites the whole data file: expensive writes, fastest reads. Your notes cover both; in M3 we run both on real data and compare. [Docs: row-level deletes](https://iceberg.apache.org/spec/#row-level-deletes), [Trino: Iceberg UPDATE/DELETE/MERGE](https://trino.io/docs/current/connector/iceberg.html#data-management)

### Time travel

Open the interactive shell:

```bash
make trino
```

Copy the `snapshot_id` of the snapshot with `added_records = 3`, then:

```sql
SELECT order_id, order_status
FROM iceberg.playground.orders_demo
FOR VERSION AS OF <paste_snapshot_id_here>;
```

`o1` is back to `created`: you are reading the table as it was before the UPDATE. Nothing was restored; Iceberg simply followed an older snapshot's file list, and those old files still exist. Type `quit` to leave the shell.

[Docs: Trino time travel](https://trino.io/docs/current/connector/iceberg.html#time-travel-queries)

## 5. Open the files: what a table really is

In the MinIO console (http://localhost:9001), open `warehouse` → `playground` → `orders_demo-<uuid>`. You will find two folders:

```
orders_demo-<uuid>/
├── data/
│   ├── order_purchase_timestamp_day=2017-10-02/   *.parquet  (3 files: 2 data + 1 delete)
│   └── order_purchase_timestamp_day=2017-10-03/   *.parquet  (1 data file)
└── metadata/
    ├── 00000-<uuid>.metadata.json    table state after CREATE
    ├── 00001-<uuid>.metadata.json    after INSERT
    ├── 00002-<uuid>.metadata.json    after UPDATE  <- the catalog points here
    ├── snap-<snapshot_id>-....avro   manifest LIST: one per snapshot
    └── <uuid>-m0.avro, -m1.avro      MANIFESTS: list data files + per-file stats
```

That is the Iceberg metadata tree, bottom to top:

```
catalog pointer ──> metadata.json  (schema, partition spec, list of snapshots)
                        └─> manifest list (one per snapshot)
                               └─> manifests (which files, with min/max per column)
                                      └─> Parquet data / delete files
```

Exercises (5 minutes, worth it):

1. Download `00002-...metadata.json` from the console and open it in your editor. Find `current-snapshot-id`, `schemas`, `partition-specs` and the `snapshots` array. Compare `current-snapshot-id` with the output of the snapshots query.
2. Ask the catalog where the table's metadata is:
   ```bash
   curl -s localhost:8181/v1/namespaces/playground/tables/orders_demo | python3 -m json.tool | head -20
   ```
   The `metadata-location` field is the pointer. Run another `UPDATE` in `make trino`, call `curl` again, and watch it move to `00003-...`.
3. In the shell, read a Parquet file's footer stats through Iceberg:
   ```sql
   SELECT file_path, lower_bounds, upper_bounds
   FROM iceberg.playground."orders_demo$files";
   ```
   You get maps like `{1=o1, 2=c1, 3=delivered, 4=2017-10-02 10:56:33}`. The keys are Iceberg **column ids**, not names (1 = order_id, ...). Ids are why renaming a column in Iceberg is safe: files refer to ids, never names.
   Those min/max values are what lets Trino skip whole files for `WHERE order_purchase_timestamp > ...`.

## 5b. Behind the scenes: what happens for each statement

Section 5 showed the files *after* the demo. This section shows **who does what, in which order**, for every statement in `sql/m1_first_table.sql`. I checked it by re-running the script and watching MinIO and the SQLite file.

### Who's who

| Name | What it is |
| --- | --- |
| `iceberg` (in `iceberg.playground.orders_demo`) | Trino's name for the connection config `infra/trino/catalog/iceberg.properties`. Not a folder anywhere |
| Trino | The SQL engine. Reads and writes **data files and manifests** in MinIO directly |
| iceberg-rest | The catalog service. Decides locations, owns the table pointers, writes **`metadata.json`** files |
| SQLite (`/home/iceberg/iceberg_catalog.db`) | iceberg-rest's private storage: names + pointers only. Trino never talks to it |
| MinIO bucket `warehouse` | All files: `metadata.json`, Avro manifests, Parquet data |
| `playground` | A schema = a row in SQLite + a path prefix `warehouse/playground/` |
| `orders_demo` | A table = a row in SQLite + a folder `playground/orders_demo-<uuid>/` |

What SQLite actually contains after the demo (I opened the file):

| Table | Row |
| --- | --- |
| `iceberg_namespace_properties` | `playground`, `location` = `s3://warehouse/playground` |
| `iceberg_tables` | `playground`, `orders_demo`, `metadata_location` = `.../metadata/00002-....metadata.json`, `previous_metadata_location` = `.../00001-...` |

### ① `CREATE SCHEMA IF NOT EXISTS iceberg.playground WITH (location = 's3://warehouse/playground')`
1. Trino → iceberg-rest: "create namespace `playground` with this location".
2. iceberg-rest → SQLite: insert into `iceberg_namespace_properties`.
3. MinIO: **nothing**. A schema has no files, and S3 has no real folders. (This is why a schema pointing to a missing bucket "succeeds": nothing touches storage yet.)

### ② `DROP TABLE IF EXISTS iceberg.playground.orders_demo`
1. Trino → iceberg-rest: drop the table, with purge.
2. iceberg-rest → SQLite: delete the `iceberg_tables` row.
3. The old table's files in MinIO are deleted. On my test, `orders_demo-d19f...` disappeared and a new `orders_demo-a32b...` appeared after ③.

### ③ `CREATE TABLE iceberg.playground.orders_demo (...) WITH (partitioning = ...)`
1. Trino picks the location `s3://warehouse/playground/orders_demo-<new uuid>` and **checks in MinIO** that it is empty. This is the first storage access: with a missing bucket, it fails here.
2. Trino → iceberg-rest: create the table with this schema and partition spec.
3. iceberg-rest → MinIO: writes `metadata/00000-....metadata.json` (schema, partition spec, one empty snapshot).
4. iceberg-rest → SQLite: inserts the `iceberg_tables` row pointing to `00000`.

### ④ `INSERT INTO ... VALUES (3 rows)`
1. Trino → iceberg-rest: "load table" (gets the current metadata).
2. Trino → MinIO **directly**: writes 2 Parquet data files (one per partition day), a manifest `*-m0.avro` and a manifest list `snap-<id>-....avro`.
3. Trino → iceberg-rest: "commit: add snapshot S1, *expecting* the current metadata to be `00000`".
4. iceberg-rest → MinIO: writes `00001-....metadata.json`.
5. iceberg-rest → SQLite: moves the pointer from `00000` to `00001`, **only if it is still `00000`**. This compare-and-swap is the atomic commit. If another writer committed first, the swap fails, nothing becomes visible, and Trino retries on top of the new state.

### ⑤ `UPDATE ... SET order_status = 'delivered' WHERE order_id = 'o1'`
1. Trino loads the table and **reads** the data files to find `o1`.
2. Trino → MinIO directly: writes a **position-delete file** ("row 0 of file X is deleted") and a **new data file** with o1 = delivered, plus new manifests and a manifest list. The original Parquet file is not modified (merge-on-read).
3. Commit exactly as in ④: iceberg-rest writes `00002-....metadata.json` and moves the SQLite pointer.

### ⑥ `SELECT * FROM iceberg.playground.orders_demo`
1. Trino → iceberg-rest: "load table". iceberg-rest reads the pointer from SQLite, reads `00002-....metadata.json` from MinIO and **returns its content** to Trino.
2. Trino → MinIO directly: reads the manifest list, then the manifests (skipping files using partition values and min/max stats), then the Parquet files, and applies the delete file.
3. **No writes anywhere.**

### ⑦ `SELECT ... FROM "orders_demo$snapshots"` / `"orders_demo$files"`
Same as ⑥, but Trino returns the metadata itself instead of reading Parquet. `$snapshots` comes from the snapshot list in `metadata.json`, and `$files` comes from the manifests.

### ⑧ Time travel: `SELECT ... FOR VERSION AS OF <snapshot_id>`
Same as ⑥, but Trino follows the **older** snapshot's manifest list. Those files still exist, because commits never delete or modify files.

### Summary

| Statement | SQLite (via iceberg-rest) | MinIO, written by iceberg-rest | MinIO, written by Trino |
| --- | --- | --- | --- |
| CREATE SCHEMA | insert namespace row | – | – |
| DROP TABLE | delete table row | – | old files deleted (purge) |
| CREATE TABLE | insert table row → `00000` | `00000.metadata.json` | – (only checks the location) |
| INSERT / UPDATE | pointer moves (compare-and-swap) | new `metadata.json` | Parquet data/delete files, manifests, manifest list |
| SELECT / `$files` / time travel | read pointer | – (reads `metadata.json`) | – (reads only) |

**Rule of thumb:** iceberg-rest handles *metadata*, and only metadata. The heavy data traffic flows directly between the engine (Trino, and Spark from M2) and MinIO. That's why one small catalog can serve huge tables.

## 6. Stopping, restarting, resetting

| Command | Effect |
| --- | --- |
| `make down` | Stops containers. **Data is kept** in Docker volumes `minio-data` and `catalog-data`. |
| `make up` | Starts again; your tables are still there. |
| `make reset` | Asks for confirmation, then deletes **everything**: buckets, files, catalog. Use it to start over. |

Try it: `make down`, `make up`, wait for healthy, then `make trino` and run `SELECT count(*) FROM iceberg.playground.orders_demo;`. Still 3. The table survived because both the files (MinIO volume) and the pointer (catalog volume) persisted. If only one survived, the table would be broken. That is why the catalog is critical infrastructure in production.

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `Cannot connect to the Docker daemon` | Colima is not running | `colima start` (it remembers your settings) |
| `port is already allocated` for 8080/9000/9001/8181 | Another app uses that port | Stop it, or change the left number in `ports:`, e.g. `"18080:8080"` |
| `trino` restarts or exits 137 | Out of memory | `colima list` should show 8GiB; close other heavy apps |
| `iceberg-rest` exits right away | Usually the buckets were not created | `docker compose logs minio-init iceberg-rest` |
| Query fails `Schema 'playground' does not exist` after `make reset` | Expected: reset wiped it | Run `make m1-demo` again |

## Done when

- [ ] `make ps` shows minio, iceberg-rest and trino healthy, minio-init exited (0)
- [ ] `make m1-demo` prints 3 snapshots and 4 files
- [ ] You ran a time travel query and saw `o1` as `created`
- [ ] You found the `metadata/` and `data/` folders in the MinIO console and watched `metadata-location` move after an UPDATE

Questions to think about (we discuss them before M2):

1. Why does the catalog need to do an *atomic* swap of the pointer? What would break if two writers updated the table at the same time without it?
2. After 1,000 small UPDATEs, how many delete files would exist, and why would reads slow down? (This is the "small files problem" we fix in M7.)
3. We partitioned by `day(...)`. Olist has about 100 to 300 orders per day. Is daily partitioning a good choice for this volume? (Hint: look at your answer to M0 question 5.)

## A note on your Colima setup

Your Colima VM runs with 8 GB RAM, 4 CPUs and a 40 GB disk, and its files live on the SSD through symlinks (`~/.colima` and `~/Library/Caches/colima` point into `/Volumes/SandiskSSD/colima/`). All images and Docker volumes from this milestone land in that disk image on the SSD, not on your internal drive. Keep the SSD plugged in whenever Colima runs. The disk image grows but never shrinks; `docker system prune` frees space *inside* it only.

Next: **M2, Spark loads the real Olist CSVs into bronze Iceberg tables.**
