# ShopLake: a hands-on lakehouse on your laptop

A small but real lakehouse for an e-commerce marketplace, built one milestone at a time to learn the modern data stack: **MinIO (S3), Apache Iceberg, Apache Spark, Trino, dbt and Apache Airflow**, all running in Docker Compose.

Data: the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (~100k orders, 2016 to 2018), replayed one day at a time to simulate a live source.

## Architecture

```
                 Airflow (daily DAG: replay -> Spark ingest -> dbt build -> dbt test)
                    |                    |                          |
              Replayer (Python)    Spark (PySpark)              dbt-trino
                    |               |    |      \                   | SQL
                    v               |    |       -> Iceberg REST <- Trino
     MinIO:   [landing: raw CSV] ---+    |          catalog          |
              [warehouse: Iceberg tables bronze / silver / gold] <---+
```

Spark and Trino share one copy of the data: each asks the REST catalog which metadata file is current, then reads and writes Parquet files in MinIO directly.

## Milestones

| # | What you build | Concepts | Status |
| --- | --- | --- | --- |
| M0 | Machine setup, dataset download, first look at the data | Idempotency, grain, watermark candidates | done |
| M1 | MinIO + REST catalog + Trino; first Iceberg table by hand | Iceberg metadata tree, snapshots, time travel | done ([guide](docs/M1-storage-catalog-trino.md)) |
| M2 | PySpark: landing CSV to bronze Iceberg | DataFrames, partitions, bronze layer | in progress ([guide](docs/M2-spark-bronze.md)) |
| M3 | Daily replay, incremental loads, `MERGE INTO` | Watermarks, CoW vs MoR, partition evolution | |
| M3b | Streaming: order events through Kafka into Iceberg (Iceberg Kafka Connect sink) | Commit interval vs freshness, exactly-once, small files, batch vs streaming | planned |
| M4 | dbt-trino silver layer with tests | Sources, `ref()`, data quality | |
| M5 | Gold star schema, SCD2 customers, marts | Star schema, grain, SCD2 | |
| M6 | Airflow DAG + backfill | Data intervals, retries, idempotency | |
| M7 | Write-audit-publish with branches, maintenance | Branching, compaction, snapshot expiry | |

Each milestone has a guide in [`docs/`](docs/). Start with [M0](docs/M0-setup.md).

## Layout

```
docker-compose.yml  All services (MinIO, Iceberg REST, Trino; more added later)
infra/     Service config, e.g. infra/trino/catalog/ (from M1)
sql/       Hand-written SQL exercises (from M1)
spark/     Spark image, config and PySpark jobs (from M2)
dbt/       dbt project (from M4)
airflow/   DAGs (from M6)
scripts/   Helper scripts: machine check, dataset download, exploration
docs/      One guide per milestone
data/raw/  Downloaded CSVs (git-ignored)
```

## Quick start

```bash
cp .env.example .env
make check     # Docker resources and mounts
make venv      # local helper tools
make data      # download Olist into data/raw
make explore   # profile the data with DuckDB

make up        # start MinIO, Iceberg REST catalog, Trino (M1)
make ps        # check they are healthy
make m1-demo   # create a first Iceberg table, inspect snapshots and files
make trino     # interactive Trino SQL shell

make build     # build the Spark image (M2)
make landing   # upload data/raw CSVs to the landing bucket
make bronze    # PySpark: landing CSVs -> iceberg.bronze.* tables
make pyspark   # interactive PySpark shell
make down      # stop (data kept); make reset wipes all data
```

| UI | URL |
| --- | --- |
| MinIO console | http://localhost:9001 (admin / password) |
| Trino web UI | http://localhost:8080 |
| Iceberg REST catalog | http://localhost:8181/v1/namespaces |
| Spark UI (while a job runs) | http://localhost:4040 |
