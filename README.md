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
| M0 | Machine setup, dataset download, first look at the data | Idempotency, grain, watermark candidates | in progress |
| M1 | MinIO + REST catalog + Trino; first Iceberg table by hand | Iceberg metadata tree, snapshots, time travel | |
| M2 | PySpark: landing CSV to bronze Iceberg | DataFrames, partitions, bronze layer | |
| M3 | Daily replay, incremental loads, `MERGE INTO` | Watermarks, CoW vs MoR, partition evolution | |
| M4 | dbt-trino silver layer with tests | Sources, `ref()`, data quality | |
| M5 | Gold star schema, SCD2 customers, marts | Star schema, grain, SCD2 | |
| M6 | Airflow DAG + backfill | Data intervals, retries, idempotency | |
| M7 | Write-audit-publish with branches, maintenance | Branching, compaction, snapshot expiry | |

Each milestone has a guide in [`docs/`](docs/). Start with [M0](docs/M0-setup.md).

## Layout

```
infra/     Docker Compose and service config (from M1)
spark/     PySpark jobs (from M2)
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
```
