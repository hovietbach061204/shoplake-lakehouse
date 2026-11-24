# M3b (plan): Kafka topics → Iceberg tables

Status: **planned**, built after M3. This page fixes the design so M3 can prepare for it.

Inspiration: Confluent's video on **Tableflow**, which turns Kafka topics into Iceberg (or Delta) tables with no hand-written ETL. Tableflow is a managed feature of Confluent Cloud, so it can't run on a laptop. In M3b we build the **open-source equivalent** of each Tableflow piece. That way you learn what Tableflow automates, and you'll recognize the same building blocks in any company's stack.

## The problem (from the video)

Moving data from Kafka to a lake is usually a pile of fragile glue code:
- **Schema mismatches:** the producer adds a field, and the pipeline breaks or silently drops it.
- **Small files:** streaming writes many tiny files, and query speed collapses.
- **Duplicates or gaps:** a consumer crashes between "wrote the file" and "committed the offset".
- **Two copies of the truth:** the operational data (Kafka) and the analytical data (lake) drift apart.

## Tableflow piece → what we build locally

| Tableflow (Confluent Cloud) | ShopLake M3b (open source) | Concept you learn |
| --- | --- | --- |
| Kafka topic `orders` | Kafka (KRaft mode, 1 broker) + a Python producer replaying Olist order events: created → approved → shipped → delivered | Events vs state; keys and partitions; ordering per key |
| Schema from Schema Registry | Events carry a schema (JSON with schema, or Avro + a schema registry); the sink creates the table from it and evolves it when a field is added | Schema evolution end to end |
| **Materializer** turns records into Parquet files | **Apache Iceberg Kafka Connect sink**: Connect workers consume the topic and write Parquet data files to MinIO | Kafka Connect: workers, tasks, connectors, converters |
| **Committer**: atomic commits, exactly-once | The sink's **coordinator** collects the files from all workers through a *control topic* and makes **one Iceberg commit** per interval, storing the Kafka offsets in the snapshot | Exactly-once = files + offsets committed atomically together |
| Configurable freshness | `iceberg.control.commit.interval-ms` (e.g. 10 s vs 5 min) | Freshness vs file count vs cost |
| Background **compaction** service | A scheduled Spark job: `CALL iceberg.system.rewrite_data_files(...)` (automated with Airflow in M6/M7) | Small-files problem, and why compaction is needed |
| Query from Snowflake / Athena / Redshift | Query from **Trino and Spark** via the same REST catalog | "One copy, many engines" (the M1 idea) |

**One real difference to discuss:** Tableflow's materializer reads Kafka's log segments **directly from object storage** (tiered storage), so it doesn't load the brokers. A Kafka Connect sink is a normal consumer: it reads through the brokers. Same result, different cost profile. This is a good interview-level detail.

## What you'll do in M3b (outline)

1. `docker compose --profile streaming up -d`: Kafka + Kafka Connect start only when asked (about +1.5 GB RAM).
2. Produce Olist order *events* into topic `orders.events` (keyed by `order_id`).
3. Register the Iceberg sink connector (a JSON config, posted to Connect's REST API) targeting `iceberg.stream.order_events`.
4. Watch snapshots appear every commit interval: `SELECT * FROM iceberg.stream."order_events$snapshots"`.
5. Kill the Connect container mid-stream, restart it, and prove there are **no duplicates and no gaps** (exactly-once).
6. Add a field to the producer's events and watch the table schema evolve.
7. Count the small files, run compaction, count again, and compare query time.
8. Compare with M3's batch `MERGE`: the same orders arrive two ways. When would you choose each?

Docs to read first: [Iceberg Kafka Connect sink](https://iceberg.apache.org/docs/latest/kafka-connect/), [Iceberg maintenance: rewrite data files](https://iceberg.apache.org/docs/latest/maintenance/#compact-data-files), [Confluent Tableflow overview](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html).

**Optional cloud extra:** Confluent Cloud has a free trial. You could enable Tableflow on a cloud topic and compare the managed experience with what you built.

**Related tools worth knowing by name:** Redpanda Iceberg Topics (same idea, built into a Kafka-compatible broker), Flink with the Iceberg sink (when you need transformations in flight), and Spark Structured Streaming (the Spark you already know, in streaming mode).
