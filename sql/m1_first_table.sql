-- M1: your first Iceberg table, created by hand in Trino.
-- Run with: docker compose exec -T trino trino < sql/m1_first_table.sql
-- or open `docker compose exec trino trino` and paste statement by statement.

CREATE SCHEMA IF NOT EXISTS iceberg.playground -- Schema would be created in Trino
WITH (location = 's3://warehouse/playground');
-- s3://warehouse/playground is default storage location on MinIO. The names can differ from the schema name in Trino above.

DROP TABLE IF EXISTS iceberg.playground.orders_demo;

-- A tiny copy of the Olist orders shape. Partitioned by purchase day.
CREATE TABLE iceberg.playground.orders_demo (
    order_id                 VARCHAR,
    customer_id              VARCHAR,
    order_status             VARCHAR,
    order_purchase_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(order_purchase_timestamp)']
);

-- Commit 1 -> snapshot 1
INSERT INTO iceberg.playground.orders_demo VALUES
    ('o1', 'c1', 'created',   TIMESTAMP '2017-10-02 10:56:33'),
    ('o2', 'c2', 'created',   TIMESTAMP '2017-10-02 11:20:00'),
    ('o3', 'c3', 'created',   TIMESTAMP '2017-10-03 09:00:00');

-- Commit 2 -> snapshot 2 (a row-level change, like an order getting delivered)
UPDATE iceberg.playground.orders_demo
SET order_status = 'delivered'
WHERE order_id = 'o1';

-- Current state
SELECT * FROM iceberg.playground.orders_demo ORDER BY order_id;

-- Iceberg metadata tables: every commit is a snapshot
-- Expect 3: CREATE (empty), INSERT (append), UPDATE (overwrite)
SELECT snapshot_id, parent_id, operation, committed_at,
       element_at(summary, 'added-records') AS added_records,
       element_at(summary, 'added-delete-files') AS added_delete_files
FROM iceberg.playground."orders_demo$snapshots"
ORDER BY committed_at;

-- Which files exist. content: 0 = data file, 1 = position-delete file.
-- The UPDATE did NOT rewrite the Parquet file: it wrote a small delete file
-- ("row 0 of file X is gone") plus a new data file with the new row.
-- That is merge-on-read. We compare it with copy-on-write in M3.
SELECT content, partition, record_count, file_size_in_bytes, file_path
FROM iceberg.playground."orders_demo$files";

-- Time travel: Trino needs a literal snapshot id, so this one is manual.
-- Copy the snapshot_id of the row with added_records = 3 from the query above,
-- then run (in the interactive `trino` shell):
--
--   SELECT * FROM iceberg.playground.orders_demo FOR VERSION AS OF <snapshot_id>;
--
-- You should see o1 back as 'created': the table as it was before the UPDATE.

CREATE SCHEMA IF NOT EXISTS iceberg.demo
WITH (location = 's3://warehouse/custom');

DROP TABLE IF EXISTS iceberg.demo.customers;


CREATE TABLE iceberg.demo.customers (
    customer_id              VARCHAR,
    customer_name            VARCHAR,
    dob                      DATE,
    email                    VARCHAR,
    country                  VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['country']
);