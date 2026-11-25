-- M3: inspect the incremental tables.  Run: make m3-inspect

-- 1. Row counts: the history keeps every version, the current tables one row per order
SELECT 'orders_changes' AS t, count(*) AS rows, count(DISTINCT order_id) AS orders FROM iceberg.bronze.orders_changes
UNION ALL SELECT 'orders_current_cow', count(*), count(DISTINCT order_id) FROM iceberg.bronze.orders_current_cow
UNION ALL SELECT 'orders_current_mor', count(*), count(DISTINCT order_id) FROM iceberg.bronze.orders_current_mor;

-- 2. One partition per loaded day in the history table
SELECT partition._batch_date AS batch_date, record_count, file_count
FROM iceberg.bronze."orders_changes$partitions" ORDER BY 1;

-- 3. The life of the order that changed most often: one row per day it changed
SELECT order_id, _batch_date, order_status, updated_at
FROM iceberg.bronze.orders_changes
WHERE order_id = (SELECT order_id FROM iceberg.bronze.orders_changes
                  GROUP BY 1 ORDER BY count(*) DESC, order_id LIMIT 1)
ORDER BY _batch_date;

-- 4. Same data, two write modes: should return 0 rows of difference
SELECT count(*) AS rows_that_differ FROM (
  SELECT * FROM iceberg.bronze.orders_current_cow
  EXCEPT SELECT * FROM iceberg.bronze.orders_current_mor);

-- 5. Live files. content 0 = data file, 1 = position-delete file
SELECT 'cow' AS mode, content, count(*) AS files, sum(record_count) AS records
FROM iceberg.bronze."orders_current_cow$files" GROUP BY 1, 2
UNION ALL
SELECT 'mor', content, count(*), sum(record_count)
FROM iceberg.bronze."orders_current_mor$files" GROUP BY 1, 2
ORDER BY 1, 2;

-- 6. What each MERGE commit wrote
SELECT 'cow' AS mode, committed_at, operation,
       element_at(summary, 'added-data-files')   AS added_data_files,
       element_at(summary, 'deleted-data-files') AS removed_data_files,
       element_at(summary, 'added-delete-files') AS added_delete_files,
       element_at(summary, 'added-records')      AS rows_written
FROM iceberg.bronze."orders_current_cow$snapshots"
UNION ALL
SELECT 'mor', committed_at, operation,
       element_at(summary, 'added-data-files'),
       element_at(summary, 'deleted-data-files'),
       element_at(summary, 'added-delete-files'),
       element_at(summary, 'added-records')
FROM iceberg.bronze."orders_current_mor$snapshots"
ORDER BY 1, 2;
