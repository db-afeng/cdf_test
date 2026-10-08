# Databricks notebook source
"""Idempotent initialization/reset: restore missing seed rows without losing CDF history."""
import re

for widget, default in (("catalog", "afeng"), ("schema", "cdf_delete_demo"), ("seed_rows", "1000")):
    dbutils.widgets.text(widget, default)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
seed_rows = int(dbutils.widgets.get("seed_rows"))
if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) for name in (catalog, schema)):
    raise ValueError("Demo catalog and schema must use letters, numbers and underscores.")
if not 50 <= seed_rows <= 100000:
    raise ValueError("seed_rows must be between 50 and 100000.")
namespace = f"`{catalog}`.`{schema}`"
source = f"{namespace}.source_orders"

# The bundle owns the schema; never replace/drop the source or its streaming checkpoints.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {source} (
    order_id BIGINT, customer STRING, region STRING,
    amount DECIMAL(12,2), order_date DATE
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 30 days'
)
""")
spark.sql(f"""
MERGE INTO {source} target
USING (
    SELECT id + 1 AS order_id,
           concat('Customer ', lpad(CAST(id % 100 + 1 AS STRING), 3, '0')) AS customer,
           element_at(array('NSW','VIC','QLD','WA'), CAST(id % 4 + 1 AS INT)) AS region,
           CAST(20 + (id * 37 % 9800) / 100.0 AS DECIMAL(12,2)) AS amount,
           date_add(DATE '2026-01-01', CAST(id % 30 AS INT)) AS order_date
    FROM range({seed_rows})
) seed ON target.order_id = seed.order_id
WHEN NOT MATCHED THEN INSERT *
""")
version = spark.sql(f"DESCRIBE HISTORY {source} LIMIT 1").first()["version"]
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {namespace}.demo_baseline (
    baseline_version BIGINT, seed_rows BIGINT, reset_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
MERGE INTO {namespace}.demo_baseline target
USING (SELECT CAST({version} AS BIGINT) baseline_version,
              CAST({seed_rows} AS BIGINT) seed_rows, current_timestamp() reset_at) seed
ON true
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print(f"Restored {seed_rows} source orders; round baseline version = {version}. Consumers will resume incrementally.")
