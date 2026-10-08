"""Engineer A: enriched row-level output, with an independent CDF checkpoint."""
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

SOURCE = spark.conf.get("demo.source")


@dp.table(name="engineer_a_cdf", comment="Append-only evidence of source changes consumed by A.")
def cdf_ledger():
    # Initial run reads a snapshot as inserts; subsequent runs resume from SDP's checkpoint.
    return spark.readStream.option("readChangeFeed", "true").table(SOURCE)


@dp.temporary_view(name="engineer_a_changes")
def transformed_changes():
    return (
        spark.readStream.table("engineer_a_cdf")
        .filter(col("_change_type") != "update_preimage")
        .withColumn("amount_with_tax", expr("CAST(amount * 1.10 AS DECIMAL(12,2))"))
    )


dp.create_streaming_table(name="orders_enriched", comment="One row per live source order.")
dp.create_auto_cdc_flow(
    name="apply_order_changes",
    target="orders_enriched",
    source="engineer_a_changes",
    keys=["order_id"],
    sequence_by=col("_commit_version"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type=1,
)

