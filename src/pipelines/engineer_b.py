"""Engineer B: two derived line items per order; deletes retain source lineage."""
from pyspark import pipelines as dp
from pyspark.sql.functions import array, col, explode, expr, lit

SOURCE = spark.conf.get("demo.source")


@dp.table(name="engineer_b_cdf", comment="Append-only evidence of source changes consumed by B.")
def cdf_ledger():
    return spark.readStream.option("readChangeFeed", "true").table(SOURCE)


@dp.temporary_view(name="engineer_b_changes")
def transformed_changes():
    # Expand DELETE events as well as inserts, so both derived keys are removed.
    return (
        spark.readStream.table("engineer_b_cdf")
        .filter(col("_change_type") != "update_preimage")
        .withColumn("line_number", explode(array(lit(1), lit(2))))
        .withColumn("line_amount", expr("CAST(amount / 2 AS DECIMAL(12,2))"))
        .select("order_id", "line_number", "customer", "region", "line_amount",
                "_change_type", "_commit_version", "_commit_timestamp")
    )


dp.create_streaming_table(name="order_lines", comment="Two derived rows per live source order.")
dp.create_auto_cdc_flow(
    name="apply_line_changes",
    target="order_lines",
    source="engineer_b_changes",
    keys=["order_id", "line_number"],
    sequence_by=col("_commit_version"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type=1,
)

