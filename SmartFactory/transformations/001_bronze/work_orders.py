from pyspark import pipelines as dp
from pyspark.sql.protobuf.functions import to_protobuf, from_protobuf
from pyspark.sql.types import *
from pyspark.sql import functions as F
# Work Orders

work_orders_cdc_bronze_file = "/Volumes/dlt_lakehouse/default/raw/work_orders/*.jsonl"
work_orders_cdc_schema = StructType(
    [
        StructField("batch_id", StringType(), True),
        StructField("line_id", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField(
            "operator",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("shift", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("priority", StringType(), True),
        StructField(
            "product",
            StructType(
                [
                    StructField("family", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("revision", StringType(), True),
                    StructField("sku", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "quantity",
            StructType(
                [
                    StructField("ordered", StringType(), True),
                    StructField("produced", StringType(), True),
                    StructField("rejected", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("sequence_no", StringType(), True),
        StructField("spec", StringType(), True),
        StructField("status", StringType(), True),
        StructField(
            "times",
            StructType(
                [
                    StructField("completed_at", StringType(), True),
                    StructField("created_at", StringType(), True),
                    StructField("due_at", StringType(), True),
                    StructField("started_at", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "traceability",
            StructType(
                [
                    StructField(
                        "components",
                        ArrayType(
                            StructType(
                                [
                                    StructField("mfr", StringType(), True),
                                    StructField("placed_qty", StringType(), True),
                                    StructField("ref", StringType(), True),
                                    StructField("value", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        True,
                    ),
                    StructField("lot_number", StringType(), True),
                    StructField("reflow_profile_id", StringType(), True),
                    StructField("supplier_batch", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("work_order_id", StringType(), True),
    ]
)


# Table definition
@dp.table(
    name="work_orders_cdc_bronze",
    comment="Data with Work Orders events",
    table_properties={"layer": "bronze", "throughput": "high", "filetype": "jsonl"},
)
def work_orders_cdc_bronze_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("primitivesAsString", True)
        .option("mode", "PERMISSIVE")
        .option('schema', work_orders_cdc_schema)
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("multiLine", True)
    ).load(work_orders_cdc_bronze_file)
