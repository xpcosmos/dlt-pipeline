from pyspark import pipelines as dp
from pyspark.sql.types import *

# Quality Table

## Variables and schema definition:
quality_cdc_bronze_file = "/Volumes/dlt_lakehouse/default/raw/quality/*.json"
quality_schema = StructType(
    [
        StructField("defects", StringType()),
        StructField("inspection_id", StringType()),
        StructField("inspector", StringType()),
        StructField("lot_number", StringType()),
        StructField("machine_id", StringType()),
        StructField("measurements", StringType()),
        StructField("rework", StringType()),
        StructField("sample", StringType()),
        StructField("station", StringType()),
        StructField("timestamp", StringType()),
        StructField("verdict", StringType()),
        StructField("work_order_id", StringType()),
        StructField("_rescued_data", StringType()),
    ]
)


# Table definition
@dp.table(
    name="quality_cdc_bronze",
    comment="Data with quality events",
    table_properties={"layer": "bronze", "throughput": "high", "filetype": "json"},
    schema=quality_schema,
)
def quality_cdc_bronze_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("multiLine", True)
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(quality_cdc_bronze_file)
    )

