from pyspark import pipelines as dp
from pyspark.sql.types import *

# Mainteinance Table

## Variables and schema definition:
maintenance_cdc_bronze_file = f"/Volumes/dlt_lakehouse/default/raw/maintenance/*.orc"
maintenance_schema = StructType(
    [
        StructField("maintenance_id", StringType()),
        StructField("machine_id", StringType()),
        StructField("work_order_id", StringType()),
        StructField("type", StringType()),
        StructField("priority", StringType()),
        StructField("technician", StringType()),
        StructField("window", StringType()),
        StructField("duration", StringType()),
        StructField("root_cause", StringType()),
        StructField("parts_replaced", StringType()),
        StructField("cost", StringType()),
        StructField("status", StringType()),
        StructField("follow_up", StringType()),
    ]
)


# Table definition
@dp.table(
    name="maintenance_cdc_bronze",
    comment="Data with maintainance events",
    table_properties={"layer": "bronze", "throughput": "high", "filetype": "orc"},
    schema=maintenance_schema,
)
def maintenance_cdc_bronze_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "orc")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .load(maintenance_cdc_bronze_file)
    )

