from pyspark import pipelines as dp
from pyspark.sql.types import *

# Telemetry Table

## Variables and schema definition:
telemetry_cdc_bronze_file = "/Volumes/dlt_lakehouse/default/raw/telemetry/*.avro"
with open("/Volumes/dlt_lakehouse/default/raw/telemetry/telemetry.avsc", mode="r") as f:
    telemetry_schema = f.read()

# Table definition
@dp.table(
    name="telemetry_cdc_bronze",
    comment="Data with Telemetry events",
    table_properties={"layer": "bronze", "throughput": "high", "filetype": "avro"},
)
def telemetry_cdc_bronze_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "avro")
        .option("avroSchema", telemetry_schema)
        .load(telemetry_cdc_bronze_file)
    )
