from pyspark import pipelines as dp
from pyspark.sql.types import *


machines_cdc_bronze_file = f"/Volumes/dlt_lakehouse/default/raw/machines"

machine_schema = StructType(
    [
        StructField("machine_id", StringType()),
        StructField("machine_serial", StringType()),
        StructField("vendor", StringType()),
        StructField("model", StringType()),
        StructField("station_type", StringType()),
        StructField("line_id", StringType()),
        StructField("cell", StringType()),
        StructField("location", StringType()),
        StructField("firmware_version", StringType()),
        StructField("controller_ip", StringType()),
        StructField("installation_date", StringType()),
        StructField("last_calibration_date", StringType()),
        StructField("calibration_due_date", StringType()),
        StructField("rated", StringType()),
        StructField("status", StringType()),
        StructField("owner", StringType()),
        StructField("telemetry", StringType()),
        StructField("_rescued_data", StringType()),
    ]
)
@dp.table(
    name="machines_cdc_bronze",
    comment="Data with machhine definitions",
    table_properties={"layer": "bronze", "throughput": "low", "filetype": "csv"},
    schema=machine_schema,
)
def machines_cdc_bronze_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", ";")
        .load(machines_cdc_bronze_file)
    )
