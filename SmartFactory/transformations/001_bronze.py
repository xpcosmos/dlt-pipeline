from pyspark import pipelines as dp
from pyspark.sql.protobuf.functions import to_protobuf, from_protobuf
from pyspark.sql.types import *
from pyspark.sql import functions as F

catalog = "dlt_lakehouse"


events_cdc_bronze_file = f"/Volumes/{catalog}/default/raw/events/*.pb"
descriptor_file = f"/Volumes/{catalog}/default/raw/events.desc"


@dp.table(
    name="events_cdc_bronze",
    comment="New customer data incrementally ingested from cloud object storage landing zone",
    table_properties={"layer": "bronze", "throughput": "high", "filetype": "protobuf"},
)
def events_cdc_bronze_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .load(events_cdc_bronze_file)
    )


machines_cdc_bronze_file = f"/Volumes/dlt_lakehouse/default/raw/machines"
machine_schema = StructType(
    [
        StructField("machine_id", StringType(), True),
        StructField("machine_serial", StringType(), True),
        StructField("vendor", StringType(), True),
        StructField("model", StringType(), True),
        StructField("station_type", StringType(), True),
        StructField("line_id", StringType(), True),
        StructField("cell", StringType(), True),
        StructField("location", StringType(), True),
        StructField("firmware_version", StringType(), True),
        StructField("controller_ip", StringType(), True),
        StructField("installation_date", StringType(), True),
        StructField("last_calibration_date", StringType(), True),
        StructField("calibration_due_date", StringType(), True),
        StructField("rated", StringType(), True),
        StructField("status", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("telemetry", StringType(), True),
        StructField('_rescued_data', StringType(), True)
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
        .option('sep',";")
        .load(machines_cdc_bronze_file)
    )
