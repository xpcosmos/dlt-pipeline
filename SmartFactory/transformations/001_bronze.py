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


# Telemetry Table

## Variables and schema definition:
telemetry_cdc_bronze_file = "/Volumes/dlt_lakehouse/default/raw/telemetry/*.avro"
with open("/Volumes/dlt_lakehouse/default/raw/telemetry/telemetry.avsc", mode='r') as f:
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
