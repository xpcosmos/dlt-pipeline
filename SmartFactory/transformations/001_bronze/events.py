from pyspark import pipelines as dp


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