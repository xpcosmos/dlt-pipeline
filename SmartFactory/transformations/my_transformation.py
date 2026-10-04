from pyspark import pipelines as dp
from pyspark.sql.protobuf.functions import to_protobuf, from_protobuf
from pyspark.sql.types import *
from pyspark.sql import functions as F

catalog = "dlt_lakehouse"


data_file = f"/Volumes/{catalog}/default/raw/events/*.pb"
descriptor_file = f"/Volumes/{catalog}/default/raw/events.desc"

dp.create_streaming_table(
    "events_cdc_bronze",
    comment="New customer data incrementally ingested from cloud object storage landing zone",
    table_properties={"layer": "bronze", "throughput": "high", "filetype": "protobuf"},
    expect_all_or_drop={"machine_id_not_null": "proto_data.machine_id IS NOT NULL"}
)

@dp.append_flow(
    target='events_cdc_bronze',
    name="events_cdc_bronze_flow",
    comment="New customer data incrementally ingested from cloud object storage landing zone",
)
def events_cdc_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .load(data_file)
        .withColumn(
            "trimmed_content", F.expr("substring(content, 3, length(content) - 2)")
        )
        .withColumn(
            "proto_data",
            from_protobuf(
                data="trimmed_content",
                messageName="factory.smart_manufacturing.Events",
                descFilePath=descriptor_file,
                options={"mode": "PERMISSIVE"},
            ),
        )
    )
