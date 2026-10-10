from pyspark import pipelines as dp
from pyspark.sql.types import *
from pyspark.sql.protobuf.functions import to_protobuf, from_protobuf
from pyspark.sql import functions as F

descriptor_file = "/Volumes/dlt_lakehouse/default/raw/events.desc"


@dp.table(
    name="silver.events_cdc_silver",
    comment="Decoded data from `bronze.events_cdc_bronze`",
    table_properties={"layer": "silver", "throughput": "high", "filetype": "table"},
)
@dp.expect_all_or_drop(
    {
        "machine_id_not_null": "machine_id IS NOT NULL",
        "event_ts_is_not_invalid":"event_ts != 'not-a-timestamp'"
        })
def events_cdc_silver_flow():
    return (
        spark.read.table("dlt_lakehouse.bronze.events_cdc_bronze")
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
        .select("proto_data.*", "*")
        .drop("event_message", "length", "content", "trimmed_content", "proto_data")
    )
