# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import pyspark.pipelines as dp
from pyspark.sql.functions import col, expr, max, from_json
from pyspark.sql.types import StructField, StructType, TimestampType
from get_spark import spark



# "{\"scheduled_start\":\"2026-05-11T01:40:21.398Z\",\"scheduled_end\":\"2026-05-11T03:40:21.398Z\",\"actual_start\":\"2026-05-11T02:02:13.877Z\",\"actual_end\":\"2026-05-11T03:46:13.877Z\"}",
window_col_schema = StructType(
    [
        StructField("scheduled_start", TimestampType()),
        StructField("scheduled_end", TimestampType()),
        StructField("actual_start", TimestampType()),
        StructField("actual_end", TimestampType()),
    ]
)


@dp.table(name="silver.latest_machine_maintance")
def latest_maintanance_cdc_flow():
    return (
        spark.readStream.table("dlt_lakehouse.bronze.maintenance_cdc_bronze")
        .select("machine_id", "window")
        .withColumn("window", from_json(col("window"), schema=window_col_schema))
        .select("machine_id", col("window.actual_start").alias("actual_start"))
        .groupBy("machine_id")
        .agg(max(col("actual_start")).alias('latest_date'))
    )

@dp.table(name="silver.maintance_cdc_silver")
def latest_maintanance_cdc_flow():
    return (
        spark.readStream.table("dlt_lakehouse.bronze.maintenance_cdc_bronze")
        .withColumn("window", from_json(col("window"), schema=window_col_schema))
        .select("window.*", "*")
        
    )

    # dp.create_streaming_table("users_current")
