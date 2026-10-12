import pyspark.pipelines as dp
from pyspark.sql.functions import col, expr, max, from_json, broadcast
from pyspark.sql.types import StructField, StructType, TimestampType
from get_spark import spark


dp.create_streaming_table("silver.machine_maintenance_cdc")

dp.create_auto_cdc_flow(
  target="silver.machine_maintenance_cdc",
  source="silver.maintance_cdc_silver",
  keys = ["machine_id"],
  sequence_by = col("actual_start"),
  column_list=['machine_id', 'status','work_order_id','type', 'priority', 'technician','parts_replaced','actual_start'],
  stored_as_scd_type = 1
)




