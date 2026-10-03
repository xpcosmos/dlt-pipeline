from pyspark import pipelines as dp

@dp.temporary_view(name='view_raw_event')