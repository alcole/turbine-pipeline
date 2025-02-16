import dlt
from pyspark.sql.functions import  avg, max, min


def publish_to_gold(spark):
    @dlt.table(
    comment="A table summarizing statistics by day for each turbine"
    )
    def turbine_sensor_gold():
        return (
            spark.read.table("turbine_sensor_silver")
            .groupBy("turbine_id", "date")
            .agg(avg("power_output").alias("average_power_output"), max("power_output").alias("max_power_output"), min("power_output").alias("min_power_output"))
        )