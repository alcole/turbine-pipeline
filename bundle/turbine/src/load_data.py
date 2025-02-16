import dlt
from pyspark.sql.functions import col, current_timestamp, to_date
from pyspark.sql import DataFrame

def load_turbine_data(spark, volume_path):
  @dlt.table(
  comment="Raw Turbine Sensor streaming table"
  )
  def turbine_sensor_bronze():
    return (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", True)
      .schema("timestamp TIMESTAMP, turbine_id INT, wind_speed DOUBLE, wind_direction INT, power_output DOUBLE")
      .load(volume_path)
      .withColumn("file_path", col("_metadata.file_path"))
      .withColumn("processed_timestamp", current_timestamp())
      .withColumn("date", to_date(col("timestamp")))
    )
