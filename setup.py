# Databricks notebook source
# MAGIC %md
# MAGIC # Turbine Pipeline
# MAGIC
# MAGIC This notebook has the code for the setup of the environment
# MAGIC
# MAGIC Adding required schema and volume locations
# MAGIC
# MAGIC If you update the locations here you'll also need to update the configuration of the 2 dlt pipelines to match
# MAGIC

# COMMAND ----------

my_catalog = "main"
my_schema = "turbine"
my_volume = "raw_sensor"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {my_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {my_catalog}.{my_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {my_catalog}.{my_schema}.{my_volume}")

# COMMAND ----------


my_schema = "turbine_test"
my_volume = "raw_sensor"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {my_catalog}.{my_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {my_catalog}.{my_schema}.{my_volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC Create a test data set for the test pipeline

# COMMAND ----------

from datetime import datetime
import pandas as pd
from pyspark.sql import Row

my_schema = "turbine_test"
my_volume = "raw_sensor"

destination_folder_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"

df = spark.createDataFrame([
    # normal row
    Row(timestamp=datetime.now(), turbine_id=1, wind_speed=12.1, wind_direction=30, power_output=3.1),
    # timestamp missing
    Row(timestamp='', turbine_id=1, wind_speed=12.1, wind_direction=30, power_output=3),
    # turbine id missing
    Row(timestamp=datetime.now(), turbine_id='', wind_speed=12.1, wind_direction=30, power_output=3),
    # turbine id error
    Row(timestamp=datetime.now(), turbine_id='a', wind_speed=12.1, wind_direction=30, power_output=2.5),
    # wind speed missing
    Row(timestamp=datetime.now(), turbine_id=1, wind_speed='', wind_direction=30, power_output=3),
    # wind speed negative
    Row(timestamp=datetime.now(), turbine_id=1, wind_speed=-2, wind_direction=30, power_output=3),
    # wind direction limit
    Row(timestamp=datetime.now(), turbine_id=2, wind_speed=0.8, wind_direction=0, power_output=2),
    # wind direction limit
    Row(timestamp=datetime.now(), turbine_id=3, wind_speed=12.1, wind_direction=359, power_output=2.2),
    # wind direction missing
    Row(timestamp=datetime.now(), turbine_id=3, wind_speed=12.1, wind_direction='', power_output=2.6),
    # wind direction invalid
    Row(timestamp=datetime.now(), turbine_id=3, wind_speed=12.1, wind_direction=360, power_output=2.6),
    # power output missing
    Row(timestamp=datetime.now(), turbine_id=3, wind_speed=12.1, wind_direction=36, power_output=''),
    # power output negative
    Row(timestamp=datetime.now(), turbine_id=3, wind_speed=12.1, wind_direction=35, power_output=-1.2),
    # power output outside of 2 sds
    Row(timestamp=datetime.now(), turbine_id=3, wind_speed=12.1, wind_direction=35, power_output=100),
    #Row(timestamp=datetime.now(), turbine_id=3, wind_speed=12.1, wind_direction=35, power_output=0),
], schema='timestamp string, turbine_id string, wind_speed string, wind_direction string, power_output string')

pd_df = df.toPandas()
pd_df.to_csv(f"{destination_folder_path}/{filename}", index=False)
#df.write.options(header='true', delimiter=',').mode('overwrite').csv(temp_file_path)

# Copy the file to the destination using dbutils.fs.cp
#dbutils.fs.cp(temp_file_path, destination_file_path, recurse=True)

# COMMAND ----------

from datetime import datetime
import pandas as pd
from pyspark.sql import Row


my_schema = "turbine_test"
my_volume = "raw_sensor"

# Temporary path to save the CSV file
temp_folder_path = "/tmp/"
filename = "test_data.csv"
temp_file_path = temp_folder_path + filename

destination_folder_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"

test_csv = """
timestamp,turbine_id,wind_speed,wind_direction,power_output
2021-01-01 00:00:00,1,12.1,30,3.1
2021-01-01 00:00:00,1,,30,3
"""

dbutils.fs.put(destination_folder_path + filename, test_csv, True)