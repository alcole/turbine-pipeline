# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from src.load_data import load_turbine_data
from src.clean_data import create_stats_validation_view, create_turbine_sensor_silver
from src.publish_data import publish_to_gold

# Assign pipeline parameters to variables
my_catalog = spark.conf.get("my_catalog")
my_schema = spark.conf.get("my_schema")
my_volume = spark.conf.get("my_volume")

# Define the path to source data
volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"

# Load data to Bronze table
load_turbine_data(spark, volume_path)

@dlt.table(
    comment="Check rows loaded into Bronze from test data set"
    )
@dlt.expect_all_or_fail({
    "keep_all_rows": "num_rows == 13",
    #"null_ids_removed": "null_ids == 0"
})
def TEST_turbine_bronze():
    return (
        dlt.read("turbine_sensor_bronze")
            .select(
                F.count("*").alias("num_rows")
        )
    )


# add 2 standard deviation bounds to the data
create_stats_validation_view(spark)

# filter out outliers and load to silver table
create_turbine_sensor_silver(spark)

@dlt.table(
    comment="Check rows cleaning rules applied to silver table"
    )
@dlt.expect_all({
    "valid_rows": "num_rows == 3",
    #"null_ids_removed": "null_ids == 0"
})
def TEST_turbine_silver():
    return (
        dlt.read("turbine_sensor_silver")
            .select(
                F.count("*").alias("num_rows"), 
        )
    )

# Define a materialized view that has an aggregated view of the data
publish_to_gold(spark)

@dlt.table(
    comment="Check transforms applied to gold table"
    )
@dlt.expect_all({
    "valid_rows": "num_rows == 3",
    #"null_ids_removed": "null_ids == 0"
})
def TEST_turbine_gold():
    return (
        dlt.read("turbine_sensor_gold")
            .select(
                F.count("*").alias("num_rows"), 
        )
    )

