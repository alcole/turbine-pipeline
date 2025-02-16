# Databricks notebook source
import dlt
from load_data import load_turbine_data
from clean_data import create_stats_validation_view, create_turbine_sensor_silver
from publish_data import publish_to_gold

# Assign pipeline parameters to variables
my_catalog = spark.conf.get("my_catalog")
my_schema = spark.conf.get("my_schema")
my_volume = spark.conf.get("my_volume")

# Define the path to source data
volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"

# Load data to Bronze table
load_turbine_data(spark, volume_path)

# add 2 standard deviation bounds to the data
create_stats_validation_view(spark)

# filter out outliers and load to silver table
create_turbine_sensor_silver(spark)

# Define a materialized view that has an aggregated view of the data
publish_to_gold(spark)

