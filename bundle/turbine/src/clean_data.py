import dlt

valid_reading = {
        "null timestamp": "timestamp IS NOT NULL", 
        "valid turbine id": "turbine_id IS NOT NULL AND turbine_id > 0", 
        "non negative wind_speed": "wind_speed >= 0", 
        "wind speed not null": "wind_speed IS NOT NULL", 
        "wind direction not null": "wind_direction IS NOT NULL", 
        "wind direction valid value": "wind_direction BETWEEN 0 AND 359",
        "power output not null or below 0": "power_output >= 0 AND power_output IS NOT NULL"}

def create_stats_validation_view(spark):
    @dlt.view(
    comment="Create view with the standard deviations added"
    )
    def stats_validation_view():
    # Calculate statistical bounds from historical data
        bounds = spark.sql("""
            WITH max_date AS (
            SELECT max(date) as max_date
            FROM turbine_sensor_bronze
            )
            SELECT
            avg(power_output) - 2 * stddev(power_output) as lower_bound,
            avg(power_output) + 2 * stddev(power_output) as upper_bound
            FROM turbine_sensor_bronze, max_date
            WHERE
            date >= ( max_date.max_date - INTERVAL 30 DAYS )
        """)

        # Join with new data and apply bounds
        return spark.read.table("turbine_sensor_bronze").crossJoin(bounds)
    
def create_turbine_sensor_silver(spark):
    @dlt.table(
        comment="Turbine Sensor data cleaned and validated"
    )
    @dlt.expect_all_or_drop(valid_reading)
    @dlt.expect_or_drop(
        "within_statistical_range",
        "power_output BETWEEN lower_bound AND upper_bound"
    )
    def turbine_sensor_silver():
        return (dlt.read("stats_validation_view")
            .select("timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output", "date", "lower_bound", "upper_bound")
    )
