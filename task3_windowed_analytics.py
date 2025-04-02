from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Schema for input JSON
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# Spark session
spark = SparkSession.builder \
    .appName("Task3_Windowed_Fare_Analytics") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read from socket stream
lines = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999).load()

# Parse JSON and convert timestamp
parsed_df = lines.select(from_json(col("value"), schema).alias("data")).select("data.*")
parsed_df = parsed_df.withColumn("event_time", col("timestamp").cast(TimestampType()))
parsed_df = parsed_df.withWatermark("event_time", "1 minute")

# Windowed sum of fare amount (5 min window, 1 min slide)
windowed_df = parsed_df.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")
).agg(
    sum("fare_amount").alias("total_fare")
)

# Flatten window struct
flattened_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# Write output to CSV
flattened_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "data/task3_windowed_fare") \
    .option("checkpointLocation", "checkpoints/task3_windowed_fare") \
    .start() \
    .awaitTermination()
