from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg, window
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
    .appName("Task2_Windowed_Driver_Aggregation") \
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

# Aggregate by time window and driver_id
aggregated_df = parsed_df.groupBy(
    window(col("event_time"), "5 minutes", "1 minute"),
    col("driver_id")
).agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Flatten window struct
flattened_df = aggregated_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("driver_id"),
    col("total_fare"),
    col("avg_distance")
)

# Write output to CSV
flattened_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "data/task2_driver_windowed") \
    .option("checkpointLocation", "checkpoints/task2_windowed") \
    .start() \
    .awaitTermination()
