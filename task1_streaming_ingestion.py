from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RideSharingStreaming") \
    .getOrCreate()

# Define schema for the incoming data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", FloatType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# Ingest data from socket
ride_data_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse the incoming JSON data into columns
ride_events = ride_data_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Output the parsed data to a CSV file in your workspace
query = ride_events.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/ride_data") \
    .option("checkpointLocation", "checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()