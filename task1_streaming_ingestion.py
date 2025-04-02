from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Schema for input JSON
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# Spark session
spark = SparkSession.builder \
    .appName("Task1_Streaming_Ingestion") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read from socket stream
lines = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999).load()

# Parse JSON to structured DataFrame
parsed_df = lines.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Write parsed data to CSV
parsed_df.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "data/task1_output") \
    .option("checkpointLocation", "checkpoints/task1") \
    .start() \
    .awaitTermination()
