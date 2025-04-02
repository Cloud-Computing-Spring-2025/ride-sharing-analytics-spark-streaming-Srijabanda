from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, window
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os



spark = SparkSession.builder \
    .appName("WindowedFareAnalytics") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")




schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())  # string initially




raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()




parsed_stream = raw_stream \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))




windowed_agg = parsed_stream \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(window(col("event_time"), "5 minutes", "1 minute")) \
    .agg(_sum("fare_amount").alias("total_fare"))



flattened = windowed_agg \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_fare")
    )




def write_to_csv(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        output_path = f"./output_task3/batch_{batch_id}"
        os.makedirs(output_path, exist_ok=True)
        batch_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
        print(f" Batch {batch_id} written to {output_path}")
    else:
        print(f"  Batch {batch_id} is empty – skipping.")




query = flattened.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "./chk_task3") \
    .start()

query.awaitTermination()