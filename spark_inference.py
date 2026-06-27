from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window,
    avg, last, to_timestamp, to_json, struct,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
)

schema = StructType([
    StructField("symbol", StringType()),
    StructField("exchange", StringType()),
    StructField("ltp", DoubleType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("timestamp", StringType()),
])


spark = (
    SparkSession.builder
    .appName("RealTimeMovingAvg")
    .config("spark.sql.shuffle.partitions", "1")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "indian_stocks")
    .option("startingOffsets", "latest")
    .load()
)


parsed = (
    raw_stream
    .select(
        from_json(col("value").cast("string"), schema).alias("d")
    )
    .select(
        col("d.symbol"),
        col("d.ltp"),
        # Convert the ISO-8601 string to a proper Spark TimestampType
        # so window() can do arithmetic on it
        to_timestamp(col("d.timestamp")).alias("timestamp"),
    )
    .filter(col("timestamp").isNotNull())
)

# ---------------------------------------------------------------------------
# Windowed aggregation
#
# window("timestamp", "5 minutes", "30 seconds")
#   → 5-minute sliding window that advances every 30 seconds
#   → each price tick falls into multiple overlapping windows
#
# withWatermark("timestamp", "3 minutes")
#   → Spark waits up to 3 minutes for late-arriving messages
#   → windows older than (max_seen_event_time − 3 min) are finalised & evicted
#
# Per-symbol aggregation inside each window:
#   avg("ltp")  → the 5-minute moving average we want to plot
#   last("ltp") → the most recent price seen IN THIS WINDOW
# ---------------------------------------------------------------------------
aggregated = (
    parsed
    .withWatermark("timestamp", "3 minutes")
    .groupBy(
        window("timestamp", "5 minutes", "30 seconds"),
        "symbol",
    )
    .agg(
        avg("ltp").alias("moving_avg"),
        last("ltp", ignorenulls=True).alias("ltp"),
    )
)

output = aggregated.select(
    col("symbol"),
    col("moving_avg").alias("avg"),
    col("ltp"),
    col("window.start").cast("string").alias("start"),
    col("window.end").cast("string").alias("end"),
)

# ---------------------------------------------------------------------------
# Sink: write aggregated results to the "indian_avg" Kafka topic
#
# outputMode("update")
#   → only rows whose window results CHANGED this trigger are written
#   → more efficient than "complete" (which rewrites every window every trigger)
#
# checkpointLocation
#   → Spark writes progress metadata here so it can resume exactly where it
#     left off after a crash or restart — required for all streaming sinks
# ---------------------------------------------------------------------------
query = (
    output
    .select(
        to_json(struct(
            col("symbol"),
            col("avg"),
            col("ltp"),
            col("start"),
            col("end"),
        )).alias("value")
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "indian_avg")
    .option("checkpointLocation", "/tmp/spark_avg_checkpoint")
    .outputMode("update")
    .start()
)

print("Spark Structured Streaming started.")
print("Reading from : indian_stocks")
print("Writing to : indian_avg")
print("Window : 5 minutes, sliding every 30 seconds")
print("Watermark : 3 minutes")

query.awaitTermination()
