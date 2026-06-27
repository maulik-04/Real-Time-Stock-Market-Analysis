from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window,
    avg, last, to_timestamp, to_json, struct,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
)

# ---------------------------------------------------------------------------
# Schema — must exactly match the payload dict produced by producer.py
# ---------------------------------------------------------------------------
schema = StructType([
    StructField("symbol",    StringType()),
    StructField("exchange",  StringType()),
    StructField("ltp",       DoubleType()),
    StructField("open",      DoubleType()),
    StructField("high",      DoubleType()),
    StructField("low",       DoubleType()),
    StructField("close",     DoubleType()),
    StructField("volume",    DoubleType()),
    StructField("timestamp", StringType()),
])

# ---------------------------------------------------------------------------
# SparkSession
# shuffle.partitions=1 is intentional for local/single-node mode —
# raise this to match the number of Kafka partitions in production
# ---------------------------------------------------------------------------
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

# Reduce Spark's verbose logging so our print statements stay readable
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Source: read the raw Kafka stream from the "indian_stocks" topic
# Each Kafka message has many columns; the JSON payload is in "value"
# ---------------------------------------------------------------------------
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "indian_stocks")
    # "latest" means we only process messages that arrive AFTER this job starts.
    # Use "earliest" if you want to reprocess all historical messages in the topic.
    .option("startingOffsets", "latest")
    .load()
)

# ---------------------------------------------------------------------------
# Parse the JSON value into typed columns
# We only keep the three columns we actually need for windowed aggregation
# ---------------------------------------------------------------------------
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
    # Drop rows where timestamp parsing failed (malformed messages)
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
#                 (fix: previously used max("ltp") which gives the highest
#                  price in the window, not the most recent one)
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
        # last() with ignorenulls=True gives the final non-null ltp
        # seen within the window — a correct proxy for "current price"
        last("ltp", ignorenulls=True).alias("ltp"),
    )
)

# ---------------------------------------------------------------------------
# Flatten the nested window struct into plain string columns for JSON output
# ---------------------------------------------------------------------------
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
print("Writing to   : indian_avg")
print("Window       : 5 minutes, sliding every 30 seconds")
print("Watermark    : 3 minutes")

query.awaitTermination()
