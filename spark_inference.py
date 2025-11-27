from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, max, to_timestamp, to_json, struct
from pyspark.sql.types import *



## Defining the structure of the incoming data
## This must match with the structure of the data we are getting from Kafka Producer
schema = StructType([
    StructField("symbol", StringType()),
    StructField("exchange", StringType()),
    StructField("ltp", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("timestamp", StringType())
])


## Initialising SparkSession
## It is the entry point for programming Spark with the Dataset and DataFrame API
spark = SparkSession.builder \
    .appName("RealTimeMovingAvgKafkaOutput") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4") \
    .getOrCreate()


## Reading data from Kafka as a streaming DataFrame.
# This sets up a continuous stream of data from the specified Kafka topic (indian_stocks).
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "indian_stocks") \
    .option("startingOffsets", "latest") \
    .load()


## When we get our Spark Structured Streaming DataFrame, it contains variuos default columns like Key, Value(where our json data is stored), Topic, Partition, Offest, TimeStamp (internal to Kafka) etc. 
## We are interested in the value column

#3 So we are using the from_json function to get our expected StructType from the string.
processed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.symbol"),
    col("data.ltp"),
    to_timestamp(col("data.timestamp")).alias("timestamp")
).filter("timestamp IS NOT NULL")

## After these steps, we get a clean DataFrame where we have 3 columns :
### 1. symbol: StringType
### 2. ltp: DoubleType
### 3. timestamp: TimestampType



# Updated: 5-min sliding window with 30s step
## Now applying windowed aggregation for calculating moving average.
## - withWatermark: Specifies a watermark on the 'timestamp' column to handle late data.
##  "3 minutes" means data arriving up to 3 minutes late will be processed; older data might be dropped.
## - groupBy(window(...), "symbol"): Defines a sliding window for aggregation.
## - window("timestamp", "5 minutes", "30 seconds"): Creates a 5-minute window that slides every 30 seconds.
## This means a new window calculation occurs every 30 seconds, covering the last 5 minutes of data.
## - "symbol": Aggregations are performed per stock symbol within each window.
## - agg(...): Calculates the aggregate functions within each group/window.
## - avg("ltp").alias("moving_avg"): Calculates the average of 'ltp' for the window.
##   - max("ltp").alias("ltp"): Gets the maximum 'ltp' within the window. This is used here as a proxy

agg = processed \
    .withWatermark("timestamp", "3 minutes") \
    .groupBy(
        window("timestamp", "5 minutes", "30 seconds"),
        "symbol"
    ).agg(
        avg("ltp").alias("moving_avg"),
        max("ltp").alias("ltp")  # still gives latest-ish ltp
    )



## Preparing the output DataFrame for writing to Kafka.
## Selects and renames columns to be sent as the final output message.
## The window.start and window.end are cast to string for JSON serialization.
output = agg.select(
    col("symbol"),
    col("moving_avg").alias("avg"),
    col("ltp"),
    col("window.start").cast("string").alias("start"),
    col("window.end").cast("string").alias("end")
)

## Writing the aggregated streaming data back to a new Kafka topic.
## 1. to_json(struct(...)): Converts the structured DataFrame row into a single JSON string.
##    'struct' is used to group the desired columns into a single complex type before converting to JSON.
##    This JSON string will be the 'value' of the Kafka message.
## 2. format("kafka"): Specifies Kafka as the sink.
## 3. option("kafka.bootstrap.servers", "localhost:9092"): Kafka broker address.
## 4. option("topic", "indian_avg"): The target Kafka topic for the output.
## 5. option("checkpointLocation", "/tmp/spark_avg_checkpoint"): Required for fault tolerance in Spark Structured Streaming.
##    Spark uses this directory to store metadata and progress information, allowing recovery from failures.
## 6. outputMode("update"): Specifies how new results are written to the sink.
##    "update" mode writes new or updated rows in the result table (suitable for aggregations where old windows
##    might not produce new results but new windows do).
## 7. start(): Starts the streaming query.
query = output.select(
    to_json(struct(
        col("symbol"),
        col("avg"),
        col("ltp"),
        col("start"),
        col("end")
    )).alias("value")
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "indian_avg") \
    .option("checkpointLocation", "/tmp/spark_avg_checkpoint") \
    .outputMode("update") \
    .start()


# Await termination of the streaming query.
# This keeps the application running until the query is terminated manually or due to an error.
print("Spark streaming (5-min window) started and writing to topic: indian_avg")
query.awaitTermination()

