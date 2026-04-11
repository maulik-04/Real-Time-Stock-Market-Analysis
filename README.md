# Real-Time-Stock-Market-Data-Processing


This project is a real-time stock market data pipeline built to simulate how modern financial systems ingest, process, and visualise live market data.

The system continuously fetches live stock prices of selected Indian companies, such as:

RELIANCE
TCS
HDFCBANK
INFY

It then processes this incoming data stream using Apache Kafka and Apache Spark Structured Streaming, calculates rolling metrics such as 5-minute moving averages, and displays the results on a live interactive dashboard built with Flask + Chart.js.

Stock prices change every second. Traditional static dashboards or CSV-based analysis cannot handle continuously changing data efficiently.

This project was built to solve that problem by creating a streaming analytics pipeline that can:

-> Fetch live stock prices continuously
-> Process data in near real-time
-> Calculate rolling averages/trends
-> Visualise market movement live
-> Demonstrate scalable big-data architecture

1️) *Fetching Real-Time Stock Data*
What?

The first step was to get live Indian stock prices.

Why?

For a realistic streaming project, dummy/random data is not enough. We needed real market data.

How?

Used Upstox Market Quote API.

Python producer sends REST API requests every few seconds and receives JSON responses like:

{
  "symbol": "RELIANCE",
  "ltp": 1437.8,
  "high": 1440,
  "low": 1430,
  "volume": 120000
}

2️) *Sending Data to Kafka*
What?

The fetched stock prices are sent to Apache Kafka.

Why Kafka?

Kafka is ideal for streaming pipelines because it is:

-> Distributed
-> Fault-tolerant
-> Highly scalable
-> High throughput
-> Real-time message streaming platform

How?

Python producer acts as Kafka Producer:

producer.send("indian_stocks", payload)

This continuously pushes stock price messages into Kafka topic:

indian_stocks

3️) *Real-Time Processing with Apache Spark*
What?

Spark consumes live messages from Kafka and performs analytics.

Why Spark?

Apache Spark is used because:

-> Handles big data efficiently
-> Distributed parallel processing
-> Real-time stream processing
-> Supports SQL/DataFrames
-> Scalable for future expansion

How?

Spark Structured Streaming reads Kafka topic:

spark.readStream.format("kafka")

Then parses JSON data into columns:

symbol
ltp
timestamp

4️) *Sliding Window Analytics*
What?

Calculated 5-minute moving average stock prices.

Why?

Raw stock prices fluctuate heavily every second.

Moving average helps smooth data and identify trend direction.

How?

Used Spark windowing:

window("timestamp", "5 minutes", "30 seconds")

Meaning:

Use last 5 minutes data
Refresh every 30 seconds

5️) *Watermarking*
What?

Used watermarking to handle delayed data.

Why?

Sometimes network/API messages arrive late.

Without watermarking, Spark may keep windows open forever.

How?

.withWatermark("timestamp", "3 minutes")

Meaning:

Spark waits max 3 minutes for delayed records.

6️) *Writing Processed Output Back to Kafka*

After analytics, Spark writes results into another Kafka topic:

indian_avg

This topic contains:

symbol
current price
moving average
window start time
window end time

7️) *Flask Backend*
What?

Flask serves dashboard data.

Why Flask?

Lightweight Python backend ideal for APIs and dashboards.

How?

Created routes:

/

Serves dashboard page

/data

Returns latest processed JSON data.



