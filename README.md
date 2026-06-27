# Real-Time Stock Market Data Processing

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.8+-231F20?logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4-E25A1C?logo=apachespark&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.0-black?logo=flask&logoColor=white)

A real-time streaming data pipeline that continuously ingests live NSE stock prices, processes them with Apache Spark Structured Streaming, computes rolling 5-minute moving averages, and visualises the results on an auto-refreshing dashboard — all running locally without any paid cloud infrastructure.

I built this project because I wanted to understand how large-scale financial systems actually work under the hood. Static CSV-based analysis was never going to teach me that. The only way to really learn streaming was to build something that breaks in interesting ways when the data keeps moving.

**Live tracked symbols:** RELIANCE · TCS · HDFCBANK · INFY

---

## What This Project Does

Stock prices change every second. A traditional pipeline that reads a file, processes it, and writes results somewhere is completely unsuitable for this — by the time you've processed the batch, the market has moved on.

This project solves that by building a proper streaming pipeline where:

- A **Python producer** continuously fetches live prices from the Upstox API and publishes them to Kafka every 15 seconds per symbol
- **Apache Spark** consumes that stream and computes a 5-minute sliding moving average in near-real-time, writing results back to a second Kafka topic
- A **Flask dashboard** reads the processed results and serves them to a live Chart.js frontend that updates every 3 seconds

The end result is a dashboard where you can watch four NSE stocks tick in real time, with their moving averages smoothing out the noise.

---

## Architecture

```
Upstox Market Quote API
        │
        │  REST request every 15 s per symbol
        ▼
  ┌─────────────┐
  │ producer.py │  — Kafka Producer, round-robins through 4 symbols
  └─────────────┘
        │
        │  JSON messages → topic: indian_stocks
        ▼
  ┌──────────────────────┐
  │  Apache Kafka Broker │  localhost:9092
  └──────────────────────┘
        │
        │  Spark reads stream
        ▼
  ┌──────────────────────────┐
  │   spark_inference.py     │
  │                          │
  │  • Parse JSON schema     │
  │  • 3-min watermark       │
  │  • 5-min sliding window  │
  │    (step: 30 seconds)    │
  │  • avg(ltp), last(ltp)   │
  └──────────────────────────┘
        │
        │  Aggregated results → topic: indian_avg
        ▼
  ┌──────────────────────┐
  │  Apache Kafka Broker │
  └──────────────────────┘
        │
        │  Flask consumes topic
        ▼
  ┌──────────────────────────┐
  │     dashboard.py         │
  │                          │
  │  • Kafka consumer thread │
  │  • Per-symbol ring buf   │
  │  • /data REST endpoint   │
  └──────────────────────────┘
        │
        │  JSON poll every 3 s
        ▼
  ┌──────────────────────────┐
  │     dashboard.html       │
  │  Chart.js time-series    │
  │  Per-symbol stat cards   │
  └──────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Why I chose it |
|---|---|---|
| Data source | Upstox v2 Market Quote API | Free tier, real NSE data, simple REST interface |
| Message broker | Apache Kafka | Industry-standard for streaming pipelines; decouples every stage so each can fail and recover independently |
| Stream processing | Apache Spark Structured Streaming | Handles windowed aggregations natively; `window()` + watermarking is exactly the right primitive for moving averages |
| Backend | Flask | Lightweight; all I needed was one consumer thread and two routes |
| Frontend | Chart.js + Luxon | Chart.js handles time-series axes cleanly; Luxon manages IST timezone conversion without pain |
| Secrets | python-dotenv | Keeps API credentials out of source code |

---

## How Each Component Works

### 1. Producer (`producer.py`)

The producer round-robins through the four symbols with a 15-second sleep between each, so every symbol gets a fresh price approximately every 60 seconds — well within the 5-minute window Spark is computing over.

Each tick is flattened into a JSON payload with OHLC fields and a UTC ISO-8601 timestamp:

```json
{
  "symbol": "RELIANCE",
  "exchange": "NSE",
  "ltp": 1437.80,
  "open": 1430.00,
  "high": 1441.50,
  "low": 1428.00,
  "close": 1435.00,
  "volume": 120000,
  "timestamp": "2025-07-08T07:12:33.412+00:00"
}
```

A `producer.flush()` call after every send ensures messages are actually delivered to the broker — without it, buffered messages can be silently lost if the process is interrupted.

The producer exits immediately with a clear message if Upstox returns HTTP 401, because that means the access token has expired and there is no point retrying.

### 2. Spark Streaming (`spark_inference.py`)

This is the core of the pipeline. Spark reads the raw `indian_stocks` topic as an unbounded DataFrame, parses the JSON against an explicit schema, and applies windowed aggregation:

```python
.withWatermark("timestamp", "3 minutes")
.groupBy(window("timestamp", "5 minutes", "30 seconds"), "symbol")
.agg(
    avg("ltp").alias("moving_avg"),
    last("ltp", ignorenulls=True).alias("ltp"),
)
```

A few things worth explaining:

**Why `withWatermark`?** Network delays and API latency mean messages can arrive out of order. Without a watermark, Spark would hold every window open indefinitely waiting for stragglers. The 3-minute watermark tells Spark: "wait up to 3 minutes for late data, then finalise the window."

**Why `last("ltp")` and not `max("ltp")`?** An earlier version of this project used `max("ltp")` as a proxy for the current price. That's wrong — `max` gives the *highest* price seen in the 5-minute window, which could be from 4 minutes ago. `last("ltp")` gives the most recently seen price in the window, which is what the dashboard should display.

**Why `outputMode("update")`?** `complete` mode rewrites every single window result to the sink on every 30-second trigger — expensive. `update` mode only writes rows that changed this trigger. Much cheaper for a high-cardinality stream.

Results are serialised back to JSON and written to the `indian_avg` topic via a Kafka sink with checkpointing, which means Spark can resume exactly where it left off after a restart.

### 3. Dashboard (`dashboard.py` + `dashboard.html`)

The Flask backend runs a dedicated Kafka consumer thread that reads `indian_avg` and appends each message to a per-symbol `deque(maxlen=500)`. That's roughly 4 hours of data at the 30-second trigger interval.

I deliberately chose per-symbol buffers over a single shared deque because it makes the `/data` endpoint trivial — it just serialises each deque to a list. A shared deque would require re-grouping on every request.

All access to the shared buffers is protected by a `threading.Lock()`. In CPython, `deque` operations are GIL-protected so they won't *corrupt*, but Flask's request threads and the Kafka consumer thread can still produce inconsistent snapshots without an explicit lock around multi-step reads.

The consumer thread wraps the Kafka connection in a `while True` + `try/except` retry loop with a 5-second backoff. This means the dashboard recovers automatically if Kafka briefly drops — instead of silently serving stale data forever.

The frontend polls `/data` every 3 seconds, updates per-symbol stat cards (one per stock, showing LTP, 5-min avg, and current window time in IST), and redraws the Chart.js time-series chart.

---

## Setup & Running

### Prerequisites

- Python 3.10+
- Java 8 or 11 (required by PySpark — check with `java -version`)
- Apache Kafka running on `localhost:9092`
- An [Upstox developer account](https://developer.upstox.com/) with a registered app (free)

### Installation

```bash
# 1. Clone the repo
git clone https://github.com/maulik78/real-time-stock-market-analysis.git
cd real-time-stock-market-analysis

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Set up credentials — never commit .env to Git
cp .env.example .env
# Open .env and fill in your UPSTOX_API_KEY and UPSTOX_ACCESS_TOKEN
```

### Running

You need three terminals running simultaneously:

```bash
# Terminal 1 — start the Kafka producer
python producer.py

# Terminal 2 — start the Spark streaming job
# (Spark downloads the Kafka connector on first run — takes ~1 min)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 \
  spark_inference.py

# Terminal 3 — start the Flask dashboard
python dashboard.py
```

Then open **http://localhost:5000** in your browser.

Data starts appearing on the dashboard within ~30 seconds of all three processes being up.

> **Important:** The Upstox access token expires at midnight IST every day. Re-generate it from your [Upstox developer console](https://developer.upstox.com/) and update `UPSTOX_ACCESS_TOKEN` in your `.env` file. The producer will exit with a clear message when it detects a 401.

---

## Key Design Decisions

These are the decisions I had to actively think through — not just "use Kafka because it's popular":

**Kafka between every stage, including Spark output → Flask**

The alternative would be Spark writing directly to Flask's memory, or Flask polling a database. Using Kafka throughout means every component is fully decoupled. The producer can restart without affecting Spark. Spark can restart without affecting the dashboard — it just replays from its checkpoint. The dashboard can restart without losing the Spark stream.

**Writing processed results back to Kafka instead of a file or database**

This keeps the dashboard completely stateless. It doesn't need to know anything about Spark's internal state — it just reads a Kafka topic. This also means I could replace Spark with Flink tomorrow and the dashboard wouldn't change at all.

**Per-symbol `deque` buffers with a `threading.Lock()`**

The Kafka consumer thread writes, Flask request threads read. Without a lock, a request could snapshot a partially-updated buffer. The lock adds negligible overhead at this scale and eliminates the race condition entirely.

**`debug=False` in Flask**

Flask's `debug=True` spawns a reloader subprocess that starts a second Kafka consumer. That doubles every message in the dashboard. Found this the hard way.

**`shuffle.partitions=1` in Spark**

Default is 200, which is sensible for a large cluster but absurd for a local 4-symbol stream. Setting it to 1 eliminates 199 empty shuffle tasks per trigger.

---

## Limitations & What I'd Add Next

This is a learning project, so I want to be upfront about where it falls short of production:

- **No persistence** — if the Flask process restarts, the chart starts empty. Adding Redis would fix this; each Spark output could be appended to a Redis sorted set scored by timestamp.
- **Single Kafka broker** — no replication, no fault tolerance at the broker level. Fine locally, not fine in production.
- **Token refresh is manual** — the Upstox token expires daily. Automating the OAuth refresh flow would make this truly unattended.
- **Only moving average** — the OHLC data is fetched and available in the payload but Spark only uses `ltp`. A candlestick chart using OHLC would be a natural next step.
- **No tests** — Spark transformations are testable with synthetic DataFrames. Adding a few assertions around the windowing logic would make refactoring much safer.

---

## Project Structure

```
├── producer.py          # Upstox API → Kafka producer
├── spark_inference.py   # Spark Structured Streaming: windowed moving avg
├── dashboard.py         # Flask backend: Kafka consumer + /data endpoint
├── dashboard.html       # Frontend: Chart.js live dashboard
├── requirements.txt     # Python dependencies
├── .env.example         # Credentials template — copy to .env, never commit
├── .gitignore
└── README.md
```

---

## What I Learned Building This

The hardest part wasn't the code — it was understanding *why* certain decisions exist. Some things that surprised me:

- Watermarking isn't just an optimisation, it's required for correctness. Without it, Spark keeps every window open forever and eventually runs out of memory.
- `max("ltp")` seems like a reasonable "current price" proxy until you think about it — the highest price in a 5-minute window tells you nothing about what the price is *right now*.
- Flask's `debug=True` causing a second Kafka consumer thread is the kind of bug that only shows up in integration, not in unit tests. The symptom was every message appearing twice on the dashboard.
- `producer.flush()` being optional in the Kafka client API is a trap. The default batch behaviour is great for throughput but means you can silently lose messages at process exit.

If you're building something similar and have questions, feel free to open an issue.
